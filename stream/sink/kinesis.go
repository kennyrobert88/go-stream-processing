package sink

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"

	"github.com/kennyrobert88/go-stream-processing/stream"
)

type KinesisSinkConfig struct {
	StreamName       string
	Region           string
	MaxRetries       int
	RetryBaseDelay   time.Duration
}

type KinesisSink struct {
	cfg    KinesisSinkConfig
	client *kinesis.Client
	cb     *stream.CircuitBreaker
	logger stream.Logger
}

func NewKinesisSink(cfg KinesisSinkConfig) *KinesisSink {
	return &KinesisSink{
		cfg:    cfg,
		cb:     stream.NewCircuitBreaker(stream.DefaultCircuitBreakerConfig("kinesis-sink")),
		logger: &stream.NopLogger{},
	}
}

func (s *KinesisSink) WithLogger(l stream.Logger) *KinesisSink {
	s.logger = l
	return s
}

func (s *KinesisSink) Open(ctx context.Context) error {
	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(s.cfg.Region))
	if err != nil {
		return fmt.Errorf("kinesis sink config: %w", err)
	}
	s.client = kinesis.NewFromConfig(awsCfg)
	s.logger.Info(ctx, "kinesis sink connected", "stream", s.cfg.StreamName)
	return nil
}

func (s *KinesisSink) Flush(_ context.Context) error { return nil }

func (s *KinesisSink) Close(_ context.Context) error {
	s.logger.Info(context.Background(), "kinesis sink closed")
	return nil
}

func (s *KinesisSink) Write(ctx context.Context, msg stream.Message[[]byte]) error {
	return s.cb.Execute(ctx, func(ctx context.Context) error {
		partitionKey := string(msg.Key)
		if partitionKey == "" {
			partitionKey = "default"
		}

		_, err := s.client.PutRecord(ctx, &kinesis.PutRecordInput{
			StreamName:   aws.String(s.cfg.StreamName),
			Data:         msg.Value,
			PartitionKey: aws.String(partitionKey),
		})
		if err != nil {
			var throttled *types.ProvisionedThroughputExceededException
			if errors.As(err, &throttled) {
				return stream.NewRetryableError(fmt.Errorf("kinesis sink throttled: %w", err))
			}
			return fmt.Errorf("kinesis sink write: %w", err)
		}
		return nil
	})
}

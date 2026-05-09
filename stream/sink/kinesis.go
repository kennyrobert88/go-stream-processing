package sink

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"

	"github.com/kennyrobert88/go-stream-processing/stream"
)

type KinesisSinkConfig struct {
	StreamName string
	Region     string
}

type KinesisSink struct {
	cfg    KinesisSinkConfig
	client *kinesis.Client
}

func NewKinesisSink(cfg KinesisSinkConfig) *KinesisSink {
	return &KinesisSink{cfg: cfg}
}

func (s *KinesisSink) Open(ctx context.Context) error {
	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(s.cfg.Region))
	if err != nil {
		return fmt.Errorf("kinesis sink config: %w", err)
	}
	s.client = kinesis.NewFromConfig(awsCfg)
	return nil
}

func (s *KinesisSink) Close(_ context.Context) error {
	return nil
}

func (s *KinesisSink) Write(ctx context.Context, msg stream.Message[[]byte]) error {
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
		return fmt.Errorf("kinesis sink write: %w", err)
	}
	return nil
}

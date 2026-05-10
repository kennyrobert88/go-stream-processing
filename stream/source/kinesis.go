package source

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"

	"github.com/kennyrobert88/go-stream-processing/stream"
)

type KinesisSourceConfig struct {
	StreamName        string
	Region            string
	ShardIteratorType types.ShardIteratorType
	MaxRecordsPerCall int32
	PollInterval      time.Duration

	ReconnectDelay    time.Duration
	MaxReconnects     int
}

type KinesisSourceOption func(*KinesisSourceConfig)

func WithStreamName(name string) KinesisSourceOption {
	return func(c *KinesisSourceConfig) { c.StreamName = name }
}
func WithRegion(region string) KinesisSourceOption {
	return func(c *KinesisSourceConfig) { c.Region = region }
}
func WithShardIteratorType(t types.ShardIteratorType) KinesisSourceOption {
	return func(c *KinesisSourceConfig) { c.ShardIteratorType = t }
}

func DefaultKinesisSourceConfig() KinesisSourceConfig {
	return KinesisSourceConfig{
		ShardIteratorType: types.ShardIteratorTypeLatest,
		MaxRecordsPerCall: 100,
		PollInterval:      time.Second,
		ReconnectDelay:    time.Second,
		MaxReconnects:     10,
	}
}

type kinesisShardReader struct {
	shardID      string
	iterator     *string
	records      []types.Record
	recordIndex  int
}

type KinesisSource struct {
	cfg    KinesisSourceConfig
	client *kinesis.Client
	shards []*kinesisShardReader
	mu     sync.Mutex
	msgCh  chan stream.Message[[]byte]
	done   chan struct{}
	wg     sync.WaitGroup
	cb     *stream.CircuitBreaker
	logger stream.Logger
}

func NewKinesisSource(cfg KinesisSourceConfig) *KinesisSource {
	return &KinesisSource{
		cfg:    cfg,
		msgCh:  make(chan stream.Message[[]byte], 1000),
		done:   make(chan struct{}),
		cb:     stream.NewCircuitBreaker(stream.DefaultCircuitBreakerConfig("kinesis-source")),
		logger: &stream.NopLogger{},
	}
}

func NewKinesisSourceWithOptions(opts ...KinesisSourceOption) *KinesisSource {
	cfg := DefaultKinesisSourceConfig()
	for _, o := range opts {
		o(&cfg)
	}
	return NewKinesisSource(cfg)
}

func (s *KinesisSource) WithLogger(l stream.Logger) *KinesisSource {
	s.logger = l
	return s
}

func (s *KinesisSource) WithCircuitBreaker(cb *stream.CircuitBreaker) *KinesisSource {
	s.cb = cb
	return s
}

func (s *KinesisSource) Open(ctx context.Context) error {
	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(s.cfg.Region))
	if err != nil {
		return stream.NewNonRetryableError(fmt.Errorf("kinesis source config: %w", err))
	}
	s.client = kinesis.NewFromConfig(awsCfg)

	shards, err := s.client.ListShards(ctx, &kinesis.ListShardsInput{
		StreamName: aws.String(s.cfg.StreamName),
	})
	if err != nil {
		return stream.NewRetryableError(fmt.Errorf("kinesis source list shards: %w", err))
	}
	if len(shards.Shards) == 0 {
		return stream.NewNonRetryableError(fmt.Errorf("kinesis source: no shards found"))
	}

	for _, shard := range shards.Shards {
		shardID := aws.ToString(shard.ShardId)
		iter, err := s.client.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{
			StreamName:        aws.String(s.cfg.StreamName),
			ShardId:           aws.String(shardID),
			ShardIteratorType: s.cfg.ShardIteratorType,
		})
		if err != nil {
			return stream.NewRetryableError(fmt.Errorf("kinesis source shard iterator %s: %w", shardID, err))
		}
		s.shards = append(s.shards, &kinesisShardReader{
			shardID:  shardID,
			iterator: iter.ShardIterator,
		})
	}

	s.logger.Info(ctx, "kinesis source connected",
		"stream", s.cfg.StreamName,
		"shards", len(s.shards),
	)

	for _, sr := range s.shards {
		s.wg.Add(1)
		go s.readShard(sr)
	}

	return nil
}

func (s *KinesisSource) readShard(sr *kinesisShardReader) {
	defer s.wg.Done()
	for {
		select {
		case <-s.done:
			return
		default:
		}

		if sr.iterator == nil {
			newIter, err := s.client.GetShardIterator(context.Background(), &kinesis.GetShardIteratorInput{
				StreamName:        aws.String(s.cfg.StreamName),
				ShardId:           aws.String(sr.shardID),
				ShardIteratorType: types.ShardIteratorTypeLatest,
			})
			if err != nil {
				time.Sleep(s.cfg.PollInterval)
				continue
			}
			sr.iterator = newIter.ShardIterator
		}

		output, err := s.client.GetRecords(context.Background(), &kinesis.GetRecordsInput{
			ShardIterator: sr.iterator,
			Limit:         aws.Int32(s.cfg.MaxRecordsPerCall),
		})
		if err != nil {
			sr.iterator = nil
			time.Sleep(s.cfg.PollInterval)
			continue
		}

		sr.iterator = output.NextShardIterator

		for _, rec := range output.Records {
			msg := stream.Message[[]byte]{
				Value: rec.Data,
				Key:   []byte(aws.ToString(rec.PartitionKey)),
			}
			if rec.ApproximateArrivalTimestamp != nil {
				msg.Timestamp = *rec.ApproximateArrivalTimestamp
			}
			select {
			case s.msgCh <- msg:
			case <-s.done:
				return
			}
		}

		if len(output.Records) == 0 {
			time.Sleep(s.cfg.PollInterval)
		}
	}
}

func (s *KinesisSource) Close(_ context.Context) error {
	close(s.done)
	s.wg.Wait()
	return nil
}

func (s *KinesisSource) Read(ctx context.Context) (stream.Message[[]byte], error) {
	select {
	case <-ctx.Done():
		return stream.Message[[]byte]{}, ctx.Err()
	case msg, ok := <-s.msgCh:
		if !ok {
			return stream.Message[[]byte]{}, stream.NewNonRetryableError(fmt.Errorf("kinesis source: closed"))
		}
		return msg, nil
	}
}

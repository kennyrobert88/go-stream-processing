package source

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"

	"github.com/kennyrobert88/go-stream-processing/stream"
)

type KinesisSourceConfig struct {
	StreamName string
	Region     string
}

type KinesisSource struct {
	cfg    KinesisSourceConfig
	client *kinesis.Client
	shard  string
}

func NewKinesisSource(cfg KinesisSourceConfig) *KinesisSource {
	return &KinesisSource{cfg: cfg}
}

func (s *KinesisSource) Open(ctx context.Context) error {
	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(s.cfg.Region))
	if err != nil {
		return fmt.Errorf("kinesis source config: %w", err)
	}
	s.client = kinesis.NewFromConfig(awsCfg)

	shards, err := s.client.ListShards(ctx, &kinesis.ListShardsInput{
		StreamName: aws.String(s.cfg.StreamName),
	})
	if err != nil {
		return fmt.Errorf("kinesis source list shards: %w", err)
	}
	if len(shards.Shards) == 0 {
		return fmt.Errorf("kinesis source: no shards found")
	}
	s.shard = aws.ToString(shards.Shards[0].ShardId)
	return nil
}

func (s *KinesisSource) Close(_ context.Context) error {
	return nil
}

func (s *KinesisSource) Read(ctx context.Context) (stream.Message[[]byte], error) {
	records, err := s.client.GetRecords(ctx, &kinesis.GetRecordsInput{
		ShardIterator: aws.String(s.shard),
	})
	if err != nil {
		return stream.Message[[]byte]{}, fmt.Errorf("kinesis source read: %w", err)
	}
	if len(records.Records) == 0 {
		return stream.Message[[]byte]{}, fmt.Errorf("kinesis source: no records")
	}
	rec := records.Records[0]
	key := aws.ToString(rec.PartitionKey)
	return stream.Message[[]byte]{
		Value: rec.Data,
		Key:   []byte(key),
	}, nil
}

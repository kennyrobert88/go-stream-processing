package source

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"

	"github.com/kennyrobert88/go-stream-processing/stream"
)

type KafkaSourceConfig struct {
	Brokers []string
	Topic   string
	GroupID string
}

type KafkaSource struct {
	cfg    KafkaSourceConfig
	reader *kafka.Reader
}

func NewKafkaSource(cfg KafkaSourceConfig) *KafkaSource {
	return &KafkaSource{cfg: cfg}
}

func (s *KafkaSource) Open(_ context.Context) error {
	s.reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers: s.cfg.Brokers,
		Topic:   s.cfg.Topic,
		GroupID: s.cfg.GroupID,
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})
	return nil
}

func (s *KafkaSource) Close(_ context.Context) error {
	if s.reader != nil {
		return s.reader.Close()
	}
	return nil
}

func (s *KafkaSource) Read(ctx context.Context) (stream.Message[[]byte], error) {
	kafkaMsg, err := s.reader.ReadMessage(ctx)
	if err != nil {
		return stream.Message[[]byte]{}, fmt.Errorf("kafka source read: %w", err)
	}
	return stream.Message[[]byte]{
		Key:       kafkaMsg.Key,
		Value:     kafkaMsg.Value,
		Headers:   headersToMap(kafkaMsg.Headers),
		Topic:     kafkaMsg.Topic,
		Partition: int32(kafkaMsg.Partition),
		Offset:    kafkaMsg.Offset,
	}, nil
}

func headersToMap(headers []kafka.Header) map[string][]byte {
	m := make(map[string][]byte, len(headers))
	for _, h := range headers {
		m[h.Key] = h.Value
	}
	return m
}

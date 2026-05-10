package sink

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"

	"github.com/kennyrobert88/go-stream-processing/stream"
)

type KafkaSinkConfig struct {
	Brokers []string
	Topic   string
}

type KafkaSink struct {
	cfg    KafkaSinkConfig
	writer *kafka.Writer
}

func NewKafkaSink(cfg KafkaSinkConfig) *KafkaSink {
	return &KafkaSink{cfg: cfg}
}

func (s *KafkaSink) Open(_ context.Context) error {
	s.writer = &kafka.Writer{
		Addr:     kafka.TCP(s.cfg.Brokers...),
		Topic:    s.cfg.Topic,
		Balancer: &kafka.LeastBytes{},
	}
	return nil
}

func (s *KafkaSink) Flush(_ context.Context) error { return nil }

func (s *KafkaSink) Close(_ context.Context) error {
	if s.writer != nil {
		return s.writer.Close()
	}
	return nil
}

func (s *KafkaSink) Write(ctx context.Context, msg stream.Message[[]byte]) error {
	kafkaMsg := kafka.Message{
		Key:   msg.Key,
		Value: msg.Value,
	}
	if len(msg.Headers) > 0 {
		kafkaMsg.Headers = make([]kafka.Header, 0, len(msg.Headers))
		for k, v := range msg.Headers {
			kafkaMsg.Headers = append(kafkaMsg.Headers, kafka.Header{Key: k, Value: v})
		}
	}
	if err := s.writer.WriteMessages(ctx, kafkaMsg); err != nil {
		return fmt.Errorf("kafka sink write: %w", err)
	}
	return nil
}

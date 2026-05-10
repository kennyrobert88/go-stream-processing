package source

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/kennyrobert88/go-stream-processing/stream"
)

type KafkaSourceConfig struct {
	Brokers      []string
	Topic        string
	GroupID      string
	ManualCommit bool
}

type KafkaSourceOption func(*KafkaSourceConfig)

func WithBrokers(brokers ...string) KafkaSourceOption {
	return func(c *KafkaSourceConfig) { c.Brokers = brokers }
}
func WithTopic(topic string) KafkaSourceOption {
	return func(c *KafkaSourceConfig) { c.Topic = topic }
}
func WithGroupID(groupID string) KafkaSourceOption {
	return func(c *KafkaSourceConfig) { c.GroupID = groupID }
}
func WithManualCommit(v bool) KafkaSourceOption {
	return func(c *KafkaSourceConfig) { c.ManualCommit = v }
}

func DefaultKafkaSourceConfig() KafkaSourceConfig {
	return KafkaSourceConfig{ManualCommit: false}
}

func NewKafkaSource(cfg KafkaSourceConfig) *KafkaSource {
	return &KafkaSource{cfg: cfg}
}

func NewKafkaSourceWithOptions(opts ...KafkaSourceOption) *KafkaSource {
	cfg := DefaultKafkaSourceConfig()
	for _, o := range opts {
		o(&cfg)
	}
	return &KafkaSource{cfg: cfg}
}

type kafkaAck struct {
	reader  *kafka.Reader
	msg     kafka.Message
}

func (a *kafkaAck) Ack(_ context.Context) error {
	return a.reader.CommitMessages(context.Background(), a.msg)
}

func (a *kafkaAck) Nack(_ context.Context) error {
	return nil
}

type KafkaSource struct {
	cfg    KafkaSourceConfig
	reader *kafka.Reader
}

func (s *KafkaSource) Open(_ context.Context) error {
	s.reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:        s.cfg.Brokers,
		Topic:          s.cfg.Topic,
		GroupID:        s.cfg.GroupID,
		MinBytes:       10e3,
		MaxBytes:       10e6,
		CommitInterval: 0,
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
		return stream.Message[[]byte]{}, stream.NewRetryableError(fmt.Errorf("kafka source read: %w", err))
	}
	msg := stream.Message[[]byte]{
		Key:       kafkaMsg.Key,
		Value:     kafkaMsg.Value,
		Headers:   headersToMap(kafkaMsg.Headers),
		Topic:     kafkaMsg.Topic,
		Partition: int32(kafkaMsg.Partition),
		Offset:    kafkaMsg.Offset,
		Timestamp: kafkaMsg.Time,
	}
	if s.cfg.ManualCommit {
		ack := &kafkaAck{reader: s.reader, msg: kafkaMsg}
		msg.SetAckNack(ack.Ack, ack.Nack)
	}
	return msg, nil
}

func headersToMap(headers []kafka.Header) map[string][]byte {
	m := make(map[string][]byte, len(headers))
	for _, h := range headers {
		m[h.Key] = h.Value
	}
	return m
}

var _ time.Time

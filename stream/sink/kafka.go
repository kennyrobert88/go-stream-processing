package sink

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"

	"github.com/kennyrobert88/go-stream-processing/stream"
)

type KafkaSinkConfig struct {
	Brokers       []string
	Topic         string
	BatchSize     int
	BatchTimeout  time.Duration
	MaxRetries    int
	WriteTimeout  time.Duration
	RequiredAcks kafka.RequiredAcks
	TLS          stream.TLSConfig
	SASLUsername string
	SASLPassword string
}

type KafkaSinkOption func(*KafkaSinkConfig)

func WithKafkaSinkTLS(cfg stream.TLSConfig) KafkaSinkOption {
	return func(c *KafkaSinkConfig) { c.TLS = cfg }
}

func DefaultKafkaSinkConfig() KafkaSinkConfig {
	return KafkaSinkConfig{
		BatchSize:    100,
		BatchTimeout: time.Second,
		MaxRetries:   3,
		WriteTimeout: 30 * time.Second,
		RequiredAcks: -1,
	}
}

type KafkaSink struct {
	cfg    KafkaSinkConfig
	writer *kafka.Writer
	cb     *stream.CircuitBreaker
	logger stream.Logger
}

func NewKafkaSink(cfg KafkaSinkConfig) *KafkaSink {
	return &KafkaSink{
		cfg:    cfg,
		cb:     stream.NewCircuitBreaker(stream.DefaultCircuitBreakerConfig("kafka-sink")),
		logger: &stream.NopLogger{},
	}
}

func NewKafkaSinkWithOptions(opts ...KafkaSinkOption) *KafkaSink {
	cfg := DefaultKafkaSinkConfig()
	for _, o := range opts {
		o(&cfg)
	}
	return NewKafkaSink(cfg)
}

func (s *KafkaSink) WithLogger(l stream.Logger) *KafkaSink {
	s.logger = l
	return s
}

func (s *KafkaSink) Open(_ context.Context) error {
	tlsCfg, err := s.cfg.TLS.Build()
	if err != nil {
		return fmt.Errorf("kafka sink tls: %w", err)
	}

	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
	}
	if tlsCfg != nil {
		dialer.TLS = tlsCfg
	}
	if s.cfg.SASLUsername != "" {
		dialer.SASLMechanism = plain.Mechanism{
			Username: s.cfg.SASLUsername,
			Password: s.cfg.SASLPassword,
		}
	}

	s.writer = &kafka.Writer{
		Addr:         kafka.TCP(s.cfg.Brokers...),
		Topic:        s.cfg.Topic,
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    s.cfg.BatchSize,
		BatchTimeout: s.cfg.BatchTimeout,
		BatchBytes:   1e6,
		WriteTimeout: s.cfg.WriteTimeout,
		RequiredAcks: s.cfg.RequiredAcks,
		Transport:    &kafka.Transport{Dial: dialer.DialFunc},
	}
	s.logger.Info(context.Background(), "kafka sink connected",
		"brokers", s.cfg.Brokers,
		"topic", s.cfg.Topic,
	)
	return nil
}

func (s *KafkaSink) Flush(_ context.Context) error { return nil }

func (s *KafkaSink) Close(_ context.Context) error {
	if s.writer != nil {
		err := s.writer.Close()
		s.logger.Info(context.Background(), "kafka sink closed")
		return err
	}
	return nil
}

func (s *KafkaSink) Write(ctx context.Context, msg stream.Message[[]byte]) error {
	return s.cb.Execute(ctx, func(ctx context.Context) error {
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
	})
}

package source

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"

	"github.com/kennyrobert88/go-stream-processing/stream"
)

type KafkaSourceConfig struct {
	Brokers      []string
	Topic        string
	GroupID      string
	ManualCommit bool
	MinBytes     int
	MaxBytes     int

	TLS                stream.TLSConfig
	SASLUsername       string
	SASLPassword       string
	ReconnectDelay     time.Duration
	MaxReconnects      int
	HeartbeatInterval  time.Duration
	SessionTimeout     time.Duration
	RebalanceTimeout   time.Duration
	CooperativeRebalance bool
	LagMonitorInterval time.Duration
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
func WithKafkaTLS(cfg stream.TLSConfig) KafkaSourceOption {
	return func(c *KafkaSourceConfig) { c.TLS = cfg }
}
func WithSASLPlain(username, password string) KafkaSourceOption {
	return func(c *KafkaSourceConfig) { c.SASLUsername = username; c.SASLPassword = password }
}
func WithKafkaReconnect(delay time.Duration, max int) KafkaSourceOption {
	return func(c *KafkaSourceConfig) { c.ReconnectDelay = delay; c.MaxReconnects = max }
}
func WithCooperativeRebalance(v bool) KafkaSourceOption {
	return func(c *KafkaSourceConfig) { c.CooperativeRebalance = v }
}
func WithLagMonitor(interval time.Duration) KafkaSourceOption {
	return func(c *KafkaSourceConfig) { c.LagMonitorInterval = interval }
}

func DefaultKafkaSourceConfig() KafkaSourceConfig {
	return KafkaSourceConfig{
		ManualCommit:      false,
		MinBytes:          10e3,
		MaxBytes:          10e6,
		ReconnectDelay:    time.Second,
		MaxReconnects:     10,
		HeartbeatInterval: 3 * time.Second,
		SessionTimeout:    30 * time.Second,
		RebalanceTimeout:  60 * time.Second,
	}
}

type kafkaAck struct {
	reader *kafka.Reader
	msg    kafka.Message
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
	cb     *stream.CircuitBreaker
	logger stream.Logger
}

func NewKafkaSource(cfg KafkaSourceConfig) *KafkaSource {
	return &KafkaSource{
		cfg: cfg,
		cb: stream.NewCircuitBreaker(stream.DefaultCircuitBreakerConfig("kafka-source")),
		logger: &stream.NopLogger{},
	}
}

func NewKafkaSourceWithOptions(opts ...KafkaSourceOption) *KafkaSource {
	cfg := DefaultKafkaSourceConfig()
	for _, o := range opts {
		o(&cfg)
	}
	src := NewKafkaSource(cfg)
	return src
}

func (s *KafkaSource) WithLogger(l stream.Logger) *KafkaSource {
	s.logger = l
	return s
}

func (s *KafkaSource) WithCircuitBreaker(cb *stream.CircuitBreaker) *KafkaSource {
	s.cb = cb
	return s
}

func (s *KafkaSource) Open(ctx context.Context) error {
	return s.connect(ctx)
}

func (s *KafkaSource) connect(ctx context.Context) error {
	tlsCfg, err := s.cfg.TLS.Build()
	if err != nil {
		return stream.NewNonRetryableError(fmt.Errorf("kafka source tls: %w", err))
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

	readerCfg := kafka.ReaderConfig{
		Brokers:          s.cfg.Brokers,
		Topic:            s.cfg.Topic,
		GroupID:          s.cfg.GroupID,
		GroupTopics:      []string{s.cfg.Topic},
		MinBytes:         s.cfg.MinBytes,
		MaxBytes:         s.cfg.MaxBytes,
		CommitInterval:   0,
		HeartbeatInterval: s.cfg.HeartbeatInterval,
		SessionTimeout:   s.cfg.SessionTimeout,
		RebalanceTimeout: s.cfg.RebalanceTimeout,
		Dialer:           dialer,
		WatchPartitionChanges: true,
	}
	if s.cfg.CooperativeRebalance {
		readerCfg.GroupBalancers = []kafka.GroupBalancer{kafka.RoundRobinGroupBalancer{}}
	}
	s.reader = kafka.NewReader(readerCfg)

	s.logger.Info(ctx, "kafka source connected",
		"brokers", s.cfg.Brokers,
		"topic", s.cfg.Topic,
		"group", s.cfg.GroupID,
	)
	return nil
}

func (s *KafkaSource) Close(ctx context.Context) error {
	if s.reader != nil {
		err := s.reader.Close()
		s.logger.Info(ctx, "kafka source closed")
		return err
	}
	return nil
}

func (s *KafkaSource) Read(ctx context.Context) (stream.Message[[]byte], error) {
	var kafkaMsg kafka.Message
	err := s.cb.Execute(ctx, func(ctx context.Context) error {
		readCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		var err error
		kafkaMsg, err = s.reader.ReadMessage(readCtx)
		return err
	})
	if err != nil {
		if s.cb.State() == stream.CBStateOpen {
			s.logger.Error(ctx, "kafka source circuit breaker open, attempting reconnect",
				"brokers", s.cfg.Brokers)
			if err := s.tryReconnect(ctx); err != nil {
				return stream.Message[[]byte]{}, stream.NewRetryableError(fmt.Errorf("kafka source reconnection failed: %w", err))
			}
			s.cb.Reset()
		}
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

func (s *KafkaSource) tryReconnect(ctx context.Context) error {
	for i := 0; i < s.cfg.MaxReconnects; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(s.cfg.ReconnectDelay):
		}
		s.logger.Info(ctx, "kafka source reconnecting", "attempt", i+1, "max", s.cfg.MaxReconnects)
		if s.reader != nil {
			_ = s.reader.Close()
		}
		if err := s.connect(ctx); err == nil {
			s.logger.Info(ctx, "kafka source reconnected", "attempt", i+1)
			return nil
		}
	}
	return fmt.Errorf("max reconnection attempts exceeded")
}

func headersToMap(headers []kafka.Header) map[string][]byte {
	m := make(map[string][]byte, len(headers))
	for _, h := range headers {
		m[h.Key] = h.Value
	}
	return m
}

var _ time.Time

package source

import (
	"context"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/kennyrobert88/go-stream-processing/stream"
)

type RabbitMQSourceConfig struct {
	URL        string
	Queue      string
	Exchange   string
	RoutingKey string

	TLS                stream.TLSConfig
	ReconnectDelay     time.Duration
	MaxReconnects      int
	PrefetchCount      int
	ConsumerTag        string
	Heartbeat          time.Duration
}

type RabbitMQSourceOption func(*RabbitMQSourceConfig)

func WithURL(url string) RabbitMQSourceOption {
	return func(c *RabbitMQSourceConfig) { c.URL = url }
}
func WithQueue(queue string) RabbitMQSourceOption {
	return func(c *RabbitMQSourceConfig) { c.Queue = queue }
}
func WithExchange(exchange string) RabbitMQSourceOption {
	return func(c *RabbitMQSourceConfig) { c.Exchange = exchange }
}
func WithRoutingKey(key string) RabbitMQSourceOption {
	return func(c *RabbitMQSourceConfig) { c.RoutingKey = key }
}
func WithRabbitMQTLS(cfg stream.TLSConfig) RabbitMQSourceOption {
	return func(c *RabbitMQSourceConfig) { c.TLS = cfg }
}

func DefaultRabbitMQSourceConfig() RabbitMQSourceConfig {
	return RabbitMQSourceConfig{
		ReconnectDelay: time.Second,
		MaxReconnects:  10,
		PrefetchCount:  100,
		ConsumerTag:    "",
		Heartbeat:      10 * time.Second,
	}
}

type RabbitMQSource struct {
	cfg        RabbitMQSourceConfig
	conn       *amqp.Connection
	channel    *amqp.Channel
	deliveries <-chan amqp.Delivery
	mu         sync.Mutex
	notifyConn chan *amqp.Error
	notifyCh   chan *amqp.Error
	done       chan struct{}
	wg         sync.WaitGroup
	cb         *stream.CircuitBreaker
	logger     stream.Logger
}

func NewRabbitMQSource(cfg RabbitMQSourceConfig) *RabbitMQSource {
	return &RabbitMQSource{
		cfg:    cfg,
		done:   make(chan struct{}),
		cb:     stream.NewCircuitBreaker(stream.DefaultCircuitBreakerConfig("rabbitmq-source")),
		logger: &stream.NopLogger{},
	}
}

func NewRabbitMQSourceWithOptions(opts ...RabbitMQSourceOption) *RabbitMQSource {
	cfg := DefaultRabbitMQSourceConfig()
	for _, o := range opts {
		o(&cfg)
	}
	return NewRabbitMQSource(cfg)
}

func (s *RabbitMQSource) WithLogger(l stream.Logger) *RabbitMQSource {
	s.logger = l
	return s
}

func (s *RabbitMQSource) Open(ctx context.Context) error {
	return s.connect(ctx)
}

func (s *RabbitMQSource) connect(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	tlsCfg, err := s.cfg.TLS.Build()
	if err != nil {
		return stream.NewNonRetryableError(fmt.Errorf("rabbitmq source tls: %w", err))
	}

	amqpCfg := amqp.Config{
		Heartbeat: s.cfg.Heartbeat,
	}
	if tlsCfg != nil {
		amqpCfg.TLSClientConfig = tlsCfg
	}

	conn, err := amqp.DialConfig(s.cfg.URL, amqpCfg)
	if err != nil {
		return stream.NewRetryableError(fmt.Errorf("rabbitmq source dial: %w", err))
	}
	s.conn = conn
	s.notifyConn = conn.NotifyClose(make(chan *amqp.Error, 1))

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return stream.NewRetryableError(fmt.Errorf("rabbitmq source channel: %w", err))
	}
	s.channel = ch
	s.notifyCh = ch.NotifyClose(make(chan *amqp.Error, 1))

	if err := ch.Qos(s.cfg.PrefetchCount, 0, false); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return stream.NewRetryableError(fmt.Errorf("rabbitmq source qos: %w", err))
	}

	if s.cfg.Exchange != "" {
		if err := ch.ExchangeDeclare(s.cfg.Exchange, "direct", true, false, false, false, nil); err != nil {
			_ = ch.Close()
			_ = conn.Close()
			return stream.NewRetryableError(fmt.Errorf("rabbitmq source exchange declare: %w", err))
		}
	}

	q, err := ch.QueueDeclare(s.cfg.Queue, true, false, false, false, nil)
	if err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return stream.NewRetryableError(fmt.Errorf("rabbitmq source queue declare: %w", err))
	}

	if s.cfg.Exchange != "" {
		if err := ch.QueueBind(q.Name, s.cfg.RoutingKey, s.cfg.Exchange, false, nil); err != nil {
			_ = ch.Close()
			_ = conn.Close()
			return stream.NewRetryableError(fmt.Errorf("rabbitmq source queue bind: %w", err))
		}
	}

	deliveries, err := ch.Consume(q.Name, s.cfg.ConsumerTag, false, false, false, false, nil)
	if err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return stream.NewRetryableError(fmt.Errorf("rabbitmq source consume: %w", err))
	}
	s.deliveries = deliveries

	s.wg.Add(1)
	go s.handleReconnect()

	s.logger.Info(ctx, "rabbitmq source connected",
		"queue", s.cfg.Queue,
		"exchange", s.cfg.Exchange,
	)
	return nil
}

func (s *RabbitMQSource) handleReconnect() {
	defer s.wg.Done()
	select {
	case <-s.done:
		return
	case err := <-s.notifyConn:
		if err != nil {
			s.logger.Warn(context.Background(), "rabbitmq source connection lost",
				"reason", err.Reason,
				"code", err.Code,
			)
		}
	case <-s.notifyCh:
		s.logger.Warn(context.Background(), "rabbitmq source channel closed, reconnecting")
	}

	for i := 0; i < s.cfg.MaxReconnects; i++ {
		select {
		case <-s.done:
			return
		case <-time.After(s.cfg.ReconnectDelay):
		}
		s.logger.Info(context.Background(), "rabbitmq source reconnecting", "attempt", i+1)
		if err := s.connect(context.Background()); err == nil {
			s.logger.Info(context.Background(), "rabbitmq source reconnected", "attempt", i+1)
			return
		}
	}
}

func (s *RabbitMQSource) Close(ctx context.Context) error {
	close(s.done)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.channel != nil {
		_ = s.channel.Close()
	}
	if s.conn != nil {
		return s.conn.Close()
	}
	return nil
}

func (s *RabbitMQSource) Read(ctx context.Context) (stream.Message[[]byte], error) {
	select {
	case <-ctx.Done():
		return stream.Message[[]byte]{}, ctx.Err()
	case d, ok := <-s.deliveries:
		if !ok {
			return stream.Message[[]byte]{}, stream.NewNonRetryableError(fmt.Errorf("rabbitmq source: delivery channel closed"))
		}
		msg := stream.Message[[]byte]{
			Value:  d.Body,
			Topic:  s.cfg.Queue,
			Offset: int64(d.DeliveryTag),
		}
		var delivery = d
		msg.SetAckNack(
			func(_ context.Context) error {
				return delivery.Ack(false)
			},
			func(_ context.Context) error {
				return delivery.Nack(false, true)
			},
		)
		msg.Headers = make(map[string][]byte, len(d.Headers))
		for k, v := range d.Headers {
			if str, ok := v.(string); ok {
				msg.Headers[k] = []byte(str)
			}
		}
		return msg, nil
	}
}

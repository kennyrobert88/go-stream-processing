package sink

import (
	"context"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/kennyrobert88/go-stream-processing/stream"
)

type RabbitMQSinkConfig struct {
	URL        string
	Exchange   string
	RoutingKey string

	TLS                stream.TLSConfig
	ReconnectDelay     time.Duration
	MaxReconnects      int
	Heartbeat          time.Duration
	DeliveryMode       uint8
}

type RabbitMQSink struct {
	cfg     RabbitMQSinkConfig
	conn    *amqp.Connection
	channel *amqp.Channel
	mu      sync.Mutex
	notify  chan *amqp.Error
	cb      *stream.CircuitBreaker
	logger  stream.Logger
}

func NewRabbitMQSink(cfg RabbitMQSinkConfig) *RabbitMQSink {
	return &RabbitMQSink{
		cfg:    cfg,
		cb:     stream.NewCircuitBreaker(stream.DefaultCircuitBreakerConfig("rabbitmq-sink")),
		logger: &stream.NopLogger{},
	}
}

func (s *RabbitMQSink) WithLogger(l stream.Logger) *RabbitMQSink {
	s.logger = l
	return s
}

func (s *RabbitMQSink) Open(ctx context.Context) error {
	return s.connect(ctx)
}

func (s *RabbitMQSink) connect(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	tlsCfg, err := s.cfg.TLS.Build()
	if err != nil {
		return fmt.Errorf("rabbitmq sink tls: %w", err)
	}

	amqpCfg := amqp.Config{
		Heartbeat: s.cfg.Heartbeat,
	}
	if tlsCfg != nil {
		amqpCfg.TLSClientConfig = tlsCfg
	}

	conn, err := amqp.DialConfig(s.cfg.URL, amqpCfg)
	if err != nil {
		return fmt.Errorf("rabbitmq sink dial: %w", err)
	}
	s.conn = conn
	s.notify = conn.NotifyClose(make(chan *amqp.Error, 1))

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return fmt.Errorf("rabbitmq sink channel: %w", err)
	}
	s.channel = ch

	if s.cfg.Exchange != "" {
		if err := ch.ExchangeDeclare(s.cfg.Exchange, "direct", true, false, false, false, nil); err != nil {
			ch.Close()
			conn.Close()
			return fmt.Errorf("rabbitmq sink exchange declare: %w", err)
		}
	}

	go s.handleReconnect()

	s.logger.Info(ctx, "rabbitmq sink connected", "exchange", s.cfg.Exchange)
	return nil
}

func (s *RabbitMQSink) handleReconnect() {
	err, ok := <-s.notify
	if !ok {
		return
	}
	s.logger.Warn(context.Background(), "rabbitmq sink connection lost",
		"reason", err.Reason, "code", err.Code,
	)

	for i := 0; i < s.cfg.MaxReconnects; i++ {
		time.Sleep(s.cfg.ReconnectDelay)
		s.logger.Info(context.Background(), "rabbitmq sink reconnecting", "attempt", i+1)
		if err := s.connect(context.Background()); err == nil {
			s.logger.Info(context.Background(), "rabbitmq sink reconnected", "attempt", i+1)
			return
		}
	}
}

func (s *RabbitMQSink) Flush(_ context.Context) error { return nil }

func (s *RabbitMQSink) Close(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.channel != nil {
		s.channel.Close()
	}
	if s.conn != nil {
		err := s.conn.Close()
		s.logger.Info(ctx, "rabbitmq sink closed")
		return err
	}
	return nil
}

func (s *RabbitMQSink) ensureChannel(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.channel == nil || s.conn == nil {
		return s.connect(ctx)
	}
	return nil
}

func (s *RabbitMQSink) Write(ctx context.Context, msg stream.Message[[]byte]) error {
	return s.cb.Execute(ctx, func(ctx context.Context) error {
		if err := s.ensureChannel(ctx); err != nil {
			return err
		}

		headers := amqp.Table{}
		for k, v := range msg.Headers {
			headers[k] = string(v)
		}

		err := s.channel.PublishWithContext(ctx,
			s.cfg.Exchange,
			s.cfg.RoutingKey,
			false,
			false,
			amqp.Publishing{
				ContentType:  "application/octet-stream",
				Body:         msg.Value,
				Headers:      headers,
				DeliveryMode: s.cfg.DeliveryMode,
			},
		)
		if err != nil {
			return fmt.Errorf("rabbitmq sink publish: %w", err)
		}
		return nil
	})
}

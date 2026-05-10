package source

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/kennyrobert88/go-stream-processing/stream"
)

type RabbitMQSourceConfig struct {
	URL        string
	Queue      string
	Exchange   string
	RoutingKey string
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

func DefaultRabbitMQSourceConfig() RabbitMQSourceConfig {
	return RabbitMQSourceConfig{}
}

type RabbitMQSource struct {
	cfg       RabbitMQSourceConfig
	conn      *amqp.Connection
	channel   *amqp.Channel
	deliveries <-chan amqp.Delivery
}

func NewRabbitMQSource(cfg RabbitMQSourceConfig) *RabbitMQSource {
	return &RabbitMQSource{cfg: cfg}
}

func NewRabbitMQSourceWithOptions(opts ...RabbitMQSourceOption) *RabbitMQSource {
	cfg := DefaultRabbitMQSourceConfig()
	for _, o := range opts {
		o(&cfg)
	}
	return &RabbitMQSource{cfg: cfg}
}

func (s *RabbitMQSource) Open(_ context.Context) error {
	conn, err := amqp.Dial(s.cfg.URL)
	if err != nil {
		return stream.NewRetryableError(fmt.Errorf("rabbitmq source dial: %w", err))
	}
	s.conn = conn

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return stream.NewRetryableError(fmt.Errorf("rabbitmq source channel: %w", err))
	}
	s.channel = ch

	if s.cfg.Exchange != "" {
		if err := ch.ExchangeDeclare(s.cfg.Exchange, "direct", true, false, false, false, nil); err != nil {
			ch.Close()
			conn.Close()
			return stream.NewRetryableError(fmt.Errorf("rabbitmq source exchange declare: %w", err))
		}
	}

	q, err := ch.QueueDeclare(s.cfg.Queue, true, false, false, false, nil)
	if err != nil {
		ch.Close()
		conn.Close()
		return stream.NewRetryableError(fmt.Errorf("rabbitmq source queue declare: %w", err))
	}

	if s.cfg.Exchange != "" {
		if err := ch.QueueBind(q.Name, s.cfg.RoutingKey, s.cfg.Exchange, false, nil); err != nil {
			ch.Close()
			conn.Close()
			return stream.NewRetryableError(fmt.Errorf("rabbitmq source queue bind: %w", err))
		}
	}

	deliveries, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		ch.Close()
		conn.Close()
		return stream.NewRetryableError(fmt.Errorf("rabbitmq source consume: %w", err))
	}
	s.deliveries = deliveries
	return nil
}

func (s *RabbitMQSource) Close(_ context.Context) error {
	if s.channel != nil {
		s.channel.Close()
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

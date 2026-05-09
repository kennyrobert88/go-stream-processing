package sink

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/kennyrobert88/go-stream-processing/stream"
)

type RabbitMQSinkConfig struct {
	URL        string
	Exchange   string
	RoutingKey string
}

type RabbitMQSink struct {
	cfg     RabbitMQSinkConfig
	conn    *amqp.Connection
	channel *amqp.Channel
}

func NewRabbitMQSink(cfg RabbitMQSinkConfig) *RabbitMQSink {
	return &RabbitMQSink{cfg: cfg}
}

func (s *RabbitMQSink) Open(_ context.Context) error {
	conn, err := amqp.Dial(s.cfg.URL)
	if err != nil {
		return fmt.Errorf("rabbitmq sink dial: %w", err)
	}
	s.conn = conn

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
	return nil
}

func (s *RabbitMQSink) Close(_ context.Context) error {
	if s.channel != nil {
		s.channel.Close()
	}
	if s.conn != nil {
		return s.conn.Close()
	}
	return nil
}

func (s *RabbitMQSink) Write(ctx context.Context, msg stream.Message[[]byte]) error {
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
			ContentType: "application/octet-stream",
			Body:        msg.Value,
			Headers:     headers,
		},
	)
	if err != nil {
		return fmt.Errorf("rabbitmq sink publish: %w", err)
	}
	return nil
}

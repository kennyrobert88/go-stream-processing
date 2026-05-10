package sink

import (
	"context"
	"fmt"

	"cloud.google.com/go/pubsub/v2"

	"github.com/kennyrobert88/go-stream-processing/stream"
)

type PubSubSinkConfig struct {
	ProjectID           string
	TopicID             string
	NumGoroutines       int
	MaxOutstandingBytes int
}

type PubSubSink struct {
	cfg       PubSubSinkConfig
	client    *pubsub.Client
	publisher *pubsub.Publisher
	cb        *stream.CircuitBreaker
	logger    stream.Logger
}

func NewPubSubSink(cfg PubSubSinkConfig) *PubSubSink {
	return &PubSubSink{
		cfg:    cfg,
		cb:     stream.NewCircuitBreaker(stream.DefaultCircuitBreakerConfig("pubsub-sink")),
		logger: &stream.NopLogger{},
	}
}

func (s *PubSubSink) WithLogger(l stream.Logger) *PubSubSink {
	s.logger = l
	return s
}

func (s *PubSubSink) Open(ctx context.Context) error {
	client, err := pubsub.NewClient(ctx, s.cfg.ProjectID)
	if err != nil {
		return fmt.Errorf("pubsub sink client: %w", err)
	}
	s.client = client
	s.publisher = client.Publisher(s.cfg.TopicID)
	if s.cfg.NumGoroutines > 0 {
		s.publisher.PublishSettings.NumGoroutines = s.cfg.NumGoroutines
	} else {
		s.publisher.PublishSettings.NumGoroutines = 10
	}
	if s.cfg.MaxOutstandingBytes > 0 {
		s.publisher.PublishSettings.FlowControlSettings.MaxOutstandingBytes = s.cfg.MaxOutstandingBytes
	}
	s.logger.Info(ctx, "pubsub sink connected",
		"project", s.cfg.ProjectID,
		"topic", s.cfg.TopicID,
	)
	return nil
}

func (s *PubSubSink) Flush(_ context.Context) error { return nil }

func (s *PubSubSink) Close(ctx context.Context) error {
	if s.publisher != nil {
		s.publisher.Stop()
	}
	if s.client != nil {
		err := s.client.Close()
		s.logger.Info(ctx, "pubsub sink closed")
		return err
	}
	return nil
}

func (s *PubSubSink) Write(ctx context.Context, msg stream.Message[[]byte]) error {
	return s.cb.Execute(ctx, func(ctx context.Context) error {
		attrs := make(map[string]string, len(msg.Headers))
		for k, v := range msg.Headers {
			attrs[k] = string(v)
		}
		result := s.publisher.Publish(ctx, &pubsub.Message{
			Data:       msg.Value,
			Attributes: attrs,
		})
		if _, err := result.Get(ctx); err != nil {
			return fmt.Errorf("pubsub sink publish: %w", err)
		}
		return nil
	})
}

package sink

import (
	"context"
	"fmt"

	"cloud.google.com/go/pubsub"

	"github.com/kennyrobert88/go-stream-processing/stream"
)

type PubSubSinkConfig struct {
	ProjectID string
	TopicID   string
}

type PubSubSink struct {
	cfg    PubSubSinkConfig
	client *pubsub.Client
	topic  *pubsub.Topic
}

func NewPubSubSink(cfg PubSubSinkConfig) *PubSubSink {
	return &PubSubSink{cfg: cfg}
}

func (s *PubSubSink) Open(ctx context.Context) error {
	client, err := pubsub.NewClient(ctx, s.cfg.ProjectID)
	if err != nil {
		return fmt.Errorf("pubsub sink client: %w", err)
	}
	s.client = client
	s.topic = client.Topic(s.cfg.TopicID)
	s.topic.PublishSettings.NumGoroutines = 1
	return nil
}

func (s *PubSubSink) Flush(_ context.Context) error { return nil }

func (s *PubSubSink) Close(_ context.Context) error {
	if s.client != nil {
		return s.client.Close()
	}
	return nil
}

func (s *PubSubSink) Write(ctx context.Context, msg stream.Message[[]byte]) error {
	attrs := make(map[string]string, len(msg.Headers))
	for k, v := range msg.Headers {
		attrs[k] = string(v)
	}
	result := s.topic.Publish(ctx, &pubsub.Message{
		Data:       msg.Value,
		Attributes: attrs,
	})
	if _, err := result.Get(ctx); err != nil {
		return fmt.Errorf("pubsub sink publish: %w", err)
	}
	return nil
}

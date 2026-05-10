package source

import (
	"context"
	"fmt"
	"sync"
	"time"

	"cloud.google.com/go/pubsub/v2"

	"github.com/kennyrobert88/go-stream-processing/stream"
)

type PubSubSourceConfig struct {
	ProjectID      string
	SubscriptionID string

	MaxOutstandingMessages int
	MaxOutstandingBytes    int
	NumGoroutines          int
	ReconnectDelay         time.Duration
	MaxReconnects          int
}

type PubSubSourceOption func(*PubSubSourceConfig)

func WithProjectID(id string) PubSubSourceOption {
	return func(c *PubSubSourceConfig) { c.ProjectID = id }
}
func WithSubscriptionID(id string) PubSubSourceOption {
	return func(c *PubSubSourceConfig) { c.SubscriptionID = id }
}
func WithPubSubNumGoroutines(n int) PubSubSourceOption {
	return func(c *PubSubSourceConfig) { c.NumGoroutines = n }
}

func DefaultPubSubSourceConfig() PubSubSourceConfig {
	return PubSubSourceConfig{
		MaxOutstandingMessages: 100,
		MaxOutstandingBytes:    1e9,
		NumGoroutines:          10,
		ReconnectDelay:         time.Second,
		MaxReconnects:          10,
	}
}

type PubSubSource struct {
	cfg        PubSubSourceConfig
	client     *pubsub.Client
	subscriber *pubsub.Subscriber
	cancel     context.CancelFunc
	msgs       chan *pubsub.Message
	wg         sync.WaitGroup
	cb         *stream.CircuitBreaker
	logger     stream.Logger
}

func NewPubSubSource(cfg PubSubSourceConfig) *PubSubSource {
	return &PubSubSource{
		cfg:    cfg,
		msgs:   make(chan *pubsub.Message, cfg.MaxOutstandingMessages),
		cb:     stream.NewCircuitBreaker(stream.DefaultCircuitBreakerConfig("pubsub-source")),
		logger: &stream.NopLogger{},
	}
}

func NewPubSubSourceWithOptions(opts ...PubSubSourceOption) *PubSubSource {
	cfg := DefaultPubSubSourceConfig()
	for _, o := range opts {
		o(&cfg)
	}
	return NewPubSubSource(cfg)
}

func (s *PubSubSource) WithLogger(l stream.Logger) *PubSubSource {
	s.logger = l
	return s
}

func (s *PubSubSource) Open(ctx context.Context) error {
	client, err := pubsub.NewClient(ctx, s.cfg.ProjectID)
	if err != nil {
		return stream.NewNonRetryableError(fmt.Errorf("pubsub source client: %w", err))
	}
	s.client = client

	s.subscriber = client.Subscriber(s.cfg.SubscriptionID)
	s.subscriber.ReceiveSettings.MaxOutstandingMessages = s.cfg.MaxOutstandingMessages
	s.subscriber.ReceiveSettings.MaxOutstandingBytes = s.cfg.MaxOutstandingBytes
	s.subscriber.ReceiveSettings.NumGoroutines = s.cfg.NumGoroutines

	recvCtx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer close(s.msgs)
		s.receiveLoop(recvCtx)
	}()

	s.logger.Info(ctx, "pubsub source connected",
		"project", s.cfg.ProjectID,
		"subscription", s.cfg.SubscriptionID,
	)
	return nil
}

func (s *PubSubSource) receiveLoop(ctx context.Context) {
	for {
		err := s.subscriber.Receive(ctx, func(_ context.Context, msg *pubsub.Message) {
			select {
			case s.msgs <- msg:
			case <-ctx.Done():
				msg.Nack()
			}
		})
		if err != nil {
			if err == context.Canceled {
				return
			}
			s.logger.Error(ctx, "pubsub source receive error", "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(s.cfg.ReconnectDelay):
			}
		}
	}
}

func (s *PubSubSource) Close(_ context.Context) error {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
	if s.client != nil {
		return s.client.Close()
	}
	return nil
}

func (s *PubSubSource) Read(ctx context.Context) (stream.Message[[]byte], error) {
	select {
	case <-ctx.Done():
		return stream.Message[[]byte]{}, ctx.Err()
	case psMsg, ok := <-s.msgs:
		if !ok {
			return stream.Message[[]byte]{}, stream.NewNonRetryableError(fmt.Errorf("pubsub source: closed"))
		}
		msg := stream.Message[[]byte]{
			Value:   psMsg.Data,
			Key:     []byte(psMsg.ID),
			Headers: make(map[string][]byte, len(psMsg.Attributes)),
		}
		for k, v := range psMsg.Attributes {
			msg.Headers[k] = []byte(v)
		}
		var m = psMsg
		msg.SetAckNack(
			func(_ context.Context) error { m.Ack(); return nil },
			func(_ context.Context) error { m.Nack(); return nil },
		)
		return msg, nil
	}
}

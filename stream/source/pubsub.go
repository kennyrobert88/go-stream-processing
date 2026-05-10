package source

import (
	"context"
	"fmt"
	"sync"

	"cloud.google.com/go/pubsub"

	"github.com/kennyrobert88/go-stream-processing/stream"
)

type PubSubSourceConfig struct {
	ProjectID      string
	SubscriptionID string
}

type PubSubSource struct {
	cfg    PubSubSourceConfig
	client *pubsub.Client
	sub    *pubsub.Subscription
	cancel context.CancelFunc
	msgs   chan *pubsub.Message
	wg     sync.WaitGroup
}

func NewPubSubSource(cfg PubSubSourceConfig) *PubSubSource {
	return &PubSubSource{cfg: cfg}
}

func (s *PubSubSource) Open(ctx context.Context) error {
	client, err := pubsub.NewClient(ctx, s.cfg.ProjectID)
	if err != nil {
		return fmt.Errorf("pubsub source client: %w", err)
	}
	s.client = client

	s.sub = client.Subscription(s.cfg.SubscriptionID)
	s.sub.ReceiveSettings.MaxOutstandingMessages = 100

	s.msgs = make(chan *pubsub.Message, 100)

	recvCtx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer close(s.msgs)

		err := s.sub.Receive(recvCtx, func(_ context.Context, msg *pubsub.Message) {
			select {
			case s.msgs <- msg:
			case <-recvCtx.Done():
				msg.Nack()
			}
		})
		if err != nil && err != context.Canceled {
			fmt.Printf("pubsub source receive error: %v\n", err)
		}
	}()

	return nil
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
			return stream.Message[[]byte]{}, fmt.Errorf("pubsub source: source closed")
		}
		msg := stream.Message[[]byte]{
			Value:   psMsg.Data,
			Key:     []byte(psMsg.ID),
			Headers: make(map[string][]byte, len(psMsg.Attributes)),
		}
		for k, v := range psMsg.Attributes {
			msg.Headers[k] = []byte(v)
		}
		msg.SetAckNack(
			func(_ context.Context) error { psMsg.Ack(); return nil },
			func(_ context.Context) error { psMsg.Nack(); return nil },
		)
		return msg, nil
	}
}

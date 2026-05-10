package stream

import "context"

type Message[T any] struct {
	Key       []byte
	Value     T
	Headers   map[string][]byte
	Topic     string
	Partition int32
	Offset    int64

	ackFn  func(context.Context) error
	nackFn func(context.Context) error
}

func NewMessage[T any](value T) Message[T] {
	return Message[T]{Value: value}
}

func (m *Message[T]) SetAckNack(ackFn, nackFn func(context.Context) error) {
	m.ackFn = ackFn
	m.nackFn = nackFn
}

func (m *Message[T]) Ack(ctx context.Context) error {
	if m.ackFn == nil {
		return nil
	}
	return m.ackFn(ctx)
}

func (m *Message[T]) Nack(ctx context.Context) error {
	if m.nackFn == nil {
		return nil
	}
	return m.nackFn(ctx)
}

type Source[T any] interface {
	Open(ctx context.Context) error
	Close(ctx context.Context) error
	Read(ctx context.Context) (Message[T], error)
}

type Sink[T any] interface {
	Open(ctx context.Context) error
	Close(ctx context.Context) error
	Write(ctx context.Context, msg Message[T]) error
}

type TransformFunc[T any] func(ctx context.Context, msg Message[T]) (Message[T], error)

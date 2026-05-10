package stream

import (
	"context"
	"encoding/json"
	"fmt"
)

type Serializer[T any] interface {
	Serialize(ctx context.Context, msg Message[T]) (Message[[]byte], error)
}

type Deserializer[T any] interface {
	Deserialize(ctx context.Context, msg Message[[]byte]) (Message[T], error)
}

type Serde[T any] interface {
	Serializer[T]
	Deserializer[T]
}

type JSONSerde[T any] struct{}

func NewJSONSerde[T any]() *JSONSerde[T] {
	return &JSONSerde[T]{}
}

func (j *JSONSerde[T]) Serialize(_ context.Context, msg Message[T]) (Message[[]byte], error) {
	data, err := json.Marshal(msg.Value)
	if err != nil {
		return Message[[]byte]{}, fmt.Errorf("json serialize: %w", err)
	}
	return Message[[]byte]{
		Key:       msg.Key,
		Value:     data,
		Headers:   msg.Headers,
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Timestamp: msg.Timestamp,
	}, nil
}

func (j *JSONSerde[T]) Deserialize(_ context.Context, msg Message[[]byte]) (Message[T], error) {
	var val T
	if err := json.Unmarshal(msg.Value, &val); err != nil {
		return Message[T]{}, fmt.Errorf("json deserialize: %w", err)
	}
	return Message[T]{
		Key:       msg.Key,
		Value:     val,
		Headers:   msg.Headers,
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Timestamp: msg.Timestamp,
	}, nil
}

type RawSerde struct{}

func NewRawSerde() *RawSerde { return &RawSerde{} }

func (RawSerde) Serialize(_ context.Context, msg Message[[]byte]) (Message[[]byte], error) {
	return msg, nil
}

func (RawSerde) Deserialize(_ context.Context, msg Message[[]byte]) (Message[[]byte], error) {
	return msg, nil
}

type Validator interface {
	Validate(ctx context.Context) error
}

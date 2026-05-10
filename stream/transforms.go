package stream

import (
	"context"
	"errors"
)

var ErrSkip = errors.New("message skipped by filter")

func NewFilter[T any](fn func(ctx context.Context, msg Message[T]) (bool, error)) TransformFunc[T] {
	return func(ctx context.Context, msg Message[T]) (Message[T], error) {
		ok, err := fn(ctx, msg)
		if err != nil {
			return msg, err
		}
		if !ok {
			return msg, ErrSkip
		}
		return msg, nil
	}
}

type FlatMapFunc[T any] func(ctx context.Context, msg Message[T]) ([]Message[T], error)

func isFilterSkip(err error) bool {
	return errors.Is(err, ErrSkip)
}

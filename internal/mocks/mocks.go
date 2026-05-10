package mocks

import (
	"context"
	"sync"

	"github.com/kennyrobert88/go-stream-processing/stream"
)

type MockSource[T any] struct {
	Messages []stream.Message[T]
	Index    int
	mu       sync.Mutex
	OpenErr  error
	CloseErr error
	ReadErr  error
}

func NewMockSource[T any](msgs []stream.Message[T]) *MockSource[T] {
	return &MockSource[T]{Messages: msgs}
}

func (s *MockSource[T]) Open(_ context.Context) error {
	return s.OpenErr
}

func (s *MockSource[T]) Close(_ context.Context) error {
	return s.CloseErr
}

func (s *MockSource[T]) Read(_ context.Context) (stream.Message[T], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ReadErr != nil {
		return stream.Message[T]{}, s.ReadErr
	}
	if s.Index >= len(s.Messages) {
		return stream.Message[T]{}, context.Canceled
	}
	msg := s.Messages[s.Index]
	s.Index++
	return msg, nil
}

type MockSink[T any] struct {
	Messages []stream.Message[T]
	mu       sync.Mutex
	OpenErr  error
	CloseErr error
	WriteErr error
}

func NewMockSink[T any]() *MockSink[T] {
	return &MockSink[T]{}
}

func (s *MockSink[T]) Open(_ context.Context) error {
	return s.OpenErr
}

func (s *MockSink[T]) Close(_ context.Context) error {
	return s.CloseErr
}

func (s *MockSink[T]) Flush(_ context.Context) error { return nil }

func (s *MockSink[T]) Write(_ context.Context, msg stream.Message[T]) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.WriteErr != nil {
		return s.WriteErr
	}
	s.Messages = append(s.Messages, msg)
	return nil
}

func (s *MockSink[T]) Written() []stream.Message[T] {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]stream.Message[T], len(s.Messages))
	copy(result, s.Messages)
	return result
}

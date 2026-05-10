package stream

import (
	"context"
	"encoding/json"
	"sync"
)

type State interface {
	Get(ctx context.Context, key []byte) ([]byte, error)
	Set(ctx context.Context, key []byte, value []byte) error
	Delete(ctx context.Context, key []byte) error
	Snapshot(ctx context.Context) ([]byte, error)
	Restore(ctx context.Context, data []byte) error
}

type InMemoryState struct {
	mu sync.RWMutex
	m  map[string][]byte
}

func NewInMemoryState() *InMemoryState {
	return &InMemoryState{m: make(map[string][]byte)}
}

func (s *InMemoryState) Get(_ context.Context, key []byte) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.m[string(key)]
	if !ok {
		return nil, nil
	}
	return append([]byte{}, v...), nil
}

func (s *InMemoryState) Set(_ context.Context, key []byte, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[string(key)] = append([]byte{}, value...)
	return nil
}

func (s *InMemoryState) Delete(_ context.Context, key []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, string(key))
	return nil
}

func (s *InMemoryState) Snapshot(_ context.Context) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.Marshal(s.m)
}

func (s *InMemoryState) Restore(_ context.Context, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := make(map[string][]byte)
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}
	s.m = m
	return nil
}

type StatefulTransform[T any] struct {
	State State
	Fn    func(ctx context.Context, msg Message[T], state State) (Message[T], error)
}

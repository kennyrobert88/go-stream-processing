package stream

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type OffsetStore interface {
	Read(ctx context.Context, partition string) (string, error)
	Write(ctx context.Context, partition, offset string) error
	Delete(ctx context.Context, partition string) error
	List(ctx context.Context) (map[string]string, error)
	Flush(ctx context.Context) error
}

type InMemoryOffsetStore struct {
	mu      sync.RWMutex
	offsets map[string]string
}

func NewInMemoryOffsetStore() *InMemoryOffsetStore {
	return &InMemoryOffsetStore{offsets: make(map[string]string)}
}

func (s *InMemoryOffsetStore) Read(_ context.Context, partition string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.offsets[partition], nil
}

func (s *InMemoryOffsetStore) Write(_ context.Context, partition, offset string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.offsets[partition] = offset
	return nil
}

func (s *InMemoryOffsetStore) Delete(_ context.Context, partition string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.offsets, partition)
	return nil
}

func (s *InMemoryOffsetStore) List(_ context.Context) (map[string]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make(map[string]string, len(s.offsets))
	for k, v := range s.offsets {
		result[k] = v
	}
	return result, nil
}

func (s *InMemoryOffsetStore) Flush(_ context.Context) error { return nil }

type FileOffsetStore struct {
	mu      sync.Mutex
	path    string
	offsets map[string]string
}

func NewFileOffsetStore(path string) (*FileOffsetStore, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("file offset store mkdir: %w", err)
	}

	s := &FileOffsetStore{
		path:    path,
		offsets: make(map[string]string),
	}

	data, err := os.ReadFile(path)
	if err == nil {
		if err := json.Unmarshal(data, &s.offsets); err != nil {
			return nil, fmt.Errorf("file offset store unmarshal: %w", err)
		}
	}

	return s, nil
}

func (s *FileOffsetStore) Read(_ context.Context, partition string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.offsets[partition], nil
}

func (s *FileOffsetStore) Write(_ context.Context, partition, offset string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.offsets[partition] = offset
	return s.persist()
}

func (s *FileOffsetStore) Delete(_ context.Context, partition string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.offsets, partition)
	return s.persist()
}

func (s *FileOffsetStore) List(_ context.Context) (map[string]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make(map[string]string, len(s.offsets))
	for k, v := range s.offsets {
		result[k] = v
	}
	return result, nil
}

func (s *FileOffsetStore) Flush(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.persist()
}

func (s *FileOffsetStore) persist() error {
	data, err := json.Marshal(s.offsets)
	if err != nil {
		return err
	}
	return os.WriteFile(s.path, data, 0644)
}

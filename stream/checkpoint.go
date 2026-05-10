package stream

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type Checkpoint struct {
	Source      string            `json:"source"`
	Offsets     map[string]string `json:"offsets"`
	WindowState []Message[[]byte] `json:"window_state,omitempty"`
	Timestamp   time.Time         `json:"timestamp"`
}

type CheckpointStore interface {
	Save(ctx context.Context, checkpoint Checkpoint) error
	Load(ctx context.Context, source string) (*Checkpoint, error)
	Delete(ctx context.Context, source string) error
}

type FileCheckpointStore struct {
	basePath string
}

func NewFileCheckpointStore(basePath string) *FileCheckpointStore {
	return &FileCheckpointStore{basePath: basePath}
}

func (f *FileCheckpointStore) Save(ctx context.Context, checkpoint Checkpoint) error {
	checkpoint.Timestamp = time.Now()
	data, err := json.Marshal(checkpoint)
	if err != nil {
		return fmt.Errorf("checkpoint marshal: %w", err)
	}

	store, err := NewFileOffsetStore(f.pathFor(checkpoint.Source))
	if err != nil {
		return fmt.Errorf("checkpoint store: %w", err)
	}
	if err := store.Flush(ctx); err != nil {
		return fmt.Errorf("checkpoint flush: %w", err)
	}

	if err := store.Write(ctx, "checkpoint", string(data)); err != nil {
		return fmt.Errorf("checkpoint write: %w", err)
	}
	return nil
}

func (f *FileCheckpointStore) Load(ctx context.Context, source string) (*Checkpoint, error) {
	store, err := NewFileOffsetStore(f.pathFor(source))
	if err != nil {
		return nil, err
	}

	data, err := store.Read(ctx, "checkpoint")
	if err != nil || data == "" {
		return nil, nil
	}

	var checkpoint Checkpoint
	if err := json.Unmarshal([]byte(data), &checkpoint); err != nil {
		return nil, fmt.Errorf("checkpoint unmarshal: %w", err)
	}
	return &checkpoint, nil
}

func (f *FileCheckpointStore) Delete(ctx context.Context, source string) error {
	store, err := NewFileOffsetStore(f.pathFor(source))
	if err != nil {
		return err
	}
	return store.Delete(ctx, "checkpoint")
}

func (f *FileCheckpointStore) pathFor(source string) string {
	return f.basePath + "/" + source + "_checkpoint.json"
}

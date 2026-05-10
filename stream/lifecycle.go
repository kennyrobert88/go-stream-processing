package stream

import "context"

type Lifecycle interface {
	Open(ctx context.Context) error
	Close(ctx context.Context) error
}

type LifecycleFunc struct {
	OpenFn  func(context.Context) error
	CloseFn func(context.Context) error
}

func (l *LifecycleFunc) Open(ctx context.Context) error {
	if l.OpenFn != nil {
		return l.OpenFn(ctx)
	}
	return nil
}

func (l *LifecycleFunc) Close(ctx context.Context) error {
	if l.CloseFn != nil {
		return l.CloseFn(ctx)
	}
	return nil
}

type TrackedLifecycle struct {
	Lifecycle Lifecycle
	opened    bool
}

func NewTrackedLifecycle(l Lifecycle) *TrackedLifecycle {
	return &TrackedLifecycle{Lifecycle: l}
}

func (t *TrackedLifecycle) Open(ctx context.Context) error {
	if err := t.Lifecycle.Open(ctx); err != nil {
		return err
	}
	t.opened = true
	return nil
}

func (t *TrackedLifecycle) Close(ctx context.Context) error {
	if !t.opened {
		return nil
	}
	t.opened = false
	return t.Lifecycle.Close(ctx)
}

func (t *TrackedLifecycle) Opened() bool {
	return t.opened
}

type Closable interface {
	Close(ctx context.Context) error
}

package stream

import "context"

type MapPipe[T, U any] struct {
	source Source[T]
	fn     func(context.Context, Message[T]) (Message[U], error)
}

func NewMapSource[T, U any](source Source[T], fn func(context.Context, Message[T]) (Message[U], error)) *MapPipe[T, U] {
	return &MapPipe[T, U]{source: source, fn: fn}
}

func (m *MapPipe[T, U]) Open(ctx context.Context) error {
	return m.source.Open(ctx)
}

func (m *MapPipe[T, U]) Close(ctx context.Context) error {
	return m.source.Close(ctx)
}

func (m *MapPipe[T, U]) Read(ctx context.Context) (Message[U], error) {
	msg, err := m.source.Read(ctx)
	if err != nil {
		return Message[U]{}, err
	}
	return m.fn(ctx, msg)
}

type MergeSource[T any] struct {
	sources []Source[T]
	ch      chan Message[T]
	idx     int
}

func NewMergeSource[T any](sources ...Source[T]) *MergeSource[T] {
	return &MergeSource[T]{
		sources: sources,
		ch:      make(chan Message[T], len(sources)),
	}
}

func (m *MergeSource[T]) Open(ctx context.Context) error {
	for _, s := range m.sources {
		if err := s.Open(ctx); err != nil {
			return err
		}
	}
	for i := range m.sources {
		go m.readLoop(ctx, i)
	}
	return nil
}

func (m *MergeSource[T]) readLoop(ctx context.Context, idx int) {
	for {
		msg, err := m.sources[idx].Read(ctx)
		if err != nil {
			return
		}
		select {
		case m.ch <- msg:
		case <-ctx.Done():
			return
		}
	}
}

func (m *MergeSource[T]) Close(ctx context.Context) error {
	var lastErr error
	for _, s := range m.sources {
		if err := s.Close(ctx); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

func (m *MergeSource[T]) Read(ctx context.Context) (Message[T], error) {
	select {
	case msg := <-m.ch:
		return msg, nil
	case <-ctx.Done():
		return Message[T]{}, ctx.Err()
	}
}

type MapSinkFunc[T, U any] struct {
	sink Sink[U]
	fn   func(context.Context, Message[T]) (Message[U], error)
}

func NewMapSink[T, U any](sink Sink[U], fn func(context.Context, Message[T]) (Message[U], error)) *MapSinkFunc[T, U] {
	return &MapSinkFunc[T, U]{sink: sink, fn: fn}
}

func (m *MapSinkFunc[T, U]) Open(ctx context.Context) error {
	return m.sink.Open(ctx)
}

func (m *MapSinkFunc[T, U]) Close(ctx context.Context) error {
	return m.sink.Close(ctx)
}

func (m *MapSinkFunc[T, U]) Write(ctx context.Context, msg Message[T]) error {
	out, err := m.fn(ctx, msg)
	if err != nil {
		return err
	}
	return m.sink.Write(ctx, out)
}

func (m *MapSinkFunc[T, U]) Flush(ctx context.Context) error {
	return m.sink.Flush(ctx)
}

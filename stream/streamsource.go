package stream

import "context"

func AsStream[T any](ctx context.Context, source Source[T]) <-chan Message[T] {
	ch := make(chan Message[T])
	go func() {
		defer close(ch)
		for {
			msg, err := source.Read(ctx)
			if err != nil {
				return
			}
			select {
			case ch <- msg:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch
}

func ReadAll[T any](ctx context.Context, source Source[T]) ([]Message[T], error) {
	var msgs []Message[T]
	for {
		msg, err := source.Read(ctx)
		if err != nil {
			if isContextDone(ctx) {
				return msgs, ctx.Err()
			}
			return msgs, err
		}
		msgs = append(msgs, msg)
	}
}

func BatchRead[T any](ctx context.Context, source Source[T], batchSize int) ([]Message[T], error) {
	msgs := make([]Message[T], 0, batchSize)
	for i := 0; i < batchSize; i++ {
		msg, err := source.Read(ctx)
		if err != nil {
			if len(msgs) > 0 {
				return msgs, nil
			}
			return nil, err
		}
		msgs = append(msgs, msg)
	}
	return msgs, nil
}

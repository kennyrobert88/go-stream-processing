package stream

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type Pipeline[T any] struct {
	source       Source[T]
	sink         Sink[T]
	transforms   []TransformFunc[T]
	retryCfg     RetryConfig
	backpressure *Backpressure
}

func NewPipeline[T any](source Source[T], sink Sink[T]) *Pipeline[T] {
	return &Pipeline[T]{
		source:   source,
		sink:     sink,
		retryCfg: DefaultRetryConfig(),
	}
}

func (p *Pipeline[T]) WithRetryConfig(cfg RetryConfig) *Pipeline[T] {
	p.retryCfg = cfg
	return p
}

func (p *Pipeline[T]) WithBackpressure(cfg BackpressureConfig) *Pipeline[T] {
	bp, err := NewBackpressure(cfg)
	if err != nil {
		panic(fmt.Errorf("pipeline: invalid backpressure config: %w", err))
	}
	p.backpressure = bp
	return p
}

func (p *Pipeline[T]) AddTransform(fn TransformFunc[T]) *Pipeline[T] {
	p.transforms = append(p.transforms, fn)
	return p
}

func (p *Pipeline[T]) Run(ctx context.Context) error {
	if err := p.source.Open(ctx); err != nil {
		return fmt.Errorf("pipeline: source open: %w", err)
	}
	if err := p.sink.Open(ctx); err != nil {
		p.source.Close(ctx)
		return fmt.Errorf("pipeline: sink open: %w", err)
	}

	err := p.runLoop(ctx)

	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if closeErr := p.sink.Close(closeCtx); closeErr != nil && err == nil {
		err = fmt.Errorf("pipeline: sink close: %w", closeErr)
	}
	if closeErr := p.source.Close(closeCtx); closeErr != nil && err == nil {
		err = fmt.Errorf("pipeline: source close: %w", closeErr)
	}
	if p.backpressure != nil {
		p.backpressure.Close()
	}
	return err
}

func (p *Pipeline[T]) runLoop(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if p.backpressure != nil {
			if err := p.backpressure.Wait(ctx); err != nil {
				return err
			}
		}

		var msg Message[T]
		err := DoWithRetry(ctx, func(ctx context.Context) error {
			var err error
			msg, err = p.source.Read(ctx)
			return err
		}, p.retryCfg)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			continue
		}

		for _, tf := range p.transforms {
			msg, err = tf(ctx, msg)
			if err != nil {
				msg.Nack(ctx)
				break
			}
		}
		if err != nil {
			continue
		}

		err = DoWithRetry(ctx, func(ctx context.Context) error {
			return p.sink.Write(ctx, msg)
		}, p.retryCfg)
		if err != nil {
			msg.Nack(ctx)
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			continue
		}
		msg.Ack(ctx)
	}
}

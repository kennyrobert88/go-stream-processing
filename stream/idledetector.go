package stream

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var ErrIdleTimeout = errors.New("source idle timeout exceeded")

type IdleDetector struct {
	mu        sync.Mutex
	timeout   time.Duration
	lastRead  time.Time
	timer     *time.Timer
	done      chan struct{}
	triggered bool
}

func NewIdleDetector(timeout time.Duration) *IdleDetector {
	return &IdleDetector{
		timeout:  timeout,
		lastRead: time.Now(),
		done:     make(chan struct{}),
	}
}

func (d *IdleDetector) Start(ctx context.Context, onIdle func()) {
	d.mu.Lock()
	d.timer = time.AfterFunc(d.timeout, func() {
		d.mu.Lock()
		if !d.triggered && time.Since(d.lastRead) >= d.timeout {
			d.triggered = true
			d.mu.Unlock()
			if onIdle != nil {
				onIdle()
			}
			return
		}
		d.mu.Unlock()
	})
	d.mu.Unlock()

	go func() {
		select {
		case <-ctx.Done():
			d.Stop()
		case <-d.done:
		}
	}()
}

func (d *IdleDetector) RecordActivity() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.lastRead = time.Now()
	d.triggered = false
	if d.timer != nil {
		d.timer.Reset(d.timeout)
	}
}

func (d *IdleDetector) Stop() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.timer != nil {
		d.timer.Stop()
	}
	select {
	case <-d.done:
	default:
		close(d.done)
	}
}

func (d *IdleDetector) Check() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.triggered {
		return ErrIdleTimeout
	}
	return nil
}

type idleReadSource[T any] struct {
	Source[T]
	detector *IdleDetector
}

func WithIdleDetection[T any](src Source[T], timeout time.Duration) Source[T] {
	return &idleReadSource[T]{
		Source:   src,
		detector: NewIdleDetector(timeout),
	}
}

func (s *idleReadSource[T]) Read(ctx context.Context) (Message[T], error) {
	if err := s.detector.Check(); err != nil {
		return Message[T]{}, fmt.Errorf("idle detector: %w", err)
	}

	type result struct {
		msg Message[T]
		err error
	}
	ch := make(chan result, 1)

	go func() {
		msg, err := s.Source.Read(ctx)
		ch <- result{msg, err}
	}()

	select {
	case r := <-ch:
		if r.err == nil {
			s.detector.RecordActivity()
		}
		return r.msg, r.err
	case <-time.After(s.detector.timeout):
		s.detector.RecordActivity()
		return Message[T]{}, ErrIdleTimeout
	}
}

func (s *idleReadSource[T]) Open(ctx context.Context) error {
	err := s.Source.Open(ctx)
	if err == nil {
		s.detector.Start(ctx, func() {
			_ = s.Source.Close(ctx)
		})
	}
	return err
}

func (s *idleReadSource[T]) Close(ctx context.Context) error {
	s.detector.Stop()
	return s.Source.Close(ctx)
}

package stream

import (
	"context"
	"time"
)

type Metrics interface {
	MessageRead(topic string)
	MessageWritten(topic string)
	MessageFailed(topic string, err error)
	TransformError(topic string)
	MessageRetried(topic string, attempt int)
	BackpressureWait(topic string, d time.Duration)
}

type NoopMetrics struct{}

func (NoopMetrics) MessageRead(_ string)                             {}
func (NoopMetrics) MessageWritten(_ string)                          {}
func (NoopMetrics) MessageFailed(_ string, _ error)                  {}
func (NoopMetrics) TransformError(_ string)                          {}
func (NoopMetrics) MessageRetried(_ string, _ int)                   {}
func (NoopMetrics) BackpressureWait(_ string, _ time.Duration)       {}

type MetricsCounter struct {
	ReadCount      int64
	WrittenCount   int64
	FailedCount    int64
	RetryCount     int64
	TransformErr   int64
	BackpressureNs int64
}

func (c *MetricsCounter) MessageRead(_ string)                        { c.ReadCount++ }
func (c *MetricsCounter) MessageWritten(_ string)                     { c.WrittenCount++ }
func (c *MetricsCounter) MessageFailed(_ string, _ error)             { c.FailedCount++ }
func (c *MetricsCounter) TransformError(_ string)                     { c.TransformErr++ }
func (c *MetricsCounter) MessageRetried(_ string, _ int)              { c.RetryCount++ }
func (c *MetricsCounter) BackpressureWait(_ string, d time.Duration)  { c.BackpressureNs += d.Nanoseconds() }

type InstrumentedSource[T any] struct {
	source  Source[T]
	metrics Metrics
	topic   string
}

func NewInstrumentedSource[T any](source Source[T], metrics Metrics, topic string) *InstrumentedSource[T] {
	return &InstrumentedSource[T]{source: source, metrics: metrics, topic: topic}
}

func (s *InstrumentedSource[T]) Open(ctx context.Context) error {
	return s.source.Open(ctx)
}

func (s *InstrumentedSource[T]) Close(ctx context.Context) error {
	return s.source.Close(ctx)
}

func (s *InstrumentedSource[T]) Read(ctx context.Context) (Message[T], error) {
	msg, err := s.source.Read(ctx)
	if err == nil {
		s.metrics.MessageRead(s.topic)
	}
	return msg, err
}

type InstrumentedSink[T any] struct {
	sink    Sink[T]
	metrics Metrics
	topic   string
}

func NewInstrumentedSink[T any](sink Sink[T], metrics Metrics, topic string) *InstrumentedSink[T] {
	return &InstrumentedSink[T]{sink: sink, metrics: metrics, topic: topic}
}

func (s *InstrumentedSink[T]) Open(ctx context.Context) error {
	return s.sink.Open(ctx)
}

func (s *InstrumentedSink[T]) Close(ctx context.Context) error {
	return s.sink.Close(ctx)
}

func (s *InstrumentedSink[T]) Write(ctx context.Context, msg Message[T]) error {
	err := s.sink.Write(ctx, msg)
	if err != nil {
		s.metrics.MessageFailed(s.topic, err)
	} else {
		s.metrics.MessageWritten(s.topic)
	}
	return err
}

func (s *InstrumentedSink[T]) Flush(ctx context.Context) error {
	return s.sink.Flush(ctx)
}

package stream

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

type OTelMetrics struct {
	readCounter      metric.Int64Counter
	writtenCounter   metric.Int64Counter
	failedCounter    metric.Int64Counter
	transformCounter metric.Int64Counter
	retryCounter     metric.Int64Counter
	backpressureHist metric.Int64Histogram
	readLatencyHist  metric.Int64Histogram
	writeLatencyHist metric.Int64Histogram

	tracer trace.Tracer
	attrs  []attribute.KeyValue
}

func NewOTelMetrics(meterName, tracerName string, extraAttrs ...attribute.KeyValue) (*OTelMetrics, error) {
	meter := otel.Meter(meterName)
	tracer := otel.Tracer(tracerName)

	readCounter, err := meter.Int64Counter("stream.messages.read", metric.WithDescription("Messages read from source"))
	if err != nil {
		return nil, err
	}
	writtenCounter, err := meter.Int64Counter("stream.messages.written", metric.WithDescription("Messages written to sink"))
	if err != nil {
		return nil, err
	}
	failedCounter, err := meter.Int64Counter("stream.messages.failed", metric.WithDescription("Messages that failed processing"))
	if err != nil {
		return nil, err
	}
	transformCounter, err := meter.Int64Counter("stream.transforms.errored", metric.WithDescription("Transform errors"))
	if err != nil {
		return nil, err
	}
	retryCounter, err := meter.Int64Counter("stream.retries.total", metric.WithDescription("Total retry attempts"))
	if err != nil {
		return nil, err
	}
	backpressureHist, err := meter.Int64Histogram("stream.backpressure.wait_ms", metric.WithDescription("Backpressure wait time in ms"))
	if err != nil {
		return nil, err
	}
	readLatencyHist, err := meter.Int64Histogram("stream.source.read_latency_ms", metric.WithDescription("Source read latency in ms"))
	if err != nil {
		return nil, err
	}
	writeLatencyHist, err := meter.Int64Histogram("stream.sink.write_latency_ms", metric.WithDescription("Sink write latency in ms"))
	if err != nil {
		return nil, err
	}

	return &OTelMetrics{
		readCounter:      readCounter,
		writtenCounter:   writtenCounter,
		failedCounter:    failedCounter,
		transformCounter: transformCounter,
		retryCounter:     retryCounter,
		backpressureHist: backpressureHist,
		readLatencyHist:  readLatencyHist,
		writeLatencyHist: writeLatencyHist,
		tracer:           tracer,
		attrs:            extraAttrs,
	}, nil
}

func (o *OTelMetrics) StartSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return o.tracer.Start(ctx, name, opts...)
}

func (o *OTelMetrics) MessageRead(topic string) {
	o.readCounter.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String("topic", topic),
	))
}

func (o *OTelMetrics) MessageWritten(topic string) {
	o.writtenCounter.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String("topic", topic),
	))
}

func (o *OTelMetrics) MessageFailed(topic string, _ error) {
	o.failedCounter.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String("topic", topic),
	))
}

func (o *OTelMetrics) TransformError(topic string) {
	o.transformCounter.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String("topic", topic),
	))
}

func (o *OTelMetrics) MessageRetried(topic string, _ int) {
	o.retryCounter.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String("topic", topic),
	))
}

func (o *OTelMetrics) BackpressureWait(topic string, d time.Duration) {
	o.backpressureHist.Record(context.Background(), d.Milliseconds(), metric.WithAttributes(
		attribute.String("topic", topic),
	))
}

func (o *OTelMetrics) RecordReadLatency(topic string, d time.Duration) {
	o.readLatencyHist.Record(context.Background(), d.Milliseconds(), metric.WithAttributes(
		attribute.String("topic", topic),
	))
}

func (o *OTelMetrics) RecordWriteLatency(topic string, d time.Duration) {
	o.writeLatencyHist.Record(context.Background(), d.Milliseconds(), metric.WithAttributes(
		attribute.String("topic", topic),
	))
}

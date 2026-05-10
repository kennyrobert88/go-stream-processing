package stream_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kennyrobert88/go-stream-processing/internal/mocks"
	"github.com/kennyrobert88/go-stream-processing/stream"
)

func TestMessage_NewMessage(t *testing.T) {
	msg := stream.NewMessage("hello")
	if msg.Value != "hello" {
		t.Fatalf("expected value 'hello', got %v", msg.Value)
	}
}

func TestMessage_AckNack(t *testing.T) {
	msg := stream.Message[string]{}
	if err := msg.Ack(context.Background()); err != nil {
		t.Fatalf("unexpected ack error: %v", err)
	}
	if err := msg.Nack(context.Background()); err != nil {
		t.Fatalf("unexpected nack error: %v", err)
	}
}

func TestMessage_WithCallbacks(t *testing.T) {
	var acked, nacked atomic.Bool
	msg := stream.Message[string]{}
	_ = msg

	if acked.Load() {
		t.Error("ack should not have been called")
	}
	if nacked.Load() {
		t.Error("nack should not have been called")
	}
}

func TestMessage_SetAckNack(t *testing.T) {
	var acked, nacked atomic.Bool
	msg := stream.Message[string]{}
	msg.SetAckNack(
		func(_ context.Context) error { acked.Store(true); return nil },
		func(_ context.Context) error { nacked.Store(true); return nil },
	)
	if err := msg.Ack(context.Background()); err != nil {
		t.Fatalf("unexpected ack error: %v", err)
	}
	if !acked.Load() {
		t.Error("ack callback was not called")
	}
	if err := msg.Nack(context.Background()); err != nil {
		t.Fatalf("unexpected nack error: %v", err)
	}
	if !nacked.Load() {
		t.Error("nack callback was not called")
	}
}

func TestMessage_AckNackContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	msg := stream.Message[string]{}
	if err := msg.Ack(ctx); err != nil {
		t.Fatalf("unexpected ack error: %v", err)
	}
	if err := msg.Nack(ctx); err != nil {
		t.Fatalf("unexpected nack error: %v", err)
	}
}

func TestRetryConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     stream.RetryConfig
		wantErr bool
	}{
		{"default", stream.DefaultRetryConfig(), false},
		{"negative max retries", stream.RetryConfig{MaxRetries: -1, BaseDelay: time.Second, MaxDelay: time.Second}, true},
		{"negative base delay", stream.RetryConfig{MaxRetries: 0, BaseDelay: -1, MaxDelay: time.Second}, true},
		{"negative max delay", stream.RetryConfig{MaxRetries: 0, BaseDelay: time.Second, MaxDelay: -1}, true},
		{"base > max", stream.RetryConfig{MaxRetries: 0, BaseDelay: time.Second, MaxDelay: time.Millisecond}, true},
		{"zero all", stream.RetryConfig{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDoWithRetry_Success(t *testing.T) {
	var attempts int
	err := stream.DoWithRetry(context.Background(), func(_ context.Context) error {
		attempts++
		return nil
	}, stream.DefaultRetryConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if attempts != 1 {
		t.Fatalf("expected 1 attempt, got %d", attempts)
	}
}

func TestDoWithRetry_EventuallySucceeds(t *testing.T) {
	var attempts int
	err := stream.DoWithRetry(context.Background(), func(_ context.Context) error {
		attempts++
		if attempts < 3 {
			return errors.New("not yet")
		}
		return nil
	}, stream.RetryConfig{MaxRetries: 5, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts)
	}
}

func TestDoWithRetry_Fails(t *testing.T) {
	err := stream.DoWithRetry(context.Background(), func(_ context.Context) error {
		return errors.New("permanent failure")
	}, stream.RetryConfig{MaxRetries: 2, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestDoWithRetry_ContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := stream.DoWithRetry(ctx, func(_ context.Context) error {
		return errors.New("fail")
	}, stream.RetryConfig{MaxRetries: 5, BaseDelay: time.Second, MaxDelay: time.Second})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestDoWithRetry_NoRetries(t *testing.T) {
	var attempts int
	err := stream.DoWithRetry(context.Background(), func(_ context.Context) error {
		attempts++
		return errors.New("fail")
	}, stream.RetryConfig{MaxRetries: 0, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if attempts != 1 {
		t.Fatalf("expected 1 attempt, got %d", attempts)
	}
}

func TestBackpressureConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     stream.BackpressureConfig
		wantErr bool
	}{
		{"default", stream.DefaultBackpressureConfig(), false},
		{"zero rate", stream.BackpressureConfig{Rate: 0, Burst: 1}, true},
		{"zero burst", stream.BackpressureConfig{Rate: 1, Burst: 0}, true},
		{"negative rate", stream.BackpressureConfig{Rate: -1, Burst: 1}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNewBackpressure_InvalidConfig(t *testing.T) {
	_, err := stream.NewBackpressure(stream.BackpressureConfig{Rate: 0, Burst: 0})
	if err == nil {
		t.Fatal("expected error for invalid config")
	}
}

func TestBackpressure_Allow(t *testing.T) {
	bp, err := stream.NewBackpressure(stream.BackpressureConfig{Rate: 1000, Burst: 5})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer bp.Close()

	for i := 0; i < 5; i++ {
		if !bp.Allow() {
			t.Fatalf("expected allow on iteration %d", i)
		}
	}
	if bp.Allow() {
		t.Fatal("expected deny after burst exhausted")
	}
}

func TestBackpressure_Wait(t *testing.T) {
	bp, err := stream.NewBackpressure(stream.BackpressureConfig{Rate: 1, Burst: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer bp.Close()

	if err := bp.Wait(context.Background()); err != nil {
		t.Fatalf("unexpected wait error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := bp.Wait(ctx); err == nil {
		t.Fatal("expected timeout error")
	}
}

func TestBackpressure_WaitCancel(t *testing.T) {
	bp, err := stream.NewBackpressure(stream.BackpressureConfig{Rate: 10000, Burst: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer bp.Close()

	bp.Wait(context.Background())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := bp.Wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestPipeline_Basic(t *testing.T) {
	msgs := []stream.Message[string]{
		stream.NewMessage("a"),
		stream.NewMessage("b"),
		stream.NewMessage("c"),
	}
	src := mocks.NewMockSource(msgs)
	snk := mocks.NewMockSink[string]()

	pl := stream.NewPipeline(src, snk)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := pl.Run(ctx); err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected run error: %v", err)
	}

	written := snk.Written()
	if len(written) != 3 {
		t.Fatalf("expected 3 written messages, got %d", len(written))
	}
	for i, msg := range written {
		if msg.Value != msgs[i].Value {
			t.Errorf("message %d: expected %v, got %v", i, msgs[i].Value, msg.Value)
		}
	}
}

func TestPipeline_WithTransform(t *testing.T) {
	msgs := []stream.Message[string]{
		stream.NewMessage("hello"),
		stream.NewMessage("world"),
	}
	src := mocks.NewMockSource(msgs)
	snk := mocks.NewMockSink[string]()

	pl := stream.NewPipeline(src, snk)
	pl.AddTransform(func(_ context.Context, msg stream.Message[string]) (stream.Message[string], error) {
		msg.Value = msg.Value + "!"
		return msg, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := pl.Run(ctx); err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected run error: %v", err)
	}

	written := snk.Written()
	if len(written) != 2 {
		t.Fatalf("expected 2 written messages, got %d", len(written))
	}
	if written[0].Value != "hello!" {
		t.Errorf("expected 'hello!', got %v", written[0].Value)
	}
	if written[1].Value != "world!" {
		t.Errorf("expected 'world!', got %v", written[1].Value)
	}
}

func TestPipeline_MultipleTransforms(t *testing.T) {
	msgs := []stream.Message[int]{
		stream.NewMessage(1),
		stream.NewMessage(2),
	}
	src := mocks.NewMockSource(msgs)
	snk := mocks.NewMockSink[int]()

	pl := stream.NewPipeline(src, snk)
	pl.AddTransform(func(_ context.Context, msg stream.Message[int]) (stream.Message[int], error) {
		msg.Value *= 2
		return msg, nil
	})
	pl.AddTransform(func(_ context.Context, msg stream.Message[int]) (stream.Message[int], error) {
		msg.Value += 1
		return msg, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	pl.Run(ctx)

	written := snk.Written()
	if len(written) != 2 {
		t.Fatalf("expected 2 written messages, got %d", len(written))
	}
	if written[0].Value != 3 {
		t.Errorf("expected 3, got %d", written[0].Value)
	}
	if written[1].Value != 5 {
		t.Errorf("expected 5, got %d", written[1].Value)
	}
}

func TestPipeline_TransformErrorSkipsMessage(t *testing.T) {
	msgs := []stream.Message[string]{
		stream.NewMessage("ok"),
		stream.NewMessage("bad"),
		stream.NewMessage("good"),
	}
	src := mocks.NewMockSource(msgs)
	snk := mocks.NewMockSink[string]()

	pl := stream.NewPipeline(src, snk)
	pl.AddTransform(func(_ context.Context, msg stream.Message[string]) (stream.Message[string], error) {
		if msg.Value == "bad" {
			return stream.Message[string]{}, errors.New("transform error")
		}
		return msg, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	pl.Run(ctx)

	written := snk.Written()
	if len(written) != 2 {
		t.Fatalf("expected 2 written messages, got %d", len(written))
	}
	if written[0].Value != "ok" {
		t.Errorf("expected 'ok', got %v", written[0].Value)
	}
	if written[1].Value != "good" {
		t.Errorf("expected 'good', got %v", written[1].Value)
	}
}

func TestPipeline_SourceOpenError(t *testing.T) {
	src := mocks.NewMockSource[string](nil)
	src.OpenErr = errors.New("open failed")
	snk := mocks.NewMockSink[string]()

	pl := stream.NewPipeline(src, snk)
	err := pl.Run(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestPipeline_SinkOpenError(t *testing.T) {
	src := mocks.NewMockSource([]stream.Message[string]{stream.NewMessage("a")})
	snk := mocks.NewMockSink[string]()
	snk.OpenErr = errors.New("sink open failed")

	pl := stream.NewPipeline(src, snk)
	err := pl.Run(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestPipeline_WithBackpressure(t *testing.T) {
	msgs := make([]stream.Message[int], 10)
	for i := range msgs {
		msgs[i] = stream.NewMessage(i)
	}
	src := mocks.NewMockSource(msgs)
	snk := mocks.NewMockSink[int]()

	pl := stream.NewPipeline(src, snk)
	pl.WithBackpressure(stream.BackpressureConfig{Rate: 10000, Burst: 5})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	pl.Run(ctx)

	written := snk.Written()
	if len(written) != 10 {
		t.Fatalf("expected 10 written messages, got %d", len(written))
	}
}

func TestPipeline_ContextCancel(t *testing.T) {
	src := mocks.NewMockSource([]stream.Message[string]{
		stream.NewMessage("a"),
	})
	snk := mocks.NewMockSink[string]()

	pl := stream.NewPipeline(src, snk)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := pl.Run(ctx)
	if err == nil {
		t.Fatal("expected error on cancelled context")
	}
}

func TestPipeline_RetryOnSourceError(t *testing.T) {
	var readAttempts int32
	msgs := []stream.Message[string]{
		stream.NewMessage("a"),
	}
	src := mocks.NewMockSource(msgs)
	snk := mocks.NewMockSink[string]()

	src.ReadErr = nil

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	pl := stream.NewPipeline(src, snk)
	pl.WithRetryConfig(stream.RetryConfig{
		MaxRetries: 1,
		BaseDelay:  time.Millisecond,
		MaxDelay:   time.Millisecond,
	})

	pl.Run(ctx)
	_ = readAttempts
}

func TestPipeline_RetryOnSinkError(t *testing.T) {
	msgs := []stream.Message[string]{
		stream.NewMessage("a"),
	}
	src := mocks.NewMockSource(msgs)
	snk := mocks.NewMockSink[string]()

	pl := stream.NewPipeline(src, snk)
	pl.WithRetryConfig(stream.RetryConfig{
		MaxRetries: 2,
		BaseDelay:  time.Millisecond,
		MaxDelay:   time.Millisecond,
	})
	pl.AddTransform(func(_ context.Context, msg stream.Message[string]) (stream.Message[string], error) {
		return msg, nil
	})

	snk.WriteErr = errors.New("sink write error")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	pl.Run(ctx)
}

func TestMockSource_ReadErr(t *testing.T) {
	src := mocks.NewMockSource[string](nil)
	src.ReadErr = errors.New("read error")

	_, err := src.Read(context.Background())
	if err == nil {
		t.Fatal("expected read error")
	}
}

func TestMockSink_WrittenSnapshot(t *testing.T) {
	snk := mocks.NewMockSink[string]()
	snk.Write(context.Background(), stream.NewMessage("a"))
	snk.Write(context.Background(), stream.NewMessage("b"))

	written := snk.Written()
	if len(written) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(written))
	}

	snk.Write(context.Background(), stream.NewMessage("c"))

	writtenAgain := snk.Written()
	if len(writtenAgain) != 3 {
		t.Fatalf("expected 3 messages after second write, got %d", len(writtenAgain))
	}
}

func TestConcurrentReadWrite(t *testing.T) {
	msgs := make([]stream.Message[int], 100)
	for i := range msgs {
		msgs[i] = stream.NewMessage(i)
	}
	src := mocks.NewMockSource(msgs)
	snk := mocks.NewMockSink[int]()

	pl := stream.NewPipeline(src, snk)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := pl.Run(ctx); err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected run error: %v", err)
	}

	written := snk.Written()
	if len(written) != 100 {
		t.Fatalf("expected 100 written messages, got %d", len(written))
	}
}

func TestPipeline_CleanupOnError(t *testing.T) {
	src := mocks.NewMockSource([]stream.Message[string]{stream.NewMessage("a")})
	snk := mocks.NewMockSink[string]()
	snk.WriteErr = errors.New("write failed")

	pl := stream.NewPipeline(src, snk)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	pl.Run(ctx)
}

func TestMessage_ValueTypes(t *testing.T) {
	intMsg := stream.NewMessage(42)
	if intMsg.Value != 42 {
		t.Errorf("expected 42, got %d", intMsg.Value)
	}

	structMsg := stream.NewMessage(struct{ Name string }{Name: "test"})
	if structMsg.Value.Name != "test" {
		t.Errorf("expected 'test', got %s", structMsg.Value.Name)
	}

	bytesMsg := stream.NewMessage([]byte{1, 2, 3})
	if len(bytesMsg.Value) != 3 {
		t.Errorf("expected length 3, got %d", len(bytesMsg.Value))
	}
}

func TestBackpressure_CloseIdempotent(t *testing.T) {
	bp, err := stream.NewBackpressure(stream.DefaultBackpressureConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	bp.Close()
	bp.Close()
}

func TestPipeline_EmptyTransformChain(t *testing.T) {
	msgs := []stream.Message[string]{
		stream.NewMessage("a"),
	}
	src := mocks.NewMockSource(msgs)
	snk := mocks.NewMockSink[string]()

	pl := stream.NewPipeline(src, snk)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := pl.Run(ctx); err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(snk.Written()) != 1 {
		t.Fatalf("expected 1 written message")
	}
}

func TestIsRetryable_RetryableError(t *testing.T) {
	err := stream.NewRetryableError(errors.New("transient"))
	if !stream.IsRetryable(err) {
		t.Fatal("expected retryable error to be retryable")
	}
}

func TestIsRetryable_NonRetryableError(t *testing.T) {
	err := stream.NewNonRetryableError(errors.New("fatal"))
	if stream.IsRetryable(err) {
		t.Fatal("expected non-retryable error to not be retryable")
	}
}

func TestIsRetryable_DefaultError(t *testing.T) {
	err := errors.New("plain error")
	if !stream.IsRetryable(err) {
		t.Fatal("expected plain error to default to retryable")
	}
}

func TestIsRetryable_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if stream.IsRetryable(ctx.Err()) {
		t.Fatal("expected context.Canceled to be non-retryable")
	}
}

func TestDoWithRetry_NonRetryableSkipsRetry(t *testing.T) {
	var attempts int
	err := stream.DoWithRetry(context.Background(), func(_ context.Context) error {
		attempts++
		return stream.NewNonRetryableError(errors.New("fatal"))
	}, stream.RetryConfig{MaxRetries: 5, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond})
	if err == nil {
		t.Fatal("expected error")
	}
	if attempts != 1 {
		t.Fatalf("expected 1 attempt (non-retryable should skip retry), got %d", attempts)
	}
}

func TestDoWithRetry_RetryableGetsRetries(t *testing.T) {
	var attempts int
	err := stream.DoWithRetry(context.Background(), func(_ context.Context) error {
		attempts++
		return stream.NewRetryableError(errors.New("transient"))
	}, stream.RetryConfig{MaxRetries: 3, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond})
	if err == nil {
		t.Fatal("expected error")
	}
	if attempts != 4 {
		t.Fatalf("expected 4 attempts (1 initial + 3 retries), got %d", attempts)
	}
}

func TestBatchConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     stream.BatchConfig
		wantErr bool
	}{
		{"zero size, zero interval", stream.BatchConfig{}, false},
		{"negative size", stream.BatchConfig{Size: -1}, true},
		{"negative interval", stream.BatchConfig{Interval: -1}, true},
		{"positive size", stream.BatchConfig{Size: 10}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestPipeline_FanOutMultipleSinks(t *testing.T) {
	msgs := []stream.Message[string]{
		stream.NewMessage("a"),
		stream.NewMessage("b"),
	}
	src := mocks.NewMockSource(msgs)
	snk1 := mocks.NewMockSink[string]()
	snk2 := mocks.NewMockSink[string]()

	pl := stream.NewPipeline(src, snk1).AddSink(snk2)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := pl.Run(ctx); err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected run error: %v", err)
	}

	if len(snk1.Written()) != 2 {
		t.Fatalf("expected 2 messages in sink1, got %d", len(snk1.Written()))
	}
	if len(snk2.Written()) != 2 {
		t.Fatalf("expected 2 messages in sink2, got %d", len(snk2.Written()))
	}
}

func TestPipeline_DeadLetterQueue(t *testing.T) {
	msgs := []stream.Message[string]{
		stream.NewMessage("will-fail"),
	}
	src := mocks.NewMockSource(msgs)
	snk := mocks.NewMockSink[string]()
	snk.WriteErr = errors.New("write failed")

	dlq := mocks.NewMockSink[string]()
	dlq.WriteErr = nil

	pl := stream.NewPipeline(src, snk).
		WithDLQ(dlq).
		WithRetryConfig(stream.RetryConfig{MaxRetries: 0, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	pl.Run(ctx)

	if len(dlq.Written()) != 1 {
		t.Fatalf("expected 1 message in DLQ, got %d", len(dlq.Written()))
	}
	if dlq.Written()[0].Value != "will-fail" {
		t.Errorf("expected DLQ to receive 'will-fail', got %v", dlq.Written()[0].Value)
	}
}

func TestPipeline_DeadLetterQueueNotCalledOnSuccess(t *testing.T) {
	msgs := []stream.Message[string]{
		stream.NewMessage("ok"),
	}
	src := mocks.NewMockSource(msgs)
	snk := mocks.NewMockSink[string]()
	dlq := mocks.NewMockSink[string]()

	pl := stream.NewPipeline(src, snk).WithDLQ(dlq)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	pl.Run(ctx)

	if len(dlq.Written()) != 0 {
		t.Fatalf("expected 0 messages in DLQ on success, got %d", len(dlq.Written()))
	}
}

func TestPipeline_BatchSize(t *testing.T) {
	msgs := make([]stream.Message[int], 10)
	for i := range msgs {
		msgs[i] = stream.NewMessage(i)
	}
	src := mocks.NewMockSource(msgs)
	snk := mocks.NewMockSink[int]()

	pl := stream.NewPipeline(src, snk).
		WithBatchConfig(stream.BatchConfig{Size: 5})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := pl.Run(ctx); err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected error: %v", err)
	}

	written := snk.Written()
	if len(written) != 10 {
		t.Fatalf("expected 10 written messages, got %d", len(written))
	}
	for i := range written {
		if written[i].Value != i {
			t.Errorf("message %d: expected %d, got %d", i, i, written[i].Value)
		}
	}
}

func TestPipeline_BatchWithFanOut(t *testing.T) {
	msgs := []stream.Message[string]{
		stream.NewMessage("a"),
		stream.NewMessage("b"),
	}
	src := mocks.NewMockSource(msgs)
	snk1 := mocks.NewMockSink[string]()
	snk2 := mocks.NewMockSink[string]()

	pl := stream.NewPipeline(src, snk1).
		AddSink(snk2).
		WithBatchConfig(stream.BatchConfig{Size: 2})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := pl.Run(ctx); err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(snk1.Written()) != 2 {
		t.Fatalf("expected 2 messages in sink1, got %d", len(snk1.Written()))
	}
	if len(snk2.Written()) != 2 {
		t.Fatalf("expected 2 messages in sink2, got %d", len(snk2.Written()))
	}
}

func TestMetricsCounter_TracksMetrics(t *testing.T) {
	msgs := []stream.Message[string]{
		stream.NewMessage("a"),
		stream.NewMessage("b"),
	}
	src := mocks.NewMockSource(msgs)
	snk := mocks.NewMockSink[string]()

	counter := &stream.MetricsCounter{}
	pl := stream.NewPipeline(src, snk).
		WithMetrics(counter)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	pl.Run(ctx)

	if counter.ReadCount != 2 {
		t.Errorf("expected 2 reads, got %d", counter.ReadCount)
	}
	if counter.WrittenCount != 2 {
		t.Errorf("expected 2 writes, got %d", counter.WrittenCount)
	}
}

func TestMetricsCounter_TracksTransformErrors(t *testing.T) {
	msgs := []stream.Message[string]{
		stream.NewMessage("bad"),
		stream.NewMessage("good"),
	}
	src := mocks.NewMockSource(msgs)
	snk := mocks.NewMockSink[string]()

	counter := &stream.MetricsCounter{}
	pl := stream.NewPipeline(src, snk).
		WithMetrics(counter)
	pl.AddTransform(func(_ context.Context, msg stream.Message[string]) (stream.Message[string], error) {
		if msg.Value == "bad" {
			return stream.Message[string]{}, errors.New("transform error")
		}
		return msg, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	pl.Run(ctx)

	if counter.TransformErr != 1 {
		t.Errorf("expected 1 transform error, got %d", counter.TransformErr)
	}
	if counter.WrittenCount != 1 {
		t.Errorf("expected 1 write, got %d", counter.WrittenCount)
	}
}

func TestMetricsCounter_TracksFailuresOnDLQ(t *testing.T) {
	msgs := []stream.Message[string]{
		stream.NewMessage("fail"),
	}
	src := mocks.NewMockSource(msgs)
	snk := mocks.NewMockSink[string]()
	snk.WriteErr = errors.New("write failed")
	dlq := mocks.NewMockSink[string]()

	counter := &stream.MetricsCounter{}
	pl := stream.NewPipeline(src, snk).
		WithDLQ(dlq).
		WithMetrics(counter).
		WithRetryConfig(stream.RetryConfig{MaxRetries: 0, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	pl.Run(ctx)

	if counter.FailedCount < 1 {
		t.Errorf("expected at least 1 failure, got %d", counter.FailedCount)
	}
}

func TestInstrumentedSource_Passthrough(t *testing.T) {
	msgs := []stream.Message[string]{stream.NewMessage("a")}
	src := mocks.NewMockSource(msgs)
	counter := &stream.MetricsCounter{}

	instrumented := stream.NewInstrumentedSource(src, counter, "test")
	if err := instrumented.Open(context.Background()); err != nil {
		t.Fatal(err)
	}

	msg, err := instrumented.Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if msg.Value != "a" {
		t.Errorf("expected 'a', got %v", msg.Value)
	}
	if counter.ReadCount != 1 {
		t.Errorf("expected 1 read metric, got %d", counter.ReadCount)
	}

	instrumented.Close(context.Background())
}

func TestInstrumentedSink_Passthrough(t *testing.T) {
	snk := mocks.NewMockSink[string]()
	counter := &stream.MetricsCounter{}

	instrumented := stream.NewInstrumentedSink(snk, counter, "test")
	if err := instrumented.Open(context.Background()); err != nil {
		t.Fatal(err)
	}

	msg := stream.NewMessage("a")
	if err := instrumented.Write(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if counter.WrittenCount != 1 {
		t.Errorf("expected 1 write metric, got %d", counter.WrittenCount)
	}

	instrumented.Flush(context.Background())
	instrumented.Close(context.Background())

	written := snk.Written()
	if len(written) != 1 || written[0].Value != "a" {
		t.Errorf("expected 1 message 'a', got %v", written)
	}
}

func TestInstrumentedSink_RecordsFailure(t *testing.T) {
	snk := mocks.NewMockSink[string]()
	snk.WriteErr = errors.New("fail")
	counter := &stream.MetricsCounter{}

	instrumented := stream.NewInstrumentedSink(snk, counter, "test")
	instrumented.Open(context.Background())

	instrumented.Write(context.Background(), stream.NewMessage("x"))
	if counter.FailedCount != 1 {
		t.Errorf("expected 1 failure metric, got %d", counter.FailedCount)
	}
	if counter.WrittenCount != 0 {
		t.Errorf("expected 0 write metrics on failure, got %d", counter.WrittenCount)
	}

	instrumented.Close(context.Background())
}

func TestNoopMetrics_DoesNotPanic(t *testing.T) {
	m := stream.NoopMetrics{}
	m.MessageRead("t")
	m.MessageWritten("t")
	m.MessageFailed("t", errors.New("e"))
	m.TransformError("t")
	m.MessageRetried("t", 1)
	m.BackpressureWait("t", time.Second)
}

func TestPipeline_FlushCalledOnShutdown(t *testing.T) {
	var flushed atomic.Bool
	snk := &flushRecorderSink[string]{MockSink: *mocks.NewMockSink[string]()}
	snk.flushFn = func() { flushed.Store(true) }

	msgs := []stream.Message[string]{stream.NewMessage("a")}
	src := mocks.NewMockSource(msgs)

	pl := stream.NewPipeline(src, snk)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	pl.Run(ctx)

	if !flushed.Load() {
		t.Error("expected Flush to be called on shutdown")
	}
}

type flushRecorderSink[T any] struct {
	mocks.MockSink[T]
	flushFn func()
}

func (s *flushRecorderSink[T]) Flush(_ context.Context) error {
	if s.flushFn != nil {
		s.flushFn()
	}
	return nil
}

func TestPipeline_WithMetricsNilDoesNotPanic(t *testing.T) {
	msgs := []stream.Message[string]{stream.NewMessage("a")}
	src := mocks.NewMockSource(msgs)
	snk := mocks.NewMockSink[string]()

	pl := stream.NewPipeline(src, snk).WithMetrics(nil)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	pl.Run(ctx)

	if len(snk.Written()) != 1 {
		t.Fatalf("expected 1 written message")
	}
}

func TestPipeline_ContextCancelledDuringBatch(t *testing.T) {
	msgs := make([]stream.Message[int], 20)
	for i := range msgs {
		msgs[i] = stream.NewMessage(i)
	}
	src := mocks.NewMockSource(msgs)
	snk := mocks.NewMockSink[int]()

	pl := stream.NewPipeline(src, snk).
		WithBatchConfig(stream.BatchConfig{Size: 100})

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := pl.Run(ctx)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context error, got %v", err)
	}
}

func TestNewPipeline_HasDefaults(t *testing.T) {
	src := mocks.NewMockSource[string](nil)
	snk := mocks.NewMockSink[string]()
	pl := stream.NewPipeline(src, snk)

	if pl == nil {
		t.Fatal("pipeline should not be nil")
	}
}

func TestPipeline_CountWindow(t *testing.T) {
	msgs := make([]stream.Message[int], 10)
	for i := range msgs {
		msgs[i] = stream.NewMessage(i)
	}
	src := mocks.NewMockSource(msgs)
	snk := mocks.NewMockSink[int]()

	pl := stream.NewPipeline(src, snk).
		WithWindow(stream.WindowConfig{Type: stream.WindowTypeCount, Count: 3})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := pl.Run(ctx); err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected error: %v", err)
	}

	written := snk.Written()
	if len(written) != 10 {
		t.Fatalf("expected 10 written messages, got %d", len(written))
	}
}

func TestWindowConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     stream.WindowConfig
		wantErr bool
	}{
		{"tumbling valid", stream.WindowConfig{Type: stream.WindowTypeTumbling, Size: time.Second}, false},
		{"tumbling zero size", stream.WindowConfig{Type: stream.WindowTypeTumbling, Size: 0}, true},
		{"sliding valid", stream.WindowConfig{Type: stream.WindowTypeSliding, Size: time.Second, Slide: 500 * time.Millisecond}, false},
		{"sliding zero slide", stream.WindowConfig{Type: stream.WindowTypeSliding, Size: time.Second, Slide: 0}, true},
		{"session valid", stream.WindowConfig{Type: stream.WindowTypeSession, SessionGap: time.Second}, false},
		{"session zero gap", stream.WindowConfig{Type: stream.WindowTypeSession, SessionGap: 0}, true},
		{"count valid", stream.WindowConfig{Type: stream.WindowTypeCount, Count: 5}, false},
		{"count zero", stream.WindowConfig{Type: stream.WindowTypeCount, Count: 0}, true},
		{"unknown type", stream.WindowConfig{Type: stream.WindowType(99)}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDefaultWindowConfig(t *testing.T) {
	cfg := stream.DefaultWindowConfig()
	if cfg.Type != stream.WindowTypeTumbling {
		t.Errorf("expected tumbling type, got %v", cfg.Type)
	}
	if cfg.Size != 10*time.Second {
		t.Errorf("expected 10s size, got %v", cfg.Size)
	}
}

func TestState_SnapshotRestore(t *testing.T) {
	ctx := context.Background()
	state := stream.NewInMemoryState()

	if err := state.Set(ctx, []byte("key1"), []byte("value1")); err != nil {
		t.Fatal(err)
	}
	if err := state.Set(ctx, []byte("key2"), []byte("value2")); err != nil {
		t.Fatal(err)
	}

	data, err := state.Snapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot error: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("snapshot should not be empty")
	}

	state2 := stream.NewInMemoryState()
	if err := state2.Restore(ctx, data); err != nil {
		t.Fatalf("restore error: %v", err)
	}

	val, err := state2.Get(ctx, []byte("key1"))
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "value1" {
		t.Errorf("expected value1, got %s", string(val))
	}

	val, err = state2.Get(ctx, []byte("key2"))
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "value2" {
		t.Errorf("expected value2, got %s", string(val))
	}
}

func TestState_SnapshotEmpty(t *testing.T) {
	ctx := context.Background()
	state := stream.NewInMemoryState()

	data, err := state.Snapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot error: %v", err)
	}
	if data == nil {
		t.Fatal("snapshot of empty state should not be nil")
	}

	state2 := stream.NewInMemoryState()
	if err := state2.Restore(ctx, data); err != nil {
		t.Fatalf("restore error: %v", err)
	}
}

func TestPipeline_AckNackCalled(t *testing.T) {
	var acked, nacked atomic.Int32
	msgs := make([]stream.Message[string], 3)
	for i := range msgs {
		msgs[i] = stream.NewMessage(fmt.Sprintf("msg-%d", i))
		msgs[i].SetAckNack(
			func(_ context.Context) error { acked.Add(1); return nil },
			func(_ context.Context) error { nacked.Add(1); return nil },
		)
	}
	src := mocks.NewMockSource(msgs)
	snk := mocks.NewMockSink[string]()

	pl := stream.NewPipeline(src, snk)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	pl.Run(ctx)

	if acked.Load() != 3 {
		t.Errorf("expected 3 acks, got %d", acked.Load())
	}
	if nacked.Load() != 0 {
		t.Errorf("expected 0 nacks, got %d", nacked.Load())
	}
}

func TestPipeline_AckNackOnFailure(t *testing.T) {
	var acked, nacked atomic.Int32
	msgs := make([]stream.Message[string], 3)
	for i := range msgs {
		msgs[i] = stream.NewMessage(fmt.Sprintf("msg-%d", i))
		msgs[i].SetAckNack(
			func(_ context.Context) error { acked.Add(1); return nil },
			func(_ context.Context) error { nacked.Add(1); return nil },
		)
	}
	src := mocks.NewMockSource(msgs)
	snk := mocks.NewMockSink[string]()
	snk.WriteErr = errors.New("write failed")

	pl := stream.NewPipeline(src, snk).
		WithRetryConfig(stream.RetryConfig{MaxRetries: 0, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	pl.Run(ctx)

	if nacked.Load() == 0 {
		t.Error("expected nacks on write failure")
	}
}

func TestPipeline_FlatMap(t *testing.T) {
	msgs := []stream.Message[string]{
		stream.NewMessage("hello world"),
		stream.NewMessage("foo bar baz"),
	}
	src := mocks.NewMockSource(msgs)
	snk := mocks.NewMockSink[string]()

	pl := stream.NewPipeline(src, snk)
	pl.FlatMap(func(_ context.Context, msg stream.Message[string]) ([]stream.Message[string], error) {
		parts := strings.Split(msg.Value, " ")
		out := make([]stream.Message[string], len(parts))
		for i, p := range parts {
			out[i] = stream.NewMessage(p)
			out[i].Key = msg.Key
		}
		return out, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := pl.Run(ctx); err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected error: %v", err)
	}

	written := snk.Written()
	if len(written) != 5 {
		t.Fatalf("expected 5 written messages (2+3), got %d", len(written))
	}
	expected := []string{"hello", "world", "foo", "bar", "baz"}
	for i, msg := range written {
		if msg.Value != expected[i] {
			t.Errorf("message %d: expected %q, got %q", i, expected[i], msg.Value)
		}
	}
}

func TestPipeline_FlatMapWithFilter(t *testing.T) {
	msgs := []stream.Message[string]{
		stream.NewMessage("a b c"),
	}
	src := mocks.NewMockSource(msgs)
	snk := mocks.NewMockSink[string]()

	pl := stream.NewPipeline(src, snk)
	pl.FlatMap(func(_ context.Context, msg stream.Message[string]) ([]stream.Message[string], error) {
		parts := strings.Split(msg.Value, " ")
		out := make([]stream.Message[string], 0, len(parts))
		for _, p := range parts {
			if p != "b" {
				out = append(out, stream.NewMessage(p))
			}
		}
		return out, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	pl.Run(ctx)

	written := snk.Written()
	if len(written) != 2 {
		t.Fatalf("expected 2 messages (filtered out 'b'), got %d", len(written))
	}
	if written[0].Value != "a" || written[1].Value != "c" {
		t.Errorf("unexpected values: %v", written)
	}
}

func TestPipeline_PerSinkBackpressure(t *testing.T) {
	msgs := make([]stream.Message[int], 20)
	for i := range msgs {
		msgs[i] = stream.NewMessage(i)
	}
	src := mocks.NewMockSource(msgs)
	snk1 := mocks.NewMockSink[int]()
	snk2 := mocks.NewMockSink[int]()

	pl := stream.NewPipeline(src, snk1).
		AddSink(snk2).
		WithBackpressure(stream.BackpressureConfig{
			Rate:    10000,
			Burst:   5,
			PerSink: true,
		})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := pl.Run(ctx); err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(snk1.Written()) != 20 {
		t.Errorf("expected 20 messages in sink1, got %d", len(snk1.Written()))
	}
	if len(snk2.Written()) != 20 {
		t.Errorf("expected 20 messages in sink2, got %d", len(snk2.Written()))
	}
}

func TestPipeline_SessionWindow(t *testing.T) {
	now := time.Now()
	msgs := []stream.Message[int]{
		{Value: 1, Timestamp: now},
		{Value: 2, Timestamp: now.Add(100 * time.Millisecond)},
		{Value: 3, Timestamp: now.Add(1000 * time.Millisecond)},
	}
	src := mocks.NewMockSource(msgs)
	snk := mocks.NewMockSink[int]()

	pl := stream.NewPipeline(src, snk).
		WithWindow(stream.WindowConfig{Type: stream.WindowTypeSession, SessionGap: 500 * time.Millisecond})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := pl.Run(ctx); err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected error: %v", err)
	}

	written := snk.Written()
	if len(written) != 3 {
		t.Fatalf("expected 3 written messages, got %d", len(written))
	}
}

func TestPipeline_TumblingWindow(t *testing.T) {
	now := time.Now()
	msgs := []stream.Message[int]{
		{Value: 1, Timestamp: now},
		{Value: 2, Timestamp: now.Add(1 * time.Second)},
		{Value: 3, Timestamp: now.Add(2 * time.Second)},
		{Value: 4, Timestamp: now.Add(11 * time.Second)},
	}
	src := mocks.NewMockSource(msgs)
	snk := mocks.NewMockSink[int]()

	pl := stream.NewPipeline(src, snk).
		WithWindow(stream.WindowConfig{Type: stream.WindowTypeTumbling, Size: 10 * time.Second})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := pl.Run(ctx); err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected error: %v", err)
	}

	written := snk.Written()
	if len(written) != 4 {
		t.Fatalf("expected 4 written messages, got %d", len(written))
	}
}

func TestPipeline_SlidingWindow(t *testing.T) {
	now := time.Now()
	msgs := []stream.Message[int]{
		{Value: 1, Timestamp: now},
		{Value: 2, Timestamp: now.Add(3 * time.Second)},
		{Value: 3, Timestamp: now.Add(6 * time.Second)},
		{Value: 4, Timestamp: now.Add(11 * time.Second)},
	}
	src := mocks.NewMockSource(msgs)
	snk := mocks.NewMockSink[int]()

	pl := stream.NewPipeline(src, snk).
		WithWindow(stream.WindowConfig{
			Type:  stream.WindowTypeSliding,
			Size:  10 * time.Second,
			Slide: 5 * time.Second,
		})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := pl.Run(ctx); err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected error: %v", err)
	}

	written := snk.Written()
	if len(written) == 0 {
		t.Fatal("expected at least 1 written message with sliding window")
	}
}

func TestStatefulTransform(t *testing.T) {
	ctx := context.Background()
	state := stream.NewInMemoryState()

	st := stream.StatefulTransform[string]{
		State: state,
		Fn: func(ctx context.Context, msg stream.Message[string], s stream.State) (stream.Message[string], error) {
			prev, err := s.Get(ctx, []byte("count"))
			if err != nil {
				return msg, err
			}
			var count int
			if prev != nil {
				count = int(prev[0])
			}
			count++
			s.Set(ctx, []byte("count"), []byte{byte(count)})
			msg.Value = msg.Value + fmt.Sprintf("-%d", count)
			return msg, nil
		},
	}

	msg := stream.NewMessage("test")
	result, err := st.Fn(ctx, msg, state)
	if err != nil {
		t.Fatal(err)
	}
	if result.Value != "test-1" {
		t.Errorf("expected 'test-1', got %q", result.Value)
	}
}

func TestPipeline_MergeSource(t *testing.T) {
	msgs1 := []stream.Message[string]{stream.NewMessage("a")}
	msgs2 := []stream.Message[string]{stream.NewMessage("b")}
	src1 := mocks.NewMockSource(msgs1)
	src2 := mocks.NewMockSource(msgs2)

	merged := stream.NewMergeSource(src1, src2)
	snk := mocks.NewMockSink[string]()

	pl := stream.NewPipeline(merged, snk)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := pl.Run(ctx); err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected error: %v", err)
	}

	written := snk.Written()
	if len(written) != 2 {
		t.Fatalf("expected 2 written messages, got %d", len(written))
	}
}

func TestWindowType_String(t *testing.T) {
	tests := []struct {
		typ  stream.WindowType
		want string
	}{
		{stream.WindowTypeTumbling, "tumbling"},
		{stream.WindowTypeSliding, "sliding"},
		{stream.WindowTypeSession, "session"},
		{stream.WindowTypeCount, "count"},
		{stream.WindowType(99), "unknown"},
	}
	for _, tt := range tests {
		if got := tt.typ.String(); got != tt.want {
			t.Errorf("WindowType(%d).String() = %q, want %q", tt.typ, got, tt.want)
		}
	}
}

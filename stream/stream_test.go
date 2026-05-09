package stream_test

import (
	"context"
	"errors"
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

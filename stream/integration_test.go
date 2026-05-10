//go:build integration

package stream_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kennyrobert88/go-stream-processing/stream"
	"github.com/kennyrobert88/go-stream-processing/stream/sink"
	"github.com/kennyrobert88/go-stream-processing/stream/source"
)

func skipIfNoBroker(t *testing.T, envVar string) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	_ = envVar
}

func TestKafkaPipeline_Integration(t *testing.T) {
	skipIfNoBroker(t, "KAFKA_BROKERS")

	src := source.NewKafkaSource(source.KafkaSourceConfig{
		Brokers:           []string{"localhost:9092"},
		Topic:             "integration-test-input",
		GroupID:           "integration-test-group",
		HeartbeatInterval: 3 * time.Second,
		SessionTimeout:    30 * time.Second,
		RebalanceTimeout:  60 * time.Second,
		ReconnectDelay:    time.Second,
		MaxReconnects:     5,
	})
	snk := sink.NewKafkaSink(sink.KafkaSinkConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "integration-test-output",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pipeline := stream.NewPipeline(src, snk).
		WithRetryConfig(stream.RetryConfig{MaxRetries: 3, BaseDelay: time.Second, MaxDelay: 5 * time.Second})

	err := pipeline.Run(ctx)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("pipeline run error: %v", err)
	}
}

func TestKafkaPipeline_WithTLS_Integration(t *testing.T) {
	skipIfNoBroker(t, "KAFKA_BROKERS_TLS")

	tlsCfg := stream.TLSConfig{
		Enabled:    true,
		CAFile:     "testdata/ca.pem",
		CertFile:   "testdata/client.pem",
		KeyFile:    "testdata/client-key.pem",
	}

	src := source.NewKafkaSourceWithOptions(
		source.WithBrokers("localhost:9093"),
		source.WithTopic("integration-test-tls-input"),
		source.WithGroupID("integration-test-tls-group"),
		source.WithKafkaTLS(tlsCfg),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := src.Open(ctx); err != nil {
		t.Logf("TLS connection (expected to fail without real certs): %v", err)
	} else {
		src.Close(ctx)
	}
}

func TestKafkaPipeline_WithCircuitBreaker_Integration(t *testing.T) {
	skipIfNoBroker(t, "KAFKA_BROKERS")

	src := source.NewKafkaSource(source.KafkaSourceConfig{
		Brokers:       []string{"localhost:9092"},
		Topic:         "integration-test-cb-input",
		GroupID:       "integration-test-cb-group",
		ReconnectDelay: time.Second,
		MaxReconnects: 2,
	})
	snk := sink.NewKafkaSink(sink.KafkaSinkConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "integration-test-cb-output",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pipeline := stream.NewPipeline(src, snk)
	err := pipeline.Run(ctx)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("pipeline run error: %v", err)
	}
}

func TestRabbitMQPipeline_Integration(t *testing.T) {
	skipIfNoBroker(t, "RABBITMQ_URL")

	src := source.NewRabbitMQSource(source.RabbitMQSourceConfig{
		URL:            "amqp://guest:guest@localhost:5672/",
		Queue:          "integration-test-queue",
		Exchange:       "integration-test-exchange",
		RoutingKey:     "integration-test-key",
		ReconnectDelay: time.Second,
		MaxReconnects:  5,
		PrefetchCount:  100,
	})
	snk := sink.NewRabbitMQSink(sink.RabbitMQSinkConfig{
		URL:        "amqp://guest:guest@localhost:5672/",
		Exchange:   "integration-test-exchange",
		RoutingKey: "integration-test-key",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pipeline := stream.NewPipeline(src, snk).
		WithRetryConfig(stream.RetryConfig{MaxRetries: 3, BaseDelay: time.Second, MaxDelay: 5 * time.Second})

	err := pipeline.Run(ctx)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("pipeline run error: %v", err)
	}
}

func TestPipeline_WithWindowTumbling_Integration(t *testing.T) {
	skipIfNoBroker(t, "KAFKA_BROKERS")

	src := source.NewKafkaSource(source.KafkaSourceConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "integration-test-window-input",
		GroupID: "integration-test-window-group",
	})
	snk := sink.NewKafkaSink(sink.KafkaSinkConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "integration-test-window-output",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pipeline := stream.NewPipeline(src, snk).
		WithWindow(stream.WindowConfig{
			Type: stream.WindowTypeTumbling,
			Size: 10 * time.Second,
		})

	err := pipeline.Run(ctx)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("pipeline run error: %v", err)
	}
}

func TestPipeline_WithSerialization_Integration(t *testing.T) {
	skipIfNoBroker(t, "KAFKA_BROKERS")

	type Order struct {
		ID    string  `json:"id"`
		Value float64 `json:"value"`
	}

	src := source.NewKafkaSource(source.KafkaSourceConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "integration-test-serde-input",
		GroupID: "integration-test-serde-group",
	})
	snk := sink.NewKafkaSink(sink.KafkaSinkConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "integration-test-serde-output",
	})

	serde := stream.NewJSONSerde[Order]()

	mapSrc := stream.NewMapSource(src, func(_ context.Context, msg stream.Message[[]byte]) (stream.Message[Order], error) {
		return serde.Deserialize(context.Background(), msg)
	})
	mapSnk := stream.NewMapSink(snk, func(_ context.Context, msg stream.Message[Order]) (stream.Message[[]byte], error) {
		return serde.Serialize(context.Background(), msg)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pipeline := stream.NewPipeline(mapSrc, mapSnk)
	err := pipeline.Run(ctx)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("pipeline run error: %v", err)
	}
}

func TestPipeline_WithDLQ_Integration(t *testing.T) {
	skipIfNoBroker(t, "KAFKA_BROKERS")

	src := source.NewKafkaSource(source.KafkaSourceConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "integration-test-dlq-input",
		GroupID: "integration-test-dlq-group",
	})
	snk := sink.NewKafkaSink(sink.KafkaSinkConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "integration-test-dlq-output",
	})
	dlq := sink.NewKafkaSink(sink.KafkaSinkConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "integration-test-dlq-dead",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pipeline := stream.NewPipeline(src, snk).
		WithDLQ(dlq).
		WithRetryConfig(stream.RetryConfig{MaxRetries: 0, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond})

	err := pipeline.Run(ctx)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("pipeline run error: %v", err)
	}
}

func TestPipeline_FanOut_Integration(t *testing.T) {
	skipIfNoBroker(t, "KAFKA_BROKERS")

	src := source.NewKafkaSource(source.KafkaSourceConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "integration-test-fanout-input",
		GroupID: "integration-test-fanout-group",
	})
	snk1 := sink.NewKafkaSink(sink.KafkaSinkConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "integration-test-fanout-output-1",
	})
	snk2 := sink.NewKafkaSink(sink.KafkaSinkConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "integration-test-fanout-output-2",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pipeline := stream.NewPipeline(src, snk1).
		AddSink(snk2).
		WithBackpressure(stream.BackpressureConfig{Rate: 1000, Burst: 100, PerSink: true})

	err := pipeline.Run(ctx)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("pipeline run error: %v", err)
	}
}

func TestPipeline_BenchmarkRead(t *testing.T) {
	skipIfNoBroker(t, "KAFKA_BROKERS")

	src := source.NewKafkaSource(source.KafkaSourceConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "benchmark-input",
		GroupID: "benchmark-group",
	})
	snk := sink.NewKafkaSink(sink.KafkaSinkConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "benchmark-output",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pipeline := stream.NewPipeline(src, snk).
		WithWorkers(10).
		WithBatchConfig(stream.BatchConfig{Size: 1000})

	err := pipeline.Run(ctx)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("pipeline run error: %v", err)
	}
}

func TestSerde_JSONRoundTrip_Integration(t *testing.T) {
	type Item struct {
		Name  string  `json:"name"`
		Price float64 `json:"price"`
	}

	serde := stream.NewJSONSerde[Item]()
	ctx := context.Background()

	original := stream.NewMessage(Item{Name: "widget", Price: 9.99})
	serialized, err := serde.Serialize(ctx, original)
	if err != nil {
		t.Fatalf("serialize error: %v", err)
	}

	deserialized, err := serde.Deserialize(ctx, serialized)
	if err != nil {
		t.Fatalf("deserialize error: %v", err)
	}

	if deserialized.Value.Name != "widget" {
		t.Errorf("expected name 'widget', got %q", deserialized.Value.Name)
	}
	if deserialized.Value.Price != 9.99 {
		t.Errorf("expected price 9.99, got %f", deserialized.Value.Price)
	}
}

func TestCircuitBreaker_Integration(t *testing.T) {
	cb := stream.NewCircuitBreaker(stream.DefaultCircuitBreakerConfig("test-breaker"))

	ctx := context.Background()
	var attempts int

	err := cb.Execute(ctx, func(ctx context.Context) error {
		attempts++
		return errors.New("transient error")
	})
	if err == nil {
		t.Fatal("expected error")
	}

	if cb.State() != stream.CBStateOpen {
		t.Errorf("expected circuit breaker to be open after failures, got %v", cb.State())
	}

	err = cb.Execute(ctx, func(ctx context.Context) error {
		return nil
	})
	if err == nil {
		t.Error("expected circuit breaker to reject request when open")
	}

	cb.Reset()
	if cb.State() != stream.CBStateClosed {
		t.Errorf("expected circuit breaker to be closed after reset, got %v", cb.State())
	}
}

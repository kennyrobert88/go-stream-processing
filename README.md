# go-stream-processing

> A unified, type-safe stream-processing connector library for Go. Abstract over Kafka, AWS Kinesis, Google Cloud Pub/Sub, and RabbitMQ with a single `Source[T]` / `Sink[T]` interface — swap brokers by changing one constructor. Inspired by Apache Beam and Apache Flink.

[![Go Version](https://img.shields.io/badge/Go-1.24+-00ADD8?logo=go)](https://go.dev/dl/)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

---

## Recent Updates

### Idle Detection & Drain Phase

Pipeline now supports **idle detection** — when `IdleTimeout` is set in the pipeline config, the source is wrapped so that `Read()` returns `context.DeadlineExceeded` if no message arrives within the timeout. This triggers a drain cycle: all sinks are flushed, the source is closed, and the pipeline stops cleanly.

On graceful shutdown, the pipeline:
1. Flushes all sinks (drain phase)
2. Closes the source
3. Saves the window state checkpoint (`WindowState.Snapshot()`)
4. Stops backpressure and sink goroutines

### Kafka Cooperative Rebalancing

Kafka source now supports **cooperative rebalancing** (sticky partition assignment), enabled via:

```go
cfg := source.KafkaSourceConfig{
    CooperativeRebalance: true,
    LagMonitorInterval:   5 * time.Second,
}
```

This uses `RoundRobinGroupBalancer` for even partition distribution across consumers. The `StartOffset` config field has been removed (use `kafka.ReaderConfig` defaults instead).

### Event-Time Tracking in Windows

Window processing now integrates watermark tracking via `EventTimeTracker` — watermarks are advanced as messages arrive, providing better handling of out-of-order events in tumbling/sliding/session windows.

### Watermark Advance Fix

Fixed a bug where `WindowState.Advance()` was incorrectly called on the window state instead of `EventTimeTracker.Advance()` on the watermark tracker.

### Type-Safe Window Draining

The type-unsafe `WindowState.DrainN` call in the `WindowTypeCount` case has been removed — generics prevent conversion between `[]Message[[]byte]` and `[]Message[T]`, so count windows now drain through the same safe path as other window types.

### Checkpoint Store Fix

`NewFileOffsetStore` now correctly returns two values (`store, err`) instead of one, matching the updated return signature.

### Schema Registry Fix

Fixed `schemaregistry.go` returning `0` instead of `nil` for a `*SchemaMetadata` error path.

### TLS MinVersion

TLS `Build()` now explicitly sets `MinVersion: tls.VersionTLS12`, matching the minimum expected by the test suite and improving security posture.

### CLI Builder Fixes

- `source.WithBrokers` variadic spread fixed (`k.Brokers...` instead of `k.Brokers`)
- PubSub sink `TopicID` now correctly reads from the sink config instead of the source config

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Running Tests](#running-tests)
- [Step-by-Step: Building a Pipeline](#step-by-step-building-a-pipeline)
  - [1. Create a new Go project](#1-create-a-new-go-project)
  - [2. Add the dependency](#2-add-the-dependency)
  - [3. Write your first pipeline (Kafka)](#3-write-your-first-pipeline-kafka)
  - [4. Run with Docker (Kafka)](#4-run-with-docker-kafka)
  - [5. Swap to RabbitMQ](#5-swap-to-rabbitmq)
  - [6. Swap to Kinesis](#6-swap-to-kinesis)
  - [7. Add transforms](#7-add-transforms)
  - [8. Add retry and backpressure](#8-add-retry-and-backpressure)
  - [9. Graceful shutdown](#9-graceful-shutdown)
- [Production Readiness](#production-readiness)
  - [Circuit Breaker](#circuit-breaker)
  - [Health Checks](#health-checks)
  - [OpenTelemetry](#opentelemetry)
  - [Structured Logging](#structured-logging)
  - [Serialization (Serde)](#serialization-serde)
  - [TLS/SSL](#tlsssl)
  - [Connection Retry & Reconnect](#connection-retry--reconnect)
  - [Source-Level Backpressure](#source-level-backpressure)
  - [Graceful Degradation](#graceful-degradation)
- [API Reference](#api-reference)
  - [stream.Message](#streammessage)
  - [stream.Source](#streamsource)
  - [stream.Sink](#streamsink)
  - [stream.Pipeline](#streampipeline)
  - [stream.RetryConfig](#streamretryconfig)
  - [stream.BackpressureConfig](#streambackpressureconfig)
  - [stream.WindowConfig](#streamwindowconfig)
  - [stream.CircuitBreaker](#streamcircuitbreaker)
  - [stream.HealthProbe](#streamhealthprobe)
  - [stream.Logger](#streamlogger)
  - [stream.Serde](#streamserde)
  - [stream.State & Checkpointing](#streamstate--checkpointing)
- [Provider Reference](#provider-reference)
  - [Kafka](#kafka)
  - [AWS Kinesis](#aws-kinesis)
  - [RabbitMQ](#rabbitmq)
  - [Google Cloud Pub/Sub](#google-cloud-pubsub)
- [Testing Guide](#testing-guide)
  - [Unit Tests](#unit-tests)
  - [Integration Tests](#integration-tests)
  - [Benchmark Tests](#benchmark-tests)
- [Examples](#examples)
- [FAQ](#faq)

---

## Overview

`go-stream-processing` solves the problem of vendor lock-in for stream processing. Instead of coding directly to a broker's client library, you program against two generic interfaces:

```go
type Source[T any] interface {
    Open(ctx context.Context) error
    Close(ctx context.Context) error
    Read(ctx context.Context) (Message[T], error)
}

type Sink[T any] interface {
    Open(ctx context.Context) error
    Close(ctx context.Context) error
    Write(ctx context.Context, msg Message[T]) error
    Flush(ctx context.Context) error
}
```

The `Pipeline[T]` wires a `Source → Transform(s) → Sink` with built-in retry (exponential backoff with jitter), backpressure (token-bucket rate limiting), batching, windowing (tumbling/sliding/session/count), DLQ, filtering, flat-mapping, routing, metrics, error handling, stateful transforms, and concurrent workers.

### When to use this library

- You are building a data pipeline and want to avoid coupling to a specific broker.
- You need to process data from Kafka and publish results to Kinesis (or any cross-broker combination).
- You want a clean, testable abstraction with mocks instead of spinning up containers.
- You need type-safe message processing with Go generics.

### Supported brokers

| Broker       | Source | Sink | Library                    |
|-------------|--------|------|----------------------------|
| Apache Kafka | ✓      | ✓    | `segmentio/kafka-go`       |
| AWS Kinesis  | ✓      | ✓    | `aws-sdk-go-v2`            |
| Google Pub/Sub | ✓    | ✓    | `cloud.google.com/go/pubsub` |
| RabbitMQ     | ✓      | ✓    | `rabbitmq/amqp091-go`      |

---

## Architecture

```
 ┌───────────────────────────┐   ┌───────────────────────────┐   ┌───────────────────────────┐
 │        Source[T]          │   │       Pipeline[T]         │   │        Sink[T]            │
 │      Open / Close         │   │  (retry + backpressure)   │   │      Open / Close         │
 │      Read -> Msg[T]       │──▶│       transforms          │──▶│      Write(Msg[T])        │
 └┬─────┬──────┬──────┬──────┘   └───────────────────────────┘   └───┬─────┬──────┬─────┬────┘
  │     │      │      │                                              │     │      │     │
  ▼     ▼      ▼      ▼                                              ▼     ▼      ▼     ▼
Kafka Kinesis Pub/Sub RabbitMQ                                  Kafka  Kinesis  Pub/Sub   RabbitMQ
Source Source  Source  Source                                   Sink   Sink     Sink      Sink
```

### Message lifecycle through the pipeline

```
Source.Read() ──► Filter ──► Transform[0] ──► ... ──► FlatMap ──► Sink.Write()
                      │                                            │
                      │ on ErrSkip (filter)                        │ on error ──► DLQ ──► msg.Nack()
                      ▼                                            │
                   (skip message)                                  ▼
                                                               msg.Ack()

  ── Batching / Windowing / Routing inserted between transforms and sink ──
  ── Any Read/Write error triggers retry (exponential backoff + jitter) ──
```

---

## Prerequisites

- **Go 1.21+** (tested through Go 1.26)
- **Docker** (for running broker containers locally)
- **AWS credentials** (optional, only for Kinesis)

Check your Go version:

```bash
go version
```

---

## Installation

```bash
go get github.com/kennyrobert88/go-stream-processing@latest
```

This adds the library to your `go.mod` and downloads all dependencies (`kafka-go`, `aws-sdk-go-v2`, `amqp091-go`).

---

## Running Tests

The test suite uses mocks and does **not** require any external broker running.

```bash
# Run all tests with verbose output
go test ./... -v -count=1

# Run a specific test
go test ./stream/... -run TestPipeline_Basic -v

# Run with race detection
go test ./stream/... -race -count=1

# Code analysis
go vet ./...
```

Expected output (78 tests, all passing):

```
ok  	github.com/kennyrobert88/go-stream-processing/stream	2.774s
?   	github.com/kennyrobert88/go-stream-processing/internal/mocks	[no test files]
?   	github.com/kennyrobert88/go-stream-processing/stream/sink	[no test files]
?   	github.com/kennyrobert88/go-stream-processing/stream/source	[no test files]
```

---

## Step-by-Step: Building a Pipeline

### 1. Create a new Go project

```bash
mkdir my-pipeline && cd my-pipeline
go mod init my-pipeline
```

### 2. Add the dependency

```bash
go get github.com/kennyrobert88/go-stream-processing@latest
```

### 3. Write your first pipeline (Kafka)

Create `main.go`:

```go
package main

import (
    "context"
    "fmt"
    "log"
    "os"
    "os/signal"
    "syscall"

    "github.com/kennyrobert88/go-stream-processing/stream"
    "github.com/kennyrobert88/go-stream-processing/stream/sink"
    "github.com/kennyrobert88/go-stream-processing/stream/source"
)

func main() {
    ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer cancel()

    src := source.NewKafkaSource(source.KafkaSourceConfig{
        Brokers: []string{"localhost:9092"},
        Topic:   "orders-input",
        GroupID: "pipeline-consumer",
    })

    snk := sink.NewKafkaSink(sink.KafkaSinkConfig{
        Brokers: []string{"localhost:9092"},
        Topic:   "orders-output",
    })

    pipeline := stream.NewPipeline(src, snk)

    log.Fatal(pipeline.Run(ctx))
}
```

### 4. Run with Docker (Kafka)

Start a Kafka broker:

```bash
docker run -d --name kafka \
  -p 9092:9092 \
  -e KAFKA_CFG_NODE_ID=1 \
  -e KAFKA_CFG_PROCESS_ROLES=broker,controller \
  -e KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE=true \
  bitnami/kafka:latest
```

Create topics:

```bash
docker exec kafka kafka-topics.sh --create \
  --topic orders-input \
  --bootstrap-server localhost:9092

docker exec kafka kafka-topics.sh --create \
  --topic orders-output \
  --bootstrap-server localhost:9092
```

Run the pipeline:

```bash
go run main.go
```

In another terminal, produce a test message:

```bash
docker exec -it kafka kafka-console-producer.sh \
  --topic orders-input \
  --bootstrap-server localhost:9092
> {"order_id": 1, "item": "widget"}
```

Consume from the output topic:

```bash
docker exec -it kafka kafka-console-consumer.sh \
  --topic orders-output \
  --bootstrap-server localhost:9092 \
  --from-beginning
```

### 5. Swap to RabbitMQ

Change **only** the source/sink constructors. The rest of the code stays the same:

```go
// Replace Kafka source/sink constructors with:
src := source.NewRabbitMQSource(source.RabbitMQSourceConfig{
    URL:   "amqp://guest:guest@localhost:5672/",
    Queue: "orders-input",
})

snk := sink.NewRabbitMQSink(sink.RabbitMQSinkConfig{
    URL:        "amqp://guest:guest@localhost:5672/",
    Exchange:   "orders",
    RoutingKey: "processed",
})
```

Start RabbitMQ:

```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:4-management
```

Run the pipeline — it now reads from RabbitMQ and writes to RabbitMQ, no other code changes required.

### 6. Swap to Kinesis

```go
// Replace with Kinesis constructors:
src := source.NewKinesisSource(source.KinesisSourceConfig{
    StreamName: "input-stream",
    Region:     "us-east-1",
})

snk := sink.NewKinesisSink(sink.KinesisSinkConfig{
    StreamName: "output-stream",
    Region:     "us-east-1",
})
```

Requires valid [AWS credentials](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html) in `~/.aws/credentials` or environment variables.

### 6.5. Swap to Google Cloud Pub/Sub

```go
// Replace with Pub/Sub constructors:
src := source.NewPubSubSource(source.PubSubSourceConfig{
    ProjectID:      "my-project",
    SubscriptionID: "input-subscription",
})

snk := sink.NewPubSubSink(sink.PubSubSinkConfig{
    ProjectID: "my-project",
    TopicID:   "output-topic",
})
```

Requires a Google Cloud project with Pub/Sub enabled and [application default credentials](https://cloud.google.com/docs/authentication/application-default-credentials) configured. For local development, authenticate via:

```bash
gcloud auth application-default login
```

Pub/Sub uses a callback-based receive model (`sub.Receive`). The source bridges this into the `Read()` interface using an internal buffered channel, and wires `Message.Ack()`/`Message.Nack()` to the underlying Pub/Sub message acknowledgements.

**Emulator for local development:**

```bash
docker run -d --name pubsub \
  -p 8085:8085 \
  gcr.io/google.com/cloudsdktool/cloud-sdk:latest \
  gcloud beta emulators pubsub start --host-port=0.0.0.0:8085

export PUBSUB_EMULATOR_HOST=localhost:8085
```

### 7. Add transforms

Transforms are processing functions applied to every message before it reaches the sink. They compose in order:

```go
pipeline := stream.NewPipeline(src, snk)

// Transform 1: enrich
pipeline.AddTransform(func(ctx context.Context, msg stream.Message[[]byte]) (stream.Message[[]byte], error) {
    enriched := append(msg.Value, []byte(`,"enriched":true}`)...)
    msg.Value = enriched
    return msg, nil
})

// Transform 2: route based on content
pipeline.AddTransform(func(ctx context.Context, msg stream.Message[[]byte]) (stream.Message[[]byte], error) {
    if bytes.Contains(msg.Value, []byte(`"error"`)) {
        // Return error to skip and Nack this message
        return stream.Message[[]byte]{}, fmt.Errorf("skipping error message")
    }
    return msg, nil
})
```

If any transform returns an error, the message is Nacked and skipped. The pipeline continues to the next message.

**Filter (drop messages):**

```go
pipeline.Filter(func(ctx context.Context, msg stream.Message[[]byte]) (bool, error) {
    return len(msg.Value) > 0, nil // drop empty messages
})
```

Return `(false, nil)` to drop; return an error to trigger ErrSkip or actual error handling.

**FlatMap (one-to-many):**

```go
pipeline.FlatMap(func(ctx context.Context, msg stream.Message[[]byte]) ([]stream.Message[[]byte], error) {
    words := bytes.Split(msg.Value, []byte(" "))
    out := make([]stream.Message[[]byte], len(words))
    for i, w := range words {
        out[i] = stream.NewMessage(w)
        out[i].Key = msg.Key
    }
    return out, nil
})
```

FlatMap is useful for splitting, pattern matching, or exploding nested data into individual records.

### 7.5. Batching and windowing

**Batching** groups messages before writing to the sink:

```go
pipeline.WithBatchConfig(stream.BatchConfig{
    Size:     100,                // flush after 100 messages
    Interval: 5 * time.Second,    // or flush after 5s, whichever comes first
})
```

**Windowing** groups messages into logical windows before writing:

```go
// Tumbling window (fixed non-overlapping intervals)
pipeline.WithWindow(stream.WindowConfig{
    Type: stream.WindowTypeTumbling,
    Size: 10 * time.Second,
})

// Sliding window (overlapping, advances every 5s)
pipeline.WithWindow(stream.WindowConfig{
    Type:  stream.WindowTypeSliding,
    Size:  10 * time.Second,
    Slide: 5 * time.Second,
})

// Session window (closes after 30s of inactivity)
pipeline.WithWindow(stream.WindowConfig{
    Type:       stream.WindowTypeSession,
    SessionGap: 30 * time.Second,
})

// Count window (every 1000 messages)
pipeline.WithWindow(stream.WindowConfig{
    Type:  stream.WindowTypeCount,
    Count: 1000,
})
```

**Concurrent processing** for higher throughput:

```go
pipeline.WithWorkers(4) // 1 reader goroutine + 4 worker goroutines
```

**Dead-letter queue** for failed messages:

```go
dlq := sink.NewKafkaSink(sink.KafkaSinkConfig{
    Brokers: []string{"localhost:9092"},
    Topic:   "failed-messages",
})
pipeline.WithDLQ(dlq)
```

### 8. Add retry and backpressure

```go
pipeline := stream.NewPipeline(src, snk)

// Retry: exponential backoff with jitter
pipeline.WithRetryConfig(stream.RetryConfig{
    MaxRetries: 5,
    BaseDelay:  200 * time.Millisecond,
    MaxDelay:   30 * time.Second,
})

// Backpressure: limit to 500 msg/sec with burst of 50
pipeline.WithBackpressure(stream.BackpressureConfig{
    Rate:  500,  // tokens added per second
    Burst: 50,   // max instantaneous burst
})
```

**How retry works:**

```
Attempt 0: wait = BaseDelay * 2^0 + jitter   (~100ms)
Attempt 1: wait = BaseDelay * 2^1 + jitter   (~200ms)
Attempt 2: wait = BaseDelay * 2^2 + jitter   (~400ms)
...
Cap at MaxDelay, jitter = ±BaseDelay/2
```

**How backpressure works:**

A token bucket is initialized with `Burst` tokens. Each consumed message removes one token. New tokens are added at `Rate` per second. If the bucket is empty, `Wait()` blocks until a token is available or the context is cancelled.

### 9. Graceful shutdown

The pipeline honours context cancellation. Use `signal.NotifyContext` for clean shutdown:

```go
ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
defer cancel()

if err := pipeline.Run(ctx); err != nil {
    // On SIGINT/SIGTERM, Run returns context.Canceled
    log.Printf("pipeline stopped: %v", err)
}
```

On shutdown (or idle timeout):
1. The read loop sees the cancelled context (or idle timeout triggers).
2. **Drain phase**: all sinks are flushed (buffered writes are committed).
3. Source is closed (Kafka consumer leaves the group, RabbitMQ channel closes).
4. All sinks are closed (Kafka writer flushes, RabbitMQ connection closes).
5. Backpressure and per-sink backpressure token refill goroutines are stopped.
6. **Checkpoint**: window state is snapshot and saved to the checkpoint store (if both `windowState` and `checkpoint` are configured).

**Idle detection** can be configured to auto-shutdown the pipeline after a period of inactivity:

```go
cfg := stream.PipelineConfig{
    IdleTimeout: stream.Duration{Duration: 30 * time.Second},
}
```

When idle timeout fires, the pipeline drains, closes, and saves its checkpoint, then exits cleanly.

---

## Production Readiness

The library includes several features that make it suitable for production use. All are optional — you opt in by configuring them on the pipeline or provider.

### Circuit Breaker

Prevents cascading failures by stopping requests to a failing component and periodically probing for recovery.

```go
cb := stream.NewCircuitBreaker(stream.CircuitBreakerConfig{
    Name:         "kafka-source",
    MaxFailures:  5,                        // open after 5 consecutive failures
    ResetTimeout: 30 * time.Second,         // try half-open after 30s
    HalfOpenMax:  3,                        // allow 3 probe requests
    OnStateChange: func(name string, old, new stream.CBState) {
        log.Printf("circuit breaker %s: %s -> %s", name, old, new)
    },
})

err := cb.Execute(ctx, func(ctx context.Context) error {
    return someRiskyOperation(ctx)
})
if errors.Is(err, stream.ErrCircuitOpen) {
    // circuit is open, fail fast
}
```

All built-in sources and sinks include an internal circuit breaker. The pipeline uses them automatically.

### Health Checks

The `HealthProbe` provides liveness and readiness checks. Register component-level checks and expose them via HTTP or your orchestrator of choice.

```go
probe := stream.NewHealthProbe(
    // Liveness check
    func(ctx context.Context) stream.HealthReport {
        return stream.HealthReport{Status: stream.StatusHealthy, Component: "liveness"}
    },
    // Readiness check
    func(ctx context.Context) stream.HealthReport {
        return stream.HealthReport{Status: stream.StatusHealthy, Component: "readiness"}
    },
)

// Register component-level checks
probe.RegisterComponent("kafka-source", func(ctx context.Context) stream.HealthReport {
    // check kafka connectivity
    return stream.HealthReport{Status: stream.StatusHealthy}
})

// Attach to pipeline
pipeline.WithHealth(probe)

// Use in HTTP handler
http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
    report := probe.Liveness(r.Context())
    if report.Status != stream.StatusHealthy {
        w.WriteHeader(http.StatusServiceUnavailable)
    }
    json.NewEncoder(w).Encode(report)
})

http.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
    report := probe.Readiness(r.Context())
    if report.Status != stream.StatusHealthy {
        w.WriteHeader(http.StatusServiceUnavailable)
    }
    json.NewEncoder(w).Encode(report)
})
```

### OpenTelemetry

Built-in OpenTelemetry integration captures metrics and traces:

```go
import (
    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/attribute"
)

// Create OTEL metrics with default meter/tracer
otelMetrics, err := stream.NewOTelMetrics(
    "go-stream-processing",
    "go-stream-processing",
    attribute.String("environment", "production"),
)

// Use as pipeline metrics
pipeline.WithMetrics(otelMetrics)
```

Metrics captured:
- `stream.messages.read` — counter
- `stream.messages.written` — counter
- `stream.messages.failed` — counter
- `stream.transforms.errored` — counter
- `stream.retries.total` — counter
- `stream.backpressure.wait_ms` — histogram
- `stream.source.read_latency_ms` — histogram
- `stream.sink.write_latency_ms` — histogram

### Structured Logging

The `Logger` interface supports structured logging (JSON, key-value):

```go
// Use Go's standard slog with JSON output
logger := stream.NewSlogLogger(slog.LevelInfo)

// Or wrap an existing slog.Logger
import "log/slog"
slogLogger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
logger = stream.NewSlogLoggerFrom(slogLogger)

// Attach to pipeline
pipeline.WithLogger(logger)

// Attach to individual providers
kafkaSource := source.NewKafkaSource(cfg).WithLogger(logger)
kafkaSink := sink.NewKafkaSink(cfg).WithLogger(logger)
```

For testing or silencing: `stream.NopLogger{}`.

### Serialization (Serde)

The `Serde[T]` interface converts between typed messages and raw bytes:

```go
type Serde[T any] interface {
    Serializer[T]
    Deserializer[T]
}

type Serializer[T any] interface {
    Serialize(ctx context.Context, msg Message[T]) (Message[[]byte], error)
}

type Deserializer[T any] interface {
    Deserialize(ctx context.Context, msg Message[[]byte]) (Message[T], error)
}
```

Built-in implementations:
- `JSONSerde[T]` — JSON marshal/unmarshal
- `RawSerde` — pass-through for `[]byte`

Usage with `MapSource` and `MapSink`:

```go
type Order struct {
    ID    string  `json:"id"`
    Value float64 `json:"value"`
}

serde := stream.NewJSONSerde[Order]()

// Connect Kafka ([]byte) to typed pipeline
mapSrc := stream.NewMapSource(kafkaSrc, func(_ context.Context, msg stream.Message[[]byte]) (stream.Message[Order], error) {
    return serde.Deserialize(ctx, msg)
})
mapSnk := stream.NewMapSink(kafkaSnk, func(_ context.Context, msg stream.Message[Order]) (stream.Message[[]byte], error) {
    return serde.Serialize(ctx, msg)
})

pipeline := stream.NewPipeline(mapSrc, mapSnk)
```

### TLS/SSL

All providers support TLS configuration:

```go
tlsCfg := stream.TLSConfig{
    Enabled:            true,
    CertFile:           "/etc/certs/client.pem",
    KeyFile:            "/etc/certs/client-key.pem",
    CAFile:             "/etc/certs/ca.pem",
    InsecureSkipVerify: false,
    ServerName:         "kafka.example.com",
}
```

Provider-specific TLS:

```go
// Kafka source
src := source.NewKafkaSourceWithOptions(
    source.WithBrokers("kafka.example.com:9093"),
    source.WithTopic("my-topic"),
    source.WithGroupID("my-group"),
    source.WithKafkaTLS(tlsCfg),
    source.WithSASLPlain("user", "password"),
)

// RabbitMQ source
rmqSrc := source.NewRabbitMQSource(source.RabbitMQSourceConfig{
    URL:  "amqps://rabbit.example.com:5671",
    TLS:  tlsCfg,
})
```

### Connection Retry & Reconnect

All providers have configurable reconnection with exponential backoff:

```go
// Kafka source — reconnects on failure
src := source.NewKafkaSourceWithOptions(
    source.WithKafkaReconnect(2*time.Second, 10), // wait 2s between retries, max 10 attempts
)

// Kinesis source — multi-shard with auto-reconnect
kinesisSrc := source.NewKinesisSourceWithOptions(
    source.WithStreamName("my-stream"),
    source.WithRegion("us-east-1"),
)
// Automatically reads from all shards in parallel goroutines

// RabbitMQ — NotifyClose-driven reconnect
rmqSrc := source.NewRabbitMQSource(source.RabbitMQSourceConfig{
    ReconnectDelay: time.Second,
    MaxReconnects:  10,
})
```

On disconnect:
1. Circuit breaker opens (after `MaxFailures`)
2. Reconnect goroutine attempts reconnection with configured delay
3. On success, circuit breaker resets and normal operation resumes

### Source-Level Backpressure

Per-partition rate limiting at the source level, separate from the pipeline-level token bucket:

```go
bp := stream.NewSourceBackpressure(stream.SourceBackpressureConfig{
    Limit:  1000,              // max messages per window
    Window: time.Second,       // per time window
})

// Per-partition
if !bp.Allow("partition-0") {
    // backpressure applied to this partition
}
```

### Graceful Degradation

The `GracefulDegradation` mechanism allows the pipeline to continue operating in a degraded state when a non-critical component fails:

```go
degradation := stream.NewGracefulDegradation()

// Mark as degraded
degradation.Degrade("kafka source is unhealthy")

// Check state
if degradation.IsDegraded() {
    log.Printf("running in degraded mode: %s", degradation.Reason())
}

// Recover when the component is healthy again
degradation.Recover()
```

---

## API Reference

### `stream.Message`

```go
type Message[T any] struct {
    Key       []byte
    Value     T
    Headers   map[string][]byte
    Topic     string
    Partition int32
    Offset    int64
    Timestamp time.Time
}
```

Message is the core data unit flowing through a pipeline. It carries the payload (`Value` of any type `T`), routing metadata (`Key`, `Topic`, `Partition`, `Offset`, `Timestamp`), and application headers.

**Lifecycle methods:**

```go
func (m *Message[T]) Ack(ctx context.Context) error
func (m *Message[T]) Nack(ctx context.Context) error
```

- `Ack` signals successful processing. For Kafka sources, this would commit the offset. If no ack function is set, it is a no-op.
- `Nack` signals processing failure. The message is negatively acknowledged.
- Both accept a context for cancellation support.

**Constructors:**

```go
msg := stream.NewMessage("hello")              // Message[string]
msg := stream.NewMessage(42)                    // Message[int]
msg := stream.NewMessage([]byte{1, 2, 3})      // Message[[]byte]
msg := stream.NewMessage(MyStruct{Field: "x"}) // Message[MyStruct]
```

### `stream.Source`

```go
type Source[T any] interface {
    Open(ctx context.Context) error
    Close(ctx context.Context) error
    Read(ctx context.Context) (Message[T], error)
}
```

| Method    | Description |
|-----------|-------------|
| `Open`    | Initializes the connection to the broker. Called once when `Pipeline.Run()` starts. |
| `Close`   | Tears down the connection. Called once when the pipeline exits (even on error). |
| `Read`    | Blocks until a message is available, then returns it. Returns `context.Canceled` when the context is done. |

**Implementations:**

| Concrete type              | Package            | Produces         |
|----------------------------|--------------------|------------------|
| `KafkaSource`              | `source`           | `Message[[]byte]` |
| `KinesisSource`            | `source`           | `Message[[]byte]` |
| `RabbitMQSource`           | `source`           | `Message[[]byte]` |
| `PubSubSource`             | `source`           | `Message[[]byte]` |
| `MergeSource`              | `stream`           | `Message[T]`     |
| `MapSource`                | `stream`           | `Message[U]`     |

### `stream.Sink`

```go
type Sink[T any] interface {
    Open(ctx context.Context) error
    Close(ctx context.Context) error
    Write(ctx context.Context, msg Message[T]) error
    Flush(ctx context.Context) error
}
```

| Method    | Description |
|-----------|-------------|
| `Open`    | Initializes the connection to the broker. Called once when `Pipeline.Run()` starts. |
| `Close`   | Tears down the connection. Called once when the pipeline exits. |
| `Write`   | Publishes the message to the broker. Should honour context cancellation for timeout support. |
| `Flush`   | Flushes any buffered writes. Called on pipeline shutdown. |

**Implementations:**

| Concrete type              | Package            | Consumes         |
|----------------------------|--------------------|------------------|
| `KafkaSink`                | `sink`             | `Message[[]byte]` |
| `KinesisSink`              | `sink`             | `Message[[]byte]` |
| `RabbitMQSink`             | `sink`             | `Message[[]byte]` |
| `PubSubSink`               | `sink`             | `Message[[]byte]` |
| `MapSink`                  | `stream`           | `Message[T]`     |

### `stream.Pipeline`

```go
type Pipeline[T any] struct { ... }

func NewPipeline[T any](source Source[T], sink Sink[T]) *Pipeline[T]
```

Connects a `Source[T]` to a `Sink[T]` with optional transforms, retry, and backpressure.

**Configuration methods (all return `*Pipeline[T]` for chaining):**

| Method                             | Description |
|------------------------------------|-------------|
| `WithRetryConfig(cfg)`             | Set retry parameters for source reads and sink writes. |
| `WithBackpressure(cfg)`            | Enable token-bucket rate limiting (global or per-sink). |
| `WithBatchConfig(cfg)`             | Enable batching (size and/or interval). |
| `WithWindow(cfg)`                  | Enable windowing (tumbling, sliding, session, count). |
| `WithMetrics(m)`                   | Attach a metrics collector. |
| `WithDLQ(sink)`                    | Set a dead-letter queue for failed messages. |
| `WithErrorHandler(h)`              | Set an error handler for read/transform/write errors. |
| `WithState(state)`                 | Attach state for stateful transforms. |
| `WithWorkers(n)`                   | Set concurrency level (reader + N workers). |
| `AddTransform(fn)`                 | Append a processing function to the transform chain. |
| `AddSink(sink)`                    | Fan-out to an additional sink. |
| `Filter(fn)`                       | Append a filter transform (returns ErrSkip to drop). |
| `FlatMap(fn)`                      | Append a flat-map transform (one-to-many). |
| `Split(routes, sinks)`             | Route messages to different sinks based on content. |

**Execution:**

```go
func (p *Pipeline[T]) Run(ctx context.Context) error
```

Run starts the pipeline loop:
1. Opens the source and all sinks.
2. Repeatedly: Read → [backpressure.Wait] → [retry Read] → [transforms/filter/flatMap] → [window/batch] → [retry Write] → Ack.
3. On context cancellation or unrecoverable error: flushes all sinks, closes source and sinks, then returns.

### `stream.RetryConfig`

```go
type RetryConfig struct {
    MaxRetries int
    BaseDelay  time.Duration
    MaxDelay   time.Duration
}

func DefaultRetryConfig() RetryConfig // MaxRetries: 3, BaseDelay: 100ms, MaxDelay: 10s
```

Applies to both source reads and sink writes independently within the pipeline.

**Validation rules:**
- `MaxRetries >= 0` (zero means attempt once, no retry)
- `BaseDelay >= 0`
- `MaxDelay >= 0`
- `BaseDelay <= MaxDelay`

### `stream.BackpressureConfig`

```go
type BackpressureConfig struct {
    Rate    int  // tokens per second
    Burst   int  // maximum accumulated tokens
    PerSink bool // create a separate limiter per sink (for fan-out)
}

func DefaultBackpressureConfig() BackpressureConfig // Rate: 1000, Burst: 100, PerSink: false
```

**Validation rules:**
- `Rate > 0`
- `Burst > 0`

When `PerSink` is true and the pipeline has multiple sinks, each sink gets its own independent token bucket.

### `stream.BatchConfig`

```go
type BatchConfig struct {
    Size     int           // messages per batch (0 = disabled)
    Interval time.Duration // max wait before flushing batch (0 = disabled)
}
```

When `Size > 0`, messages are buffered until the batch reaches the target size. When `Interval > 0`, the batch is also flushed periodically even if not full. Both can be combined.

### `stream.WindowConfig`

```go
type WindowType int

const (
    WindowTypeTumbling WindowType = iota
    WindowTypeSliding
    WindowTypeSession
    WindowTypeCount
)

type WindowConfig struct {
    Type       WindowType
    Size       time.Duration // window length (tumbling, sliding)
    Slide      time.Duration // slide interval (sliding only)
    Count      int           // message count threshold (count only)
    SessionGap time.Duration // inactivity gap (session only)
    Offset     time.Duration // alignment offset
}

func DefaultWindowConfig() WindowConfig // tumbling, 10s Size
```

**Window types:**

| Type      | Trigger condition                                          |
|-----------|------------------------------------------------------------|
| Tumbling  | Fixed non-overlapping intervals of `Size` duration.        |
| Sliding   | Overlapping windows of `Size` duration, advancing by `Slide`. |
| Session   | Closes window when gap between messages exceeds `SessionGap`. |
| Count     | Emits after `Count` messages accumulated.                  |

### `stream.State` (Checkpointing)

```go
type State interface {
    Get(ctx context.Context, key []byte) ([]byte, error)
    Set(ctx context.Context, key []byte, value []byte) error
    Delete(ctx context.Context, key []byte) error
    Snapshot(ctx context.Context) ([]byte, error)
    Restore(ctx context.Context, data []byte) error
}
```

Built-in implementation: `InMemoryState` — concurrent-safe, supports JSON snapshot/restore for checkpointing.

### `stream.StatefulTransform`

```go
type StatefulTransform[T any] struct {
    State State
    Fn    func(ctx context.Context, msg Message[T], state State) (Message[T], error)
}
```

Use with `WithState()` on the pipeline to maintain state across messages (e.g., aggregations, deduplication).

### `stream.FlatMapFunc`

```go
type FlatMapFunc[T any] func(ctx context.Context, msg Message[T]) ([]Message[T], error)
```

A flat-map transform produces zero, one, or many output messages from a single input. Use via `pipeline.FlatMap(fn)`.

### `stream.ErrorHandler`

```go
type ErrorHandler interface {
    OnTransformError(ctx context.Context, msg Message[[]byte], err error)
    OnWriteError(ctx context.Context, msg Message[[]byte], err error)
    OnReadError(ctx context.Context, err error)
}
```

Use `ErrorHandlerFunc` for inline handlers. Attach via `pipeline.WithErrorHandler(h)`.

### `stream.Metrics`

```go
type Metrics interface {
    MessageRead(topic string)
    MessageWritten(topic string)
    MessageFailed(topic string, err error)
    TransformError(topic string)
    MessageRetried(topic string, attempt int)
    BackpressureWait(topic string, d time.Duration)
}
```

Built-in implementations: `NoopMetrics` (default), `MetricsCounter` (atomic counters). Attach via `pipeline.WithMetrics(m)`.

### Adapters

| Adapter        | Description                                        |
|----------------|----------------------------------------------------|
| `MergeSource`  | Fan-in: merges multiple sources into one           |
| `MapSource`    | Transforms source output type                      |
| `MapSink`      | Transforms sink input type                         |
| `AsStream`     | Converts `Source[T]` to `<-chan Message[T]`        |
| `BatchRead`    | Reads N messages from a source at once             |
| `ReadAll`      | Reads all messages until error/context done        |

---

## Provider Reference

### Kafka

| Constructor                   | Config struct           | Key config fields                    |
|-------------------------------|-------------------------|--------------------------------------|
| `source.NewKafkaSource`       | `KafkaSourceConfig`     | `Brokers`, `Topic`, `GroupID`        |
| `source.NewKafkaSourceWithOptions` | functional options | `WithBrokers`, `WithTopic`, `WithGroupID`, `WithKafkaTLS`, `WithSASLPlain`, `WithKafkaReconnect`, `WithManualCommit`, `WithCooperativeRebalance`, `WithLagMonitor` |
| `sink.NewKafkaSink`           | `KafkaSinkConfig`       | `Brokers`, `Topic`                   |

**Source details:**
- Uses `kafka.Reader` with consumer groups for offset management.
- `GroupID` is required (enables checkpointing and rebalancing).
- `WatchPartitionChanges: true` for dynamic partition discovery.
- **Cooperative rebalancing**: enable via `CooperativeRebalance: true` for sticky partition assignment using `RoundRobinGroupBalancer`. Optionally set `LagMonitorInterval` to monitor consumer lag.
- TLS: configure via `WithKafkaTLS(tlsConfig)`.
- SASL/PLAIN auth: configure via `WithSASLPlain(username, password)`.
- Connection retry: `WithKafkaReconnect(delay, maxAttempts)`.
- Circuit breaker integrated internally (5 failures → 30s open).
- `HeartbeatInterval`, `SessionTimeout`, `RebalanceTimeout` configurable.
- Manual offset commit via `WithManualCommit(true)`.

**Sink details:**
- Uses `kafka.Writer` with `LeastBytes` balancer.
- TLS, SASL, and batch settings configurable.
- `BatchSize` (default 100) and `BatchTimeout` (default 1s) for batching.
- `WriteTimeout` (default 30s) and `RequiredAcks` (default -1/all).
- Circuit breaker integrated internally.

### AWS Kinesis

| Constructor                   | Config struct           | Key config fields                    |
|-------------------------------|-------------------------|--------------------------------------|
| `source.NewKinesisSource`     | `KinesisSourceConfig`   | `StreamName`, `Region`               |
| `source.NewKinesisSourceWithOptions` | functional options | `WithStreamName`, `WithRegion`, `WithShardIteratorType` |
| `sink.NewKinesisSink`         | `KinesisSinkConfig`     | `StreamName`, `Region`               |

**Source details:**
- Loads AWS config via `config.LoadDefaultConfig`.
- **Multi-shard**: reads from ALL shards in parallel goroutines into a shared channel (no longer limited to shard 0).
- Configurable `ShardIteratorType` (default `LATEST`).
- `MaxRecordsPerCall` (default 100) and `PollInterval` (default 1s).
- Automatic shard iterator refresh on expiry.
- Circuit breaker integrated internally.

**Sink details:**
- Uses `PutRecord` to write to the stream.
- Partition key defaults to `"default"` if `msg.Key` is empty.
- Automatic throttling detection: `ProvisionedThroughputExceededException` is wrapped as `RetryableError`.
- Circuit breaker integrated internally.

**IAM policy example:**
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "kinesis:ListShards",
                "kinesis:GetRecords",
                "kinesis:GetShardIterator",
                "kinesis:PutRecord"
            ],
            "Resource": "arn:aws:kinesis:us-east-1:*:stream/my-stream"
        }
    ]
}
```

### RabbitMQ

| Constructor                      | Config struct                | Key config fields                         |
|----------------------------------|------------------------------|-------------------------------------------|
| `source.NewRabbitMQSource`       | `RabbitMQSourceConfig`       | `URL`, `Queue`, `Exchange`, `RoutingKey`  |
| `source.NewRabbitMQSourceWithOptions` | functional options      | `WithURL`, `WithQueue`, `WithExchange`, `WithRoutingKey`, `WithRabbitMQTLS` |
| `sink.NewRabbitMQSink`           | `RabbitMQSinkConfig`         | `URL`, `Exchange`, `RoutingKey`           |

**Source details:**
- Connects, declares exchange (if set), declares queue, binds queue to exchange, starts consuming.
- Manual acknowledgment mode (auto-ack is off).
- TLS: configure via `WithRabbitMQTLS(tlsConfig)` or `TLS` field.
- **Auto-reconnect**: `NotifyClose` channels trigger automatic reconnection with configurable `ReconnectDelay` and `MaxReconnects`.
- `PrefetchCount` (default 100) for QoS.
- `Heartbeat` (default 10s).
- Circuit breaker integrated internally.

**Sink details:**
- Connects, declares exchange (if set), publishes messages.
- TLS config via `TLS` field.
- Auto-reconnect on connection loss.
- `DeliveryMode` configurable (2 = persistent).
- On write failure, automatically ensures channel is re-established.
- Circuit breaker integrated internally.

**Docker setup:**
```bash
docker run -d --name rabbitmq \
  -p 5672:5672 -p 15672:15672 \
  rabbitmq:4-management
```

### Google Cloud Pub/Sub

| Constructor                    | Config struct              | Key config fields                       |
|--------------------------------|----------------------------|-----------------------------------------|
| `source.NewPubSubSource`       | `PubSubSourceConfig`       | `ProjectID`, `SubscriptionID`           |
| `source.NewPubSubSourceWithOptions` | functional options    | `WithProjectID`, `WithSubscriptionID`, `WithPubSubNumGoroutines` |
| `sink.NewPubSubSink`           | `PubSubSinkConfig`         | `ProjectID`, `TopicID`                  |

**Source details:**
- Creates a `pubsub.Client` and subscribes via `sub.Receive`.
- Bridges Pub/Sub's callback API into `Read()` using a buffered channel.
- `MaxOutstandingMessages` (default 100), `MaxOutstandingBytes` (default 1GB), `NumGoroutines` (default 10).
- **Auto-reconnect**: `Receive` errors trigger reconnection with configurable `ReconnectDelay` and `MaxReconnects`.
- Circuit breaker integrated internally.

**Sink details:**
- Creates a `pubsub.Client` and gets a handle to the topic.
- Publishes with `topic.Publish()`; waits for `result.Get(ctx)`.
- Configurable `NumGoroutines` (default 10) and `MaxOutstandingBytes`.
- Circuit breaker integrated internally.

**IAM roles (minimal):**

| Resource       | Required roles                                      |
|----------------|-----------------------------------------------------|
| Source         | `roles/pubsub.subscriber` (or `roles/pubsub.viewer` + `roles/pubsub.subscriber`) |
| Sink           | `roles/pubsub.publisher`                            |

**Docker emulator setup:**
```bash
docker run -d --name pubsub \
  -p 8085:8085 \
  gcr.io/google.com/cloudsdktool/cloud-sdk:latest \
  gcloud beta emulators pubsub start --host-port=0.0.0.0:8085

export PUBSUB_EMULATOR_HOST=localhost:8085
```

**Create topic and subscription via gcloud:**
```bash
gcloud pubsub topics create output-topic
gcloud pubsub subscriptions create input-subscription --topic input-topic
```

Or, using the emulator REST API:
```bash
# Create topic
curl -X PUT http://localhost:8085/v1/projects/my-project/topics/input-topic
curl -X PUT http://localhost:8085/v1/projects/my-project/topics/output-topic

# Create subscription
curl -X PUT http://localhost:8085/v1/projects/my-project/subscriptions/input-subscription \
  -H "Content-Type: application/json" \
  -d '{"topic": "projects/my-project/topics/input-topic"}'
```

---

## Testing Guide

### Unit tests (no external dependencies)

```bash
go test ./stream/... -v -count=1
```

The test suite covers:

| Test category             | What it verifies                                      |
|---------------------------|-------------------------------------------------------|
| **Message**               | Construction, Ack/Nack with/without callbacks, context cancellation, type safety with int/struct/bytes. |
| **RetryConfig**           | Validation (negative values, zero, base > max), defaults. |
| **DoWithRetry**           | Immediate success, eventual success after N failures, permanent failure, context cancellation, zero retries, non-retryable skips retry, retryable gets retries. |
| **BackpressureConfig**    | Validation (zero rate/burst, negative rate), defaults. |
| **Backpressure**          | Allow consumption up to burst, deny after burst exhausted, Wait block until timeout, context cancellation during Wait, idempotent Close. |
| **WindowConfig**          | Validation for all 4 types (tumbling, sliding, session, count), defaults, WindowType.String(). |
| **Pipeline**              | Basic, single/chained transforms, transform error skips, source/sink open errors, backpressure, context cancellation, retry on source/sink errors, 100-message concurrent throughput, cleanup, empty transform chain, batch (size + fan-out), DLQ (success + failure), metrics (counter + transform errors + DLQ failures), flush on shutdown, nil metrics, context cancel during batch, per-sink backpressure, Ack/Nack callbacks, FlatMap, FlatMap with filter, count/session/sliding/tumbling windows, merge source, stateful transform. |
| **State**                 | Snapshot/Restore with data and empty state. |
| **Mocks**                 | Read error returns error, Written() returns snapshot (concurrent-safe). |

### Integration tests

Integration tests require running broker instances. They are tagged with `//go:build integration` and are excluded from `go test ./...` by default.

```bash
# Start required brokers via Docker
docker run -d --name kafka -p 9092:9092 bitnami/kafka:latest
docker run -d --name rabbitmq -p 5672:5672 rabbitmq:4-management

# Run integration tests explicitly
go test -tags integration ./stream/... -v -count=1 -run Integration -timeout=120s

# Run in short mode to skip integration tests
go test -short ./...
```

Available integration tests:

| Test                                    | What it verifies                                   |
|-----------------------------------------|----------------------------------------------------|
| `TestKafkaPipeline_Integration`         | Kafka end-to-end with retry config                 |
| `TestKafkaPipeline_WithTLS_Integration` | Kafka TLS connection (requires real certs)         |
| `TestKafkaPipeline_WithCircuitBreaker_Integration` | Kafka with circuit breaker state transitions |
| `TestRabbitMQPipeline_Integration`      | RabbitMQ end-to-end with reconnect                 |
| `TestPipeline_WithWindowTumbling_Integration` | Tumbling window with Kafka                  |
| `TestPipeline_WithSerialization_Integration` | JSON Serde round-trip via Kafka              |
| `TestPipeline_WithDLQ_Integration`      | Dead-letter queue with write failures              |
| `TestPipeline_FanOut_Integration`       | Multi-sink fan-out with per-sink backpressure      |
| `TestPipeline_BenchmarkRead`            | High-throughput benchmark (10 workers, batch 1000) |
| `TestSerde_JSONRoundTrip_Integration`   | JSON Serde encode/decode correctness               |
| `TestCircuitBreaker_Integration`        | Circuit breaker state transitions                  |

### Benchmark tests

For throughput benchmarking:

```bash
go test -tags integration -bench=. -benchtime=30s ./stream/
```

The `TestPipeline_BenchmarkRead` integration test can be used as a throughput benchmark with 10 concurrent workers and batch sizes of 1000.

### Mocking in your own tests

```go
import "github.com/kennyrobert88/go-stream-processing/internal/mocks"

// Mock source that returns predefined messages
src := mocks.NewMockSource([]stream.Message[string]{
    stream.NewMessage("hello"),
    stream.NewMessage("world"),
})

// Mock sink that captures written messages
snk := mocks.NewMockSink[string]()

// Inject errors
src.ReadErr = errors.New("simulated failure")
snk.WriteErr = errors.New("simulated failure")

// After pipeline run:
written := snk.Written() // concurrent-safe snapshot
```

---

## Examples

Run the full example:

```bash
go run ./examples/pipeline/
```

This starts goroutines running pipelines for Kafka, Kinesis, RabbitMQ, and a FlatMap example. All listen for SIGINT/SIGTERM for graceful shutdown. Note: each broker requires its own running infrastructure.

---

## FAQ

**Q: Why `Message[[]byte]` for all built-in sources/sinks?**

The built-in sources read raw bytes from the wire. To work with typed data, add a transform that deserializes (e.g., `json.Unmarshal`). This keeps the library decoupled from serialization formats while still being generic.

**Q: How do I use a different type than `[]byte`?**

Implement `Source[T]` and `Sink[T]` for your type `T`. The `Message[T]` struct works with any Go type.

**Q: What happens if the source continuously returns errors?**

Each read (and write) is wrapped in `DoWithRetry` with exponential backoff. After `MaxRetries` consecutive failures, the error is logged and the pipeline continues to the next read. Context cancellation is the only way to stop the loop.

**Q: Can I mix brokers? (e.g., Kafka source → Kinesis sink)**

Yes. The source and sink are independent interfaces. You can read from Kafka and write to Kinesis:

```go
pl := stream.NewPipeline(
    source.NewKafkaSource(kafkaCfg),
    sink.NewKinesisSink(kinesisCfg),
)
```

**Q: Does the source auto-commit offsets?**

For Kafka, offsets are committed by the `kafka.Reader` based on its configured commit interval (default 1 second). The `Ack()` method is available for future manual commit strategies but currently the library does not call it on external systems. For RabbitMQ, manual ack mode is used so messages are not auto-acknowledged.

**Q: What Go versions are supported?**

Go 1.21+ through the latest release. The library uses generics (introduced in Go 1.18) but requires Go 1.21+ due to upstream dependency requirements (`aws-sdk-go-v2`).

**Q: Is this production ready?**

Yes for moderate-throughput use cases. The library includes circuit breakers, health checks, OpenTelemetry metrics, structured logging, TLS/SSL, automatic reconnection, multi-shard Kinesis reads, and comprehensive unit tests. For higher throughput requirements, benchmark with your workload using `WithWorkers(n)` and `WithBatchConfig()`.

**Q: How do I configure TLS for Kafka?**

```go
tlsCfg := stream.TLSConfig{
    Enabled:  true,
    CAFile:   "/etc/certs/ca.pem",
    CertFile: "/etc/certs/client.pem",
    KeyFile:  "/etc/certs/client-key.pem",
}

src := source.NewKafkaSourceWithOptions(
    source.WithBrokers("kafka.example.com:9093"),
    source.WithTopic("my-topic"),
    source.WithGroupID("my-group"),
    source.WithKafkaTLS(tlsCfg),
    source.WithSASLPlain("user", "password"),
)
```

**Q: Does Kinesis read from all shards?**

Yes. The rewritten Kinesis source spawns a goroutine per shard and merges results into a shared channel. It also handles shard iterator expiry by requesting a new iterator automatically.

**Q: What happens when a broker connection drops?**

All providers implement automatic reconnection:
1. Circuit breaker detects failures and opens after `MaxFailures` (default 5).
2. A reconnect goroutine attempts reconnection with `ReconnectDelay` between attempts, up to `MaxReconnects`.
3. On successful reconnect, the circuit breaker resets to closed and normal operation resumes.

**Q: How do I add OpenTelemetry?**

```go
otelMetrics, err := stream.NewOTelMetrics("my-app", "my-app")
pipeline.WithMetrics(otelMetrics)
```

This captures read/write/failure counters, retry counts, backpressure wait times, and read/write latencies as OpenTelemetry instruments.

**Q: How do I add structured logging?**

```go
logger := stream.NewSlogLogger(slog.LevelInfo)
pipeline.WithLogger(logger)
// Or per-provider:
kafkaSource.WithLogger(logger)
```

Log output is JSON by default. Wraps Go's `log/slog`.

---

## License

MIT

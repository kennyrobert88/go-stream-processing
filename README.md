# go-stream-processing

> A unified, type-safe stream-processing connector library for Go. Abstract over Kafka, AWS Kinesis, Google Cloud Pub/Sub, and RabbitMQ with a single `Source[T]` / `Sink[T]` interface — swap brokers by changing one constructor. Inspired by Apache Beam and Apache Flink.

[![Go Version](https://img.shields.io/badge/Go-1.24+-00ADD8?logo=go)](https://go.dev/dl/)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

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
- [API Reference](#api-reference)
  - [stream.Message](#streammessage)
  - [stream.Source](#streamsource)
  - [stream.Sink](#streamsink)
  - [stream.Pipeline](#streampipeline)
  - [stream.RetryConfig](#streamretryconfig)
  - [stream.BackpressureConfig](#streambackpressureconfig)
- [Provider Reference](#provider-reference)
  - [Kafka](#kafka)
  - [AWS Kinesis](#aws-kinesis)
  - [RabbitMQ](#rabbitmq)
  - [Google Cloud Pub/Sub](#google-cloud-pubsub)
- [Testing Guide](#testing-guide)
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
}
```

The `Pipeline[T]` wires a `Source → Transform(s) → Sink` with built-in retry (exponential backoff with jitter) and backpressure (token-bucket rate limiting).

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
Source.Read() ──► Transform[0] ──► ... ──► Transform[N] ──► Sink.Write()
                      │                                            │
                      │ on error                                   │ on success
                      ▼                                            ▼
                   msg.Nack()                                   msg.Ack()

  ── Any Read/Write error triggers retry (configurable) ──
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

Expected output (30 tests, all passing):

```
ok  	github.com/kennyrobert88/go-stream-processing/stream	6.817s
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

On shutdown:
1. The read loop sees the cancelled context and exits.
2. Source is closed (Kafka consumer leaves the group, RabbitMQ channel closes).
3. Sink is closed (Kafka writer flushes, RabbitMQ connection closes).
4. Backpressure token refill goroutine is stopped.

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
}
```

Message is the core data unit flowing through a pipeline. It carries the payload (`Value` of any type `T`), routing metadata (`Key`, `Topic`, `Partition`, `Offset`), and application headers.

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

### `stream.Sink`

```go
type Sink[T any] interface {
    Open(ctx context.Context) error
    Close(ctx context.Context) error
    Write(ctx context.Context, msg Message[T]) error
}
```

| Method    | Description |
|-----------|-------------|
| `Open`    | Initializes the connection to the broker. Called once when `Pipeline.Run()` starts. |
| `Close`   | Tears down the connection. Called once when the pipeline exits. |
| `Write`   | Publishes the message to the broker. Should honour context cancellation for timeout support. |

**Implementations:**

| Concrete type              | Package            | Consumes         |
|----------------------------|--------------------|------------------|
| `KafkaSink`                | `sink`             | `Message[[]byte]` |
| `KinesisSink`              | `sink`             | `Message[[]byte]` |
| `RabbitMQSink`             | `sink`             | `Message[[]byte]` |

### `stream.Pipeline`

```go
type Pipeline[T any] struct { ... }

func NewPipeline[T any](source Source[T], sink Sink[T]) *Pipeline[T]
```

Connects a `Source[T]` to a `Sink[T]` with optional transforms, retry, and backpressure.

**Configuration methods (all return `*Pipeline[T]` for chaining):**

| Method                       | Description |
|------------------------------|-------------|
| `WithRetryConfig(cfg)`       | Set retry parameters for source reads and sink writes. |
| `WithBackpressure(cfg)`      | Enable token-bucket rate limiting. |
| `AddTransform(fn)`           | Append a processing function to the transform chain. |

**Execution:**

```go
func (p *Pipeline[T]) Run(ctx context.Context) error
```

Run starts the pipeline loop:
1. Opens the source and sink.
2. Repeatedly: Read → [backpressure.Wait] → [retry Read] → [transforms] → [retry Write] → Ack.
3. On context cancellation or unrecoverable error: closes source and sink, then returns.

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
    Rate  int  // tokens per second
    Burst int  // maximum accumulated tokens
}

func DefaultBackpressureConfig() BackpressureConfig // Rate: 1000, Burst: 100
```

**Validation rules:**
- `Rate > 0`
- `Burst > 0`

---

## Provider Reference

### Kafka

| Constructor              | Config struct         | Key config fields              |
|--------------------------|-----------------------|--------------------------------|
| `source.NewKafkaSource`  | `KafkaSourceConfig`   | `Brokers`, `Topic`, `GroupID`  |
| `sink.NewKafkaSink`      | `KafkaSinkConfig`     | `Brokers`, `Topic`             |

**Source details:**
- Uses `kafka.Reader` with consumer groups for offset management.
- `GroupID` is required for source (enables checkpointing and rebalancing).
- `MinBytes: 10KB`, `MaxBytes: 10MB` defaults.

**Sink details:**
- Uses `kafka.Writer` with `LeastBytes` balancer for partition distribution.
- Produces messages with key, value, and headers.
- Topic auto-creation depends on broker config.

### AWS Kinesis

| Constructor                | Config struct           | Key config fields            |
|----------------------------|-------------------------|------------------------------|
| `source.NewKinesisSource`  | `KinesisSourceConfig`   | `StreamName`, `Region`       |
| `sink.NewKinesisSink`      | `KinesisSinkConfig`     | `StreamName`, `Region`       |

**Source details:**
- Loads AWS config via `config.LoadDefaultConfig` (uses `~/.aws/credentials`, env vars, or IAM role).
- Lists shards and reads from the first shard. For production, use a shard iterator strategy appropriate for your use case (the current implementation reads from the tip).
- Requires `ListShards` and `GetRecords` IAM permissions.

**Sink details:**
- Uses `PutRecord` to write to the stream.
- Partition key defaults to `"default"` if `msg.Key` is empty.
- Requires `PutRecord` IAM permission.

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

| Constructor                  | Config struct              | Key config fields                     |
|------------------------------|----------------------------|---------------------------------------|
| `source.NewRabbitMQSource`   | `RabbitMQSourceConfig`     | `URL`, `Queue`, `Exchange`, `RoutingKey` |
| `sink.NewRabbitMQSink`       | `RabbitMQSinkConfig`       | `URL`, `Exchange`, `RoutingKey`       |

**Source details:**
- Connects, declares exchange (if set), declares queue, binds queue to exchange, starts consuming.
- Manual acknowledgment mode (auto-ack is off).
- If `Exchange` is empty, only queue operations are performed (no exchange binding).
- Headers are typed `amqp.Table`; only string values are carried over.

**Sink details:**
- Connects, declares exchange (if set), publishes messages.
- Content type set to `application/octet-stream`.

**Docker setup:**
```bash
docker run -d --name rabbitmq \
  -p 5672:5672 -p 15672:15672 \
  rabbitmq:4-management
```

### Google Cloud Pub/Sub

| Constructor                  | Config struct              | Key config fields                     |
|------------------------------|----------------------------|---------------------------------------|
| `source.NewPubSubSource`     | `PubSubSourceConfig`       | `ProjectID`, `SubscriptionID`         |
| `sink.NewPubSubSink`         | `PubSubSinkConfig`         | `ProjectID`, `TopicID`                |

**Source details:**
- Creates a `pubsub.Client` and subscribes to the given subscription via `sub.Receive`.
- Pub/Sub uses a callback-driven API; the source bridges this into the `Read()` interface using an internal buffered channel (capacity 100).
- A background goroutine runs `sub.Receive` and pushes messages into the channel.
- `Message.Ack()` and `Message.Nack()` are wired to the underlying Pub/Sub message's `Ack()` and `Nack()` methods, so acknowledgements propagate correctly back to the subscription.
- The goroutine is lifecycle-managed: `Close()` cancels the receive context via `context.CancelFunc`, waits for the goroutine to exit, then closes the client.
- If `Receive` encounters a non-context error, it is logged to stdout and the source enters a terminal error state.

**Sink details:**
- Creates a `pubsub.Client` and gets a handle to the topic.
- Publishes with `topic.Publish()`; waits for the publish result with `result.Get(ctx)`.
- Message headers are mapped to Pub/Sub `Attributes` (string-keyed string values).
- The topic is assumed to exist; no `CreateTopic` call is made.

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
| **DoWithRetry**           | Immediate success, eventual success after N failures, permanent failure, context cancellation, zero retries. |
| **BackpressureConfig**    | Validation (zero rate/burst, negative rate), defaults. |
| **Backpressure**          | Allow consumption up to burst, deny after burst exhausted, Wait block until timeout, context cancellation during Wait, idempotent Close. |
| **Pipeline**              | Basic (3 messages through), single transform, chained transforms, transform error skips message, source/sink open errors, backpressure integration, context cancellation propagation, retry on source/sink errors, 100-message concurrent throughput, cleanup on write error, empty transform chain. |
| **Mocks**                 | Read error returns error, Written() returns snapshot (concurrent-safe). |

### Integration tests (require running broker)

You can write integration tests using `testcontainers-go` or manually managed Docker containers. Example pattern:

```go
func TestKafkaPipelineIntegration(t *testing.T) {
    if testing.Short() {
        t.Skip("skipping integration test")
    }

    src := source.NewKafkaSource(source.KafkaSourceConfig{
        Brokers: []string{"localhost:9092"},
        Topic:   "test-input",
        GroupID: "test-group",
    })
    snk := sink.NewKafkaSink(sink.KafkaSinkConfig{
        Brokers: []string{"localhost:9092"},
        Topic:   "test-output",
    })

    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()

    pipeline := stream.NewPipeline(src, snk)
    log.Fatal(pipeline.Run(ctx))
}
```

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

This starts three goroutines, each running a pipeline for Kafka, Kinesis, and RabbitMQ respectively. All listen for SIGINT/SIGTERM for graceful shutdown. Note: each broker requires its own running infrastructure.

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

---

## License

MIT

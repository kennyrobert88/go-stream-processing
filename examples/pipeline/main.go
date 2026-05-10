package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"log/slog"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/kennyrobert88/go-stream-processing/stream"
	"github.com/kennyrobert88/go-stream-processing/stream/sink"
	"github.com/kennyrobert88/go-stream-processing/stream/source"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	logger := stream.NewSlogLogger(slog.LevelInfo)

	// --- Kafka Pipeline with TLS and SASL options ---
	kafkaSrc := source.NewKafkaSourceWithOptions(
		source.WithBrokers("localhost:9092"),
		source.WithTopic("input-topic"),
		source.WithGroupID("my-group"),
		source.WithKafkaReconnect(2*time.Second, 10),
	)
	kafkaSnk := sink.NewKafkaSink(sink.KafkaSinkConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "output-topic",
	})

	kafkaPipeline := stream.NewPipeline(kafkaSrc, kafkaSnk).
		WithRetryConfig(stream.DefaultRetryConfig()).
		WithBackpressure(stream.DefaultBackpressureConfig()).
		WithLogger(logger).
		AddTransform(func(_ context.Context, msg stream.Message[[]byte]) (stream.Message[[]byte], error) {
			msg.Value = []byte(strings.ToUpper(string(msg.Value)))
			return msg, nil
		})

	go func() {
		if err := kafkaPipeline.Run(ctx); err != nil {
			log.Printf("kafka pipeline stopped: %v", err)
		}
	}()

	// --- Kinesis Pipeline (multi-shard) ---
	kinesisSrc := source.NewKinesisSourceWithOptions(
		source.WithStreamName("my-stream"),
		source.WithRegion("us-east-1"),
	)
	kinesisSnk := sink.NewKinesisSink(sink.KinesisSinkConfig{
		StreamName: "my-stream-output",
		Region:     "us-east-1",
	})

	kinesisPipeline := stream.NewPipeline(kinesisSrc, kinesisSnk).
		WithRetryConfig(stream.RetryConfig{MaxRetries: 5, BaseDelay: 200, MaxDelay: 5000}).
		WithLogger(logger)

	go func() {
		if err := kinesisPipeline.Run(ctx); err != nil {
			log.Printf("kinesis pipeline stopped: %v", err)
		}
	}()

	// --- RabbitMQ Pipeline with reconnect ---
	rmqSrc := source.NewRabbitMQSource(source.RabbitMQSourceConfig{
		URL:            "amqp://guest:guest@localhost:5672/",
		Queue:          "input-queue",
		Exchange:       "input-exchange",
		RoutingKey:     "input-key",
		ReconnectDelay: time.Second,
		MaxReconnects:  10,
		PrefetchCount:  100,
	})
	rmqSnk := sink.NewRabbitMQSink(sink.RabbitMQSinkConfig{
		URL:        "amqp://guest:guest@localhost:5672/",
		Exchange:   "output-exchange",
		RoutingKey: "output-key",
		DeliveryMode: 2, // persistent
	})

	rmqPipeline := stream.NewPipeline(rmqSrc, rmqSnk).
		AddTransform(func(_ context.Context, msg stream.Message[[]byte]) (stream.Message[[]byte], error) {
			log.Printf("routing message: %s", string(msg.Value))
			return msg, nil
		}).
		WithLogger(logger)

	go func() {
		if err := rmqPipeline.Run(ctx); err != nil {
			log.Printf("rabbitmq pipeline stopped: %v", err)
		}
	}()

	// --- Pipeline with batching, windowing, and DLQ ---
	batchSrc := source.NewKafkaSource(source.KafkaSourceConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "batch-input",
		GroupID: "batch-group",
	})
	batchSnk := sink.NewKafkaSink(sink.KafkaSinkConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "batch-output",
	})
	dlqSnk := sink.NewKafkaSink(sink.KafkaSinkConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "batch-dlq",
	})

	batchPipeline := stream.NewPipeline(batchSrc, batchSnk).
		WithBatchConfig(stream.BatchConfig{Size: 100, Interval: 5 * time.Second}).
		WithDLQ(dlqSnk).
		WithWorkers(4).
		WithLogger(logger)

	go func() {
		if err := batchPipeline.Run(ctx); err != nil {
			log.Printf("batch pipeline stopped: %v", err)
		}
	}()

	// --- FlatMap pipeline (splitting messages) ---
	flatMapSrc := source.NewKafkaSource(source.KafkaSourceConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "multi-input",
		GroupID: "flatmap-group",
	})
	flatMapSnk := sink.NewKafkaSink(sink.KafkaSinkConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "multi-output",
	})

	flatMapPipeline := stream.NewPipeline(flatMapSrc, flatMapSnk).
		FlatMap(func(_ context.Context, msg stream.Message[[]byte]) ([]stream.Message[[]byte], error) {
			words := bytes.Split(msg.Value, []byte(" "))
			out := make([]stream.Message[[]byte], len(words))
			for i, w := range words {
				out[i] = stream.NewMessage(w)
				out[i].Key = msg.Key
			}
			return out, nil
		}).
		WithLogger(logger)

	go func() {
		if err := flatMapPipeline.Run(ctx); err != nil {
			log.Printf("flatmap pipeline stopped: %v", err)
		}
	}()

	fmt.Println("pipelines running. Press Ctrl+C to stop.")
	<-ctx.Done()
	fmt.Println("shutting down...")
}

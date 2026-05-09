package main

import (
	"context"
	"fmt"
	"log"
	"os/signal"
	"strings"
	"syscall"

	"github.com/kennyrobert88/go-stream-processing/stream"
	"github.com/kennyrobert88/go-stream-processing/stream/sink"
	"github.com/kennyrobert88/go-stream-processing/stream/source"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// --- Kafka Pipeline ---
	kafkaSrc := source.NewKafkaSource(source.KafkaSourceConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "input-topic",
		GroupID: "my-group",
	})
	kafkaSnk := sink.NewKafkaSink(sink.KafkaSinkConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "output-topic",
	})

	kafkaPipeline := stream.NewPipeline(kafkaSrc, kafkaSnk).
		WithRetryConfig(stream.DefaultRetryConfig()).
		WithBackpressure(stream.DefaultBackpressureConfig()).
		AddTransform(func(_ context.Context, msg stream.Message[[]byte]) (stream.Message[[]byte], error) {
			msg.Value = []byte(strings.ToUpper(string(msg.Value)))
			return msg, nil
		})

	go func() {
		if err := kafkaPipeline.Run(ctx); err != nil {
			log.Printf("kafka pipeline stopped: %v", err)
		}
	}()

	// --- Kinesis Pipeline ---
	kinesisSrc := source.NewKinesisSource(source.KinesisSourceConfig{
		StreamName: "my-stream",
		Region:     "us-east-1",
	})
	kinesisSnk := sink.NewKinesisSink(sink.KinesisSinkConfig{
		StreamName: "my-stream-output",
		Region:     "us-east-1",
	})

	kinesisPipeline := stream.NewPipeline(kinesisSrc, kinesisSnk).
		WithRetryConfig(stream.RetryConfig{MaxRetries: 5, BaseDelay: 200, MaxDelay: 5000})

	go func() {
		if err := kinesisPipeline.Run(ctx); err != nil {
			log.Printf("kinesis pipeline stopped: %v", err)
		}
	}()

	// --- RabbitMQ Pipeline ---
	rmqSrc := source.NewRabbitMQSource(source.RabbitMQSourceConfig{
		URL:        "amqp://guest:guest@localhost:5672/",
		Queue:      "input-queue",
		Exchange:   "input-exchange",
		RoutingKey: "input-key",
	})
	rmqSnk := sink.NewRabbitMQSink(sink.RabbitMQSinkConfig{
		URL:        "amqp://guest:guest@localhost:5672/",
		Exchange:   "output-exchange",
		RoutingKey: "output-key",
	})

	rmqPipeline := stream.NewPipeline(rmqSrc, rmqSnk).
		AddTransform(func(_ context.Context, msg stream.Message[[]byte]) (stream.Message[[]byte], error) {
			log.Printf("routing message: %s", string(msg.Value))
			return msg, nil
		})

	go func() {
		if err := rmqPipeline.Run(ctx); err != nil {
			log.Printf("rabbitmq pipeline stopped: %v", err)
		}
	}()

	<-ctx.Done()
	fmt.Println("shutting down...")
}

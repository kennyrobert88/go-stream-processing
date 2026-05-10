package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kennyrobert88/go-stream-processing/stream"
	"github.com/kennyrobert88/go-stream-processing/stream/sink"
	"github.com/kennyrobert88/go-stream-processing/stream/source"
)

func main() {
	configPath := flag.String("config", "", "Path to pipeline config file (JSON)")
	pipelineName := flag.String("pipeline", "", "Pipeline name to run")
	flag.Parse()

	if *configPath == "" {
		fmt.Fprintf(os.Stderr, "Usage: stream --config <config.json> [--pipeline <name>]\n")
		os.Exit(1)
	}

	cfg, err := stream.LoadPipelineConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	logger := stream.NewSlogLogger(slog.LevelInfo)

	var p *stream.Pipeline[[]byte]
	switch cfg.Source.Type {
	case "kafka":
		p = buildKafkaPipeline(cfg, logger)
	case "kinesis":
		p = buildKinesisPipeline(cfg, logger)
	case "rabbitmq":
		p = buildRabbitMQPipeline(cfg, logger)
	case "pubsub":
		p = buildPubSubPipeline(cfg, logger)
	default:
		log.Fatalf("Unknown source type: %s", cfg.Source.Type)
	}

	if cfg.Retry != nil {
		p.WithRetryConfig(*cfg.Retry)
	}
	if cfg.Backpressure != nil {
		p.WithBackpressure(*cfg.Backpressure)
	}
	if cfg.Batch != nil {
		p.WithBatchConfig(*cfg.Batch)
	}
	if cfg.Window != nil {
		p.WithWindow(*cfg.Window)
	}
	if cfg.Workers > 0 {
		p.WithWorkers(cfg.Workers)
	}
	p.WithLogger(logger)

	probe := stream.NewHealthProbe(
		func(ctx context.Context) stream.HealthReport {
			return stream.HealthReport{Status: stream.StatusHealthy, Component: "liveness", Time: time.Now()}
		},
		func(ctx context.Context) stream.HealthReport {
			return stream.HealthReport{Status: stream.StatusHealthy, Component: "readiness", Time: time.Now()}
		},
	)
	p.WithHealth(probe)

	healthServer := stream.NewHealthHTTPServer(":8080", probe)
	if err := healthServer.Start(); err != nil {
		log.Printf("Warning: health server: %v", err)
	}

	if cfg.Name != "" || *pipelineName != "" {
		log.Printf("Starting pipeline: %s", cfg.Name)
	}

	if err := p.Run(ctx); err != nil {
		log.Printf("Pipeline stopped: %v", err)
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	healthServer.Stop(shutdownCtx)
}

func buildKafkaPipeline(cfg *stream.PipelineConfig, logger stream.Logger) *stream.Pipeline[[]byte] {
	k := cfg.Source.Kafka
	src := source.NewKafkaSource(source.KafkaSourceConfig{
		Brokers: k.Brokers,
		Topic:   k.Topic,
		GroupID: k.GroupID,
	})

	if cfg.TLS != nil {
		src = source.NewKafkaSourceWithOptions(
			source.WithBrokers(k.Brokers...),
			source.WithTopic(k.Topic),
			source.WithGroupID(k.GroupID),
			source.WithKafkaTLS(*cfg.TLS),
		)
	}
	src.WithLogger(logger)

	var snks []stream.Sink[[]byte]
	for _, sc := range cfg.Sinks {
		switch sc.Type {
		case "kafka":
			sk := sc.Kafka
			snk := sink.NewKafkaSink(sink.KafkaSinkConfig{
				Brokers: sk.Brokers,
				Topic:   sk.Topic,
			})
			snk.WithLogger(logger)
			snks = append(snks, snk)
		default:
			log.Printf("Unknown sink type: %s", sc.Type)
		}
	}

	if len(snks) == 0 {
		log.Fatal("no sinks configured")
	}

	p := stream.NewPipeline(src, snks[0])
	for _, s := range snks[1:] {
		p.AddSink(s)
	}
	return p
}

func buildKinesisPipeline(cfg *stream.PipelineConfig, logger stream.Logger) *stream.Pipeline[[]byte] {
	k := cfg.Source.Kinesis
	src := source.NewKinesisSourceWithOptions(
		source.WithStreamName(k.StreamName),
		source.WithRegion(k.Region),
	)
	src.WithLogger(logger)

	snk := sink.NewKinesisSink(sink.KinesisSinkConfig{
		StreamName: k.StreamName,
		Region:     k.Region,
	})
	snk.WithLogger(logger)

	return stream.NewPipeline(src, snk)
}

func buildRabbitMQPipeline(cfg *stream.PipelineConfig, logger stream.Logger) *stream.Pipeline[[]byte] {
	r := cfg.Source.RabbitMQ
	src := source.NewRabbitMQSource(source.RabbitMQSourceConfig{
		URL:        r.URL,
		Queue:      r.Queue,
		Exchange:   r.Exchange,
		RoutingKey: r.RoutingKey,
	})
	src.WithLogger(logger)

	snk := sink.NewRabbitMQSink(sink.RabbitMQSinkConfig{
		URL:        r.URL,
		Exchange:   r.Exchange,
		RoutingKey: r.RoutingKey,
	})
	snk.WithLogger(logger)

	return stream.NewPipeline(src, snk)
}

func buildPubSubPipeline(cfg *stream.PipelineConfig, logger stream.Logger) *stream.Pipeline[[]byte] {
	srcCfg := cfg.Source.PubSub
	src := source.NewPubSubSource(source.PubSubSourceConfig{
		ProjectID:      srcCfg.ProjectID,
		SubscriptionID: srcCfg.SubscriptionID,
	})
	src.WithLogger(logger)

	snkCfg := cfg.Sinks[0].PubSub
	snk := sink.NewPubSubSink(sink.PubSubSinkConfig{
		ProjectID: snkCfg.ProjectID,
		TopicID:   snkCfg.TopicID,
	})
	snk.WithLogger(logger)

	return stream.NewPipeline(src, snk)
}

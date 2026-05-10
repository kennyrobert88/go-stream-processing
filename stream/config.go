package stream

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

type PipelineConfig struct {
	Name        string          `json:"name,omitempty" yaml:"name,omitempty"`
	Source      SourceConfig    `json:"source" yaml:"source"`
	Sinks       []SinkConfig    `json:"sinks" yaml:"sinks"`
	Transforms  []TransformConfig `json:"transforms,omitempty" yaml:"transforms,omitempty"`
	Retry       *RetryConfig    `json:"retry,omitempty" yaml:"retry,omitempty"`
	Backpressure *BackpressureConfig `json:"backpressure,omitempty" yaml:"backpressure,omitempty"`
	Batch       *BatchConfig    `json:"batch,omitempty" yaml:"batch,omitempty"`
	Window      *WindowConfig   `json:"window,omitempty" yaml:"window,omitempty"`
	Workers     int             `json:"workers,omitempty" yaml:"workers,omitempty"`
	TLS         *TLSConfig      `json:"tls,omitempty" yaml:"tls,omitempty"`
	Logger      *LoggerConfig   `json:"logger,omitempty" yaml:"logger,omitempty"`
	IdleTimeout Duration        `json:"idle_timeout,omitempty" yaml:"idle_timeout,omitempty"`
}

type SourceConfig struct {
	Type           string          `json:"type" yaml:"type"`
	Kafka          *KafkaSourceCfg  `json:"kafka,omitempty" yaml:"kafka,omitempty"`
	Kinesis        *KinesisSourceCfg `json:"kinesis,omitempty" yaml:"kinesis,omitempty"`
	RabbitMQ       *RabbitMQSourceCfg `json:"rabbitmq,omitempty" yaml:"rabbitmq,omitempty"`
	PubSub         *PubSubSourceCfg   `json:"pubsub,omitempty" yaml:"pubsub,omitempty"`
}

type SinkConfig struct {
	Type     string        `json:"type" yaml:"type"`
	Kafka    *KafkaSinkCfg  `json:"kafka,omitempty" yaml:"kafka,omitempty"`
	Kinesis  *KinesisSinkCfg `json:"kinesis,omitempty" yaml:"kinesis,omitempty"`
	RabbitMQ *RabbitMQSinkCfg `json:"rabbitmq,omitempty" yaml:"rabbitmq,omitempty"`
	PubSub   *PubSubSinkCfg   `json:"pubsub,omitempty" yaml:"pubsub,omitempty"`
}

type TransformConfig struct {
	Type   string `json:"type" yaml:"type"`
	Config map[string]any `json:"config,omitempty" yaml:"config,omitempty"`
}

type LoggerConfig struct {
	Level  string `json:"level" yaml:"level"`
	Format string `json:"format" yaml:"format"`
	Output string `json:"output" yaml:"output"`
}

type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	d.Duration = parsed
	return nil
}

type KafkaSourceCfg struct {
	Brokers      []string `json:"brokers" yaml:"brokers"`
	Topic        string   `json:"topic" yaml:"topic"`
	GroupID      string   `json:"group_id" yaml:"group_id"`
	ManualCommit bool     `json:"manual_commit,omitempty" yaml:"manual_commit,omitempty"`
}

type KafkaSinkCfg struct {
	Brokers []string `json:"brokers" yaml:"brokers"`
	Topic   string   `json:"topic" yaml:"topic"`
}

type KinesisSourceCfg struct {
	StreamName string `json:"stream_name" yaml:"stream_name"`
	Region     string `json:"region" yaml:"region"`
}

type KinesisSinkCfg struct {
	StreamName string `json:"stream_name" yaml:"stream_name"`
	Region     string `json:"region" yaml:"region"`
}

type RabbitMQSourceCfg struct {
	URL        string `json:"url" yaml:"url"`
	Queue      string `json:"queue" yaml:"queue"`
	Exchange   string `json:"exchange,omitempty" yaml:"exchange,omitempty"`
	RoutingKey string `json:"routing_key,omitempty" yaml:"routing_key,omitempty"`
}

type RabbitMQSinkCfg struct {
	URL        string `json:"url" yaml:"url"`
	Exchange   string `json:"exchange,omitempty" yaml:"exchange,omitempty"`
	RoutingKey string `json:"routing_key,omitempty" yaml:"routing_key,omitempty"`
}

type PubSubSourceCfg struct {
	ProjectID      string `json:"project_id" yaml:"project_id"`
	SubscriptionID string `json:"subscription_id" yaml:"subscription_id"`
}

type PubSubSinkCfg struct {
	ProjectID string `json:"project_id" yaml:"project_id"`
	TopicID   string `json:"topic_id" yaml:"topic_id"`
}

func LoadPipelineConfig(path string) (*PipelineConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	var cfg PipelineConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	return &cfg, nil
}

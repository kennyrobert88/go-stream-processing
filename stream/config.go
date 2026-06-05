package stream

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"
)

type PipelineConfig struct {
	Name         string              `json:"name,omitempty" yaml:"name,omitempty"`
	Source       SourceConfig        `json:"source" yaml:"source"`
	Sinks        []SinkConfig        `json:"sinks" yaml:"sinks"`
	Transforms   []TransformConfig   `json:"transforms,omitempty" yaml:"transforms,omitempty"`
	Retry        *RetryConfig        `json:"retry,omitempty" yaml:"retry,omitempty"`
	Backpressure *BackpressureConfig `json:"backpressure,omitempty" yaml:"backpressure,omitempty"`
	Batch        *BatchConfig        `json:"batch,omitempty" yaml:"batch,omitempty"`
	Window       *WindowConfig       `json:"window,omitempty" yaml:"window,omitempty"`
	Workers      int                 `json:"workers,omitempty" yaml:"workers,omitempty"`
	TLS          *TLSConfig          `json:"tls,omitempty" yaml:"tls,omitempty"`
	Logger       *LoggerConfig       `json:"logger,omitempty" yaml:"logger,omitempty"`
	Health       *HealthConfig       `json:"health,omitempty" yaml:"health,omitempty"`
	IdleTimeout  Duration            `json:"idle_timeout,omitempty" yaml:"idle_timeout,omitempty"`
}

type SourceConfig struct {
	Type     string             `json:"type" yaml:"type"`
	Kafka    *KafkaSourceCfg    `json:"kafka,omitempty" yaml:"kafka,omitempty"`
	Kinesis  *KinesisSourceCfg  `json:"kinesis,omitempty" yaml:"kinesis,omitempty"`
	RabbitMQ *RabbitMQSourceCfg `json:"rabbitmq,omitempty" yaml:"rabbitmq,omitempty"`
	PubSub   *PubSubSourceCfg   `json:"pubsub,omitempty" yaml:"pubsub,omitempty"`
}

type SinkConfig struct {
	Type     string           `json:"type" yaml:"type"`
	Kafka    *KafkaSinkCfg    `json:"kafka,omitempty" yaml:"kafka,omitempty"`
	Kinesis  *KinesisSinkCfg  `json:"kinesis,omitempty" yaml:"kinesis,omitempty"`
	RabbitMQ *RabbitMQSinkCfg `json:"rabbitmq,omitempty" yaml:"rabbitmq,omitempty"`
	PubSub   *PubSubSinkCfg   `json:"pubsub,omitempty" yaml:"pubsub,omitempty"`
}

type TransformConfig struct {
	Type   string         `json:"type" yaml:"type"`
	Config map[string]any `json:"config,omitempty" yaml:"config,omitempty"`
}

type LoggerConfig struct {
	Level  string `json:"level" yaml:"level"`
	Format string `json:"format" yaml:"format"`
	Output string `json:"output" yaml:"output"`
}

type HealthConfig struct {
	Enabled    *bool  `json:"enabled,omitempty" yaml:"enabled,omitempty"`
	Addr       string `json:"addr,omitempty" yaml:"addr,omitempty"`
	IncludeAll bool   `json:"include_all,omitempty" yaml:"include_all,omitempty"`
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
	SASLUsername string   `json:"sasl_username,omitempty" yaml:"sasl_username,omitempty"`
	SASLPassword string   `json:"sasl_password,omitempty" yaml:"sasl_password,omitempty"`
}

type KafkaSinkCfg struct {
	Brokers      []string `json:"brokers" yaml:"brokers"`
	Topic        string   `json:"topic" yaml:"topic"`
	SASLUsername string   `json:"sasl_username,omitempty" yaml:"sasl_username,omitempty"`
	SASLPassword string   `json:"sasl_password,omitempty" yaml:"sasl_password,omitempty"`
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
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func (c *PipelineConfig) Validate() error {
	if c == nil {
		return fmt.Errorf("config: nil pipeline config")
	}
	if c.Workers < 0 {
		return fmt.Errorf("config: workers must be >= 0")
	}
	if c.TLS != nil {
		if err := c.TLS.Validate(); err != nil {
			return err
		}
	}
	if err := c.Source.Validate(c.TLS); err != nil {
		return err
	}
	if len(c.Sinks) == 0 {
		return fmt.Errorf("config: at least one sink is required")
	}
	for i := range c.Sinks {
		if err := c.Sinks[i].Validate(c.TLS); err != nil {
			return fmt.Errorf("config: sink[%d]: %w", i, err)
		}
	}
	return nil
}

func (c SourceConfig) Validate(tlsCfg *TLSConfig) error {
	switch c.Type {
	case "kafka":
		if c.Kafka == nil {
			return fmt.Errorf("config: kafka source config is required")
		}
		if len(c.Kafka.Brokers) == 0 || c.Kafka.Topic == "" {
			return fmt.Errorf("config: kafka source brokers and topic are required")
		}
		return validatePlaintextCredentials("kafka source", tlsCfg, c.Kafka.SASLUsername, c.Kafka.SASLPassword)
	case "kinesis":
		if c.Kinesis == nil {
			return fmt.Errorf("config: kinesis source config is required")
		}
		if c.Kinesis.StreamName == "" || c.Kinesis.Region == "" {
			return fmt.Errorf("config: kinesis source stream_name and region are required")
		}
	case "rabbitmq":
		if c.RabbitMQ == nil {
			return fmt.Errorf("config: rabbitmq source config is required")
		}
		if c.RabbitMQ.URL == "" || c.RabbitMQ.Queue == "" {
			return fmt.Errorf("config: rabbitmq source url and queue are required")
		}
		return validateRabbitMQURL("rabbitmq source", c.RabbitMQ.URL, tlsCfg)
	case "pubsub":
		if c.PubSub == nil {
			return fmt.Errorf("config: pubsub source config is required")
		}
		if c.PubSub.ProjectID == "" || c.PubSub.SubscriptionID == "" {
			return fmt.Errorf("config: pubsub source project_id and subscription_id are required")
		}
	default:
		return fmt.Errorf("config: unknown source type %q", c.Type)
	}
	return nil
}

func (c SinkConfig) Validate(tlsCfg *TLSConfig) error {
	switch c.Type {
	case "kafka":
		if c.Kafka == nil {
			return fmt.Errorf("kafka sink config is required")
		}
		if len(c.Kafka.Brokers) == 0 || c.Kafka.Topic == "" {
			return fmt.Errorf("kafka sink brokers and topic are required")
		}
		return validatePlaintextCredentials("kafka sink", tlsCfg, c.Kafka.SASLUsername, c.Kafka.SASLPassword)
	case "kinesis":
		if c.Kinesis == nil {
			return fmt.Errorf("kinesis sink config is required")
		}
		if c.Kinesis.StreamName == "" || c.Kinesis.Region == "" {
			return fmt.Errorf("kinesis sink stream_name and region are required")
		}
	case "rabbitmq":
		if c.RabbitMQ == nil {
			return fmt.Errorf("rabbitmq sink config is required")
		}
		if c.RabbitMQ.URL == "" {
			return fmt.Errorf("rabbitmq sink url is required")
		}
		return validateRabbitMQURL("rabbitmq sink", c.RabbitMQ.URL, tlsCfg)
	case "pubsub":
		if c.PubSub == nil {
			return fmt.Errorf("pubsub sink config is required")
		}
		if c.PubSub.ProjectID == "" || c.PubSub.TopicID == "" {
			return fmt.Errorf("pubsub sink project_id and topic_id are required")
		}
	default:
		return fmt.Errorf("unknown sink type %q", c.Type)
	}
	return nil
}

func validatePlaintextCredentials(component string, tlsCfg *TLSConfig, username, password string) error {
	if username == "" && password == "" {
		return nil
	}
	if tlsCfg == nil || !tlsCfg.Enabled {
		return fmt.Errorf("config: %s credentials require TLS", component)
	}
	return nil
}

func validateRabbitMQURL(component, rawURL string, tlsCfg *TLSConfig) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("config: %s url: %w", component, err)
	}
	if u.Scheme != "amqp" && u.Scheme != "amqps" {
		return fmt.Errorf("config: %s url must use amqp or amqps", component)
	}
	if tlsCfg != nil && tlsCfg.Enabled && u.Scheme != "amqps" {
		return fmt.Errorf("config: %s TLS requires amqps URL", component)
	}
	if u.User != nil && u.Scheme != "amqps" {
		password, hasPassword := u.User.Password()
		if u.User.Username() != "" || (hasPassword && password != "") {
			return fmt.Errorf("config: %s credentials require amqps URL", component)
		}
	}
	if strings.Contains(rawURL, "\n") || strings.Contains(rawURL, "\r") {
		return errors.New("config: URL contains a newline")
	}
	return nil
}

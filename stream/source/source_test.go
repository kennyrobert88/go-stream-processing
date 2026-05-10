package source

import (
	"context"
	"crypto/tls"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/kennyrobert88/go-stream-processing/stream"
)

func TestKafkaSourceConfig_Defaults(t *testing.T) {
	cfg := DefaultKafkaSourceConfig()
	if cfg.ManualCommit {
		t.Error("expected ManualCommit to be false")
	}
	if cfg.MinBytes != 10e3 {
		t.Errorf("expected MinBytes 10000, got %d", cfg.MinBytes)
	}
	if cfg.MaxBytes != 10e6 {
		t.Errorf("expected MaxBytes 10e6, got %d", cfg.MaxBytes)
	}
	if cfg.ReconnectDelay != time.Second {
		t.Errorf("expected ReconnectDelay 1s, got %v", cfg.ReconnectDelay)
	}
	if cfg.MaxReconnects != 10 {
		t.Errorf("expected MaxReconnects 10, got %d", cfg.MaxReconnects)
	}
	if cfg.HeartbeatInterval != 3*time.Second {
		t.Errorf("expected HeartbeatInterval 3s, got %v", cfg.HeartbeatInterval)
	}
	if cfg.SessionTimeout != 30*time.Second {
		t.Errorf("expected SessionTimeout 30s, got %v", cfg.SessionTimeout)
	}
}

func TestKafkaSourceOptions(t *testing.T) {
	cfg := DefaultKafkaSourceConfig()
	WithBrokers("a:9092", "b:9092")(&cfg)
	if len(cfg.Brokers) != 2 || cfg.Brokers[0] != "a:9092" {
		t.Errorf("unexpected brokers: %v", cfg.Brokers)
	}
	WithTopic("test-topic")(&cfg)
	if cfg.Topic != "test-topic" {
		t.Errorf("expected test-topic, got %s", cfg.Topic)
	}
	WithGroupID("test-group")(&cfg)
	if cfg.GroupID != "test-group" {
		t.Errorf("expected test-group, got %s", cfg.GroupID)
	}
	WithManualCommit(true)(&cfg)
	if !cfg.ManualCommit {
		t.Error("expected ManualCommit true")
	}
	tlsCfg := stream.TLSConfig{Enabled: true}
	WithKafkaTLS(tlsCfg)(&cfg)
	if !cfg.TLS.Enabled {
		t.Error("expected TLS enabled")
	}
	WithSASLPlain("user", "pass")(&cfg)
	if cfg.SASLUsername != "user" || cfg.SASLPassword != "pass" {
		t.Error("unexpected SASL credentials")
	}
	WithKafkaReconnect(5*time.Second, 20)(&cfg)
	if cfg.ReconnectDelay != 5*time.Second || cfg.MaxReconnects != 20 {
		t.Error("unexpected reconnect config")
	}
}

func TestKinesisSourceConfig_Defaults(t *testing.T) {
	cfg := DefaultKinesisSourceConfig()
	if cfg.MaxRecordsPerCall != 100 {
		t.Errorf("expected MaxRecordsPerCall 100, got %d", cfg.MaxRecordsPerCall)
	}
	if cfg.PollInterval != time.Second {
		t.Errorf("expected PollInterval 1s, got %v", cfg.PollInterval)
	}
}

func TestKinesisSourceOptions(t *testing.T) {
	cfg := DefaultKinesisSourceConfig()
	WithStreamName("my-stream")(&cfg)
	if cfg.StreamName != "my-stream" {
		t.Errorf("expected my-stream, got %s", cfg.StreamName)
	}
	WithRegion("eu-west-1")(&cfg)
	if cfg.Region != "eu-west-1" {
		t.Errorf("expected eu-west-1, got %s", cfg.Region)
	}
	WithShardIteratorType(types.ShardIteratorTypeLatest)(&cfg)
	if cfg.ShardIteratorType != types.ShardIteratorTypeLatest {
		t.Errorf("expected shard iterator type %s, got %s", types.ShardIteratorTypeLatest, cfg.ShardIteratorType)
	}
}

func TestRabbitMQSourceConfig_Defaults(t *testing.T) {
	cfg := DefaultRabbitMQSourceConfig()
	if cfg.ReconnectDelay != time.Second {
		t.Errorf("expected ReconnectDelay 1s, got %v", cfg.ReconnectDelay)
	}
	if cfg.MaxReconnects != 10 {
		t.Errorf("expected MaxReconnects 10, got %d", cfg.MaxReconnects)
	}
	if cfg.PrefetchCount != 100 {
		t.Errorf("expected PrefetchCount 100, got %d", cfg.PrefetchCount)
	}
	if cfg.Heartbeat != 10*time.Second {
		t.Errorf("expected Heartbeat 10s, got %v", cfg.Heartbeat)
	}
}

func TestRabbitMQSourceOptions(t *testing.T) {
	cfg := DefaultRabbitMQSourceConfig()
	WithURL("amqp://localhost")(&cfg)
	if cfg.URL != "amqp://localhost" {
		t.Errorf("unexpected URL: %s", cfg.URL)
	}
	WithQueue("q")(&cfg)
	if cfg.Queue != "q" {
		t.Errorf("unexpected queue: %s", cfg.Queue)
	}
	WithExchange("e")(&cfg)
	if cfg.Exchange != "e" {
		t.Errorf("unexpected exchange: %s", cfg.Exchange)
	}
	WithRoutingKey("k")(&cfg)
	if cfg.RoutingKey != "k" {
		t.Errorf("unexpected routing key: %s", cfg.RoutingKey)
	}
	tlsCfg := stream.TLSConfig{Enabled: true}
	WithRabbitMQTLS(tlsCfg)(&cfg)
	if !cfg.TLS.Enabled {
		t.Error("expected TLS enabled")
	}
}

func TestPubSubSourceConfig_Defaults(t *testing.T) {
	cfg := DefaultPubSubSourceConfig()
	if cfg.MaxOutstandingMessages != 100 {
		t.Errorf("expected MaxOutstandingMessages 100, got %d", cfg.MaxOutstandingMessages)
	}
	if cfg.NumGoroutines != 10 {
		t.Errorf("expected NumGoroutines 10, got %d", cfg.NumGoroutines)
	}
	if cfg.ReconnectDelay != time.Second {
		t.Errorf("expected ReconnectDelay 1s, got %v", cfg.ReconnectDelay)
	}
}

func TestPubSubSourceOptions(t *testing.T) {
	cfg := DefaultPubSubSourceConfig()
	WithProjectID("my-project")(&cfg)
	if cfg.ProjectID != "my-project" {
		t.Errorf("expected my-project, got %s", cfg.ProjectID)
	}
	WithSubscriptionID("my-sub")(&cfg)
	if cfg.SubscriptionID != "my-sub" {
		t.Errorf("expected my-sub, got %s", cfg.SubscriptionID)
	}
	WithPubSubNumGoroutines(20)(&cfg)
	if cfg.NumGoroutines != 20 {
		t.Errorf("expected NumGoroutines 20, got %d", cfg.NumGoroutines)
	}
}

func TestNewKafkaSourceWithOptions(t *testing.T) {
	src := NewKafkaSourceWithOptions(
		WithBrokers("localhost:9092"),
		WithTopic("test"),
		WithGroupID("g"),
	)
	if src == nil {
		t.Fatal("expected non-nil source")
	}
	if len(src.cfg.Brokers) != 1 || src.cfg.Brokers[0] != "localhost:9092" {
		t.Errorf("unexpected brokers: %v", src.cfg.Brokers)
	}
}

func TestNewKinesisSourceWithOptions(t *testing.T) {
	src := NewKinesisSourceWithOptions(
		WithStreamName("s"),
		WithRegion("r"),
	)
	if src == nil {
		t.Fatal("expected non-nil source")
	}
	if src.cfg.StreamName != "s" {
		t.Errorf("expected stream s, got %s", src.cfg.StreamName)
	}
}

func TestNewRabbitMQSourceWithOptions(t *testing.T) {
	src := NewRabbitMQSourceWithOptions(
		WithURL("amqp://localhost"),
		WithQueue("q"),
	)
	if src == nil {
		t.Fatal("expected non-nil source")
	}
	if src.cfg.URL != "amqp://localhost" {
		t.Errorf("unexpected URL: %s", src.cfg.URL)
	}
}

func TestTLSConfig_Build_Disabled(t *testing.T) {
	cfg := stream.TLSConfig{Enabled: false}
	tlsCfg, err := cfg.Build()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tlsCfg != nil {
		t.Error("expected nil TLS config when disabled")
	}
}

func TestTLSConfig_Build_EnabledNoFiles(t *testing.T) {
	cfg := stream.TLSConfig{Enabled: true}
	tlsCfg, err := cfg.Build()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tlsCfg == nil {
		t.Fatal("expected non-nil TLS config")
	}
	if tlsCfg.InsecureSkipVerify {
		t.Error("expected InsecureSkipVerify to be false")
	}
}

func TestTLSConfig_Build_MissingCertFile(t *testing.T) {
	cfg := stream.TLSConfig{
		Enabled:  true,
		CertFile: "/nonexistent/cert.pem",
		KeyFile:  "/nonexistent/key.pem",
	}
	_, err := cfg.Build()
	if err == nil {
		t.Fatal("expected error for missing cert files")
	}
}

func TestTLSConfig_Build_WithConfig(t *testing.T) {
	cfg := stream.TLSConfig{
		Enabled:            true,
		InsecureSkipVerify: true,
		ServerName:         "example.com",
	}
	tlsCfg, err := cfg.Build()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tlsCfg == nil {
		t.Fatal("expected non-nil TLS config")
	}
	if !tlsCfg.InsecureSkipVerify {
		t.Error("expected InsecureSkipVerify to be true")
	}
	if tlsCfg.ServerName != "example.com" {
		t.Errorf("expected ServerName 'example.com', got %s", tlsCfg.ServerName)
	}
}

func TestTLSConfig_Build_MinVersion(t *testing.T) {
	cfg := stream.TLSConfig{
		Enabled: true,
	}
	tlsCfg, err := cfg.Build()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tlsCfg == nil {
		t.Fatal("expected non-nil TLS config")
	}
	if tlsCfg.MinVersion != tls.VersionTLS12 {
		t.Errorf("expected MinVersion TLS 1.2, got %d", tlsCfg.MinVersion)
	}
}

func TestKafkaSource_Lifecycle(t *testing.T) {
	src := NewKafkaSourceWithOptions(
		WithBrokers("localhost:9092"),
		WithTopic("test"),
		WithGroupID("g"),
	)
	ctx := context.Background()

	err := src.Open(ctx)
	if err == nil {
		src.Close(ctx)
	}
}

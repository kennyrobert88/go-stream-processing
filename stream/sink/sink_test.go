package sink

import (
	"testing"
	"time"

	"github.com/kennyrobert88/go-stream-processing/stream"
)

func TestKafkaSinkConfig_Defaults(t *testing.T) {
	cfg := DefaultKafkaSinkConfig()
	if cfg.BatchSize != 100 {
		t.Errorf("expected BatchSize 100, got %d", cfg.BatchSize)
	}
	if cfg.BatchTimeout != time.Second {
		t.Errorf("expected BatchTimeout 1s, got %v", cfg.BatchTimeout)
	}
	if cfg.MaxRetries != 3 {
		t.Errorf("expected MaxRetries 3, got %d", cfg.MaxRetries)
	}
	if cfg.WriteTimeout != 30*time.Second {
		t.Errorf("expected WriteTimeout 30s, got %v", cfg.WriteTimeout)
	}
	if int(cfg.RequiredAcks) != -1 {
		t.Errorf("expected RequiredAcks -1, got %d", cfg.RequiredAcks)
	}
}

func TestKafkaSinkOptions(t *testing.T) {
	cfg := DefaultKafkaSinkConfig()
	tlsCfg := stream.TLSConfig{Enabled: true}
	WithKafkaSinkTLS(tlsCfg)(&cfg)
	if !cfg.TLS.Enabled {
		t.Error("expected TLS enabled")
	}
}

func TestNewKafkaSinkWithOptions(t *testing.T) {
	snk := NewKafkaSinkWithOptions()
	if snk == nil {
		t.Fatal("expected non-nil sink")
	}
	if snk.cfg.BatchSize != 100 {
		t.Errorf("expected default BatchSize 100, got %d", snk.cfg.BatchSize)
	}
}

func TestSink_Lifecycle(t *testing.T) {
	snk := NewKafkaSink(KafkaSinkConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "test",
	})
	if snk == nil {
		t.Fatal("expected non-nil sink")
	}
}

func TestKinesisSink_Defaults(t *testing.T) {
	snk := NewKinesisSink(KinesisSinkConfig{
		StreamName: "test",
		Region:     "us-east-1",
	})
	if snk == nil {
		t.Fatal("expected non-nil sink")
	}
}

func TestNewKafkaSinkConfig_Validation(t *testing.T) {
	cfg := KafkaSinkConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "test",
	}
	snk := NewKafkaSink(cfg)
	if snk.cfg.Topic != "test" {
		t.Errorf("expected topic 'test', got %s", snk.cfg.Topic)
	}
}

package stream

import (
	"context"
	"sync"
	"time"
)

type SourceBackpressure struct {
	mu      sync.RWMutex
	limit   int
	window  time.Duration
	count   map[string]*sourceCounter
}

type sourceCounter struct {
	count    int
	windowAt time.Time
}

type SourceBackpressureConfig struct {
	Limit  int
	Window time.Duration
}

func NewSourceBackpressure(cfg SourceBackpressureConfig) *SourceBackpressure {
	if cfg.Limit <= 0 {
		cfg.Limit = 1000
	}
	if cfg.Window <= 0 {
		cfg.Window = time.Second
	}
	return &SourceBackpressure{
		limit:  cfg.Limit,
		window: cfg.Window,
		count:  make(map[string]*sourceCounter),
	}
}

func (sb *SourceBackpressure) Allow(partition string) bool {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	now := time.Now()
	c, ok := sb.count[partition]
	if !ok || now.Sub(c.windowAt) > sb.window {
		sb.count[partition] = &sourceCounter{count: 0, windowAt: now}
		return true
	}
	if c.count >= sb.limit {
		return false
	}
	c.count++
	return true
}

func (sb *SourceBackpressure) Wait(ctx context.Context, partition string) error {
	for {
		if sb.Allow(partition) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

type GracefulDegradation struct {
	mu       sync.RWMutex
	degraded bool
	reason   string
}

func NewGracefulDegradation() *GracefulDegradation {
	return &GracefulDegradation{}
}

func (g *GracefulDegradation) Degrade(reason string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.degraded = true
	g.reason = reason
}

func (g *GracefulDegradation) Recover() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.degraded = false
	g.reason = ""
}

func (g *GracefulDegradation) IsDegraded() bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.degraded
}

func (g *GracefulDegradation) Reason() string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.reason
}

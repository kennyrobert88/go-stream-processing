package stream

import (
	"context"
	"sync"
	"time"
)

type CheckStatus int

const (
	StatusHealthy CheckStatus = iota
	StatusDegraded
	StatusUnhealthy
)

func (s CheckStatus) String() string {
	switch s {
	case StatusHealthy:
		return "healthy"
	case StatusDegraded:
		return "degraded"
	case StatusUnhealthy:
		return "unhealthy"
	default:
		return "unknown"
	}
}

type HealthReport struct {
	Status    CheckStatus
	Component string
	Message   string
	Latency   time.Duration
	Time      time.Time
}

type CheckFunc func(ctx context.Context) HealthReport

type HealthProbe struct {
	mu         sync.RWMutex
	liveness   CheckFunc
	readiness  CheckFunc
	components map[string]CheckFunc
}

func NewHealthProbe(liveness, readiness CheckFunc) *HealthProbe {
	return &HealthProbe{
		liveness:   liveness,
		readiness:  readiness,
		components: make(map[string]CheckFunc),
	}
}

func (h *HealthProbe) RegisterComponent(name string, fn CheckFunc) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.components[name] = fn
}

func (h *HealthProbe) RemoveComponent(name string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.components, name)
}

func (h *HealthProbe) Liveness(ctx context.Context) HealthReport {
	if h.liveness == nil {
		return HealthReport{Status: StatusHealthy, Component: "liveness", Time: time.Now()}
	}
	return h.liveness(ctx)
}

func (h *HealthProbe) Readiness(ctx context.Context) HealthReport {
	if h.readiness == nil {
		return HealthReport{Status: StatusHealthy, Component: "readiness", Time: time.Now()}
	}
	return h.readiness(ctx)
}

func (h *HealthProbe) CheckAll(ctx context.Context) []HealthReport {
	h.mu.RLock()
	components := make([]CheckFunc, 0, len(h.components))
	names := make([]string, 0, len(h.components))
	for name, fn := range h.components {
		names = append(names, name)
		components = append(components, fn)
	}
	h.mu.RUnlock()

	reports := make([]HealthReport, 0, 2+len(components))
	reports = append(reports, h.Liveness(ctx), h.Readiness(ctx))
	for i, fn := range components {
		r := fn(ctx)
		if r.Component == "" {
			r.Component = names[i]
		}
		reports = append(reports, r)
	}
	return reports
}

func (h *HealthProbe) Healthy(ctx context.Context) bool {
	report := h.Readiness(ctx)
	return report.Status == StatusHealthy || report.Status == StatusDegraded
}

type ProbeAware interface {
	LivenessCheck(ctx context.Context) HealthReport
	ReadinessCheck(ctx context.Context) HealthReport
}

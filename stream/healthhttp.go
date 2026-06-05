package stream

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"time"
)

type HealthHTTPServer struct {
	server     *http.Server
	probe      *HealthProbe
	includeAll bool
}

type HealthHTTPOption func(*HealthHTTPServer)

func NewHealthHTTPServer(addr string, probe *HealthProbe) *HealthHTTPServer {
	return NewHealthHTTPServerWithOptions(addr, probe, WithHealthAllEndpoint(true))
}

func NewHealthHTTPServerWithOptions(addr string, probe *HealthProbe, opts ...HealthHTTPOption) *HealthHTTPServer {
	h := &HealthHTTPServer{probe: probe}
	for _, opt := range opts {
		opt(h)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", h.livenessHandler)
	mux.HandleFunc("/readyz", h.readinessHandler)
	if h.includeAll {
		mux.HandleFunc("/health/all", h.allHandler)
	}

	h.server = &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      5 * time.Second,
	}
	return h
}

func WithHealthAllEndpoint(enabled bool) HealthHTTPOption {
	return func(h *HealthHTTPServer) {
		h.includeAll = enabled
	}
}

func (h *HealthHTTPServer) Start() error {
	listener, err := net.Listen("tcp", h.server.Addr)
	if err != nil {
		return err
	}
	go func() {
		if err := h.server.Serve(listener); err != nil && err != http.ErrServerClosed {
			h.probe.Liveness(context.Background())
		}
	}()
	return nil
}

func (h *HealthHTTPServer) Stop(ctx context.Context) error {
	return h.server.Shutdown(ctx)
}

func (h *HealthHTTPServer) livenessHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	report := h.probe.Liveness(r.Context())
	writeHealthResponse(w, report)
}

func (h *HealthHTTPServer) readinessHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	report := h.probe.Readiness(r.Context())
	writeHealthResponse(w, report)
}

func (h *HealthHTTPServer) allHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	reports := h.probe.CheckAll(r.Context())
	allHealthy := true
	for _, rep := range reports {
		if rep.Status == StatusUnhealthy {
			allHealthy = false
		}
	}

	w.Header().Set("Content-Type", "application/json")
	if !allHealthy {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	_ = json.NewEncoder(w).Encode(reports)
}

func writeHealthResponse(w http.ResponseWriter, report HealthReport) {
	w.Header().Set("Content-Type", "application/json")
	statusCode := http.StatusOK
	switch report.Status {
	case StatusUnhealthy:
		statusCode = http.StatusServiceUnavailable
	case StatusDegraded:
		statusCode = http.StatusOK
	}
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(report)
}

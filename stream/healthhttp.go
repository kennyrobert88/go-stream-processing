package stream

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"time"
)

type HealthHTTPServer struct {
	server *http.Server
	probe  *HealthProbe
}

func NewHealthHTTPServer(addr string, probe *HealthProbe) *HealthHTTPServer {
	h := &HealthHTTPServer{probe: probe}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", h.livenessHandler)
	mux.HandleFunc("/readyz", h.readinessHandler)
	mux.HandleFunc("/health/all", h.allHandler)

	h.server = &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}
	return h
}

func (h *HealthHTTPServer) Start() error {
	listener, err := net.Listen("tcp", h.server.Addr)
	if err != nil {
		return err
	}
	go h.server.Serve(listener)
	return nil
}

func (h *HealthHTTPServer) Stop(ctx context.Context) error {
	return h.server.Shutdown(ctx)
}

func (h *HealthHTTPServer) livenessHandler(w http.ResponseWriter, r *http.Request) {
	report := h.probe.Liveness(r.Context())
	writeHealthResponse(w, report)
}

func (h *HealthHTTPServer) readinessHandler(w http.ResponseWriter, r *http.Request) {
	report := h.probe.Readiness(r.Context())
	writeHealthResponse(w, report)
}

func (h *HealthHTTPServer) allHandler(w http.ResponseWriter, r *http.Request) {
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
	json.NewEncoder(w).Encode(reports)
}

func writeHealthResponse(w http.ResponseWriter, report HealthReport) {
	w.Header().Set("Content-Type", "application/json")
	statusCode := http.StatusOK
	if report.Status == StatusUnhealthy {
		statusCode = http.StatusServiceUnavailable
	} else if report.Status == StatusDegraded {
		statusCode = http.StatusOK
	}
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(report)
}

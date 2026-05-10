package stream

import (
	"sort"
	"sync"
	"time"
)

type WindowState struct {
	mu          sync.Mutex
	buf         []Message[[]byte]
	maxSize     int
	watermark   time.Time
	outOfOrder  bool
}

type WindowStateConfig struct {
	MaxBufferSize int
	AllowedLag    time.Duration
}

func DefaultWindowStateConfig() WindowStateConfig {
	return WindowStateConfig{
		MaxBufferSize: 100000,
		AllowedLag:    10 * time.Second,
	}
}

func NewWindowState(cfg WindowStateConfig) *WindowState {
	return &WindowState{
		maxSize:    cfg.MaxBufferSize,
		watermark:  time.Now().Add(-cfg.AllowedLag),
		outOfOrder: true,
	}
}

func (ws *WindowState) Add(msg Message[[]byte]) bool {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	if len(ws.buf) >= ws.maxSize {
		return false
	}

	ws.buf = append(ws.buf, msg)
	if !msg.Timestamp.IsZero() && msg.Timestamp.After(ws.watermark) {
		ws.watermark = msg.Timestamp
	}
	return true
}

func (ws *WindowState) Drain() []Message[[]byte] {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	result := ws.buf
	ws.buf = nil
	return result
}

func (ws *WindowState) DrainN(n int) []Message[[]byte] {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	if len(ws.buf) > n {
		result := ws.buf[:n]
		ws.buf = ws.buf[n:]
		return result
	}
	return ws.Drain()
}

func (ws *WindowState) Sort() {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	sort.Slice(ws.buf, func(i, j int) bool {
		if ws.buf[i].Timestamp.IsZero() || ws.buf[j].Timestamp.IsZero() {
			return false
		}
		return ws.buf[i].Timestamp.Before(ws.buf[j].Timestamp)
	})
}

func (ws *WindowState) Watermark() time.Time {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	return ws.watermark
}

func (ws *WindowState) Len() int {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	return len(ws.buf)
}

func (ws *WindowState) Snapshot() []Message[[]byte] {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	result := make([]Message[[]byte], len(ws.buf))
	copy(result, ws.buf)
	return result
}

func (ws *WindowState) Restore(msgs []Message[[]byte]) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	ws.buf = msgs
	for _, m := range msgs {
		if !m.Timestamp.IsZero() && m.Timestamp.After(ws.watermark) {
			ws.watermark = m.Timestamp
		}
	}
}

type WindowFlusher interface {
	ShouldFlush(msgs []Message[[]byte], cfg WindowConfig, watermark time.Time) bool
}

type EventTimeTracker struct {
	mu        sync.Mutex
	maxTs     time.Time
	updatedAt time.Time
}

func NewEventTimeTracker() *EventTimeTracker {
	return &EventTimeTracker{}
}

func (e *EventTimeTracker) Observe(ts time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if ts.After(e.maxTs) {
		e.maxTs = ts
	}
	e.updatedAt = time.Now()
}

func (e *EventTimeTracker) MaxTimestamp() time.Time {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.maxTs
}

func (e *EventTimeTracker) Advance(to time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if to.After(e.maxTs) {
		e.maxTs = to
	}
	e.updatedAt = time.Now()
}

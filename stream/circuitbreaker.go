package stream

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type CBState int32

const (
	CBStateClosed CBState = iota
	CBStateHalfOpen
	CBStateOpen
)

func (s CBState) String() string {
	switch s {
	case CBStateClosed:
		return "closed"
	case CBStateHalfOpen:
		return "half-open"
	case CBStateOpen:
		return "open"
	default:
		return "unknown"
	}
}

type CircuitBreaker struct {
	name          string
	maxFailures   int64
	resetTimeout  time.Duration
	halfOpenMax   int64
	halfOpenCount atomic.Int64

	breakerState atomic.Int32
	failures     atomic.Int64
	lastFail     atomic.Int64

	onStateChange func(name string, old, new CBState)
	mu            sync.Mutex
}

type CircuitBreakerConfig struct {
	Name          string
	MaxFailures   int64
	ResetTimeout  time.Duration
	HalfOpenMax   int64
	OnStateChange func(name string, old, new CBState)
}

func DefaultCircuitBreakerConfig(name string) CircuitBreakerConfig {
	return CircuitBreakerConfig{
		Name:         name,
		MaxFailures:  5,
		ResetTimeout: 30 * time.Second,
		HalfOpenMax:  3,
	}
}

func NewCircuitBreaker(cfg CircuitBreakerConfig) *CircuitBreaker {
	return &CircuitBreaker{
		name:          cfg.Name,
		maxFailures:   cfg.MaxFailures,
		resetTimeout:  cfg.ResetTimeout,
		halfOpenMax:   cfg.HalfOpenMax,
		onStateChange: cfg.OnStateChange,
	}
}

func (cb *CircuitBreaker) State() CBState {
	return CBState(cb.breakerState.Load())
}

func (cb *CircuitBreaker) Execute(ctx context.Context, fn func(ctx context.Context) error) error {
	if !cb.allow() {
		return fmt.Errorf("circuit breaker %q: %w", cb.name, ErrCircuitOpen)
	}

	err := fn(ctx)
	if err != nil {
		cb.recordFailure()
		return err
	}
	cb.recordSuccess()
	return nil
}

func (cb *CircuitBreaker) allow() bool {
	st := cb.State()
	switch st {
	case CBStateClosed:
		return true
	case CBStateOpen:
		if time.Since(time.Unix(0, cb.lastFail.Load())) > cb.resetTimeout {
			cb.mu.Lock()
			defer cb.mu.Unlock()
			if cb.State() == CBStateOpen {
				cb.setCBState(CBStateHalfOpen)
				cb.halfOpenCount.Store(0)
			}
			return true
		}
		return false
	case CBStateHalfOpen:
		return cb.halfOpenCount.Load() < cb.halfOpenMax
	default:
		return true
	}
}

func (cb *CircuitBreaker) recordFailure() {
	fails := cb.failures.Add(1)
	cb.lastFail.Store(time.Now().UnixNano())

	if cb.State() == CBStateHalfOpen {
		cb.mu.Lock()
		if cb.State() == CBStateHalfOpen {
			cb.setCBState(CBStateOpen)
		}
		cb.mu.Unlock()
		return
	}

	if fails >= cb.maxFailures {
		cb.mu.Lock()
		if cb.State() == CBStateClosed {
			cb.setCBState(CBStateOpen)
		}
		cb.mu.Unlock()
	}
}

func (cb *CircuitBreaker) recordSuccess() {
	if cb.State() == CBStateHalfOpen {
		cb.mu.Lock()
		if cb.State() == CBStateHalfOpen {
			cb.failures.Store(0)
			cb.setCBState(CBStateClosed)
		}
		cb.mu.Unlock()
	}
}

func (cb *CircuitBreaker) setCBState(newState CBState) {
	old := cb.State()
	cb.breakerState.Store(int32(newState))
	if cb.onStateChange != nil && old != newState {
		cb.onStateChange(cb.name, old, newState)
	}
}

func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failures.Store(0)
	cb.lastFail.Store(0)
	cb.halfOpenCount.Store(0)
	cb.setCBState(CBStateClosed)
}

var ErrCircuitOpen = errors.New("circuit breaker is open")

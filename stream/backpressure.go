package stream

import (
	"context"
	"sync"
	"time"
)

type Backpressure struct {
	tokens chan struct{}
	close  chan struct{}
	once   sync.Once
}

func NewBackpressure(cfg BackpressureConfig) (*Backpressure, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	bp := &Backpressure{
		tokens: make(chan struct{}, cfg.Burst),
		close:  make(chan struct{}),
	}
	for i := 0; i < cfg.Burst; i++ {
		bp.tokens <- struct{}{}
	}
	go bp.refill(cfg.Rate)
	return bp, nil
}

func (bp *Backpressure) refill(rate int) {
	interval := time.Second / time.Duration(rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-bp.close:
			return
		case <-ticker.C:
			select {
			case bp.tokens <- struct{}{}:
			default:
			}
		}
	}
}

func (bp *Backpressure) Allow() bool {
	select {
	case <-bp.tokens:
		return true
	default:
		return false
	}
}

func (bp *Backpressure) Wait(ctx context.Context) error {
	select {
	case <-bp.tokens:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (bp *Backpressure) Close() {
	bp.once.Do(func() {
		close(bp.close)
	})
}

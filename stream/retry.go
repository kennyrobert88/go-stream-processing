package stream

import (
	"context"
	"math"
	"math/rand"
	"time"
)

func DoWithRetry(ctx context.Context, fn func(context.Context) error, cfg RetryConfig) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	var err error
	for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
		if err = fn(ctx); err == nil {
			return nil
		}
		if !IsRetryable(err) {
			return err
		}
		if attempt == cfg.MaxRetries {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoffDuration(attempt, cfg)):
		}
	}
	return err
}

func backoffDuration(attempt int, cfg RetryConfig) time.Duration {
	delay := float64(cfg.BaseDelay) * math.Pow(2, float64(attempt))
	jitter := rand.Float64()*float64(cfg.BaseDelay) - float64(cfg.BaseDelay)/2
	delay = math.Min(delay+jitter, float64(cfg.MaxDelay))
	return time.Duration(delay)
}

package stream

import (
	"errors"
	"time"
)

type RetryConfig struct {
	MaxRetries int
	BaseDelay  time.Duration
	MaxDelay   time.Duration
}

func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxRetries: 3,
		BaseDelay:  100 * time.Millisecond,
		MaxDelay:   10 * time.Second,
	}
}

func (c RetryConfig) Validate() error {
	if c.MaxRetries < 0 {
		return errors.New("retry: MaxRetries must be non-negative")
	}
	if c.BaseDelay < 0 {
		return errors.New("retry: BaseDelay must be non-negative")
	}
	if c.MaxDelay < 0 {
		return errors.New("retry: MaxDelay must be non-negative")
	}
	if c.BaseDelay > c.MaxDelay {
		return errors.New("retry: BaseDelay must not exceed MaxDelay")
	}
	return nil
}

type BackpressureConfig struct {
	Rate  int
	Burst int
}

func DefaultBackpressureConfig() BackpressureConfig {
	return BackpressureConfig{
		Rate:  1000,
		Burst: 100,
	}
}

func (c BackpressureConfig) Validate() error {
	if c.Rate <= 0 {
		return errors.New("backpressure: Rate must be positive")
	}
	if c.Burst <= 0 {
		return errors.New("backpressure: Burst must be positive")
	}
	return nil
}

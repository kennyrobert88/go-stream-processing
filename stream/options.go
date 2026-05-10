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
	Rate    int
	Burst   int
	PerSink bool
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

type BatchConfig struct {
	Size     int
	Interval time.Duration
}

func (c BatchConfig) Validate() error {
	if c.Size < 0 {
		return errors.New("batch: Size must be non-negative")
	}
	if c.Interval < 0 {
		return errors.New("batch: Interval must be non-negative")
	}
	return nil
}

type WindowType int

const (
	WindowTypeTumbling WindowType = iota
	WindowTypeSliding
	WindowTypeSession
	WindowTypeCount
)

func (t WindowType) String() string {
	switch t {
	case WindowTypeTumbling:
		return "tumbling"
	case WindowTypeSliding:
		return "sliding"
	case WindowTypeSession:
		return "session"
	case WindowTypeCount:
		return "count"
	default:
		return "unknown"
	}
}

type WindowConfig struct {
	Type       WindowType
	Size       time.Duration
	Slide      time.Duration
	Count      int
	SessionGap time.Duration
	Offset     time.Duration
}

func DefaultWindowConfig() WindowConfig {
	return WindowConfig{
		Type: WindowTypeTumbling,
		Size: 10 * time.Second,
	}
}

func (c WindowConfig) Validate() error {
	switch c.Type {
	case WindowTypeTumbling:
		if c.Size <= 0 {
			return errors.New("window: Size must be positive for tumbling windows")
		}
	case WindowTypeSliding:
		if c.Size <= 0 {
			return errors.New("window: Size must be positive for sliding windows")
		}
		if c.Slide <= 0 {
			return errors.New("window: Slide must be positive for sliding windows")
		}
	case WindowTypeSession:
		if c.SessionGap <= 0 {
			return errors.New("window: SessionGap must be positive for session windows")
		}
	case WindowTypeCount:
		if c.Count <= 0 {
			return errors.New("window: Count must be positive for count windows")
		}
	default:
		return errors.New("window: unknown window type")
	}
	return nil
}

var ErrTimeout = errors.New("timeout")

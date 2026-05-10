package stream

import (
	"context"
	"errors"
)

type RetryableError struct {
	Err error
}

func (e *RetryableError) Error() string {
	return e.Err.Error()
}

func (e *RetryableError) Unwrap() error {
	return e.Err
}

func NewRetryableError(err error) error {
	return &RetryableError{Err: err}
}

type NonRetryableError struct {
	Err error
}

func (e *NonRetryableError) Error() string {
	return e.Err.Error()
}

func (e *NonRetryableError) Unwrap() error {
	return e.Err
}

func NewNonRetryableError(err error) error {
	return &NonRetryableError{Err: err}
}

func IsRetryable(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var re *RetryableError
	if errors.As(err, &re) {
		return true
	}
	var nre *NonRetryableError
	return !errors.As(err, &nre)
}

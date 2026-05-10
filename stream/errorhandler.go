package stream

import "context"

type ErrorHandler interface {
	OnTransformError(ctx context.Context, msg Message[[]byte], err error)
	OnWriteError(ctx context.Context, msg Message[[]byte], err error)
	OnReadError(ctx context.Context, err error)
}

type ErrorHandlerFunc struct {
	TransformFn func(ctx context.Context, msg Message[[]byte], err error)
	WriteFn     func(ctx context.Context, msg Message[[]byte], err error)
	ReadFn      func(ctx context.Context, err error)
}

func (h *ErrorHandlerFunc) OnTransformError(ctx context.Context, msg Message[[]byte], err error) {
	if h.TransformFn != nil {
		h.TransformFn(ctx, msg, err)
	}
}

func (h *ErrorHandlerFunc) OnWriteError(ctx context.Context, msg Message[[]byte], err error) {
	if h.WriteFn != nil {
		h.WriteFn(ctx, msg, err)
	}
}

func (h *ErrorHandlerFunc) OnReadError(ctx context.Context, err error) {
	if h.ReadFn != nil {
		h.ReadFn(ctx, err)
	}
}

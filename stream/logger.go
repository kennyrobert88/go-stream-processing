package stream

import (
	"context"
	"log/slog"
	"os"
)

type Logger interface {
	Debug(ctx context.Context, msg string, args ...any)
	Info(ctx context.Context, msg string, args ...any)
	Warn(ctx context.Context, msg string, args ...any)
	Error(ctx context.Context, msg string, args ...any)
}

type SlogLogger struct {
	logger *slog.Logger
}

func NewSlogLogger(level slog.Level) *SlogLogger {
	return &SlogLogger{
		logger: slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})),
	}
}

func NewSlogLoggerFrom(logger *slog.Logger) *SlogLogger {
	return &SlogLogger{logger: logger}
}

func (l *SlogLogger) Debug(_ context.Context, msg string, args ...any) { l.logger.Debug(msg, args...) }
func (l *SlogLogger) Info(_ context.Context, msg string, args ...any)  { l.logger.Info(msg, args...) }
func (l *SlogLogger) Warn(_ context.Context, msg string, args ...any)  { l.logger.Warn(msg, args...) }
func (l *SlogLogger) Error(_ context.Context, msg string, args ...any) { l.logger.Error(msg, args...) }

type NopLogger struct{}

func (NopLogger) Debug(_ context.Context, _ string, _ ...any) {}
func (NopLogger) Info(_ context.Context, _ string, _ ...any)  {}
func (NopLogger) Warn(_ context.Context, _ string, _ ...any)  {}
func (NopLogger) Error(_ context.Context, _ string, _ ...any) {}

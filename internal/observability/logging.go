package observability

import (
	"context"
	"log/slog"
	"os"
)

// Logger wraps slog.Logger with service context.
type Logger struct {
	*slog.Logger
}

// NewLogger creates a new structured logger for the service.
func NewLogger(serviceName, env string) *Logger {
	opts := &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}

	var handler slog.Handler
	if env == "production" {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		handler = slog.NewTextHandler(os.Stdout, opts)
	}

	logger := slog.New(handler.WithAttrs([]slog.Attr{
		slog.String("service", serviceName),
		slog.String("env", env),
	}))

	return &Logger{Logger: logger}
}

// WithContext returns a logger with context values.
func (l *Logger) WithContext(_ context.Context) *Logger {
	// Extract trace ID from context if available
	return &Logger{Logger: l.Logger}
}

// Info logs an info message.
func (l *Logger) Info(msg string, args ...any) {
	l.Logger.Info(msg, args...)
}

// Warn logs a warning message.
func (l *Logger) Warn(msg string, args ...any) {
	l.Logger.Warn(msg, args...)
}

// Error logs an error message.
func (l *Logger) Error(msg string, args ...any) {
	l.Logger.Error(msg, args...)
}

// Debug logs a debug message.
func (l *Logger) Debug(msg string, args ...any) {
	l.Logger.Debug(msg, args...)
}

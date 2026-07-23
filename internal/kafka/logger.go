package kafka

import (
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"
)

// kgoLogger bridges kgo.Logger to log/slog so franz-go diagnostic messages are
// emitted through the application's structured logger. This keeps franz-go
// internal details hidden from the public API (§13).
type kgoLogger struct {
	l *slog.Logger
}

func newKgoLogger(l *slog.Logger) kgo.Logger {
	return &kgoLogger{l: l}
}

// Level returns the minimum log level that the logger will emit. We always return
// LogLevelInfo to suppress verbose DEBUG output in production by default. A future
// config knob could expose this.
func (k *kgoLogger) Level() kgo.LogLevel {
	return kgo.LogLevelInfo
}

// Log converts a kgo log entry into a structured slog message.
func (k *kgoLogger) Log(level kgo.LogLevel, msg string, keyvals ...any) {
	var sl slog.Level
	switch level {
	case kgo.LogLevelError:
		sl = slog.LevelError
	case kgo.LogLevelWarn:
		sl = slog.LevelWarn
	case kgo.LogLevelDebug:
		sl = slog.LevelDebug
	default:
		sl = slog.LevelInfo
	}
	// keyvals from kgo are alternating key/value pairs; pass them directly to slog.
	k.l.Log(nil, sl, "[franz-go] "+msg, keyvals...) //nolint:sloglint // variadic keyvals from kgo
}

package kafka

import (
	"github.com/mortezaPRK/gstream/logging"
	"github.com/twmb/franz-go/pkg/kgo"
)

// kgoLogger bridges kgo.Logger to log/slog so franz-go diagnostic messages are
// emitted through the application's structured logger. This keeps franz-go
// internal details hidden from the public API (§13).
type kgoLogger struct {
	l logging.Logger
}

func newKgoLogger(l logging.Logger) kgo.Logger {
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
	switch level {
	case kgo.LogLevelError:
		k.l.Error("[franz-go] "+msg, keyvals...)
	case kgo.LogLevelWarn:
		k.l.Warn("[franz-go] "+msg, keyvals...)
	case kgo.LogLevelDebug:
		k.l.Debug("[franz-go] "+msg, keyvals...)
	default:
		k.l.Info("[franz-go] "+msg, keyvals...)
	}
}

// Package logging defines gstream's small structured-logging boundary.
package logging

// Logger is implemented by log/slog.Logger and compatible structured loggers.
// Arguments use alternating key/value pairs or slog.Attr values.
type Logger interface {
	Debug(message string, args ...any)
	Info(message string, args ...any)
	Warn(message string, args ...any)
	Error(message string, args ...any)
}

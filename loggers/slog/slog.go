// Package slog provides gstream's standard-library logging default.
package slog

import (
	stdslog "log/slog"
)

// Default returns standard library default logger.
func Default() *stdslog.Logger { return stdslog.Default() }

// New creates standard library logger using handler.
func New(handler stdslog.Handler) *stdslog.Logger { return stdslog.New(handler) }

// Package slog provides gstream's standard-library logging default.
package slog

import (
	stdslog "log/slog"

	"github.com/mortezaPRK/gstream/logging"
)

// Default returns standard library default logger.
func Default() logging.Logger { return stdslog.Default() }

// New creates standard library logger using handler.
func New(handler stdslog.Handler) logging.Logger { return stdslog.New(handler) }

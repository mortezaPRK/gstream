package gstream

import (
	"time"

	"mortz.dev/go/gstream/internal/topology"
)

// Window is a half-open time interval [Start, End) in Unix milliseconds.
type Window struct {
	Start int64 // inclusive
	End   int64 // exclusive
}

// Windowed pairs a key with the window it belongs to.
// Produced by windowed aggregation operators as the composite key type for the
// resulting KTable (e.g. KTable[Windowed[K], int64]).
type Windowed[K any] struct {
	Key    K
	Window Window
}

// WindowDefinition assigns windows to records and reports the maximum window
// duration used for late-record boundary calculations.
type WindowDefinition interface {
	// Assign returns all windows containing a record with timestamp ts (Unix ms).
	// For tumbling windows this is exactly one window; for hopping windows it
	// may be several overlapping windows.
	Assign(ts int64) []Window
	// MaxSizeMs returns the maximum window size in milliseconds.
	// The runtime uses this to compute the late-record boundary:
	//   lateBoundary = streamTime - MaxSizeMs() - graceMs
	MaxSizeMs() int64
}

// TimestampExtractor extracts an event timestamp (Unix ms) from a Record.
// When nil, the windowed processor falls back to r.Timestamp.
type TimestampExtractor func(r topology.Record) int64

// ---------------------------------------------------------------------------
// Tumbling windows
// ---------------------------------------------------------------------------

type tumblingWindow struct{ sizeMs int64 }

// TumblingWindows returns a WindowDefinition for non-overlapping tumbling
// windows of the given size. Each record belongs to exactly one window.
// Panics when size <= 0.
func TumblingWindows(size time.Duration) WindowDefinition {
	sizeMs := size.Milliseconds()
	if sizeMs <= 0 {
		panic("gstream: TumblingWindows size must be > 0")
	}
	return tumblingWindow{sizeMs: sizeMs}
}

// Assign returns the single window [start, start+sizeMs) that contains ts.
// start = floor(ts / sizeMs) * sizeMs  (floor == truncation for ts >= 0).
func (w tumblingWindow) Assign(ts int64) []Window {
	start := (ts / w.sizeMs) * w.sizeMs
	return []Window{{Start: start, End: start + w.sizeMs}}
}

func (w tumblingWindow) MaxSizeMs() int64 { return w.sizeMs }

// ---------------------------------------------------------------------------
// Hopping windows
// ---------------------------------------------------------------------------

type hoppingWindow struct {
	sizeMs    int64
	advanceMs int64
}

// HoppingWindows returns a WindowDefinition for overlapping hopping windows.
// Windows have the given size and step forward by advance; a single record
// typically belongs to ceil(size/advance) overlapping windows.
// Panics when advance <= 0 or advance > size.
func HoppingWindows(size, advance time.Duration) WindowDefinition {
	sizeMs := size.Milliseconds()
	advanceMs := advance.Milliseconds()
	if advanceMs <= 0 {
		panic("gstream: HoppingWindows advance must be > 0")
	}
	if advanceMs > sizeMs {
		panic("gstream: HoppingWindows advance must be <= size")
	}
	return hoppingWindow{sizeMs: sizeMs, advanceMs: advanceMs}
}

// Assign returns all windows [start, start+sizeMs) whose start is a multiple
// of advanceMs and that contain ts (i.e. start <= ts < start+sizeMs).
//
// Equivalently, start must be in (ts-sizeMs, ts], aligned to advanceMs:
//
//	earliestStart = floor((ts - sizeMs + advanceMs) / advanceMs) * advanceMs, clamped >= 0
//	latestStart   = floor(ts / advanceMs) * advanceMs
func (w hoppingWindow) Assign(ts int64) []Window {
	latestStart := floorDiv(ts, w.advanceMs) * w.advanceMs

	earliestStart := floorDiv(ts-w.sizeMs+w.advanceMs, w.advanceMs) * w.advanceMs
	if earliestStart < 0 {
		earliestStart = 0
	}

	var windows []Window
	for start := earliestStart; start <= latestStart; start += w.advanceMs {
		windows = append(windows, Window{Start: start, End: start + w.sizeMs})
	}
	return windows
}

func (w hoppingWindow) MaxSizeMs() int64 { return w.sizeMs }

// ---------------------------------------------------------------------------
// floorDiv
// ---------------------------------------------------------------------------

// floorDiv returns floor(a/b) for positive b.
// Go's integer division truncates towards zero; for negative a this differs
// from mathematical floor (e.g. -1/5 = 0 in Go but floor(-1/5) = -1).
// This function corrects that: when the remainder is negative, subtract 1.
func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b < 0 {
		q--
	}
	return q
}

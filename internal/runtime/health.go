package runtime

import (
	"sync/atomic"
)

// PipelineHealth is a lightweight fatal-error signal shared between the
// GlobalConsumer tail loop and the kafka.Client run loop.
//
// Contract:
//   - Fail(err) records the first fatal error atomically; subsequent calls are no-ops.
//   - Err() returns the stored error (nil = healthy).
//   - Healthy() returns true when no fatal error has been recorded.
//
// Design intent (minimal surface area):
// Only un-retryable store-write (Pebble) failures should call Fail.
// Transient fetch errors, serde errors, and user-logic errors use the existing
// abort+redelivery path; they must NOT call Fail.
type PipelineHealth struct {
	// err stores a *fatalErr pointer atomically.  nil = healthy.
	err atomic.Pointer[fatalErr]
}

type fatalErr struct{ err error }

// Fail records err as the fatal pipeline error.  No-op if already tripped.
func (h *PipelineHealth) Fail(err error) {
	if err == nil {
		return
	}
	h.err.CompareAndSwap(nil, &fatalErr{err: err})
}

// Err returns the stored fatal error, or nil when the pipeline is healthy.
func (h *PipelineHealth) Err() error {
	if p := h.err.Load(); p != nil {
		return p.err
	}
	return nil
}

// Healthy returns true when no fatal error has been recorded.
func (h *PipelineHealth) Healthy() bool {
	return h.err.Load() == nil
}

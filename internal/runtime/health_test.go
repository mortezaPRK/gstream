package runtime

import (
	"errors"
	"testing"
)

// TestPipelineHealth_InitiallyHealthy verifies the zero-value is healthy.
func TestPipelineHealth_InitiallyHealthy(t *testing.T) {
	var h PipelineHealth
	if !h.Healthy() {
		t.Error("zero-value PipelineHealth should be healthy")
	}
	if h.Err() != nil {
		t.Errorf("Err() should be nil initially, got %v", h.Err())
	}
}

// TestPipelineHealth_FailTripsUnhealthy verifies Fail sets the error and
// Healthy() returns false.
func TestPipelineHealth_FailTripsUnhealthy(t *testing.T) {
	var h PipelineHealth
	sentinel := errors.New("disk full")
	h.Fail(sentinel)

	if h.Healthy() {
		t.Error("expected Healthy() == false after Fail")
	}
	if got := h.Err(); !errors.Is(got, sentinel) {
		t.Errorf("Err(): got %v, want sentinel %v", got, sentinel)
	}
}

// TestPipelineHealth_FailOnceOnly verifies that subsequent Fail calls do not
// overwrite the first error.
func TestPipelineHealth_FailOnceOnly(t *testing.T) {
	var h PipelineHealth
	first := errors.New("first error")
	second := errors.New("second error")

	h.Fail(first)
	h.Fail(second) // should be a no-op

	if got := h.Err(); !errors.Is(got, first) {
		t.Errorf("Err() should return first error; got %v", got)
	}
}

// TestPipelineHealth_FailNilNoOp verifies Fail(nil) does not trip the health.
func TestPipelineHealth_FailNilNoOp(t *testing.T) {
	var h PipelineHealth
	h.Fail(nil)
	if !h.Healthy() {
		t.Error("Fail(nil) should not trip health")
	}
}

package kafka

import (
	"context"
	"errors"
	"log/slog"
	"testing"
)

// fakeProducer / fakeCommitter are injected in the batch-level integration test
// (TestRun_ProcessError_SkipsProduceAndCommit) to prove the Run loop behaviour
// without a real broker.  We test processBatch directly here because it is now
// the pure, extractable unit.

// ---------------------------------------------------------------------------
// processBatch unit tests — broker-free
// ---------------------------------------------------------------------------

func TestProcessBatch_AllSucceed(t *testing.T) {
	records := []InRecord{
		{Topic: "t", Partition: 0, Offset: 0, Key: []byte("k0"), Value: []byte("v0")},
		{Topic: "t", Partition: 0, Offset: 1, Key: []byte("k1"), Value: []byte("v1")},
	}
	process := func(_ context.Context, in InRecord) ([]OutRecord, error) {
		return []OutRecord{{Topic: "sink", Key: in.Key, Value: in.Value}}, nil
	}

	outs, ok := processBatch(context.Background(), slog.Default(), records, process)
	if !ok {
		t.Fatal("expected ok=true when all records succeed")
	}
	if len(outs) != 2 {
		t.Fatalf("expected 2 output records, got %d", len(outs))
	}
}

func TestProcessBatch_EmptyInput(t *testing.T) {
	called := false
	process := func(_ context.Context, _ InRecord) ([]OutRecord, error) {
		called = true
		return nil, nil
	}

	outs, ok := processBatch(context.Background(), slog.Default(), nil, process)
	if !ok {
		t.Fatal("expected ok=true for empty input")
	}
	if len(outs) != 0 {
		t.Fatalf("expected 0 outputs, got %d", len(outs))
	}
	if called {
		t.Fatal("process should not be called for empty input")
	}
}

// TestProcessBatch_ErrorOnSecondRecord is the regression test for the ALO bug.
//
// OLD behaviour (before extracting processBatch / fixing Run):
//
//	The record loop only did `break` on error, then execution fell through to
//	Step 2 (produce) and Step 3 (commit).  The first record's output would be
//	produced and ALL offsets committed, silently dropping the failed record.
//
// NEW behaviour (processBatch):
//
//	Returns ok=false and nil outputs on any process error. The caller (Run) skips
//	both produce and commit, so the whole batch is redelivered — at-least-once.
func TestProcessBatch_ErrorOnSecondRecord(t *testing.T) {
	boom := errors.New("boom")
	records := []InRecord{
		{Topic: "t", Partition: 0, Offset: 0, Value: []byte("good")},
		{Topic: "t", Partition: 0, Offset: 1, Value: []byte("bad")},
		{Topic: "t", Partition: 0, Offset: 2, Value: []byte("never-reached")},
	}
	callCount := 0
	process := func(_ context.Context, in InRecord) ([]OutRecord, error) {
		callCount++
		if string(in.Value) == "bad" {
			return nil, boom
		}
		return []OutRecord{{Topic: "sink", Value: in.Value}}, nil
	}

	outs, ok := processBatch(context.Background(), slog.Default(), records, process)

	// Must signal failure — the whole batch must be redelivered.
	if ok {
		t.Fatal("expected ok=false when a record returns an error")
	}
	// Outputs must be nil/empty — no partial produce should occur.
	if len(outs) != 0 {
		t.Fatalf("expected 0 outputs on batch failure, got %d: %v", len(outs), outs)
	}
	// The third record must never be processed — we stop at the first error.
	if callCount != 2 {
		t.Fatalf("expected process called 2 times (good + bad), got %d", callCount)
	}
}

// TestProcessBatch_ErrorOnFirstRecord ensures a failure on record 0 also
// produces ok=false and no outputs, verifying there is no partial-success edge
// case when the very first record fails.
func TestProcessBatch_ErrorOnFirstRecord(t *testing.T) {
	records := []InRecord{
		{Topic: "t", Offset: 0, Value: []byte("bad")},
		{Topic: "t", Offset: 1, Value: []byte("good")},
	}
	process := func(_ context.Context, in InRecord) ([]OutRecord, error) {
		if string(in.Value) == "bad" {
			return nil, errors.New("first record fails")
		}
		return []OutRecord{{Topic: "sink", Value: in.Value}}, nil
	}

	outs, ok := processBatch(context.Background(), slog.Default(), records, process)
	if ok {
		t.Fatal("expected ok=false")
	}
	if len(outs) != 0 {
		t.Fatalf("expected 0 outputs, got %d", len(outs))
	}
}

// TestProcessBatch_NoOutputRecords verifies a process func that returns no
// outputs (filter-style) is treated as success.
func TestProcessBatch_NoOutputRecords(t *testing.T) {
	records := []InRecord{
		{Topic: "t", Offset: 0, Value: []byte("drop-me")},
	}
	process := func(_ context.Context, _ InRecord) ([]OutRecord, error) {
		return nil, nil // filter: drop this record
	}

	outs, ok := processBatch(context.Background(), slog.Default(), records, process)
	if !ok {
		t.Fatal("expected ok=true for filter-style process func")
	}
	if len(outs) != 0 {
		t.Fatalf("expected 0 outputs, got %d", len(outs))
	}
}

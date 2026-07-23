package runtime

// Internal-package tests (package runtime) so unexported sweepSessionStore,
// NewTaskManager, and task are accessible directly.

import (
	"encoding/binary"
	"testing"

	gstream "github.com/mortezaPRK/gstream"
	"github.com/mortezaPRK/gstream/internal/state"
)

// encodeSessionValue builds the session value wire format:
//
//	int64(sessionEnd) big-endian (8 bytes) ‖ accumulatorBytes
//
// Mirrors the format defined by T3 (gstream.EncodeSessionValue) without importing it,
// decoupling T4 tests from T3's timing.
func encodeSessionValue(sEnd int64, acc []byte) []byte {
	buf := make([]byte, 8+len(acc))
	binary.BigEndian.PutUint64(buf[:8], uint64(sEnd))
	copy(buf[8:], acc)
	return buf
}

// TestSweepSessionStore_DeletesExpiredByEnd verifies that sweepSessionStore
// deletes sessions whose END is before the expiry boundary, not sessions
// whose START is before it.
//
// Setup:
//   - Two sessions for key "alice": one old (sEnd well below boundary), one recent.
//   - streamTime advanced so old session is expired; recent is not.
//
// Expected: only old session deleted; tombstone emitted for it; recent survives.
func TestSweepSessionStore_DeletesExpiredByEnd(t *testing.T) {
	t.Parallel()

	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	collector := &state.MutationCollector{}
	store := state.NewKeyValueStoreWithChangelog[[]byte, []byte](
		"sess", db, gstream.BytesSerde{}, gstream.BytesSerde{}, collector,
	)

	kBytes := []byte("alice")

	// Old session: start=0, end=1000ms — will be expired when streamTime=50000.
	// gapMs=5000, graceMs=0: expiryBoundary = 50000 - 5000 - 0 = 45000; sEnd=1000 < 45000.
	oldStart := int64(0)
	oldEnd := int64(1000)
	if err := store.WindowPut(kBytes, oldStart, encodeSessionValue(oldEnd, []byte(`2`))); err != nil {
		t.Fatalf("WindowPut old: %v", err)
	}

	// Recent session: start=48000, end=49000ms — NOT expired.
	// sEnd=49000 >= expiryBoundary=45000.
	recentStart := int64(48000)
	recentEnd := int64(49000)
	if err := store.WindowPut(kBytes, recentStart, encodeSessionValue(recentEnd, []byte(`1`))); err != nil {
		t.Fatalf("WindowPut recent: %v", err)
	}

	// Drain the put mutations so sweep tombstones are isolated.
	_ = collector.Drain()

	streamTime := int64(50000)
	gapMs := int64(5000)
	graceMs := int64(0)

	n, err := sweepSessionStore(store, gapMs, graceMs, streamTime)
	if err != nil {
		t.Fatalf("sweepSessionStore: %v", err)
	}
	if n != 1 {
		t.Errorf("expected 1 deletion, got %d", n)
	}

	// Old session must be gone.
	_, foundOld, err := store.WindowGet(kBytes, oldStart)
	if err != nil {
		t.Fatalf("WindowGet old after sweep: %v", err)
	}
	if foundOld {
		t.Error("old session should have been deleted")
	}

	// Recent session must survive.
	_, foundRecent, err := store.WindowGet(kBytes, recentStart)
	if err != nil {
		t.Fatalf("WindowGet recent after sweep: %v", err)
	}
	if !foundRecent {
		t.Error("recent session should NOT have been deleted")
	}

	// Exactly one tombstone emitted.
	tombstones := collector.Drain()
	if len(tombstones) != 1 {
		t.Fatalf("expected 1 tombstone, got %d", len(tombstones))
	}
	if _, ok := tombstones[0].(state.Delete); !ok {
		t.Errorf("expected state.Delete tombstone, got %T", tombstones[0])
	}
}

// TestSweepSessionStore_AmortizationSkip verifies that when expiryBoundary<=0
// (streamTime too low) sweepSessionStore returns 0 deletions without touching
// any entries.
func TestSweepSessionStore_AmortizationSkip(t *testing.T) {
	t.Parallel()

	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	collector := &state.MutationCollector{}
	store := state.NewKeyValueStoreWithChangelog[[]byte, []byte](
		"sess", db, gstream.BytesSerde{}, gstream.BytesSerde{}, collector,
	)

	kBytes := []byte("bob")
	// Write a session that would normally be expired.
	if err := store.WindowPut(kBytes, 0, encodeSessionValue(100, []byte(`1`))); err != nil {
		t.Fatalf("WindowPut: %v", err)
	}
	_ = collector.Drain()

	// streamTime=100, gapMs=5000, graceMs=0 → expiryBoundary = 100 - 5000 - 0 = -4900 ≤ 0.
	n, err := sweepSessionStore(store, 5000, 0, 100)
	if err != nil {
		t.Fatalf("sweepSessionStore: %v", err)
	}
	if n != 0 {
		t.Errorf("expected 0 deletions (amortization skip), got %d", n)
	}

	// Entry must still be present.
	_, found, err := store.WindowGet(kBytes, 0)
	if err != nil {
		t.Fatalf("WindowGet: %v", err)
	}
	if !found {
		t.Error("entry should still be present when expiryBoundary<=0")
	}

	// No mutations.
	if muts := collector.Drain(); len(muts) != 0 {
		t.Errorf("expected 0 mutations, got %d", len(muts))
	}
}

// TestOpenTask_SessionOnly_UsesStreamTimeExecutor verifies that a BuiltTopology
// with only SessionStoreBindings (no WindowStoreBindings) causes computeSweepInterval
// and the stream-time OR-condition to treat it as a stream-time topology.
//
// Broker-free: we test the computeSweepInterval and the Executor() guard
// (zero-store lazy path) rather than calling openTask which requires a live broker
// for ChangelogProducer. This directly exercises the frozen-path OR-extensions.
func TestOpenTask_SessionOnly_UsesStreamTimeExecutor(t *testing.T) {
	t.Parallel()

	// Build a BuiltTopology with a session store binding only (no window stores).
	// We build it manually since the session DSL (T3) may not be landed yet.
	bt := &gstream.BuiltTopology{
		Topology:             nil, // not needed for these assertions
		Sources:              map[string]gstream.SourceBinding{},
		Sinks:                map[string]gstream.SinkBinding{},
		StoreBindings:        map[string]gstream.StoreBinding{},
		WindowStoreBindings:  map[string]gstream.WindowStoreBinding{},
		SessionStoreBindings: map[string]gstream.SessionStoreBinding{
			"my-sess": {
				StoreBinding: gstream.StoreBinding{
					StoreName:      "my-sess",
					ChangelogTopic: "my-sess",
				},
				GapMs:   10_000,
				GraceMs: 1_000,
			},
		},
	}

	cfg, err := gstream.Configure(
		gstream.WithName("test-sess"),
		gstream.WithBrokers("localhost:9092"),
	)
	if err != nil {
		t.Fatalf("Configure: %v", err)
	}

	tm := NewTaskManager(bt, cfg, nil)

	// computeSweepInterval must return GapMs (10000) as the max interval.
	interval := tm.computeSweepInterval()
	if interval != 10_000 {
		t.Errorf("computeSweepInterval: got %d, want 10000", interval)
	}

	// Executor() for session-only topology must return nil (not the lazy zero-store path),
	// because SessionStoreBindings is non-empty — task must be assigned first via openTask.
	exec := tm.Executor(0)
	if exec != nil {
		t.Errorf("Executor for session-only topology: expected nil (requires openTask), got non-nil")
	}
}

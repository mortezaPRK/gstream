package runtime

// Internal package tests (package runtime, not runtime_test) so we can exercise
// unexported functions like sweepWindowStore directly, per the P3-T4 task guidance.

import (
	"context"
	"testing"
	"time"

	gstream "github.com/mortezaPRK/gstream"
	"github.com/mortezaPRK/gstream/internal/state"
	"github.com/mortezaPRK/gstream/internal/topology"
)

// buildWindowedTopology builds a BuiltTopology with a windowed Count store:
//
//	KStream[string,string] → GroupByKey → WindowedBy(Tumbling 10s).WithGrace(0s).Count("wc")
//
// Returns the BuiltTopology and the WindowStoreBinding for "wc".
func buildWindowedTopology(t *testing.T) *gstream.BuiltTopology {
	t.Helper()
	b := gstream.NewStreamBuilder()
	src := gstream.Stream[string, string](
		b,
		"input-topic",
		"source",
		gstream.JSONSerde[string]{},
		gstream.JSONSerde[string]{},
	)
	src.GroupByKey(gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{}).
		WindowedBy(gstream.TumblingWindows(10 * time.Second)).
		WithGrace(0).
		Count("wc")
	return b.Build()
}

// TestSweepWindowStore_DeletesExpiredAndEmitsTombstone is the primary P3-T4 unit test.
//
// It exercises sweepWindowStore (without a broker) and verifies:
//
//	(a) Window counts are correct after processing records.
//	(b) After stream-time advances past a window's expiry, sweep DELETES that
//	    window's key from the store AND appends a tombstone to the collector.
//	(c) Stream-time is persisted and readable via WriteStreamTime/ReadStreamTime.
func TestSweepWindowStore_DeletesExpiredAndEmitsTombstone(t *testing.T) {
	t.Parallel()

	bt := buildWindowedTopology(t)
	binding, ok := bt.WindowStoreBindings["wc"]
	if !ok {
		t.Fatal("expected WindowStoreBinding for 'wc'")
	}

	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	// Build store + collector.
	collector := &state.MutationCollector{}
	store := state.NewKeyValueStoreWithChangelog[[]byte, []byte](
		"wc",
		db,
		gstream.BytesSerde{},
		gstream.BytesSerde{},
		collector,
	)

	stores := map[string]any{"wc": store}

	// (a) Process records via NewExecutorWithStreamTime.
	// window 0–10s: records at t=1000ms and t=5000ms both fall into window [0,10000).
	var streamTime int64
	exec := topology.NewExecutorWithStreamTime(bt.Topology, stores, &streamTime)

	feed := func(key string, tsMs int64) {
		t.Helper()
		kbytes, err := gstream.JSONSerde[string]{}.Serialize(key)
		if err != nil {
			t.Fatalf("serialize key: %v", err)
		}
		_ = kbytes // key is used inside the processor via the topology record
		if err := exec.Process(context.Background(), "source", topology.Record{
			Key:       key,
			Value:     key,
			Timestamp: tsMs,
		}); err != nil {
			t.Fatalf("Process(%q, ts=%d): %v", key, tsMs, err)
		}
	}

	// Two records for "foo" in window [0, 10000). Stream-time advances to 5000.
	feed("foo", 1000)
	feed("foo", 5000)

	if streamTime != 5000 {
		t.Errorf("streamTime after feeding: got %d, want 5000", streamTime)
	}

	// Verify counts via the byte store.
	checkWindowCount := func(key string, windowStartMs int64, wantCount int64) {
		t.Helper()
		kbytes, err := gstream.JSONSerde[string]{}.Serialize(key)
		if err != nil {
			t.Fatalf("serialize key %q: %v", key, err)
		}
		valBytes, found, err := store.WindowGet(kbytes, windowStartMs)
		if err != nil {
			t.Fatalf("WindowGet(%q, %d): %v", key, windowStartMs, err)
		}
		if !found {
			t.Fatalf("WindowGet(%q, %d): key not found", key, windowStartMs)
		}
		got, err := gstream.JSONSerde[int64]{}.Deserialize(valBytes)
		if err != nil {
			t.Fatalf("deserialize count: %v", err)
		}
		if got != wantCount {
			t.Errorf("count[%q][win=%d]: got %d, want %d", key, windowStartMs, got, wantCount)
		}
	}

	// "foo" in window [0, 10000) should have count=2.
	checkWindowCount("foo", 0, 2)

	// (b) Sweep with stream-time NOT yet past expiry (no expiry yet).
	// expiryBoundary = 5000 - 10000 - 0 = -5000 → negative → skip.
	n, err := sweepWindowStore(store, binding.WindowDef, binding.GraceMs, streamTime)
	if err != nil {
		t.Fatalf("sweepWindowStore (no-expiry): %v", err)
	}
	if n != 0 {
		t.Errorf("expected 0 deletions before expiry, got %d", n)
	}

	// Drain mutations so far (from the two WindowPut calls) — we're interested in
	// the tombstones emitted by the sweep, so clear the collector first.
	_ = collector.Drain()

	// Advance stream-time to 25000ms so window [0,10000) is past the expiry.
	// expiryBoundary = 25000 - 10000 - 0 = 15000; windowStart=0 < 15000 → expired.
	streamTime = 25000

	n, err = sweepWindowStore(store, binding.WindowDef, binding.GraceMs, streamTime)
	if err != nil {
		t.Fatalf("sweepWindowStore (with expiry): %v", err)
	}
	if n != 1 {
		t.Errorf("expected 1 deletion (window [0,10000) for 'foo'), got %d", n)
	}

	// Verify the key is gone from the store.
	kbytes, _ := gstream.JSONSerde[string]{}.Serialize("foo")
	_, found, err := store.WindowGet(kbytes, 0)
	if err != nil {
		t.Fatalf("WindowGet after sweep: %v", err)
	}
	if found {
		t.Error("expected window key to be deleted after sweep, but it still exists")
	}

	// Verify tombstone was appended to the collector.
	tombstones := collector.Drain()
	if len(tombstones) != 1 {
		t.Fatalf("expected 1 tombstone mutation, got %d", len(tombstones))
	}
	if _, ok := tombstones[0].(state.Delete); !ok {
		t.Errorf("expected state.Delete tombstone, got %T", tombstones[0])
	}

	// (c) Persist stream-time and verify readback.
	if err := state.WriteStreamTime(db, streamTime); err != nil {
		t.Fatalf("WriteStreamTime: %v", err)
	}
	readTs, found, err := state.ReadStreamTime(db)
	if err != nil {
		t.Fatalf("ReadStreamTime: %v", err)
	}
	if !found {
		t.Fatal("ReadStreamTime: expected found=true after write")
	}
	if readTs != streamTime {
		t.Errorf("ReadStreamTime: got %d, want %d", readTs, streamTime)
	}
}

// TestSweepWindowStore_AmortizationSkipsEarlyCall verifies that runSweep is a
// no-op when stream-time has not advanced by >= sweepInterval since lastSweepTime.
func TestSweepWindowStore_AmortizationSkipsEarlyCall(t *testing.T) {
	t.Parallel()

	bt := buildWindowedTopology(t)
	binding, ok := bt.WindowStoreBindings["wc"]
	if !ok {
		t.Fatal("expected WindowStoreBinding for 'wc'")
	}
	_ = binding

	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	collector := &state.MutationCollector{}
	store := state.NewKeyValueStoreWithChangelog[[]byte, []byte](
		"wc", db, gstream.BytesSerde{}, gstream.BytesSerde{}, collector,
	)

	// Write an entry that would be expired at high stream-time.
	kbytes, _ := gstream.JSONSerde[string]{}.Serialize("key")
	valBytes, _ := gstream.JSONSerde[int64]{}.Serialize(1)
	if err := store.WindowPut(kbytes, 0, valBytes); err != nil {
		t.Fatalf("WindowPut: %v", err)
	}
	_ = collector.Drain() // clear the put mutation

	cfg, err := gstream.Configure(
		gstream.WithName("test"),
		gstream.WithBrokers("localhost:9092"),
	)
	if err != nil {
		t.Fatalf("Configure: %v", err)
	}
	tm := NewTaskManager(bt, cfg, nil)

	// Set up a fake task where lastSweepTime == streamTime (no advancement).
	fakeTask := &task{
		db:            db,
		stores:        map[string]any{"wc": store},
		collectors:    map[string]*state.MutationCollector{"wc": collector},
		streamTime:    25000,
		lastSweepTime: 25000, // no advancement since last sweep
	}

	// runSweep should skip because streamTime - lastSweepTime = 0 < sweepInterval (10000).
	if err := tm.runSweep(fakeTask); err != nil {
		t.Fatalf("runSweep: %v", err)
	}

	// Window key should still be present (sweep was skipped).
	_, found, err := store.WindowGet(kbytes, 0)
	if err != nil {
		t.Fatalf("WindowGet: %v", err)
	}
	if !found {
		t.Error("expected key to be present after amortized-skip sweep")
	}
	if muts := collector.Drain(); len(muts) != 0 {
		t.Errorf("expected 0 mutations from skipped sweep, got %d", len(muts))
	}
}

// TestWindowStoreWiring_ExecutorProcessesAndSweep tests the full openTask-like
// pattern: build stores map, NewExecutorWithStreamTime, process records, then
// sweep — verifying the end-to-end wiring without a broker.
func TestWindowStoreWiring_ExecutorProcessesAndSweep(t *testing.T) {
	t.Parallel()

	bt := buildWindowedTopology(t)
	binding, ok := bt.WindowStoreBindings["wc"]
	if !ok {
		t.Fatal("expected WindowStoreBinding for 'wc'")
	}

	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	// Build stores map the same way openTask does.
	collector := &state.MutationCollector{}
	store := state.NewKeyValueStoreWithChangelog[[]byte, []byte](
		"wc", db, gstream.BytesSerde{}, gstream.BytesSerde{}, collector,
	)
	stores := map[string]any{"wc": store}

	var streamTime int64
	exec := topology.NewExecutorWithStreamTime(bt.Topology, stores, &streamTime)

	// Feed two records into window [0, 10000).
	for _, ts := range []int64{1000, 3000} {
		if err := exec.Process(context.Background(), "source", topology.Record{
			Key: "bar", Value: "bar", Timestamp: ts,
		}); err != nil {
			t.Fatalf("Process(ts=%d): %v", ts, err)
		}
	}

	// Advance stream-time past expiry for window [0,10000): need streamTime > 10000.
	// Feed a record with ts=25000.
	if err := exec.Process(context.Background(), "source", topology.Record{
		Key: "bar", Value: "bar", Timestamp: 25000,
	}); err != nil {
		t.Fatalf("Process(ts=25000): %v", err)
	}

	// streamTime should now be 25000.
	if streamTime != 25000 {
		t.Errorf("streamTime: got %d, want 25000", streamTime)
	}

	// "bar" in [0, 10000) should have count=2 (the ts=25000 record opened [20000, 30000)).
	kbytes, _ := gstream.JSONSerde[string]{}.Serialize("bar")
	valBytes, found, err := store.WindowGet(kbytes, 0)
	if err != nil || !found {
		t.Fatalf("WindowGet('bar',0): found=%v err=%v", found, err)
	}
	count, _ := gstream.JSONSerde[int64]{}.Deserialize(valBytes)
	if count != 2 {
		t.Errorf("count['bar'][0]: got %d, want 2", count)
	}

	// Drain Put mutations so sweep tombstones are isolated.
	_ = collector.Drain()

	// Sweep: expiryBoundary = 25000 - 10000 - 0 = 15000; window [0,10000) expired.
	n, err := sweepWindowStore(store, binding.WindowDef, binding.GraceMs, streamTime)
	if err != nil {
		t.Fatalf("sweepWindowStore: %v", err)
	}
	// Expect: window [0,10000) for "bar" deleted. window [20000,30000) for "bar" NOT deleted (20000 >= 15000).
	if n != 1 {
		t.Errorf("expected 1 deletion, got %d", n)
	}

	// Verify [0,10000) is gone.
	_, found, err = store.WindowGet(kbytes, 0)
	if err != nil {
		t.Fatalf("WindowGet after sweep: %v", err)
	}
	if found {
		t.Error("window [0,10000) should have been deleted by sweep")
	}

	// Verify [20000,30000) still present.
	valBytes2, found2, err := store.WindowGet(kbytes, 20000)
	if err != nil {
		t.Fatalf("WindowGet [20000,30000): %v", err)
	}
	if !found2 {
		t.Error("window [20000,30000) should NOT have been deleted by sweep")
	}
	count2, _ := gstream.JSONSerde[int64]{}.Deserialize(valBytes2)
	if count2 != 1 {
		t.Errorf("count['bar'][20000]: got %d, want 1", count2)
	}

	// Tombstone emitted.
	muts := collector.Drain()
	if len(muts) != 1 {
		t.Fatalf("expected 1 tombstone, got %d", len(muts))
	}
	if _, ok := muts[0].(state.Delete); !ok {
		t.Errorf("expected state.Delete tombstone, got %T", muts[0])
	}
}

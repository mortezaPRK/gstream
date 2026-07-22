// Package gstream_test provides broker-free exit-criterion tests for
// windowed aggregation. Tests use package gstream_test (external) because
// internal/state imports gstream, so an internal test that imports
// internal/state would cause a circular import.
package gstream_test

import (
	"context"
	"testing"
	"time"

	"github.com/mortezaPRK/gstream"
	"github.com/mortezaPRK/gstream/internal/state"
	"github.com/mortezaPRK/gstream/internal/topology"
)

// TestWindowedCount_OutOfOrderAndGrace is the P3 exit-criterion test.
//
// Pipeline: KStream[string,string] → GroupByKey → WindowedBy(Tumbling 10s).WithGrace(5s).Count("wc")
//
// Records fed (key="k"):
//   - ts=5000   → window [0,10000)      count=1
//   - ts=15000  → window [10000,20000)  count=1  (streamTime advances to 15000)
//   - ts=25000  → window [20000,30000)  count=1  (streamTime advances to 25000)
//   - ts=12000  → window [10000,20000)  count=2  (in-grace: lateBoundary=25000-10000-5000=10000; 12000>=10000 → ACCEPTED)
//   - ts=3000   → window [0,10000)      DROPPED  (3000<10000, lateCount++)
//
// Expected store after all records:
//
//	[0,10000)     → count=1
//	[10000,20000) → count=2
//	[20000,30000) → count=1
//
// Late counter must equal 1.
func TestWindowedCount_OutOfOrderAndGrace(t *testing.T) {
	t.Parallel()

	b := gstream.NewStreamBuilder()
	src := gstream.Stream[string, string](b, "input", "src",
		gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{})

	tws := src.GroupByKey(gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{}).
		WindowedBy(gstream.TumblingWindows(10 * time.Second)).
		WithGrace(5 * time.Second)

	_ = tws.Count("wc")

	bt := b.Build()

	// WindowStoreBinding must be registered.
	if _, ok := bt.WindowStoreBindings["wc"]; !ok {
		t.Fatal("expected WindowStoreBindings[\"wc\"] to be registered")
	}

	// Open an in-memory byte store. Supply *state.KeyValueStore[[]byte,[]byte]
	// which satisfies both the DSL's windowStore interface (WindowGet/WindowPut)
	// and the test's direct WindowGet call below.
	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	byteStore := state.NewKeyValueStoreWithChangelog[[]byte, []byte](
		"wc", db, gstream.BytesSerde{}, gstream.BytesSerde{}, nil,
	)

	var streamTime int64
	exec := topology.NewExecutorWithStreamTime(bt.Topology,
		map[string]any{"wc": byteStore}, &streamTime)

	// Feed records.
	type rec struct{ ts int64 }
	recs := []rec{{5000}, {15000}, {25000}, {12000}, {3000}}
	for _, r := range recs {
		if err := exec.Process(context.Background(), "src", topology.Record{Key: "k", Value: "v", Timestamp: r.ts}); err != nil {
			t.Fatalf("Process ts=%d: %v", r.ts, err)
		}
	}

	// Assert late counter.
	if got := tws.LateCount(); got != 1 {
		t.Errorf("LateCount: got %d, want 1", got)
	}

	// Assert window counts via byteStore.WindowGet — no hand-rolled composite key.
	// The format lives once in state.WindowCompositeKey (called internally by WindowGet).
	keySerde := gstream.JSONSerde[string]{}
	intSerde := gstream.JSONSerde[int64]{}

	checkCount := func(winStartMs int64, want int64) {
		t.Helper()
		kb, err := keySerde.Serialize("k")
		if err != nil {
			t.Fatalf("serialize key: %v", err)
		}
		valBytes, found, err := byteStore.WindowGet(kb, winStartMs)
		if err != nil {
			t.Fatalf("WindowGet window [%d,...): %v", winStartMs, err)
		}
		if !found {
			t.Fatalf("WindowGet window [%d,...): key not found (want count=%d)", winStartMs, want)
		}
		count, err := intSerde.Deserialize(valBytes)
		if err != nil {
			t.Fatalf("deserialize count for window [%d,...): %v", winStartMs, err)
		}
		if count != want {
			t.Errorf("count window [%d,...): got %d, want %d", winStartMs, count, want)
		}
	}

	checkCount(0, 1)     // [0,10000)     → 1
	checkCount(10000, 2) // [10000,20000) → 2
	checkCount(20000, 1) // [20000,30000) → 1
}

// TestWindowedCount_AllLate verifies that when stream-time is well ahead, all
// records outside the grace window are dropped.
func TestWindowedCount_AllLate(t *testing.T) {
	t.Parallel()

	b := gstream.NewStreamBuilder()
	src := gstream.Stream[string, string](b, "input", "src",
		gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{})

	tws := src.GroupByKey(gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{}).
		WindowedBy(gstream.TumblingWindows(10 * time.Second)).
		WithGrace(0)

	_ = tws.Count("wc2")

	bt := b.Build()

	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	byteStore := state.NewKeyValueStoreWithChangelog[[]byte, []byte](
		"wc2", db, gstream.BytesSerde{}, gstream.BytesSerde{}, nil,
	)

	var streamTime int64
	exec := topology.NewExecutorWithStreamTime(bt.Topology,
		map[string]any{"wc2": byteStore}, &streamTime)

	// Advance stream-time with a high-ts record first.
	if err := exec.Process(context.Background(), "src", topology.Record{Key: "k", Value: "v", Timestamp: 50000}); err != nil {
		t.Fatalf("Process ts=50000: %v", err)
	}

	// Now send a record at ts=1000; lateBoundary = 50000-10000-0=40000; 1000<40000 → DROPPED.
	if err := exec.Process(context.Background(), "src", topology.Record{Key: "k", Value: "v", Timestamp: 1000}); err != nil {
		t.Fatalf("Process ts=1000: %v", err)
	}

	if got := tws.LateCount(); got != 1 {
		t.Errorf("LateCount: got %d, want 1", got)
	}
}

// TestWindowedCount_WindowStoreBinding verifies the WindowStoreBinding metadata.
func TestWindowedCount_WindowStoreBinding(t *testing.T) {
	t.Parallel()

	b := gstream.NewStreamBuilder()
	src := gstream.Stream[string, string](b, "input", "src",
		gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{})
	_ = src.GroupByKey(gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{}).
		WindowedBy(gstream.TumblingWindows(10 * time.Second)).
		WithGrace(3 * time.Second).
		Count("my-window-store")

	bt := b.Build()

	wsb, ok := bt.WindowStoreBindings["my-window-store"]
	if !ok {
		t.Fatal("WindowStoreBindings[\"my-window-store\"] not found")
	}
	if wsb.StoreName != "my-window-store" {
		t.Errorf("StoreName: got %q, want %q", wsb.StoreName, "my-window-store")
	}
	if wsb.GraceMs != 3000 {
		t.Errorf("GraceMs: got %d, want 3000", wsb.GraceMs)
	}
	if wsb.WindowDef == nil {
		t.Error("WindowDef is nil")
	}
	if wsb.WindowDef != nil && wsb.WindowDef.MaxSizeMs() != 10000 {
		t.Errorf("WindowDef.MaxSizeMs(): got %d, want 10000", wsb.WindowDef.MaxSizeMs())
	}
}

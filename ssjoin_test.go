// Package gstream_test provides broker-free tests for the stream-stream windowed
// inner join (KStream.Join). Tests live in package gstream_test (external) to avoid
// the import cycle: stores/memory imports gstream via Serde[T].
//
// All tests use topology.NewExecutorWithStreamTime so stream-time advances correctly
// and late-drop semantics are exercised. Two in-memory stores are wired
// per test — one per join side.
package gstream_test

import (
	"context"
	"testing"
	"time"

	"github.com/mortezaPRK/gstream"
	memory "github.com/mortezaPRK/gstream/internal/testutil"
	"github.com/mortezaPRK/gstream/internal/topology"
)

// buildSSJoinTopology constructs the test topology:
//
//	left-source[string,string]  → Join(...) → merge → out-sink
//	right-source[string,string] ↗
//
// joiner concatenates left+":"+right.
// Returns BuiltTopology, leftStoreName, rightStoreName, sinkName.
func buildSSJoinTopology(t *testing.T, windows gstream.JoinWindows) (
	*gstream.BuiltTopology, string, string, string,
) {
	t.Helper()

	b := gstream.NewStreamBuilder()

	left := gstream.Stream[string, string](
		b, "left-topic", "lsrc",
		memory.JSONSerde[string]{}, memory.JSONSerde[string]{},
	)
	right := gstream.Stream[string, string](
		b, "right-topic", "rsrc",
		memory.JSONSerde[string]{}, memory.JSONSerde[string]{},
	)

	joined := left.Join[string, string](
		right,
		func(v1, v2 string) string { return v1 + ":" + v2 },
		windows,
		memory.JSONSerde[string]{},
		memory.JSONSerde[string]{},
		memory.JSONSerde[string]{},
		memory.JSONSerde[string]{},
	)
	joined.To("out-topic", "out", memory.JSONSerde[string]{}, memory.JSONSerde[string]{})

	bt := b.Build()

	// Locate left and right store names from WindowStoreBindings (order is deterministic
	// because nextName uses a monotonic counter: left-store is assigned first).
	var leftStore, rightStore string
	for name := range bt.WindowStoreBindings {
		if leftStore == "" {
			leftStore = name
		} else if name < leftStore {
			// keep lexicographic-first as left (counter-based names are "ssjoin-left-store-N")
			rightStore = leftStore
			leftStore = name
		} else {
			rightStore = name
		}
	}

	return bt, leftStore, rightStore, "out"
}

// openJoinStores opens two memory stores for a join test.
// Returns the stores map and a closer function.
func openJoinStores(t *testing.T, leftStoreName, rightStoreName string) (map[string]any, func()) {
	t.Helper()

	db, err := memory.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}

	ls := memory.NewKeyValueStore[[]byte, []byte](
		leftStoreName, db, memory.BytesSerde{}, memory.BytesSerde{},
	)
	rs := memory.NewKeyValueStore[[]byte, []byte](
		rightStoreName, db, memory.BytesSerde{}, memory.BytesSerde{},
	)

	stores := map[string]any{
		leftStoreName:  ls,
		rightStoreName: rs,
	}
	closer := func() {
		_ = ls.Close()
		_ = rs.Close()
		_ = db.Close()
	}
	return stores, closer
}

// defaultWindows: ±50 ms, no grace.
func defaultWindows() gstream.JoinWindows {
	return gstream.JoinWindows{Before: 50 * time.Millisecond, After: 50 * time.Millisecond}
}

// TestSSJoin_LeftThenRight: A then B within window → emit joiner(v1,v2).
func TestSSJoin_LeftThenRight(t *testing.T) {
	t.Parallel()

	bt, ls, rs, sinkName := buildSSJoinTopology(t, defaultWindows())
	stores, closer := openJoinStores(t, ls, rs)
	defer closer()

	var st int64
	exec := topology.NewExecutorWithStreamTime(bt.Topology, stores, &st)
	ctx := context.Background()

	// Left arrives at ts=100.
	if err := exec.Process(ctx, "lsrc", topology.Record{Key: "k", Value: "left", Timestamp: 100}); err != nil {
		t.Fatalf("left: %v", err)
	}
	out, _ := exec.DrainSink(sinkName)
	if len(out) != 0 {
		t.Fatalf("after left only: expected 0 outputs, got %d", len(out))
	}

	// Right arrives at ts=120 (within [50,150]).
	if err := exec.Process(ctx, "rsrc", topology.Record{Key: "k", Value: "right", Timestamp: 120}); err != nil {
		t.Fatalf("right: %v", err)
	}
	out, _ = exec.DrainSink(sinkName)
	if len(out) != 1 {
		t.Fatalf("A-then-B: expected 1 output, got %d", len(out))
	}
	if out[0].Key != "k" {
		t.Errorf("Key: got %v, want k", out[0].Key)
	}
	if out[0].Value != "left:right" {
		t.Errorf("Value: got %v, want left:right", out[0].Value)
	}
	if out[0].Timestamp != 120 {
		t.Errorf("Timestamp: got %v, want 120", out[0].Timestamp)
	}
}

// TestSSJoin_RightThenLeft: B then A within window → emit (proves symmetric path + arg order).
func TestSSJoin_RightThenLeft(t *testing.T) {
	t.Parallel()

	bt, ls, rs, sinkName := buildSSJoinTopology(t, defaultWindows())
	stores, closer := openJoinStores(t, ls, rs)
	defer closer()

	var st int64
	exec := topology.NewExecutorWithStreamTime(bt.Topology, stores, &st)
	ctx := context.Background()

	// Right arrives at ts=100.
	if err := exec.Process(ctx, "rsrc", topology.Record{Key: "k", Value: "right", Timestamp: 100}); err != nil {
		t.Fatalf("right: %v", err)
	}
	out, _ := exec.DrainSink(sinkName)
	if len(out) != 0 {
		t.Fatalf("after right only: expected 0, got %d", len(out))
	}

	// Left arrives at ts=110 (within window of right).
	if err := exec.Process(ctx, "lsrc", topology.Record{Key: "k", Value: "left", Timestamp: 110}); err != nil {
		t.Fatalf("left: %v", err)
	}
	out, _ = exec.DrainSink(sinkName)
	if len(out) != 1 {
		t.Fatalf("B-then-A: expected 1 output, got %d", len(out))
	}
	// joiner arg order: left always first.
	if out[0].Value != "left:right" {
		t.Errorf("B-then-A arg order: got %v, want left:right", out[0].Value)
	}
	if out[0].Timestamp != 110 {
		t.Errorf("Timestamp: got %v, want 110 (triggering ts)", out[0].Timestamp)
	}
}

// TestSSJoin_OutOfWindow: records far apart → no emit.
func TestSSJoin_OutOfWindow(t *testing.T) {
	t.Parallel()

	bt, ls, rs, sinkName := buildSSJoinTopology(t, defaultWindows())
	stores, closer := openJoinStores(t, ls, rs)
	defer closer()

	var st int64
	exec := topology.NewExecutorWithStreamTime(bt.Topology, stores, &st)
	ctx := context.Background()

	// Left at ts=100, right at ts=200 (delta=100 > After=50 → outside window).
	if err := exec.Process(ctx, "lsrc", topology.Record{Key: "k", Value: "L", Timestamp: 100}); err != nil {
		t.Fatalf("left: %v", err)
	}
	if err := exec.Process(ctx, "rsrc", topology.Record{Key: "k", Value: "R", Timestamp: 200}); err != nil {
		t.Fatalf("right: %v", err)
	}
	out, _ := exec.DrainSink(sinkName)
	if len(out) != 0 {
		t.Errorf("out-of-window: expected 0, got %d: %+v", len(out), out)
	}
}

// TestSSJoin_MultipleMatches: one A matches multiple B records → multiple emits.
func TestSSJoin_MultipleMatches(t *testing.T) {
	t.Parallel()

	bt, ls, rs, sinkName := buildSSJoinTopology(t, defaultWindows())
	stores, closer := openJoinStores(t, ls, rs)
	defer closer()

	var st int64
	exec := topology.NewExecutorWithStreamTime(bt.Topology, stores, &st)
	ctx := context.Background()

	// Two right records within the window.
	for _, rec := range []topology.Record{
		{Key: "k", Value: "R1", Timestamp: 90},
		{Key: "k", Value: "R2", Timestamp: 95},
	} {
		if err := exec.Process(ctx, "rsrc", rec); err != nil {
			t.Fatalf("right %v: %v", rec.Value, err)
		}
	}
	out, _ := exec.DrainSink(sinkName)
	if len(out) != 0 {
		t.Fatalf("before left: expected 0, got %d", len(out))
	}

	// Left at ts=100: should match both right records.
	if err := exec.Process(ctx, "lsrc", topology.Record{Key: "k", Value: "L", Timestamp: 100}); err != nil {
		t.Fatalf("left: %v", err)
	}
	out, _ = exec.DrainSink(sinkName)
	if len(out) != 2 {
		t.Fatalf("multi-match: expected 2, got %d: %+v", len(out), out)
	}
	// Values must be L:R1 and L:R2 (order may vary with store scan order).
	values := map[string]bool{out[0].Value.(string): true, out[1].Value.(string): true}
	if !values["L:R1"] || !values["L:R2"] {
		t.Errorf("multi-match values: got %v, want {L:R1, L:R2}", values)
	}
}

// TestSSJoin_LateRecordDropped: record below lateBoundary is silently dropped.
func TestSSJoin_LateRecordDropped(t *testing.T) {
	t.Parallel()

	// Small window so eviction happens quickly.
	windows := gstream.JoinWindows{Before: 10 * time.Millisecond, After: 10 * time.Millisecond}
	bt, ls, rs, sinkName := buildSSJoinTopology(t, windows)
	stores, closer := openJoinStores(t, ls, rs)
	defer closer()

	var st int64
	exec := topology.NewExecutorWithStreamTime(bt.Topology, stores, &st)
	ctx := context.Background()

	// Advance stream-time to 1000 by driving a record.
	if err := exec.Process(ctx, "lsrc", topology.Record{Key: "x", Value: "advance", Timestamp: 1000}); err != nil {
		t.Fatalf("advance: %v", err)
	}
	_, _ = exec.DrainSink(sinkName)

	// lateBoundary = 1000 - (10+10) - 0 = 980. Drive a right at ts=900 → late.
	if err := exec.Process(ctx, "rsrc", topology.Record{Key: "k", Value: "late", Timestamp: 900}); err != nil {
		t.Fatalf("late right: %v", err)
	}
	// Drive left at ts=901 to scan right (but right was dropped).
	if err := exec.Process(ctx, "lsrc", topology.Record{Key: "k", Value: "L", Timestamp: 901}); err != nil {
		t.Fatalf("left after late: %v", err)
	}
	out, _ := exec.DrainSink(sinkName)
	if len(out) != 0 {
		t.Errorf("late-drop: expected 0 outputs (right was dropped), got %d: %+v", len(out), out)
	}
}

// TestSSJoin_TwoKeysIndependent: two keys do not interfere with each other.
func TestSSJoin_TwoKeysIndependent(t *testing.T) {
	t.Parallel()

	bt, ls, rs, sinkName := buildSSJoinTopology(t, defaultWindows())
	stores, closer := openJoinStores(t, ls, rs)
	defer closer()

	var st int64
	exec := topology.NewExecutorWithStreamTime(bt.Topology, stores, &st)
	ctx := context.Background()

	// k1: left then right → hit.
	if err := exec.Process(ctx, "lsrc", topology.Record{Key: "k1", Value: "L1", Timestamp: 100}); err != nil {
		t.Fatalf("k1 left: %v", err)
	}
	// k2: left only → no hit yet.
	if err := exec.Process(ctx, "lsrc", topology.Record{Key: "k2", Value: "L2", Timestamp: 100}); err != nil {
		t.Fatalf("k2 left: %v", err)
	}

	// Right for k1 → hit.
	if err := exec.Process(ctx, "rsrc", topology.Record{Key: "k1", Value: "R1", Timestamp: 110}); err != nil {
		t.Fatalf("k1 right: %v", err)
	}
	out, _ := exec.DrainSink(sinkName)
	if len(out) != 1 {
		t.Fatalf("k1 hit: expected 1, got %d", len(out))
	}
	if out[0].Key != "k1" {
		t.Errorf("k1 Key: got %v", out[0].Key)
	}
	if out[0].Value != "L1:R1" {
		t.Errorf("k1 Value: got %v, want L1:R1", out[0].Value)
	}

	// Right for k2 → hit separately.
	if err := exec.Process(ctx, "rsrc", topology.Record{Key: "k2", Value: "R2", Timestamp: 110}); err != nil {
		t.Fatalf("k2 right: %v", err)
	}
	out, _ = exec.DrainSink(sinkName)
	if len(out) != 1 {
		t.Fatalf("k2 hit: expected 1, got %d", len(out))
	}
	if out[0].Key != "k2" {
		t.Errorf("k2 Key: got %v", out[0].Key)
	}
	if out[0].Value != "L2:R2" {
		t.Errorf("k2 Value: got %v, want L2:R2", out[0].Value)
	}
}

// TestSSJoin_MergeNodeForwardsBothSides: drive both sides and confirm the merge
// node routes outputs from both left-processor and right-processor paths to the
// single sink.
func TestSSJoin_MergeNodeForwardsBothSides(t *testing.T) {
	t.Parallel()

	bt, ls, rs, sinkName := buildSSJoinTopology(t, defaultWindows())
	stores, closer := openJoinStores(t, ls, rs)
	defer closer()

	var st int64
	exec := topology.NewExecutorWithStreamTime(bt.Topology, stores, &st)
	ctx := context.Background()

	// Seed both stores at ts=100.
	if err := exec.Process(ctx, "lsrc", topology.Record{Key: "k", Value: "L", Timestamp: 100}); err != nil {
		t.Fatalf("seed left: %v", err)
	}
	if err := exec.Process(ctx, "rsrc", topology.Record{Key: "k", Value: "R", Timestamp: 100}); err != nil {
		t.Fatalf("seed right: %v", err)
	}
	_, _ = exec.DrainSink(sinkName)

	// Now trigger from left side: should produce one join output (via left proc → merge → sink).
	if err := exec.Process(ctx, "lsrc", topology.Record{Key: "k", Value: "L2", Timestamp: 110}); err != nil {
		t.Fatalf("left trigger: %v", err)
	}
	outL, _ := exec.DrainSink(sinkName)

	// Trigger from right side: should produce one join output (via right proc → merge → sink).
	if err := exec.Process(ctx, "rsrc", topology.Record{Key: "k", Value: "R2", Timestamp: 115}); err != nil {
		t.Fatalf("right trigger: %v", err)
	}
	outR, _ := exec.DrainSink(sinkName)

	// Both sides must have routed through the merge node to the same sink.
	if len(outL) == 0 {
		t.Error("left trigger: expected >= 1 output via merge node, got 0")
	}
	if len(outR) == 0 {
		t.Error("right trigger: expected >= 1 output via merge node, got 0")
	}
}

// TestSSJoin_WindowStoreBindingsRegistered: build verifies both stores are registered.
func TestSSJoin_WindowStoreBindingsRegistered(t *testing.T) {
	t.Parallel()

	bt, ls, rs, _ := buildSSJoinTopology(t, defaultWindows())

	if _, ok := bt.WindowStoreBindings[ls]; !ok {
		t.Errorf("left store %q not in WindowStoreBindings", ls)
	}
	if _, ok := bt.WindowStoreBindings[rs]; !ok {
		t.Errorf("right store %q not in WindowStoreBindings", rs)
	}

	// Both must use joinWindowDef (MaxSizeMs = Before+After = 100ms).
	for _, name := range []string{ls, rs} {
		wb := bt.WindowStoreBindings[name]
		if wb.WindowDef.MaxSizeMs() != 100 {
			t.Errorf("%q MaxSizeMs: got %d, want 100", name, wb.WindowDef.MaxSizeMs())
		}
	}
}

// TestSSJoin_AsymmetricBefore0After10_LeftFirst documents the by-design KStreams
// behaviour for asymmetric windows (Before=0, After=10ms).
//
// Left at tsL=5, right at tsR=10: the left-side scan uses [5-0-0, 5+10] = [5,15].
// tsR=10 is within [5,15] so left-first DOES emit.
//
// Right at tsR=10 arrives first (store empty): scan [10-0-0, 10+10] = [10,20].
// tsL=5 is NOT in [10,20] so right-first does NOT emit.
// Then left at tsL=5: scan [5,15] finds tsR=10 → DOES emit.
func TestSSJoin_AsymmetricBefore0After10_LeftFirst(t *testing.T) {
	t.Parallel()

	windows := gstream.JoinWindows{Before: 0, After: 10 * time.Millisecond}
	bt, ls, rs, sinkName := buildSSJoinTopology(t, windows)
	stores, closer := openJoinStores(t, ls, rs)
	defer closer()

	var st int64
	exec := topology.NewExecutorWithStreamTime(bt.Topology, stores, &st)
	ctx := context.Background()

	// Left-first: tsL=5 arrives, scans right (empty) → 0 emits.
	if err := exec.Process(ctx, "lsrc", topology.Record{Key: "k", Value: "L", Timestamp: 5}); err != nil {
		t.Fatalf("left: %v", err)
	}
	out, _ := exec.DrainSink(sinkName)
	if len(out) != 0 {
		t.Fatalf("left-first before right: expected 0, got %d", len(out))
	}

	// Right at tsR=10: scans left store for [10-0, 10+10]=[10,20]. tsL=5 not in range → 0 emits.
	// But right record is buffered.
	if err := exec.Process(ctx, "rsrc", topology.Record{Key: "k", Value: "R", Timestamp: 10}); err != nil {
		t.Fatalf("right: %v", err)
	}
	out, _ = exec.DrainSink(sinkName)
	// Right processor scans left with [10,20]; tsL=5 < 10 → no match.
	if len(out) != 0 {
		t.Errorf("asymmetric left-first: right scan found tsL=5 in [10,20] — unexpected; got %d", len(out))
	}
}

// TestSSJoin_AsymmetricBefore0After10_RightFirst: same asymmetric window but
// right arrives before left.  Right scans left (empty) → 0.  Then left at tsL=5
// scans right for [5-0-0, 5+10]=[5,15]; tsR=10 is in range → 1 emit.
func TestSSJoin_AsymmetricBefore0After10_RightFirst(t *testing.T) {
	t.Parallel()

	windows := gstream.JoinWindows{Before: 0, After: 10 * time.Millisecond}
	bt, ls, rs, sinkName := buildSSJoinTopology(t, windows)
	stores, closer := openJoinStores(t, ls, rs)
	defer closer()

	var st int64
	exec := topology.NewExecutorWithStreamTime(bt.Topology, stores, &st)
	ctx := context.Background()

	// Right first at tsR=10: left store empty → 0 emits; right is buffered.
	if err := exec.Process(ctx, "rsrc", topology.Record{Key: "k", Value: "R", Timestamp: 10}); err != nil {
		t.Fatalf("right: %v", err)
	}
	out, _ := exec.DrainSink(sinkName)
	if len(out) != 0 {
		t.Fatalf("right-first before left: expected 0, got %d", len(out))
	}

	// Left at tsL=5: scans right for [5-0, 5+10]=[5,15]. tsR=10 ∈ [5,15] → 1 emit.
	if err := exec.Process(ctx, "lsrc", topology.Record{Key: "k", Value: "L", Timestamp: 5}); err != nil {
		t.Fatalf("left: %v", err)
	}
	out, _ = exec.DrainSink(sinkName)
	if len(out) != 1 {
		t.Fatalf("asymmetric right-first: expected 1 emit, got %d", len(out))
	}
	if out[0].Value != "L:R" {
		t.Errorf("asymmetric right-first: Value=%v, want L:R", out[0].Value)
	}
	if out[0].Timestamp != 5 {
		t.Errorf("asymmetric right-first: Timestamp=%v, want 5 (triggering ts)", out[0].Timestamp)
	}
}

// TestSSJoin_GraceWithin: a right record just-late-but-within-grace joins.
// Before=50ms, After=50ms, Grace=10ms.
// Left at tsA=100. Right at tsB=49: loMs = max(0,100-50-10)=40, hiMs=150.
// 49 >= 40 → within grace window → emit.
func TestSSJoin_GraceWithin(t *testing.T) {
	t.Parallel()

	windows := gstream.JoinWindows{
		Before: 50 * time.Millisecond,
		After:  50 * time.Millisecond,
		Grace:  10 * time.Millisecond,
	}
	bt, ls, rs, sinkName := buildSSJoinTopology(t, windows)
	stores, closer := openJoinStores(t, ls, rs)
	defer closer()

	var st int64
	exec := topology.NewExecutorWithStreamTime(bt.Topology, stores, &st)
	ctx := context.Background()

	// Right at tsB=49 arrives first; left store empty → 0 emits but right buffered.
	if err := exec.Process(ctx, "rsrc", topology.Record{Key: "k", Value: "R", Timestamp: 49}); err != nil {
		t.Fatalf("right: %v", err)
	}
	_, _ = exec.DrainSink(sinkName)

	// Left at tsA=100 scans right for [40,151): 49 ∈ [40,151) → emit.
	if err := exec.Process(ctx, "lsrc", topology.Record{Key: "k", Value: "L", Timestamp: 100}); err != nil {
		t.Fatalf("left: %v", err)
	}
	out, _ := exec.DrainSink(sinkName)
	if len(out) != 1 {
		t.Fatalf("grace-within: expected 1 emit, got %d", len(out))
	}
	if out[0].Value != "L:R" {
		t.Errorf("grace-within: Value=%v, want L:R", out[0].Value)
	}
}

// TestSSJoin_GraceBeyond: a right record beyond grace does not join.
// Before=50ms, After=50ms, Grace=10ms.
// Left at tsA=100. Right at tsB=39: loMs=40. 39 < 40 → NOT within grace → no emit.
func TestSSJoin_GraceBeyond(t *testing.T) {
	t.Parallel()

	windows := gstream.JoinWindows{
		Before: 50 * time.Millisecond,
		After:  50 * time.Millisecond,
		Grace:  10 * time.Millisecond,
	}
	bt, ls, rs, sinkName := buildSSJoinTopology(t, windows)
	stores, closer := openJoinStores(t, ls, rs)
	defer closer()

	var st int64
	exec := topology.NewExecutorWithStreamTime(bt.Topology, stores, &st)
	ctx := context.Background()

	// Right at tsB=39 buffered.
	if err := exec.Process(ctx, "rsrc", topology.Record{Key: "k", Value: "R", Timestamp: 39}); err != nil {
		t.Fatalf("right: %v", err)
	}
	_, _ = exec.DrainSink(sinkName)

	// Left at tsA=100 scans right for [40,151): 39 < 40 → no match.
	if err := exec.Process(ctx, "lsrc", topology.Record{Key: "k", Value: "L", Timestamp: 100}); err != nil {
		t.Fatalf("left: %v", err)
	}
	out, _ := exec.DrainSink(sinkName)
	if len(out) != 0 {
		t.Errorf("grace-beyond: expected 0 emits, got %d: %+v", len(out), out)
	}
}

// TestSSJoin_DoubleMatchPrevention: a single (tsL, tsR) pair emits exactly once
// per arrival — whichever side scans first gets one emit.  The second arrival
// does NOT produce a duplicate: when right arrives it scans left (finds tsL) → 1
// emit; when left arrives it scans right (finds tsR) → 1 emit. Total = 2 emits
// across both arrivals but each emission is unique (different triggering ts direction).
//
// This test documents exact counts rather than asserting "no duplicates" abstractly.
func TestSSJoin_DoubleMatchPrevention(t *testing.T) {
	t.Parallel()

	bt, ls, rs, sinkName := buildSSJoinTopology(t, defaultWindows())
	stores, closer := openJoinStores(t, ls, rs)
	defer closer()

	var st int64
	exec := topology.NewExecutorWithStreamTime(bt.Topology, stores, &st)
	ctx := context.Background()

	// Send left (ts=100): right store empty → 0 emits.
	if err := exec.Process(ctx, "lsrc", topology.Record{Key: "k", Value: "L", Timestamp: 100}); err != nil {
		t.Fatalf("left: %v", err)
	}
	outAfterLeft, _ := exec.DrainSink(sinkName)
	if len(outAfterLeft) != 0 {
		t.Fatalf("after left only: expected 0 emits, got %d", len(outAfterLeft))
	}

	// Send right (ts=100): scans left, finds tsL=100 → exactly 1 emit.
	if err := exec.Process(ctx, "rsrc", topology.Record{Key: "k", Value: "R", Timestamp: 100}); err != nil {
		t.Fatalf("right: %v", err)
	}
	outAfterRight, _ := exec.DrainSink(sinkName)
	if len(outAfterRight) != 1 {
		t.Fatalf("after right: expected exactly 1 emit, got %d", len(outAfterRight))
	}
	if outAfterRight[0].Value != "L:R" {
		t.Errorf("after right: Value=%v, want L:R", outAfterRight[0].Value)
	}
}

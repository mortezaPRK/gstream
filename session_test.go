// Package gstream_test provides broker-free tests for session-window aggregation.
// Uses package gstream_test (external) because internal/state imports gstream,
// so an internal test importing internal/state would create a circular import.
package gstream_test

import (
	"context"
	"testing"
	"time"

	"github.com/mortezaPRK/gstream"
	"github.com/mortezaPRK/gstream/internal/state"
	"github.com/mortezaPRK/gstream/internal/topology"
)

// newSessionCount builds a pipeline:
//
//	KStream[string,string] → GroupByKey → SessionWindowedBy(gap).WithGrace(grace).Count(storeName)
//
// Returns the built topology, the stream-time pointer, the executor, the byte store, and the
// SessionWindowedStream (for LateCount).
func newSessionCount(
	t *testing.T,
	gap, grace time.Duration,
	storeName string,
) (*gstream.BuiltTopology, *int64, *topology.Executor, *state.KeyValueStore[[]byte, []byte], gstream.SessionWindowedStream[string, string]) {
	t.Helper()

	b := gstream.NewStreamBuilder()
	src := gstream.Stream[string, string](b, "input", "src",
		gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{})

	sws := src.GroupByKey(gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{}).
		SessionWindowedBy(gstream.SessionWindow(gap)).
		WithGrace(grace)

	_ = sws.Count(storeName)

	bt := b.Build()

	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	byteStore := state.NewKeyValueStoreWithChangelog[[]byte, []byte](
		storeName, db, gstream.BytesSerde{}, gstream.BytesSerde{}, nil,
	)

	var streamTime int64
	exec := topology.NewExecutorWithStreamTime(bt.Topology,
		map[string]any{storeName: byteStore}, &streamTime)

	return bt, &streamTime, exec, byteStore, sws
}

// readSession retrieves the count and session bounds for key k from byteStore.
// Returns (count, sessionStart, sessionEnd, found).
func readSession(t *testing.T, byteStore *state.KeyValueStore[[]byte, []byte], key string, expectedStart int64) (count int64, sessionStart int64, sessionEnd int64, found bool) {
	t.Helper()

	keySerde := gstream.JSONSerde[string]{}
	kBytes, err := keySerde.Serialize(key)
	if err != nil {
		t.Fatalf("serialize key %q: %v", key, err)
	}

	// Find the session that starts at expectedStart.
	valBytes, ok, err := byteStore.WindowGet(kBytes, expectedStart)
	if err != nil {
		t.Fatalf("WindowGet sStart=%d: %v", expectedStart, err)
	}
	if !ok {
		return 0, 0, 0, false
	}

	sessionEnd, accBytes, err := gstream.DecodeSessionValue(valBytes)
	if err != nil {
		t.Fatalf("DecodeSessionValue: %v", err)
	}

	intSerde := gstream.JSONSerde[int64]{}
	cnt, err := intSerde.Deserialize(accBytes)
	if err != nil {
		t.Fatalf("deserialize count: %v", err)
	}
	return cnt, expectedStart, sessionEnd, true
}

// countSessions counts how many sessions exist for key k.
func countSessions(t *testing.T, byteStore *state.KeyValueStore[[]byte, []byte], key string) int {
	t.Helper()

	keySerde := gstream.JSONSerde[string]{}
	kBytes, err := keySerde.Serialize(key)
	if err != nil {
		t.Fatalf("serialize key %q: %v", key, err)
	}

	var n int
	if err := byteStore.RangeForKey(kBytes, func(_ int64, _ []byte) bool {
		n++
		return true
	}); err != nil {
		t.Fatalf("RangeForKey: %v", err)
	}
	return n
}

// TestSessionCount_Basic: 3 records for one key within the gap → one merged session, count=3.
//
// gap=10s, records at ts=1000,5000,9000 (all within 10s of each other).
// Expected: one session [1000,9000], count=3.
func TestSessionCount_Basic(t *testing.T) {
	t.Parallel()

	_, _, exec, byteStore, _ := newSessionCount(t, 10*time.Second, 0, "sc-basic")

	timestamps := []int64{1000, 5000, 9000}
	for _, ts := range timestamps {
		if err := exec.Process(context.Background(), "src",
			topology.Record{Key: "k", Value: "v", Timestamp: ts}); err != nil {
			t.Fatalf("Process ts=%d: %v", ts, err)
		}
	}

	// Expect exactly one session.
	if n := countSessions(t, byteStore, "k"); n != 1 {
		t.Fatalf("expected 1 session, got %d", n)
	}

	cnt, sStart, sEnd, found := readSession(t, byteStore, "k", 1000)
	if !found {
		t.Fatal("session starting at 1000 not found")
	}
	if sStart != 1000 {
		t.Errorf("session start: got %d, want 1000", sStart)
	}
	if sEnd != 9000 {
		t.Errorf("session end: got %d, want 9000", sEnd)
	}
	if cnt != 3 {
		t.Errorf("count: got %d, want 3", cnt)
	}
}

// TestSessionCount_Bridging: two separate sessions bridged by an out-of-order record.
//
// gap=15s=15000ms, grace=30s=30000ms.
//
// Build phase (records in wall-clock order, ascending ts):
//   - ts=20000 → session A=[20000,20000], count=1
//   - ts=30000 → merges into A=[20000,30000], count=2   (30000-20000=10000<15000)
//   - ts=60000 → new session B=[60000,60000], count=1   (60000-30000=30000>15000)
//   - ts=70000 → merges into B=[60000,70000], count=2   (70000-60000=10000<15000)
//
// streamTime=70000, lateBoundary=70000-15000-30000=25000.
//
// Out-of-order bridge record at ts=45000 (> lateBoundary=25000 → ACCEPTED):
//   A=[20000,30000]: sEnd(30000)+15000=45000>=45000 ✓; sStart(20000)-15000=5000<=45000 ✓ → MATCH
//   B=[60000,70000]: sEnd(70000)+15000=85000>=45000 ✓; sStart(60000)-15000=45000<=45000 ✓ → MATCH
//
// Both match → merged into [20000,70000], count=5.
func TestSessionCount_Bridging(t *testing.T) {
	t.Parallel()

	// grace=30s ensures the bridge record at ts=45000 is not dropped as late after
	// streamTime is advanced to 70000: lateBoundary=70000-15000-30000=25000 < 45000.
	_, _, exec, byteStore, _ := newSessionCount(t, 15*time.Second, 30*time.Second, "sc-bridge")

	// Build session A: two records within gap.
	for _, ts := range []int64{20000, 30000} {
		if err := exec.Process(context.Background(), "src",
			topology.Record{Key: "k", Value: "v", Timestamp: ts}); err != nil {
			t.Fatalf("Process ts=%d: %v", ts, err)
		}
	}
	// Build session B: two records within gap but >15000ms from session A end.
	for _, ts := range []int64{60000, 70000} {
		if err := exec.Process(context.Background(), "src",
			topology.Record{Key: "k", Value: "v", Timestamp: ts}); err != nil {
			t.Fatalf("Process ts=%d: %v", ts, err)
		}
	}

	// Verify two sessions before bridge.
	if n := countSessions(t, byteStore, "k"); n != 2 {
		t.Fatalf("before bridge: expected 2 sessions, got %d", n)
	}

	// Out-of-order bridge record at ts=45000.
	// lateBoundary=70000-15000-30000=25000; 45000>25000 → accepted.
	// A=[20000,30000]: sEnd(30000)+15000=45000>=45000 ✓; sStart(20000)-15000=5000<=45000 ✓
	// B=[60000,70000]: sEnd(70000)+15000=85000>=45000 ✓; sStart(60000)-15000=45000<=45000 ✓
	if err := exec.Process(context.Background(), "src",
		topology.Record{Key: "k", Value: "v", Timestamp: 45000}); err != nil {
		t.Fatalf("Process ts=45000: %v", err)
	}

	// Expect exactly one merged session.
	if n := countSessions(t, byteStore, "k"); n != 1 {
		t.Fatalf("after bridge: expected 1 session, got %d", n)
	}

	cnt, sStart, sEnd, found := readSession(t, byteStore, "k", 20000)
	if !found {
		t.Fatal("merged session starting at 20000 not found")
	}
	if sStart != 20000 {
		t.Errorf("merged session start: got %d, want 20000", sStart)
	}
	if sEnd != 70000 {
		t.Errorf("merged session end: got %d, want 70000", sEnd)
	}
	if cnt != 5 {
		t.Errorf("merged session count: got %d, want 5", cnt)
	}
}

// TestSessionCount_GraceLate: record older than streamTime-gap-grace is dropped.
//
// gap=10s, grace=5s.
// Record 1 at ts=100000 advances streamTime to 100000.
// lateBoundary = 100000 - 10000 - 5000 = 85000.
// Record 2 at ts=80000 < 85000 → dropped; LateCount==1; sessions unchanged.
func TestSessionCount_GraceLate(t *testing.T) {
	t.Parallel()

	_, _, exec, byteStore, sws := newSessionCount(t, 10*time.Second, 5*time.Second, "sc-grace")

	// Advance stream-time.
	if err := exec.Process(context.Background(), "src",
		topology.Record{Key: "k", Value: "v", Timestamp: 100000}); err != nil {
		t.Fatalf("Process ts=100000: %v", err)
	}

	// Late record.
	if err := exec.Process(context.Background(), "src",
		topology.Record{Key: "k", Value: "v", Timestamp: 80000}); err != nil {
		t.Fatalf("Process ts=80000: %v", err)
	}

	// LateCount must be 1.
	if got := sws.LateCount(); got != 1 {
		t.Errorf("LateCount: got %d, want 1", got)
	}

	// Sessions for key "k": only the one at ts=100000.
	if n := countSessions(t, byteStore, "k"); n != 1 {
		t.Fatalf("expected 1 session (from ts=100000), got %d", n)
	}
	cnt, _, sEnd, found := readSession(t, byteStore, "k", 100000)
	if !found {
		t.Fatal("session at 100000 not found")
	}
	if sEnd != 100000 {
		t.Errorf("session end: got %d, want 100000", sEnd)
	}
	if cnt != 1 {
		t.Errorf("count for ts=100000 session: got %d, want 1", cnt)
	}
}

// TestSessionCount_SessionStoreBinding verifies the SessionStoreBinding metadata.
func TestSessionCount_SessionStoreBinding(t *testing.T) {
	t.Parallel()

	b := gstream.NewStreamBuilder()
	src := gstream.Stream[string, string](b, "input", "src",
		gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{})
	_ = src.GroupByKey(gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{}).
		SessionWindowedBy(gstream.SessionWindow(15 * time.Second)).
		WithGrace(3 * time.Second).
		Count("my-session-store")

	bt := b.Build()

	ssb, ok := bt.SessionStoreBindings["my-session-store"]
	if !ok {
		t.Fatal("SessionStoreBindings[\"my-session-store\"] not found")
	}
	if ssb.StoreName != "my-session-store" {
		t.Errorf("StoreName: got %q, want %q", ssb.StoreName, "my-session-store")
	}
	if ssb.GapMs != 15000 {
		t.Errorf("GapMs: got %d, want 15000", ssb.GapMs)
	}
	if ssb.GraceMs != 3000 {
		t.Errorf("GraceMs: got %d, want 3000", ssb.GraceMs)
	}
}

// TestDecodeSessionValue_TooShort verifies that DecodeSessionValue errors on short input.
func TestDecodeSessionValue_TooShort(t *testing.T) {
	t.Parallel()

	_, _, err := gstream.DecodeSessionValue([]byte{1, 2, 3})
	if err == nil {
		t.Fatal("expected error on 3-byte input, got nil")
	}
}

// newSessionAggregate builds a pipeline:
//
//	KStream[string,string] → GroupByKey → SessionWindowedBy(gap).WithGrace(grace).Aggregate(...)
//
// Returns the built topology, the stream-time pointer, the executor, the byte store.
func newSessionAggregate[A any](
	t *testing.T,
	gap, grace time.Duration,
	storeName string,
	initFn func() A,
	aggFn func(string, string, A) A,
	mergeFn func(string, A, A) A,
	accSerde gstream.Serde[A],
) (*gstream.BuiltTopology, *int64, *topology.Executor, *state.KeyValueStore[[]byte, []byte]) {
	t.Helper()

	b := gstream.NewStreamBuilder()
	src := gstream.Stream[string, string](b, "input", "src",
		gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{})

	_ = src.GroupByKey(gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{}).
		SessionWindowedBy(gstream.SessionWindow(gap)).
		WithGrace(grace).
		Aggregate(storeName, initFn, aggFn, mergeFn, accSerde)

	bt := b.Build()

	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	byteStore := state.NewKeyValueStoreWithChangelog[[]byte, []byte](
		storeName, db, gstream.BytesSerde{}, gstream.BytesSerde{}, nil,
	)

	var streamTime int64
	exec := topology.NewExecutorWithStreamTime(bt.Topology,
		map[string]any{storeName: byteStore}, &streamTime)

	return bt, &streamTime, exec, byteStore
}

// readSessionA is like readSession but for a generic accumulator A.
func readSessionA[A any](t *testing.T, byteStore *state.KeyValueStore[[]byte, []byte], key string, expectedStart int64, accSerde gstream.Serde[A]) (acc A, sessionStart int64, sessionEnd int64, found bool) {
	t.Helper()

	keySerde := gstream.JSONSerde[string]{}
	kBytes, err := keySerde.Serialize(key)
	if err != nil {
		t.Fatalf("serialize key %q: %v", key, err)
	}

	valBytes, ok, err := byteStore.WindowGet(kBytes, expectedStart)
	if err != nil {
		t.Fatalf("WindowGet sStart=%d: %v", expectedStart, err)
	}
	if !ok {
		var zero A
		return zero, 0, 0, false
	}

	sEnd, accBytes, err := gstream.DecodeSessionValue(valBytes)
	if err != nil {
		t.Fatalf("DecodeSessionValue: %v", err)
	}

	result, err := accSerde.Deserialize(accBytes)
	if err != nil {
		t.Fatalf("deserialize acc: %v", err)
	}
	return result, expectedStart, sEnd, true
}

// TestSessionAggregate_NonIdentityMerge verifies that the session merge seeds from
// the first matched accumulator rather than from initFn().
//
// If the code incorrectly folds initFn() into the merge (the pre-fix behaviour),
// a mergeFn where initFn() is NOT an identity element corrupts the result.
//
// Setup: init=100, mergeFn=min(a,b), aggFn(k,v,acc)=acc+1.
//
// Build two separate sessions (gap=10s, grace=30s):
//   - ts=1000  → acc = aggFn("k","v",initFn()) = 100+1 = 101  session A=[1000,1000]
//   - ts=50000 → acc = 101                                      session B=[50000,50000]
//
// streamTime=50000, lateBoundary=50000-10000-30000=10000.
//
// Bridge record at ts=30000 (>lateBoundary=10000 → accepted):
//   A=[1000,1000]:   sEnd(1000)+10000=11000 >= 30000? NO → does NOT match A.
//   B=[50000,50000]: sEnd(50000)+10000=60000>=30000 ✓; sStart(50000)-10000=40000<=30000? NO → does NOT match B.
//
// Hmm, need to reconsider the gap to make them both match.  Use gap=30s=30000ms:
//   A=[1000,1000]:   sEnd(1000)+30000=31000>=30000 ✓; sStart(1000)-30000=-29000<=30000 ✓ → MATCH
//   B=[50000,50000]: sEnd(50000)+30000=80000>=30000 ✓; sStart(50000)-30000=20000<=30000 ✓ → MATCH
//   lateBoundary = 50000-30000-30000 = -10000 < 30000 → accepted.
//
// Merge with old (buggy) code: mergedAcc = min(100, A.acc=101) = 100,
//                              mergedAcc = min(100, B.acc=101) = 100,
//                              mergedAcc = aggFn("k","v",100) = 101.
// Merge with new (correct) code: mergedAcc = A.acc = 101,
//                                mergedAcc = min(101, B.acc=101) = 101,
//                                mergedAcc = aggFn("k","v",101) = 102.
//
// Assert merged acc == 102.
func TestSessionAggregate_NonIdentityMerge(t *testing.T) {
	t.Parallel()

	// init=100 is NOT an identity for min (identity for min over int64 would be MaxInt64).
	initFn := func() int64 { return 100 }
	aggFn := func(_ string, _ string, acc int64) int64 { return acc + 1 }
	mergeFn := func(_ string, a, b int64) int64 {
		if a < b {
			return a
		}
		return b
	}
	accSerde := gstream.JSONSerde[int64]{}

	_, _, exec, byteStore := newSessionAggregate(
		t,
		30*time.Second,  // gap
		30*time.Second,  // grace
		"non-identity",
		initFn,
		aggFn,
		mergeFn,
		accSerde,
	)

	// Record 1 at ts=1000 → creates session A=[1000,1000], acc=101.
	if err := exec.Process(context.Background(), "src",
		topology.Record{Key: "k", Value: "v", Timestamp: 1000}); err != nil {
		t.Fatalf("Process ts=1000: %v", err)
	}
	// Record 2 at ts=50000 → creates session B=[50000,50000], acc=101.
	if err := exec.Process(context.Background(), "src",
		topology.Record{Key: "k", Value: "v", Timestamp: 50000}); err != nil {
		t.Fatalf("Process ts=50000: %v", err)
	}

	// Sanity: two separate sessions before bridge.
	if n := countSessions(t, byteStore, "k"); n != 2 {
		t.Fatalf("before bridge: expected 2 sessions, got %d", n)
	}

	// Bridge record at ts=30000: lateBoundary=50000-30000-30000=-10000 → accepted.
	// Both A and B match (see predicate analysis in doc above).
	if err := exec.Process(context.Background(), "src",
		topology.Record{Key: "k", Value: "v", Timestamp: 30000}); err != nil {
		t.Fatalf("Process ts=30000: %v", err)
	}

	// Expect exactly one merged session.
	if n := countSessions(t, byteStore, "k"); n != 1 {
		t.Fatalf("after bridge: expected 1 session, got %d", n)
	}

	// Correct result: seed from A.acc=101, min(101,B.acc=101)=101, +1=102.
	// Old (buggy) result: min(initFn()=100, A.acc=101)=100, min(100, B.acc=101)=100, +1=101.
	acc, sStart, sEnd, found := readSessionA(t, byteStore, "k", 1000, accSerde)
	if !found {
		t.Fatal("merged session starting at 1000 not found")
	}
	if sStart != 1000 {
		t.Errorf("merged session start: got %d, want 1000", sStart)
	}
	if sEnd != 50000 {
		t.Errorf("merged session end: got %d, want 50000", sEnd)
	}
	if acc != 102 {
		t.Errorf("merged acc: got %d, want 102 (buggy pre-fix value would be 101)", acc)
	}
}

// TestEncodeDecodeSessionValue_Roundtrip verifies round-trip encode/decode.
func TestEncodeDecodeSessionValue_Roundtrip(t *testing.T) {
	t.Parallel()

	accBytes := []byte("hello-acc")
	sessionEnd := int64(12345678)
	encoded := gstream.EncodeSessionValue(sessionEnd, accBytes)

	gotEnd, gotAcc, err := gstream.DecodeSessionValue(encoded)
	if err != nil {
		t.Fatalf("DecodeSessionValue: %v", err)
	}
	if gotEnd != sessionEnd {
		t.Errorf("sessionEnd: got %d, want %d", gotEnd, sessionEnd)
	}
	if string(gotAcc) != string(accBytes) {
		t.Errorf("accBytes: got %q, want %q", gotAcc, accBytes)
	}
}

// Package gstream_test provides broker-free tests for the stateful DSL
// operators: GroupByKey, Count, and Aggregate. Tests live in package
// gstream_test (external) rather than package gstream because
// internal/state imports gstream (for Serde[T]), so a package-internal test
// that imports internal/state would create a circular import.
//
// # Store type: []byte/[]byte (Option A)
//
// After the P2-S7fix type-erasure boundary fix, stateful operators assert against
// a kvBytesStore (Get([]byte)([]byte,bool,error) / Put([]byte,[]byte)error). The
// runtime supplies a *state.KeyValueStore[[]byte,[]byte] with identity (BytesSerde)
// serdes. Tests must wire the store accordingly — using KeyValueStore[string,int64]
// would still satisfy the test assertion on the *store* variable but would fail
// the runtime kvBytesStore assertion inside the StatefulProcessFunc.
package gstream_test

import (
	"testing"

	"github.com/mortezaPRK/gstream"
	"github.com/mortezaPRK/gstream/internal/state"
	"github.com/mortezaPRK/gstream/internal/topology"
)

// ---------------------------------------------------------------------------
// Count tests
// ---------------------------------------------------------------------------

// TestGroupByKey_Count verifies that Count correctly accumulates per-key
// record counts using a real *state.KeyValueStore[[]byte,[]byte] byte store
// wired into a topology.Executor, and asserts store contents by decoding with
// JSONSerde[int64].
//
// Pipeline: source[string,string] → GroupByKey → Count("wc")
//
// This test exercises the REAL store.Get/Put path (not a fake), using the same
// store type the runtime supplies: NewKeyValueStoreWithChangelog[[]byte,[]byte]
// with identity BytesSerde. The Aggregate StatefulProcessFunc encodes keys and
// values itself using the captured serdes.
func TestGroupByKey_Count(t *testing.T) {
	t.Parallel()

	b := gstream.NewStreamBuilder()
	src := gstream.Stream[string, string](b, "input", "src", gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{})
	grouped := src.GroupByKey(gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{})
	_ = grouped.Count("wc")

	bt := b.Build()

	// Assert that StoreBinding was registered.
	if _, ok := bt.StoreBindings["wc"]; !ok {
		t.Fatal("expected StoreBindings[\"wc\"] to be registered; not found")
	}
	if bt.StoreBindings["wc"].StoreName != "wc" {
		t.Errorf("StoreBinding.StoreName: got %q, want %q", bt.StoreBindings["wc"].StoreName, "wc")
	}

	// Open an in-memory Pebble DB and wire a []byte/[]byte byte store.
	// This is the same type the runtime supplies via NewKeyValueStoreWithChangelog.
	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	// BytesSerde is the identity serde used by the runtime for the bytes boundary.
	byteStore := state.NewKeyValueStoreWithChangelog[[]byte, []byte](
		"wc", db, gstream.BytesSerde{}, gstream.BytesSerde{}, nil,
	)

	stores := map[string]any{"wc": byteStore}
	exec := topology.NewExecutor(bt.Topology, stores)

	// Feed keys: a, b, a, a → a=3, b=1.
	inputs := []string{"a", "b", "a", "a"}
	for _, key := range inputs {
		rec := topology.Record{Key: key, Value: key, Timestamp: 1}
		if err := exec.Process("src", rec); err != nil {
			t.Fatalf("Process key=%q: %v", key, err)
		}
	}

	// Assert store contents by decoding the raw bytes with JSONSerde[int64].
	// The Aggregate processor serialises counts via JSONSerde[int64] (Count delegates
	// to Aggregate with accSerde = JSONSerde[int64]). The key is serialised by
	// JSONSerde[string] — so "a" becomes the JSON bytes `"a"` (with quotes).
	intSerde := gstream.JSONSerde[int64]{}
	keySerde := gstream.JSONSerde[string]{}

	checkCount := func(key string, want int64) {
		t.Helper()
		kb, err := keySerde.Serialize(key)
		if err != nil {
			t.Fatalf("serialize key %q: %v", key, err)
		}
		valBytes, found, err := byteStore.Get(kb)
		if err != nil {
			t.Fatalf("store.Get(%q): %v", key, err)
		}
		if !found {
			t.Fatalf("store.Get(%q): key not found", key)
		}
		count, err := intSerde.Deserialize(valBytes)
		if err != nil {
			t.Fatalf("deserialize count for %q: %v", key, err)
		}
		if count != want {
			t.Errorf("count[%q]: got %d, want %d", key, count, want)
		}
	}

	checkCount("a", 3)
	checkCount("b", 1)
}

// TestCount_NoBufferLeak verifies that driving 1000 records through a
// Count-only topology does not accumulate records in the internal ktable-out
// sink buffer. Because Aggregate never calls ctx.Forward in P2, the
// Executor's sink buffer for "ktable-out-N" stays nil across all calls.
//
// topology.Builder.Build() requires >=1 sink (panics on empty), so an internal
// "ktable-out-N" sink is registered. This test proves that sink is O(1) fixed
// overhead — not an unbounded accumulator — by draining it after 1000 records
// and asserting zero buffered records.
func TestCount_NoBufferLeak(t *testing.T) {
	t.Parallel()

	b := gstream.NewStreamBuilder()
	src := gstream.Stream[string, string](b, "input", "src", gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{})
	_ = src.GroupByKey(gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{}).Count("wc-leak")

	bt := b.Build()

	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	byteStore := state.NewKeyValueStoreWithChangelog[[]byte, []byte](
		"wc-leak", db, gstream.BytesSerde{}, gstream.BytesSerde{}, nil,
	)
	exec := topology.NewExecutor(bt.Topology, map[string]any{"wc-leak": byteStore})

	// Drive 1000 records through the Count-only topology.
	for i := 0; i < 1000; i++ {
		if err := exec.Process("src", topology.Record{Key: "k", Value: "v", Timestamp: int64(i)}); err != nil {
			t.Fatalf("Process[%d]: %v", i, err)
		}
	}

	// The internal ktable-out sink must not have accumulated any records.
	for _, sinkName := range bt.Topology.SinkNames() {
		records, err := exec.DrainSink(sinkName)
		if err != nil {
			t.Fatalf("DrainSink(%q): %v", sinkName, err)
		}
		if len(records) != 0 {
			t.Errorf("DrainSink(%q) after 1000 records: got %d records, want 0 (buffer leak)", sinkName, len(records))
		}
	}

	// Sanity: the store actually counted all 1000 records.
	keySerde := gstream.JSONSerde[string]{}
	intSerde := gstream.JSONSerde[int64]{}
	kb, _ := keySerde.Serialize("k")
	valBytes, found, err := byteStore.Get(kb)
	if err != nil {
		t.Fatalf("store.Get(\"k\"): %v", err)
	}
	if !found {
		t.Fatal("store.Get(\"k\"): key not found after 1000 records")
	}
	count, err := intSerde.Deserialize(valBytes)
	if err != nil {
		t.Fatalf("deserialize count for \"k\": %v", err)
	}
	if count != 1000 {
		t.Errorf("count[\"k\"]: got %d, want 1000", count)
	}
}

// ---------------------------------------------------------------------------
// Aggregate tests
// ---------------------------------------------------------------------------

// TestGroupByKey_Aggregate verifies that Aggregate (called as a method on
// KGroupedStream) correctly sums value lengths per key and that the
// StoreBinding is registered in BuiltTopology.
//
// Pipeline: source[string,string] → GroupByKey → g.Aggregate[int64]("lensum", sum of len(v))
//
// We wire a []byte/[]byte byte store (same type the runtime supplies), drive
// [("a","hi"),("b","world"),("a","go")], and assert lensum["a"]=4 (2+2), lensum["b"]=5.
func TestGroupByKey_Aggregate(t *testing.T) {
	t.Parallel()

	b := gstream.NewStreamBuilder()
	src := gstream.Stream[string, string](b, "input", "src", gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{})
	grouped := src.GroupByKey(gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{})

	_ = grouped.Aggregate[int64](
		"lensum",
		func() int64 { return 0 },
		func(_ string, v string, acc int64) int64 { return acc + int64(len(v)) },
		gstream.JSONSerde[int64]{},
	)

	bt := b.Build()

	// Assert StoreBinding registered.
	if _, ok := bt.StoreBindings["lensum"]; !ok {
		t.Fatal("expected StoreBindings[\"lensum\"] to be registered; not found")
	}

	// Open in-memory byte store.
	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	byteStore := state.NewKeyValueStoreWithChangelog[[]byte, []byte](
		"lensum", db, gstream.BytesSerde{}, gstream.BytesSerde{}, nil,
	)
	exec := topology.NewExecutor(bt.Topology, map[string]any{"lensum": byteStore})

	type kv struct{ key, val string }
	inputs := []kv{{"a", "hi"}, {"b", "world"}, {"a", "go"}}
	for _, pair := range inputs {
		if err := exec.Process("src", topology.Record{Key: pair.key, Value: pair.val, Timestamp: 1}); err != nil {
			t.Fatalf("Process key=%q val=%q: %v", pair.key, pair.val, err)
		}
	}

	// Decode stored results and assert values.
	keySerde := gstream.JSONSerde[string]{}
	intSerde := gstream.JSONSerde[int64]{}

	checkSum := func(key string, want int64) {
		t.Helper()
		kb, err := keySerde.Serialize(key)
		if err != nil {
			t.Fatalf("serialize key %q: %v", key, err)
		}
		valBytes, found, err := byteStore.Get(kb)
		if err != nil {
			t.Fatalf("store.Get(%q): %v", key, err)
		}
		if !found {
			t.Fatalf("store.Get(%q): key not found", key)
		}
		sum, err := intSerde.Deserialize(valBytes)
		if err != nil {
			t.Fatalf("deserialize sum for %q: %v", key, err)
		}
		if sum != want {
			t.Errorf("lensum[%q]: got %d, want %d", key, sum, want)
		}
	}

	// "a": len("hi")+len("go") = 2+2 = 4
	checkSum("a", 4)

	// "b": len("world") = 5
	checkSum("b", 5)
}

// ---------------------------------------------------------------------------
// StoreBinding field tests
// ---------------------------------------------------------------------------

// TestKTable_StoreBinding_Fields verifies the StoreBinding registered by Count
// has the expected StoreName and ChangelogTopic values and non-nil serde closures.
func TestKTable_StoreBinding_Fields(t *testing.T) {
	t.Parallel()

	b := gstream.NewStreamBuilder()
	src := gstream.Stream[string, string](b, "input", "src", gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{})
	_ = src.GroupByKey(gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{}).Count("my-store")
	bt := b.Build()

	sb, ok := bt.StoreBindings["my-store"]
	if !ok {
		t.Fatal("StoreBindings[\"my-store\"] not found")
	}
	if sb.StoreName != "my-store" {
		t.Errorf("StoreName: got %q, want %q", sb.StoreName, "my-store")
	}
	if sb.ChangelogTopic != "my-store" {
		t.Errorf("ChangelogTopic: got %q, want %q", sb.ChangelogTopic, "my-store")
	}
	if sb.EncodeKey == nil || sb.DecodeKey == nil || sb.EncodeVal == nil || sb.DecodeVal == nil {
		t.Error("StoreBinding has nil serde closure(s)")
	}
}

// ---------------------------------------------------------------------------
// Store type assertion test (the regression guard for the P2-S7fix bug)
// ---------------------------------------------------------------------------

// TestAggregate_ByteStoreAssertionSucceeds is the regression test for the
// P2-S7fix type-erasure boundary bug. It verifies that when the runtime supplies
// a *state.KeyValueStore[[]byte,[]byte] (the correct byte-store type), the
// Aggregate StatefulProcessFunc does NOT return a "store type mismatch" error.
//
// The original bug: the processor asserted ctx.Store(name).(kvStoreI[K,A]) with
// concrete K,A (e.g. string,int64). The runtime supplied erasedStore which
// implements kvStoreI[any,any]; since Go generics do not allow covariant interface
// satisfaction, the assertion failed at runtime on the first real record.
//
// The fix (Option A): the processor now asserts ctx.Store(name).(kvBytesStore) and
// does its own serde. A *KeyValueStore[[]byte,[]byte] satisfies kvBytesStore.
func TestAggregate_ByteStoreAssertionSucceeds(t *testing.T) {
	t.Parallel()

	b := gstream.NewStreamBuilder()
	src := gstream.Stream[string, string](b, "input", "src", gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{})
	_ = src.GroupByKey(gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{}).Count("tstore")
	bt := b.Build()

	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	// Supply the CORRECT byte store type.
	byteStore := state.NewKeyValueStoreWithChangelog[[]byte, []byte](
		"tstore", db, gstream.BytesSerde{}, gstream.BytesSerde{}, nil,
	)
	exec := topology.NewExecutor(bt.Topology, map[string]any{"tstore": byteStore})

	// This must NOT return a "store type mismatch" error.
	rec := topology.Record{Key: "x", Value: "y", Timestamp: 1}
	if err := exec.Process("src", rec); err != nil {
		t.Fatalf("Process: unexpected error (P2-S7fix regression): %v", err)
	}
}

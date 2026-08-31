// Package gstream_test provides broker-free tests for the stateful DSL
// operators: GroupByKey, Count, and Aggregate. Tests live in package
// gstream_test (external) rather than package gstream because
// store/memory imports gstream (for Serde[T]), so a package-internal test
// that imports it would create a circular import.
//
// # Store type: []byte/[]byte (Option A)
//
// After the P2-S7fix type-erasure boundary fix, stateful operators assert against
// a kvBytesStore (Get([]byte)([]byte,bool,error) / Put([]byte,[]byte)error). The
// runtime supplies a compatible byte store with identity (BytesSerde) serdes.
// Tests must wire the store accordingly — using KeyValueStore[string,int64]
// would still satisfy the test assertion on the *store* variable but would fail
// the runtime kvBytesStore assertion inside the StatefulProcessFunc.
package gstream_test

import (
	"context"
	"testing"

	"github.com/mortezaPRK/gstream"
	"github.com/mortezaPRK/gstream/internal/topology"
	"github.com/mortezaPRK/gstream/store/memory"
)

// ---------------------------------------------------------------------------
// Count tests
// ---------------------------------------------------------------------------

// TestGroupByKey_Count verifies that Count correctly accumulates per-key
// record counts using an in-memory []byte/[]byte store
// wired into a topology.Executor, and asserts store contents by decoding with
// JSONSerde[int64].
//
// Pipeline: source[string,string] → GroupByKey → Count("wc")
//
// This test exercises store.Get/Put through public memory store, using same
// byte-store contract as runtime. Aggregate StatefulProcessFunc encodes keys and
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

	// Open an in-memory DB and wire a []byte/[]byte byte store.
	// Memory store satisfies same byte-store contract as durable runtime store.
	db, err := memory.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	// BytesSerde is the identity serde used by the runtime for the bytes boundary.
	byteStore := memory.NewKeyValueStore[[]byte, []byte](
		"wc", db, gstream.BytesSerde{}, gstream.BytesSerde{},
	)

	stores := map[string]any{"wc": byteStore}
	exec := topology.NewExecutor(bt.Topology, stores)

	// Feed keys: a, b, a, a → a=3, b=1.
	inputs := []string{"a", "b", "a", "a"}
	for _, key := range inputs {
		rec := topology.Record{Key: key, Value: key, Timestamp: 1}
		if err := exec.Process(context.Background(), "src", rec); err != nil {
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
// Count-only topology (no KTable.To() call) does not accumulate records in the
// internal ktable-out sink buffer when the buffer is drained per-call, mirroring
// the runtime's drainSinks-per-record pattern.
//
// After KTable.To() was introduced, Aggregate calls ctx.Forward on every update.
// The internal ktable-out sink receives one record per input, but since the
// runtime drains after each Process call the buffer never exceeds 1 entry per
// batch. This test proves the drain-per-call pattern keeps the buffer at O(1).
//
// Also asserts that WITHOUT calling To(), bt.Sinks is empty — the SinkBinding is
// not registered and the runtime discards the forwarded records.
func TestCount_NoBufferLeak(t *testing.T) {
	t.Parallel()

	b := gstream.NewStreamBuilder()
	src := gstream.Stream[string, string](b, "input", "src", gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{})
	_ = src.GroupByKey(gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{}).Count("wc-leak")

	bt := b.Build()

	// Without KTable.To(), bt.Sinks must be empty: no SinkBinding registered.
	if len(bt.Sinks) != 0 {
		t.Errorf("expected bt.Sinks empty (To() not called), got %v", bt.Sinks)
	}

	db, err := memory.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	byteStore := memory.NewKeyValueStore[[]byte, []byte](
		"wc-leak", db, gstream.BytesSerde{}, gstream.BytesSerde{},
	)
	exec := topology.NewExecutor(bt.Topology, map[string]any{"wc-leak": byteStore})

	// Drive 1000 records through the Count-only topology, draining after each call
	// (the runtime's per-record drain pattern). After each drain the buffer is empty.
	sinkNames := bt.Topology.SinkNames()
	for i := 0; i < 1000; i++ {
		if err := exec.Process(context.Background(), "src", topology.Record{Key: "k", Value: "v", Timestamp: int64(i)}); err != nil {
			t.Fatalf("Process[%d]: %v", i, err)
		}
		// Drain after each Process, simulating runtime drainSinks.
		for _, sinkName := range sinkNames {
			if _, err := exec.DrainSink(sinkName); err != nil {
				t.Fatalf("DrainSink(%q)[%d]: %v", sinkName, i, err)
			}
		}
	}

	// After draining per-call, the buffer must now be empty.
	for _, sinkName := range sinkNames {
		records, err := exec.DrainSink(sinkName)
		if err != nil {
			t.Fatalf("DrainSink(%q) final: %v", sinkName, err)
		}
		if len(records) != 0 {
			t.Errorf("DrainSink(%q) after per-call drain: got %d records, want 0", sinkName, len(records))
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
	db, err := memory.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	byteStore := memory.NewKeyValueStore[[]byte, []byte](
		"lensum", db, gstream.BytesSerde{}, gstream.BytesSerde{},
	)
	exec := topology.NewExecutor(bt.Topology, map[string]any{"lensum": byteStore})

	type kv struct{ key, val string }
	inputs := []kv{{"a", "hi"}, {"b", "world"}, {"a", "go"}}
	for _, pair := range inputs {
		if err := exec.Process(context.Background(), "src", topology.Record{Key: pair.key, Value: pair.val, Timestamp: 1}); err != nil {
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
// a *memory.KeyValueStore[[]byte,[]byte] (the correct byte-store type), the
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

	db, err := memory.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	// Supply the CORRECT byte store type.
	byteStore := memory.NewKeyValueStore[[]byte, []byte](
		"tstore", db, gstream.BytesSerde{}, gstream.BytesSerde{},
	)
	exec := topology.NewExecutor(bt.Topology, map[string]any{"tstore": byteStore})

	// This must NOT return a "store type mismatch" error.
	rec := topology.Record{Key: "x", Value: "y", Timestamp: 1}
	if err := exec.Process(context.Background(), "src", rec); err != nil {
		t.Fatalf("Process: unexpected error (P2-S7fix regression): %v", err)
	}
}

// ---------------------------------------------------------------------------
// KTable.To() DSL tests
// ---------------------------------------------------------------------------

// TestKTable_To_SinkBindingRegistered verifies that calling KTable.To() registers
// a SinkBinding in bt.Sinks with the correct topic and working encode closures.
//
// Pipeline: source[string,string] → GroupByKey → Count → KTable.To("sink-topic", ...)
func TestKTable_To_SinkBindingRegistered(t *testing.T) {
	t.Parallel()

	b := gstream.NewStreamBuilder()
	src := gstream.Stream[string, string](b, "input", "src", gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{})
	table := src.GroupByKey(gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{}).Count("tosink-store")
	table.To("my-sink-topic", gstream.JSONSerde[string]{}, gstream.JSONSerde[int64]{})

	bt := b.Build()

	// Exactly one SinkBinding must be registered.
	if len(bt.Sinks) != 1 {
		t.Fatalf("expected 1 entry in bt.Sinks, got %d: %v", len(bt.Sinks), bt.Sinks)
	}

	// Find the binding.
	var found *gstream.SinkBinding
	for _, sb := range bt.Sinks {
		sb := sb
		found = &sb
	}

	if found.Topic != "my-sink-topic" {
		t.Errorf("SinkBinding.Topic: got %q, want %q", found.Topic, "my-sink-topic")
	}
	if found.EncodeKey == nil {
		t.Fatal("SinkBinding.EncodeKey is nil")
	}
	if found.EncodeVal == nil {
		t.Fatal("SinkBinding.EncodeVal is nil")
	}

	// Round-trip: EncodeKey receives a typed string key.
	kb, err := found.EncodeKey("hello")
	if err != nil {
		t.Fatalf("EncodeKey(%q): %v", "hello", err)
	}
	gotKey, err := gstream.JSONSerde[string]{}.Deserialize(kb)
	if err != nil {
		t.Fatalf("decode EncodeKey result: %v", err)
	}
	if gotKey != "hello" {
		t.Errorf("EncodeKey round-trip: got %q, want %q", gotKey, "hello")
	}

	// Round-trip: EncodeVal receives a typed int64 value (Count accumulator).
	vb, err := found.EncodeVal(int64(42))
	if err != nil {
		t.Fatalf("EncodeVal(42): %v", err)
	}
	gotVal, err := gstream.JSONSerde[int64]{}.Deserialize(vb)
	if err != nil {
		t.Fatalf("decode EncodeVal result: %v", err)
	}
	if gotVal != 42 {
		t.Errorf("EncodeVal round-trip: got %d, want 42", gotVal)
	}

	// EncodeKey with wrong type must return error (not panic).
	if _, err := found.EncodeKey(123); err == nil {
		t.Error("EncodeKey(int): expected error for wrong type, got nil")
	}

	// EncodeVal with wrong type must return error (not panic).
	if _, err := found.EncodeVal("not-an-int64"); err == nil {
		t.Error("EncodeVal(string): expected error for wrong type, got nil")
	}
}

// TestKTable_To_Absent verifies that WITHOUT calling To(), bt.Sinks is empty.
// This is the backward-compatibility contract: existing topologies are unchanged.
func TestKTable_To_Absent(t *testing.T) {
	t.Parallel()

	b := gstream.NewStreamBuilder()
	src := gstream.Stream[string, string](b, "input", "src", gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{})
	_ = src.GroupByKey(gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{}).Count("no-to-store")

	bt := b.Build()

	if len(bt.Sinks) != 0 {
		t.Errorf("expected bt.Sinks empty (no To() call), got %d entries: %v", len(bt.Sinks), bt.Sinks)
	}
}

// TestKTable_To_RecordsReachSink verifies that records forwarded by the aggregate
// processor actually reach the registered sink buffer. This is empirical proof that
// the DAG wiring (aggregate → ktable-out sink node → SinkBinding) is correct.
//
// Pipeline: source[string,string] → GroupByKey → Count → KTable.To("out-topic", ...)
// Input: a, b, a → after processing, the sink must have buffered records for key "a"
// (count 1 then 2) and key "b" (count 1).
func TestKTable_To_RecordsReachSink(t *testing.T) {
	t.Parallel()

	b := gstream.NewStreamBuilder()
	src := gstream.Stream[string, string](b, "input", "src", gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{})
	table := src.GroupByKey(gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{}).Count("reach-store")
	table.To("out-topic", gstream.JSONSerde[string]{}, gstream.JSONSerde[int64]{})

	bt := b.Build()

	db, err := memory.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	byteStore := memory.NewKeyValueStore[[]byte, []byte](
		"reach-store", db, gstream.BytesSerde{}, gstream.BytesSerde{},
	)
	exec := topology.NewExecutor(bt.Topology, map[string]any{"reach-store": byteStore})

	keySerde := gstream.JSONSerde[string]{}
	valSerde := gstream.JSONSerde[int64]{}

	// Find sink name for out-topic.
	var sinkName string
	for name, sb := range bt.Sinks {
		if sb.Topic == "out-topic" {
			sinkName = name
		}
	}
	if sinkName == "" {
		t.Fatal("no sink registered for 'out-topic'")
	}

	// Process a, b, a — expect sink to buffer: a=1, b=1, a=2 (one record per update).
	inputs := []string{"a", "b", "a"}
	type wantRecord struct {
		key string
		val int64
	}
	wants := []wantRecord{{"a", 1}, {"b", 1}, {"a", 2}}

	for i, key := range inputs {
		if err := exec.Process(context.Background(), "src", topology.Record{Key: key, Value: key, Timestamp: 1}); err != nil {
			t.Fatalf("Process[%d] key=%q: %v", i, key, err)
		}
		records, err := exec.DrainSink(sinkName)
		if err != nil {
			t.Fatalf("DrainSink[%d]: %v", i, err)
		}
		if len(records) != 1 {
			t.Fatalf("Process[%d]: expected 1 sink record, got %d", i, len(records))
		}
		r := records[0]
		gotKey, ok := r.Key.(string)
		if !ok {
			t.Fatalf("record[%d].Key type: got %T, want string", i, r.Key)
		}
		gotVal, ok := r.Value.(int64)
		if !ok {
			t.Fatalf("record[%d].Value type: got %T, want int64", i, r.Value)
		}
		if gotKey != wants[i].key {
			t.Errorf("record[%d].Key: got %q, want %q", i, gotKey, wants[i].key)
		}
		if gotVal != wants[i].val {
			t.Errorf("record[%d].Value: got %d, want %d", i, gotVal, wants[i].val)
		}

		// Verify encoding round-trip via the SinkBinding closures.
		sb := bt.Sinks[sinkName]
		kb, err := sb.EncodeKey(r.Key)
		if err != nil {
			t.Fatalf("EncodeKey record[%d]: %v", i, err)
		}
		vb, err := sb.EncodeVal(r.Value)
		if err != nil {
			t.Fatalf("EncodeVal record[%d]: %v", i, err)
		}
		dk, _ := keySerde.Deserialize(kb)
		dv, _ := valSerde.Deserialize(vb)
		if dk != wants[i].key {
			t.Errorf("encode-decode key record[%d]: got %q, want %q", i, dk, wants[i].key)
		}
		if dv != wants[i].val {
			t.Errorf("encode-decode val record[%d]: got %d, want %d", i, dv, wants[i].val)
		}
	}
}

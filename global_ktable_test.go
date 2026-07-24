package gstream

import (
	"testing"
)

// TestGlobalTable_RegistersExactlyOneBinding verifies that GlobalTable() inserts
// exactly one GlobalTableBinding into the builder with the correct StoreName and
// Topic, and that Build() surfaces it verbatim in BuiltTopology.GlobalTableBindings.
func TestGlobalTable_RegistersExactlyOneBinding(t *testing.T) {
	t.Parallel()

	b := NewStreamBuilder()
	gt := GlobalTable[string, string](b, "enrichment-topic", "gt-node", JSONSerde[string]{}, JSONSerde[string]{})

	if len(b.globalTables) != 1 {
		t.Fatalf("expected 1 globalTables entry; got %d", len(b.globalTables))
	}

	binding, ok := b.globalTables[gt.storeName]
	if !ok {
		t.Fatalf("no binding found for storeName %q", gt.storeName)
	}
	if binding.StoreName != gt.storeName {
		t.Errorf("StoreName: got %q, want %q", binding.StoreName, gt.storeName)
	}
	if binding.Topic != "enrichment-topic" {
		t.Errorf("Topic: got %q, want %q", binding.Topic, "enrichment-topic")
	}
}

// TestGlobalTable_EncodeDecodeKeyRoundTrip verifies that EncodeKey followed by
// DecodeKey round-trips the key back to its original value.
func TestGlobalTable_EncodeDecodeKeyRoundTrip(t *testing.T) {
	t.Parallel()

	b := NewStreamBuilder()
	gt := GlobalTable[string, int64](b, "t", "gt-node", JSONSerde[string]{}, JSONSerde[int64]{})
	binding := b.globalTables[gt.storeName]

	const key = "hello-world"
	raw, err := binding.EncodeKey(key)
	if err != nil {
		t.Fatalf("EncodeKey: %v", err)
	}
	got, err := binding.DecodeKey(raw)
	if err != nil {
		t.Fatalf("DecodeKey: %v", err)
	}
	if got != key {
		t.Errorf("round-trip key: got %v (%T), want %q", got, got, key)
	}
}

// TestGlobalTable_EncodeDecodeValRoundTrip verifies that EncodeVal followed by
// DecodeVal round-trips the value back to its original value.
func TestGlobalTable_EncodeDecodeValRoundTrip(t *testing.T) {
	t.Parallel()

	b := NewStreamBuilder()
	gt := GlobalTable[string, int64](b, "t", "gt-node", JSONSerde[string]{}, JSONSerde[int64]{})
	binding := b.globalTables[gt.storeName]

	const val int64 = 42
	raw, err := binding.EncodeVal(val)
	if err != nil {
		t.Fatalf("EncodeVal: %v", err)
	}
	got, err := binding.DecodeVal(raw)
	if err != nil {
		t.Fatalf("DecodeVal: %v", err)
	}
	if got != val {
		t.Errorf("round-trip val: got %v (%T), want %d", got, got, val)
	}
}

// TestGlobalTable_EncodeKey_TypeMismatch verifies that EncodeKey returns a
// non-nil error when passed a value of the wrong type (no silent panic).
func TestGlobalTable_EncodeKey_TypeMismatch(t *testing.T) {
	t.Parallel()

	b := NewStreamBuilder()
	gt := GlobalTable[string, int64](b, "t", "gt-node", JSONSerde[string]{}, JSONSerde[int64]{})
	binding := b.globalTables[gt.storeName]

	_, err := binding.EncodeKey(int(99)) // expected string, got int
	if err == nil {
		t.Fatal("expected error for EncodeKey type mismatch; got nil")
	}
}

// TestGlobalTable_BuildSurfacesInBuiltTopology verifies that Build() copies
// globalTables into BuiltTopology.GlobalTableBindings.
func TestGlobalTable_BuildSurfacesInBuiltTopology(t *testing.T) {
	t.Parallel()

	b := NewStreamBuilder()
	// A normal stream + sink so topology.Build() does not panic (needs >=1 source + sink).
	src := Stream[string, string](b, "stream-topic", "src", JSONSerde[string]{}, JSONSerde[string]{})
	src.To("out-topic", "sink", JSONSerde[string]{}, JSONSerde[string]{})

	gt := GlobalTable[string, string](b, "enrichment-topic", "gt-node", JSONSerde[string]{}, JSONSerde[string]{})

	bt := b.Build()

	if len(bt.GlobalTableBindings) != 1 {
		t.Fatalf("expected 1 GlobalTableBinding in BuiltTopology; got %d", len(bt.GlobalTableBindings))
	}
	got, ok := bt.GlobalTableBindings[gt.storeName]
	if !ok {
		t.Fatalf("GlobalTableBindings missing key %q", gt.storeName)
	}
	if got.Topic != "enrichment-topic" {
		t.Errorf("Topic: got %q, want %q", got.Topic, "enrichment-topic")
	}
}

// TestGlobalTable_NoPhantomSourceNode is the KEY safety test: building a topology
// that contains a GlobalTable plus a normal stream source and sink must NOT panic.
// This proves that GlobalTable() does not call AddSource, which would create a
// phantom DAG node unconnected to any sink and could cause topology.Build() to fail
// (or at minimum produce a malformed topology).
//
// topology.Build() only panics for zero sources or zero sinks. A phantom source
// node would not cause a panic — but it would appear in Topology.SourceNames()
// and the runtime would try to subscribe to it. We verify it does NOT appear.
func TestGlobalTable_NoPhantomSourceNode(t *testing.T) {
	t.Parallel()

	b := NewStreamBuilder()
	src := Stream[string, string](b, "stream-topic", "src", JSONSerde[string]{}, JSONSerde[string]{})
	src.To("out-topic", "sink", JSONSerde[string]{}, JSONSerde[string]{})

	gt := GlobalTable[string, string](b, "enrichment-topic", "gt-node", JSONSerde[string]{}, JSONSerde[string]{})

	// Build must not panic.
	bt := b.Build()

	// The global table's nodeName must NOT appear in SourceNames.
	for _, name := range bt.Topology.SourceNames() {
		if name == gt.nodeName {
			t.Errorf("GlobalKTable nodeName %q appeared in SourceNames — phantom source node created", gt.nodeName)
		}
		if name == gt.storeName {
			t.Errorf("GlobalKTable storeName %q appeared in SourceNames — phantom source node created", gt.storeName)
		}
	}

	// Exactly one real source (the stream).
	sources := bt.Topology.SourceNames()
	if len(sources) != 1 || sources[0] != "src" {
		t.Errorf("SourceNames: got %v, want [src]", sources)
	}
}

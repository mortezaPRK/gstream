package gstream_test

import (
	"context"
	"testing"

	"github.com/mortezaPRK/gstream"
	"github.com/mortezaPRK/gstream/internal/topology"
)

// TestRepartition_BindingRegistered verifies that Repartition registers exactly
// one RepartitionBinding with the correct Name, Partitions, SinkName, and
// SourceName, and that no extra bindings are created.
func TestRepartition_BindingRegistered(t *testing.T) {
	t.Parallel()

	b := gstream.NewStreamBuilder()
	src := gstream.Stream[string, string](b, "input", "src", gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{})

	// Chain a downstream To() so Build() finds at least one sink.
	src.Repartition("rekey", 4, gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{}).
		To("output", "out", gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{})

	bt := b.Build()

	if len(bt.RepartitionBindings) != 1 {
		t.Fatalf("RepartitionBindings length: got %d, want 1", len(bt.RepartitionBindings))
	}

	rb, ok := bt.RepartitionBindings["rekey"]
	if !ok {
		t.Fatal("RepartitionBindings[\"rekey\"] not found")
	}
	if rb.Name != "rekey" {
		t.Errorf("Name: got %q, want %q", rb.Name, "rekey")
	}
	if rb.Partitions != 4 {
		t.Errorf("Partitions: got %d, want 4", rb.Partitions)
	}
	if rb.SinkName == "" {
		t.Error("SinkName is empty")
	}
	if rb.SourceName == "" {
		t.Error("SourceName is empty")
	}
	if rb.SinkName == rb.SourceName {
		t.Errorf("SinkName and SourceName must be distinct; both are %q", rb.SinkName)
	}
	if rb.EncodeKey == nil || rb.EncodeVal == nil || rb.DecodeKey == nil || rb.DecodeVal == nil {
		t.Error("RepartitionBinding has nil serde closure(s)")
	}
}

// TestRepartition_TopologyNodes verifies that the sink node is terminal (present
// in SinkNames) and the source node is a root (present in SourceNames).
func TestRepartition_TopologyNodes(t *testing.T) {
	t.Parallel()

	b := gstream.NewStreamBuilder()
	src := gstream.Stream[string, string](b, "input", "src", gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{})
	repartitioned := src.Repartition("r2", 8, gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{})
	repartitioned.To("output", "out", gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{})

	bt := b.Build()
	rb := bt.RepartitionBindings["r2"]

	sinkNames := bt.Topology.SinkNames()
	sourceNames := bt.Topology.SourceNames()

	hasSink := false
	for _, n := range sinkNames {
		if n == rb.SinkName {
			hasSink = true
			break
		}
	}
	if !hasSink {
		t.Errorf("SinkName %q not found in topology.SinkNames(): %v", rb.SinkName, sinkNames)
	}

	hasSource := false
	for _, n := range sourceNames {
		if n == rb.SourceName {
			hasSource = true
			break
		}
	}
	if !hasSource {
		t.Errorf("SourceName %q not found in topology.SourceNames(): %v", rb.SourceName, sourceNames)
	}
}

// TestRepartition_ChainDownstream verifies that the KStream returned by
// Repartition is rooted at sourceName and that downstream operators
// (.Filter + .To) build without error and produce nodes that are children
// of the repartition source.
func TestRepartition_ChainDownstream(t *testing.T) {
	t.Parallel()

	b := gstream.NewStreamBuilder()
	src := gstream.Stream[string, string](b, "input", "src", gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{})

	// Chain: Repartition → Filter → To.  Build() must not panic.
	src.Repartition("chain", 2, gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{}).
		Filter(func(_ string, v string) bool { return v != "" }).
		To("output", "chain-out", gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{})

	// Build seals; if wiring were wrong it panics.
	bt := b.Build()

	// chain-out must be in SinkNames.
	found := false
	for _, n := range bt.Topology.SinkNames() {
		if n == "chain-out" {
			found = true
			break
		}
	}
	if !found {
		t.Error("downstream sink \"chain-out\" not found in SinkNames")
	}
}

// TestRepartition_EncodeDecodeRoundTrip verifies that the closures in
// RepartitionBinding correctly round-trip key and value through their
// respective serdes.
func TestRepartition_EncodeDecodeRoundTrip(t *testing.T) {
	t.Parallel()

	b := gstream.NewStreamBuilder()
	src := gstream.Stream[string, string](b, "input", "src", gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{})
	src.Repartition("rt", 4, gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{}).
		To("output", "rt-out", gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{})

	bt := b.Build()
	rb := bt.RepartitionBindings["rt"]

	// Key round-trip.
	wantKey := "hello"
	keyBytes, err := rb.EncodeKey(wantKey)
	if err != nil {
		t.Fatalf("EncodeKey: %v", err)
	}
	gotKey, err := rb.DecodeKey(keyBytes)
	if err != nil {
		t.Fatalf("DecodeKey: %v", err)
	}
	if gotKey != wantKey {
		t.Errorf("key round-trip: got %v, want %v", gotKey, wantKey)
	}

	// Value round-trip.
	wantVal := "world"
	valBytes, err := rb.EncodeVal(wantVal)
	if err != nil {
		t.Fatalf("EncodeVal: %v", err)
	}
	gotVal, err := rb.DecodeVal(valBytes)
	if err != nil {
		t.Fatalf("DecodeVal: %v", err)
	}
	if gotVal != wantVal {
		t.Errorf("value round-trip: got %v, want %v", gotVal, wantVal)
	}
}

// TestRepartition_EncodeKey_TypeMismatch verifies that EncodeKey returns an
// error when called with a value of the wrong type (not K).
func TestRepartition_EncodeKey_TypeMismatch(t *testing.T) {
	t.Parallel()

	b := gstream.NewStreamBuilder()
	src := gstream.Stream[string, string](b, "input", "src", gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{})
	src.Repartition("tm", 2, gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{}).
		To("output", "tm-out", gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{})

	bt := b.Build()
	rb := bt.RepartitionBindings["tm"]

	_, err := rb.EncodeKey(42) // int, not string
	if err == nil {
		t.Error("EncodeKey with wrong type: expected error, got nil")
	}
}

// TestRepartition_ExecutorDrivesSource verifies that driving a record into the
// repartition source node via topology.Executor routes it through a downstream
// Filter and reaches the final sink.
func TestRepartition_ExecutorDrivesSource(t *testing.T) {
	t.Parallel()

	b := gstream.NewStreamBuilder()
	src := gstream.Stream[string, string](b, "input", "src", gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{})
	src.Repartition("exec", 4, gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{}).
		Filter(func(_ string, v string) bool { return v == "keep" }).
		To("output", "exec-out", gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{})

	bt := b.Build()
	rb := bt.RepartitionBindings["exec"]

	exec := topology.NewExecutor(bt.Topology, nil)

	// Inject a record directly into the repartition source (simulates the
	// consumer side of the repartition topic delivering a record).
	kept := topology.Record{Key: "k", Value: "keep", Timestamp: 1}
	if err := exec.Process(context.Background(), rb.SourceName, kept); err != nil {
		t.Fatalf("Process keep: %v", err)
	}

	dropped := topology.Record{Key: "k", Value: "drop", Timestamp: 2}
	if err := exec.Process(context.Background(), rb.SourceName, dropped); err != nil {
		t.Fatalf("Process drop: %v", err)
	}

	records, err := exec.DrainSink("exec-out")
	if err != nil {
		t.Fatalf("DrainSink: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("DrainSink: got %d records, want 1", len(records))
	}
	if records[0].Value != "keep" {
		t.Errorf("record value: got %v, want \"keep\"", records[0].Value)
	}
}

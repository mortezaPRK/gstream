// Package gstream_test provides broker-free tests for the stream-table inner join
// (JoinTable). Tests live in package gstream_test (external) rather than package
// gstream because internal/state imports gstream (for Serde[T]), so a
// package-internal test that imports internal/state would create a circular import.
//
// Store type: []byte/[]byte (same as grouped_test — Option A type-erasure pattern).
// The table sub-graph writes counts via accSerde (JSONSerde[int64]) into a
// *state.KeyValueStore[[]byte,[]byte]. JoinTable reads the same store, decodes via
// table.valSerde (captured accSerde), and forwards the joined record.
package gstream_test

import (
	"context"
	"testing"

	"github.com/mortezaPRK/gstream"
	"github.com/mortezaPRK/gstream/internal/state"
	"github.com/mortezaPRK/gstream/internal/topology"
)

// buildJoinTopology constructs the test topology:
//
//	table-source[string,string] → GroupByKey → Count("t") (table sub-graph)
//	stream-source[string,string] → JoinTable(table, joiner, outSerde) → out-sink
//
// Returns the BuiltTopology and the store name ("t") so callers can wire the store.
func buildJoinTopology(t *testing.T) (*gstream.BuiltTopology, string) {
	t.Helper()

	b := gstream.NewStreamBuilder()

	// table sub-graph: count occurrences per key
	tableSrc := gstream.Stream[string, string](
		b, "table-topic", "tsrc",
		gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{},
	)
	table := tableSrc.
		GroupByKey(gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{}).
		Count("t")

	// stream sub-graph: join against table
	streamSrc := gstream.Stream[string, string](
		b, "stream-topic", "ssrc",
		gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{},
	)

	// joiner: concatenate stream value + ":" + formatted count
	joined := streamSrc.JoinTable[int64, string](
		table,
		func(v string, count int64) string {
			return v + ":" + string(rune('0'+count))
		},
		gstream.JSONSerde[string]{},
	)

	joined.To("out-topic", "out", gstream.JSONSerde[string]{}, gstream.JSONSerde[string]{})

	return b.Build(), "t"
}

// TestJoinTable_Miss verifies that a stream record with no matching table entry
// produces no output (inner-join miss).
func TestJoinTable_Miss(t *testing.T) {
	t.Parallel()

	bt, storeName := buildJoinTopology(t)

	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	byteStore := state.NewKeyValueStoreWithChangelog[[]byte, []byte](
		storeName, db, gstream.BytesSerde{}, gstream.BytesSerde{}, nil,
	)
	exec := topology.NewExecutor(bt.Topology, map[string]any{storeName: byteStore})

	// Drive stream-source with no prior table data → must miss → no output.
	if err := exec.Process(context.Background(), "ssrc", topology.Record{
		Key: "k", Value: "v1", Timestamp: 1,
	}); err != nil {
		t.Fatalf("Process stream-source: %v", err)
	}

	out, err := exec.DrainSink("out")
	if err != nil {
		t.Fatalf("DrainSink: %v", err)
	}
	if len(out) != 0 {
		t.Errorf("miss: expected 0 records, got %d: %+v", len(out), out)
	}
}

// TestJoinTable_Hit verifies that after a table-source record populates the store,
// a stream-source record with the same key triggers the joiner and produces output.
func TestJoinTable_Hit(t *testing.T) {
	t.Parallel()

	bt, storeName := buildJoinTopology(t)

	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	byteStore := state.NewKeyValueStoreWithChangelog[[]byte, []byte](
		storeName, db, gstream.BytesSerde{}, gstream.BytesSerde{}, nil,
	)
	exec := topology.NewExecutor(bt.Topology, map[string]any{storeName: byteStore})
	ctx := context.Background()

	// Populate table: drive table-source once for key "k" → count becomes 1.
	if err := exec.Process(ctx, "tsrc", topology.Record{
		Key: "k", Value: "ignored", Timestamp: 1,
	}); err != nil {
		t.Fatalf("Process table-source: %v", err)
	}

	// Now drive stream-source for key "k" → should hit and forward "v:1".
	if err := exec.Process(ctx, "ssrc", topology.Record{
		Key: "k", Value: "v", Timestamp: 2,
	}); err != nil {
		t.Fatalf("Process stream-source: %v", err)
	}

	out, err := exec.DrainSink("out")
	if err != nil {
		t.Fatalf("DrainSink: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("hit: expected 1 record, got %d: %+v", len(out), out)
	}
	if out[0].Key != "k" {
		t.Errorf("hit: Key: got %v, want k", out[0].Key)
	}
	if out[0].Value != "v:1" {
		t.Errorf("hit: Value: got %v, want v:1", out[0].Value)
	}
	if out[0].Timestamp != 2 {
		t.Errorf("hit: Timestamp: got %v, want 2", out[0].Timestamp)
	}
}

// TestJoinTable_StreamBeforeTable verifies that a stream record arriving before
// any table entry is a miss (not a panic or error).
func TestJoinTable_StreamBeforeTable(t *testing.T) {
	t.Parallel()

	bt, storeName := buildJoinTopology(t)

	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	byteStore := state.NewKeyValueStoreWithChangelog[[]byte, []byte](
		storeName, db, gstream.BytesSerde{}, gstream.BytesSerde{}, nil,
	)
	exec := topology.NewExecutor(bt.Topology, map[string]any{storeName: byteStore})
	ctx := context.Background()

	// Stream arrives before table is populated → miss.
	if err := exec.Process(ctx, "ssrc", topology.Record{Key: "k", Value: "early", Timestamp: 1}); err != nil {
		t.Fatalf("stream before table: %v", err)
	}
	out, _ := exec.DrainSink("out")
	if len(out) != 0 {
		t.Errorf("stream-before-table: expected 0, got %d", len(out))
	}

	// Populate table.
	if err := exec.Process(ctx, "tsrc", topology.Record{Key: "k", Value: "x", Timestamp: 2}); err != nil {
		t.Fatalf("table populate: %v", err)
	}

	// Second stream record → hit now.
	if err := exec.Process(ctx, "ssrc", topology.Record{Key: "k", Value: "late", Timestamp: 3}); err != nil {
		t.Fatalf("stream after table: %v", err)
	}
	out, _ = exec.DrainSink("out")
	if len(out) != 1 {
		t.Fatalf("stream-after-table: expected 1, got %d", len(out))
	}
	if out[0].Value != "late:1" {
		t.Errorf("stream-after-table: Value: got %v, want late:1", out[0].Value)
	}
}

// TestJoinTable_TwoKeys verifies that two independent keys are handled correctly:
// populating one does not affect lookups for the other.
func TestJoinTable_TwoKeys(t *testing.T) {
	t.Parallel()

	bt, storeName := buildJoinTopology(t)

	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	byteStore := state.NewKeyValueStoreWithChangelog[[]byte, []byte](
		storeName, db, gstream.BytesSerde{}, gstream.BytesSerde{}, nil,
	)
	exec := topology.NewExecutor(bt.Topology, map[string]any{storeName: byteStore})
	ctx := context.Background()

	// Populate table for "k1" only.
	if err := exec.Process(ctx, "tsrc", topology.Record{Key: "k1", Value: "x", Timestamp: 1}); err != nil {
		t.Fatalf("table k1: %v", err)
	}

	// Stream record for "k2" → miss (k2 not in table).
	if err := exec.Process(ctx, "ssrc", topology.Record{Key: "k2", Value: "a2", Timestamp: 2}); err != nil {
		t.Fatalf("stream k2: %v", err)
	}
	out, _ := exec.DrainSink("out")
	if len(out) != 0 {
		t.Errorf("k2 miss: expected 0, got %d", len(out))
	}

	// Stream record for "k1" → hit.
	if err := exec.Process(ctx, "ssrc", topology.Record{Key: "k1", Value: "a1", Timestamp: 3}); err != nil {
		t.Fatalf("stream k1: %v", err)
	}
	out, _ = exec.DrainSink("out")
	if len(out) != 1 {
		t.Fatalf("k1 hit: expected 1, got %d", len(out))
	}
	if out[0].Key != "k1" {
		t.Errorf("k1 hit Key: got %v, want k1", out[0].Key)
	}
	if out[0].Value != "a1:1" {
		t.Errorf("k1 hit Value: got %v, want a1:1", out[0].Value)
	}
}

// TestJoinTable_NodeAndStore verifies that the join node exists in the topology
// and reads from the table's store name.
func TestJoinTable_NodeAndStore(t *testing.T) {
	t.Parallel()

	bt, storeName := buildJoinTopology(t)

	// The topology must have "t" in StoreBindings (registered by Count).
	if _, ok := bt.StoreBindings[storeName]; !ok {
		t.Errorf("StoreBindings[%q] not found; join relies on it", storeName)
	}

	// Verify at least one sink exists (joined stream was wired via .To()).
	sinkNames := bt.Topology.SinkNames()
	if len(sinkNames) == 0 {
		t.Fatal("no sinks registered; join output was not wired")
	}

	found := false
	for _, name := range sinkNames {
		if name == "out" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected sink named 'out', got: %v", sinkNames)
	}
}

// TestJoinTable_UpdatedTableValue verifies that the join sees the latest table
// value: if a key is updated in the table before a stream record arrives, the
// joiner receives the updated value.
func TestJoinTable_UpdatedTableValue(t *testing.T) {
	t.Parallel()

	bt, storeName := buildJoinTopology(t)

	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	byteStore := state.NewKeyValueStoreWithChangelog[[]byte, []byte](
		storeName, db, gstream.BytesSerde{}, gstream.BytesSerde{}, nil,
	)
	exec := topology.NewExecutor(bt.Topology, map[string]any{storeName: byteStore})
	ctx := context.Background()

	// Drive table 3 times for "k" → count = 3.
	for i := 0; i < 3; i++ {
		if err := exec.Process(ctx, "tsrc", topology.Record{
			Key: "k", Value: "x", Timestamp: int64(i + 1),
		}); err != nil {
			t.Fatalf("table[%d]: %v", i, err)
		}
	}

	// Stream join → joiner receives count=3, produces "v:3".
	if err := exec.Process(ctx, "ssrc", topology.Record{
		Key: "k", Value: "v", Timestamp: 10,
	}); err != nil {
		t.Fatalf("stream: %v", err)
	}
	out, _ := exec.DrainSink("out")
	if len(out) != 1 {
		t.Fatalf("expected 1 output, got %d", len(out))
	}
	if out[0].Value != "v:3" {
		t.Errorf("updated value: got %v, want v:3", out[0].Value)
	}
}

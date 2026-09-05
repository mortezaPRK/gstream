package topology_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"mortz.dev/go/gstream/internal/topology"
)

// buildPipeline constructs a simple source → filter → map → sink topology.
// filter: keeps only records whose Value is a string with len > 3.
// map: transforms Value to uppercase and sets Key to "mapped-"+original key.
func buildPipeline(t *testing.T) *topology.Topology {
	t.Helper()
	b := topology.NewBuilder()

	src := b.AddSource("source")

	filt := b.AddProcessor("filter",
		topology.Filter(func(_, v any) bool {
			s, ok := v.(string)
			return ok && len(s) > 3
		}),
		src,
	)

	mapped := b.AddProcessor("map",
		topology.Mapper(func(k, v any) (any, any) {
			key := fmt.Sprintf("mapped-%v", k)
			val := strings.ToUpper(v.(string))
			return key, val
		}),
		filt,
	)

	b.AddSink("sink", mapped)

	return b.Build()
}

// TestPipelineFiltersAndMaps is the core demonstration: a record is piped through
// source → filter → map → sink and the output is asserted exactly.
func TestPipelineFiltersAndMaps(t *testing.T) {
	topo := buildPipeline(t)
	d := topology.NewTestDriver(topo)

	inputs := []topology.Record{
		{Key: "a", Value: "hello", Timestamp: 1},  // len 5 > 3 — passes filter
		{Key: "b", Value: "hi", Timestamp: 2},     // len 2 ≤ 3 — filtered out
		{Key: "c", Value: "world", Timestamp: 3},  // len 5 > 3 — passes filter
		{Key: "d", Value: "bye", Timestamp: 4},    // len 3 ≤ 3 — filtered out
		{Key: "e", Value: "stream", Timestamp: 5}, // len 6 > 3 — passes filter
	}

	for _, r := range inputs {
		if err := d.PipeInput("source", r); err != nil {
			t.Fatalf("PipeInput(%q): unexpected error: %v", r.Key, err)
		}
	}

	out, err := d.ReadOutput("sink")
	if err != nil {
		t.Fatalf("ReadOutput: %v", err)
	}

	// Exactly 3 records should pass (hello, world, stream).
	if got, want := len(out), 3; got != want {
		t.Fatalf("output record count: got %d, want %d; records: %+v", got, want, out)
	}

	want := []topology.Record{
		{Key: "mapped-a", Value: "HELLO", Timestamp: 1},
		{Key: "mapped-c", Value: "WORLD", Timestamp: 3},
		{Key: "mapped-e", Value: "STREAM", Timestamp: 5},
	}

	for i, w := range want {
		if out[i].Key != w.Key {
			t.Errorf("record[%d].Key: got %v, want %v", i, out[i].Key, w.Key)
		}
		if out[i].Value != w.Value {
			t.Errorf("record[%d].Value: got %v, want %v", i, out[i].Value, w.Value)
		}
		if out[i].Timestamp != w.Timestamp {
			t.Errorf("record[%d].Timestamp: got %d, want %d", i, out[i].Timestamp, w.Timestamp)
		}
	}
}

// TestFilteredRecordsNeverReachSink asserts that records not passing the predicate
// produce zero output. This is a belt-and-suspenders check separate from the main test.
func TestFilteredRecordsNeverReachSink(t *testing.T) {
	topo := buildPipeline(t)
	d := topology.NewTestDriver(topo)

	shortRecords := []topology.Record{
		{Key: "x", Value: "ab", Timestamp: 10},
		{Key: "y", Value: "cd", Timestamp: 20},
		{Key: "z", Value: "ef", Timestamp: 30},
	}

	for _, r := range shortRecords {
		if err := d.PipeInput("source", r); err != nil {
			t.Fatalf("PipeInput: %v", err)
		}
	}

	out, err := d.ReadOutput("sink")
	if err != nil {
		t.Fatalf("ReadOutput: %v", err)
	}
	if len(out) != 0 {
		t.Errorf("expected 0 records after filtering; got %d: %+v", len(out), out)
	}
}

// TestDeterminism proves that piping the same inputs twice produces identical outputs.
func TestDeterminism(t *testing.T) {
	inputs := []topology.Record{
		{Key: "a", Value: "hello", Timestamp: 1},
		{Key: "b", Value: "hi", Timestamp: 2},
		{Key: "c", Value: "world", Timestamp: 3},
	}

	run := func() []topology.Record {
		topo := buildPipeline(t)
		d := topology.NewTestDriver(topo)
		for _, r := range inputs {
			if err := d.PipeInput("source", r); err != nil {
				t.Fatalf("PipeInput: %v", err)
			}
		}
		out, err := d.ReadOutput("sink")
		if err != nil {
			t.Fatalf("ReadOutput: %v", err)
		}
		return out
	}

	first := run()
	second := run()

	if len(first) != len(second) {
		t.Fatalf("different output lengths: %d vs %d", len(first), len(second))
	}
	for i := range first {
		if first[i].Key != second[i].Key || first[i].Value != second[i].Value ||
			first[i].Timestamp != second[i].Timestamp {
			t.Errorf("record[%d] mismatch: %+v vs %+v", i, first[i], second[i])
		}
	}
}

// TestReadOutputDrainsBuffer asserts that ReadOutput clears the sink buffer;
// a second call with no new inputs returns empty.
func TestReadOutputDrainsBuffer(t *testing.T) {
	topo := buildPipeline(t)
	d := topology.NewTestDriver(topo)

	if err := d.PipeInput("source", topology.Record{Key: "a", Value: "hello", Timestamp: 1}); err != nil {
		t.Fatalf("PipeInput: %v", err)
	}

	out1, _ := d.ReadOutput("sink")
	if len(out1) != 1 {
		t.Fatalf("first ReadOutput: expected 1, got %d", len(out1))
	}

	out2, _ := d.ReadOutput("sink")
	if len(out2) != 0 {
		t.Fatalf("second ReadOutput (no new inputs): expected 0, got %d", len(out2))
	}
}

// TestPeekOutputDoesNotDrainBuffer asserts that PeekOutput does not clear the buffer.
func TestPeekOutputDoesNotDrainBuffer(t *testing.T) {
	topo := buildPipeline(t)
	d := topology.NewTestDriver(topo)

	if err := d.PipeInput("source", topology.Record{Key: "a", Value: "hello", Timestamp: 1}); err != nil {
		t.Fatalf("PipeInput: %v", err)
	}

	peek1, _ := d.PeekOutput("sink")
	peek2, _ := d.PeekOutput("sink")

	if len(peek1) != 1 || len(peek2) != 1 {
		t.Fatalf("PeekOutput should not drain; peek1=%d peek2=%d", len(peek1), len(peek2))
	}

	// ReadOutput should still see the record.
	out, _ := d.ReadOutput("sink")
	if len(out) != 1 {
		t.Fatalf("ReadOutput after two Peeks: expected 1, got %d", len(out))
	}
}

// TestUnknownSourceReturnsError ensures PipeInput returns an error for missing sources.
func TestUnknownSourceReturnsError(t *testing.T) {
	topo := buildPipeline(t)
	d := topology.NewTestDriver(topo)

	err := d.PipeInput("nonexistent", topology.Record{Key: "x", Value: "y", Timestamp: 0})
	if err == nil {
		t.Fatal("expected error for unknown source, got nil")
	}
}

// TestUnknownSinkReturnsError ensures ReadOutput returns an error for missing sinks.
func TestUnknownSinkReturnsError(t *testing.T) {
	topo := buildPipeline(t)
	d := topology.NewTestDriver(topo)

	_, err := d.ReadOutput("nonexistent")
	if err == nil {
		t.Fatal("expected error for unknown sink, got nil")
	}
}

// TestPipeInput_ProcessorError_Propagates proves that an error returned by a downstream
// processor is not swallowed — it surfaces as the return value of PipeInput. This directly
// exercises the processWithErr traversal path.
func TestPipeInput_ProcessorError_Propagates(t *testing.T) {
	b := topology.NewBuilder()
	src := b.AddSource("src")

	sentinel := fmt.Errorf("processor failure")
	b.AddProcessor("failing",
		func(_ context.Context, _ topology.Record, _ topology.Forwarder) error { return sentinel },
		src,
	)
	// We still need a sink (Build panics without one), but the error should stop
	// traversal before reaching it.
	b.AddSink("sink",
		b.AddProcessor("after-failing",
			topology.Filter(func(_, _ any) bool { return true }),
			"failing",
		),
	)
	topo := b.Build()
	d := topology.NewTestDriver(topo)

	err := d.PipeInput("src", topology.Record{Key: "k", Value: "v", Timestamp: 1})
	if err == nil {
		t.Fatal("PipeInput: expected non-nil error from failing processor, got nil")
	}
	if err.Error() != sentinel.Error() {
		t.Errorf("PipeInput error: got %q, want %q", err, sentinel)
	}
}

// TestPipeInput_ProcessorSuccess confirms that a processor returning nil does not surface
// an error and the record reaches the sink unchanged.
func TestPipeInput_ProcessorSuccess(t *testing.T) {
	b := topology.NewBuilder()
	src := b.AddSource("src")
	passthrough := b.AddProcessor("passthrough",
		func(_ context.Context, r topology.Record, forward topology.Forwarder) error {
			forward(r)
			return nil
		},
		src,
	)
	b.AddSink("sink", passthrough)
	topo := b.Build()
	d := topology.NewTestDriver(topo)

	input := topology.Record{Key: "hello", Value: "world", Timestamp: 42}
	if err := d.PipeInput("src", input); err != nil {
		t.Fatalf("PipeInput: unexpected error: %v", err)
	}

	out, err := d.ReadOutput("sink")
	if err != nil {
		t.Fatalf("ReadOutput: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 record at sink, got %d", len(out))
	}
	if out[0].Key != input.Key || out[0].Value != input.Value || out[0].Timestamp != input.Timestamp {
		t.Errorf("sink record mismatch: got %+v, want %+v", out[0], input)
	}
}

// TestTimestampPreservedThroughFilter checks that timestamps survive the filter/map pipeline.
func TestTimestampPreservedThroughFilter(t *testing.T) {
	topo := buildPipeline(t)
	d := topology.NewTestDriver(topo)

	const wantTS int64 = 9999
	if err := d.PipeInput("source", topology.Record{Key: "k", Value: "hello", Timestamp: wantTS}); err != nil {
		t.Fatalf("PipeInput: %v", err)
	}
	out, _ := d.ReadOutput("sink")
	if len(out) != 1 {
		t.Fatalf("expected 1 record, got %d", len(out))
	}
	if out[0].Timestamp != wantTS {
		t.Errorf("Timestamp: got %d, want %d", out[0].Timestamp, wantTS)
	}
}

// TestMultipleSinks verifies that a topology with two sinks from the same processor
// delivers to both independently.
func TestMultipleSinks(t *testing.T) {
	b := topology.NewBuilder()
	src := b.AddSource("src")
	// Identity processor: passes all records through.
	proc := b.AddProcessor("passthrough",
		topology.Filter(func(_, _ any) bool { return true }),
		src,
	)
	b.AddSink("sinkA", proc)
	b.AddSink("sinkB", proc)
	topo := b.Build()
	d := topology.NewTestDriver(topo)

	if err := d.PipeInput("src", topology.Record{Key: "k", Value: "v", Timestamp: 1}); err != nil {
		t.Fatalf("PipeInput: %v", err)
	}

	outA, _ := d.ReadOutput("sinkA")
	outB, _ := d.ReadOutput("sinkB")

	if len(outA) != 1 {
		t.Errorf("sinkA: expected 1, got %d", len(outA))
	}
	if len(outB) != 1 {
		t.Errorf("sinkB: expected 1, got %d", len(outB))
	}
}

package topology_test

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/mortezaPRK/gstream/internal/topology"
)

// buildStatelessPipeline constructs a source → filter → map → sink topology
// (identical shape to the TestDriver tests, reused here).
// filter: keeps only records whose Value is a string with len > 3.
// map: transforms Value to uppercase and sets Key to "mapped-"+original key.
func buildStatelessPipeline(t *testing.T) *topology.Topology {
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
			return fmt.Sprintf("mapped-%v", k), strings.ToUpper(v.(string))
		}),
		filt,
	)
	b.AddSink("sink", mapped)
	return b.Build()
}

// TestExecutor_StatelessPipeline verifies that driving a stateless topology via
// Executor produces output identical to TestDriver.
func TestExecutor_StatelessPipeline(t *testing.T) {
	topo := buildStatelessPipeline(t)

	// Reference output via TestDriver.
	td := topology.NewTestDriver(topo)
	inputs := []topology.Record{
		{Key: "a", Value: "hello", Timestamp: 1},
		{Key: "b", Value: "hi", Timestamp: 2},
		{Key: "c", Value: "world", Timestamp: 3},
		{Key: "d", Value: "bye", Timestamp: 4},
		{Key: "e", Value: "stream", Timestamp: 5},
	}
	for _, r := range inputs {
		if err := td.PipeInput("source", r); err != nil {
			t.Fatalf("TestDriver.PipeInput: %v", err)
		}
	}
	want, err := td.ReadOutput("sink")
	if err != nil {
		t.Fatalf("TestDriver.ReadOutput: %v", err)
	}

	// Same output via Executor (nil stores — stateless).
	exec := topology.NewExecutor(topo, nil)
	for _, r := range inputs {
		if err := exec.Process("source", r); err != nil {
			t.Fatalf("Executor.Process: %v", err)
		}
	}
	got, err := exec.DrainSink("sink")
	if err != nil {
		t.Fatalf("Executor.DrainSink: %v", err)
	}

	if len(got) != len(want) {
		t.Fatalf("output length: got %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Key != want[i].Key || got[i].Value != want[i].Value || got[i].Timestamp != want[i].Timestamp {
			t.Errorf("record[%d]: got %+v, want %+v", i, got[i], want[i])
		}
	}
}

// TestExecutor_StatefulProcessor verifies that stateful processors can access a
// named store via ProcessorContext.Store and forward transformed records.
// The store here is a simple map[string]int that accumulates a per-key count.
func TestExecutor_StatefulProcessor(t *testing.T) {
	b := topology.NewBuilder()
	src := b.AddSource("src")

	// Stateful counter: increments a per-key counter in the store and forwards a
	// record whose Value is the new count.
	counter := topology.StatefulProcessFunc(func(r topology.Record, ctx topology.ProcessorContext) error {
		s := ctx.Store("counts")
		if s == nil {
			return fmt.Errorf("store 'counts' not found")
		}
		m := s.(map[string]int)
		key := fmt.Sprintf("%v", r.Key)
		m[key]++
		ctx.Forward(topology.Record{Key: r.Key, Value: m[key], Timestamp: r.Timestamp})
		return nil
	})

	proc := b.AddStatefulProcessor("counter", counter, []string{"counts"}, src)
	b.AddSink("sink", proc)
	topo := b.Build()

	counts := map[string]int{} // fake store
	exec := topology.NewExecutor(topo, map[string]any{"counts": counts})

	// Pipe the same key three times; we expect counts 1, 2, 3.
	for i := 0; i < 3; i++ {
		if err := exec.Process("src", topology.Record{Key: "k", Value: nil, Timestamp: int64(i)}); err != nil {
			t.Fatalf("Process[%d]: %v", i, err)
		}
	}
	out, err := exec.DrainSink("sink")
	if err != nil {
		t.Fatalf("DrainSink: %v", err)
	}
	if len(out) != 3 {
		t.Fatalf("expected 3 records, got %d", len(out))
	}
	for i, rec := range out {
		if rec.Value != i+1 {
			t.Errorf("record[%d].Value: got %v, want %d", i, rec.Value, i+1)
		}
	}
	// Confirm the store was actually mutated by the processor.
	if counts["k"] != 3 {
		t.Errorf("store counts[k]: got %d, want 3", counts["k"])
	}
}

// TestExecutor_ErrorPropagation verifies that a processor error (stateless or
// stateful) is returned from Process.
func TestExecutor_ErrorPropagation(t *testing.T) {
	sentinel := fmt.Errorf("intentional error")

	t.Run("stateless", func(t *testing.T) {
		b := topology.NewBuilder()
		src := b.AddSource("src")
		failing := b.AddProcessor("failing",
			func(_ topology.Record, _ topology.Forwarder) error { return sentinel },
			src,
		)
		b.AddSink("sink", failing)
		exec := topology.NewExecutor(b.Build(), nil)

		err := exec.Process("src", topology.Record{Key: "k", Value: "v", Timestamp: 1})
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if err.Error() != sentinel.Error() {
			t.Errorf("error: got %q, want %q", err, sentinel)
		}
	})

	t.Run("stateful", func(t *testing.T) {
		b := topology.NewBuilder()
		src := b.AddSource("src")
		failing := b.AddStatefulProcessor("failing",
			func(_ topology.Record, _ topology.ProcessorContext) error { return sentinel },
			nil,
			src,
		)
		b.AddSink("sink", failing)
		exec := topology.NewExecutor(b.Build(), nil)

		err := exec.Process("src", topology.Record{Key: "k", Value: "v", Timestamp: 1})
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if err.Error() != sentinel.Error() {
			t.Errorf("error: got %q, want %q", err, sentinel)
		}
	})
}

// TestExecutorConcurrencySafety creates ONE topology, creates TWO Executors with
// different stores maps, drives records through both (concurrently), and asserts:
//   - sink buffers are not cross-contaminated between Executors
//   - stores maps are not cross-contaminated between Executors
//
// This proves that Executor never permanently mutates the shared node graph.
func TestExecutorConcurrencySafety(t *testing.T) {
	b := topology.NewBuilder()
	src := b.AddSource("src")

	// Stateful accumulator: appends r.Value to the "log" store slice.
	accum := topology.StatefulProcessFunc(func(r topology.Record, ctx topology.ProcessorContext) error {
		s := ctx.Store("log")
		if s == nil {
			return fmt.Errorf("store 'log' not found")
		}
		log := s.(*[]string)
		*log = append(*log, fmt.Sprintf("%v", r.Value))
		ctx.Forward(r)
		return nil
	})

	proc := b.AddStatefulProcessor("accum", accum, []string{"log"}, src)
	b.AddSink("sink", proc)
	topo := b.Build()

	logA := &[]string{}
	logB := &[]string{}

	execA := topology.NewExecutor(topo, map[string]any{"log": logA})
	execB := topology.NewExecutor(topo, map[string]any{"log": logB})

	// Drive both Executors concurrently; each gets a distinct value.
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			if err := execA.Process("src", topology.Record{Key: "a", Value: fmt.Sprintf("A%d", i), Timestamp: int64(i)}); err != nil {
				t.Errorf("execA.Process: %v", err)
			}
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			if err := execB.Process("src", topology.Record{Key: "b", Value: fmt.Sprintf("B%d", i), Timestamp: int64(i)}); err != nil {
				t.Errorf("execB.Process: %v", err)
			}
		}
	}()

	wg.Wait()

	outA, err := execA.DrainSink("sink")
	if err != nil {
		t.Fatalf("execA.DrainSink: %v", err)
	}
	outB, err := execB.DrainSink("sink")
	if err != nil {
		t.Fatalf("execB.DrainSink: %v", err)
	}

	// Each Executor should have exactly 5 records in its sink buffer.
	if len(outA) != 5 {
		t.Errorf("execA sink: got %d records, want 5", len(outA))
	}
	if len(outB) != 5 {
		t.Errorf("execB sink: got %d records, want 5", len(outB))
	}

	// Sink A must contain only "A*" keys; sink B must contain only "B*" keys.
	for i, r := range outA {
		if r.Key != "a" {
			t.Errorf("execA sink[%d].Key: got %v, want a", i, r.Key)
		}
	}
	for i, r := range outB {
		if r.Key != "b" {
			t.Errorf("execB sink[%d].Key: got %v, want b", i, r.Key)
		}
	}

	// Stores must contain only values for their respective Executor.
	for _, v := range *logA {
		if len(v) < 1 || v[0] != 'A' {
			t.Errorf("logA contains non-A value: %q", v)
		}
	}
	for _, v := range *logB {
		if len(v) < 1 || v[0] != 'B' {
			t.Errorf("logB contains non-B value: %q", v)
		}
	}

	if len(*logA) != 5 {
		t.Errorf("logA length: got %d, want 5", len(*logA))
	}
	if len(*logB) != 5 {
		t.Errorf("logB length: got %d, want 5", len(*logB))
	}
}

// TestExecutor_UnknownSourceReturnsError ensures Process returns an error for
// missing source names.
func TestExecutor_UnknownSourceReturnsError(t *testing.T) {
	topo := buildStatelessPipeline(t)
	exec := topology.NewExecutor(topo, nil)

	err := exec.Process("nonexistent", topology.Record{Key: "x", Value: "y"})
	if err == nil {
		t.Fatal("expected error for unknown source, got nil")
	}
}

// TestExecutor_UnknownSinkReturnsError ensures DrainSink returns an error for
// missing sink names.
func TestExecutor_UnknownSinkReturnsError(t *testing.T) {
	topo := buildStatelessPipeline(t)
	exec := topology.NewExecutor(topo, nil)

	_, err := exec.DrainSink("nonexistent")
	if err == nil {
		t.Fatal("expected error for unknown sink, got nil")
	}
}

// TestExecutor_DrainSinkClearsBuffer ensures DrainSink clears the buffer so
// successive calls with no new records return empty.
func TestExecutor_DrainSinkClearsBuffer(t *testing.T) {
	topo := buildStatelessPipeline(t)
	exec := topology.NewExecutor(topo, nil)

	if err := exec.Process("source", topology.Record{Key: "a", Value: "hello", Timestamp: 1}); err != nil {
		t.Fatalf("Process: %v", err)
	}

	out1, _ := exec.DrainSink("sink")
	if len(out1) != 1 {
		t.Fatalf("first DrainSink: expected 1, got %d", len(out1))
	}

	out2, _ := exec.DrainSink("sink")
	if len(out2) != 0 {
		t.Fatalf("second DrainSink (no new records): expected 0, got %d", len(out2))
	}
}

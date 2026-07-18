package topology

import "fmt"

// TestDriver drives a sealed Topology synchronously and deterministically with
// synthetic Records — no Kafka broker and no Pebble store required. This is the
// primary unit-testing tool for all DSL operators (§16).
//
// # Design
//
// TestDriver wires each sink node with a collector ProcessFunc that appends
// records to an in-memory slice. PipeInput injects a Record into a named source
// and drives the DAG depth-first, synchronously. ReadOutput drains the buffered
// output for a named sink. Because execution is synchronous and single-threaded,
// tests are fully deterministic.
//
// This mirrors Kafka Streams' TopologyTestDriver conceptually, as described in §16
// of prd.md.
//
// # Usage
//
//	d := topology.NewTestDriver(topo)
//	err := d.PipeInput("my-source", topology.Record{Key: "k", Value: 42, Timestamp: 1})
//	records, err := d.ReadOutput("my-sink")
//
// # Thread safety
//
// TestDriver is NOT safe for concurrent use. Tests should drive it from a single
// goroutine. This is intentional — the TestDriver models the single-threaded,
// ordered execution of a single task (§7).
type TestDriver struct {
	topo    *Topology
	buffers map[string][]Record // keyed by sink name
}

// NewTestDriver creates a TestDriver for the given Topology. It installs a
// collector ProcessFunc on every sink node. The Topology must not be used with
// another TestDriver concurrently (not safe for concurrent modification).
func NewTestDriver(topo *Topology) *TestDriver {
	d := &TestDriver{
		topo:    topo,
		buffers: make(map[string][]Record, len(topo.sinks)),
	}

	// Wire each sink to append to its dedicated buffer slice.
	for name, sinkNode := range topo.sinks {
		// Capture loop variables explicitly (Go ≤ 1.21 closure capture fix; 1.22+ fixes
		// this automatically, but we keep it explicit for clarity).
		n := sinkNode
		sinkName := name
		d.buffers[sinkName] = nil // initialize the buffer key
		n.processFn = func(r Record, _ Forwarder) error {
			d.buffers[sinkName] = append(d.buffers[sinkName], r)
			return nil
		}
	}

	return d
}

// PipeInput injects a Record into the named source node and drives the topology
// synchronously to completion. All processors and sinks reachable from that source
// execute before PipeInput returns.
//
// Returns an error if any processor or sink returns an error, or if the source
// name is not found in the topology.
func (d *TestDriver) PipeInput(sourceName string, r Record) error {
	src, ok := d.topo.sources[sourceName]
	if !ok {
		return fmt.Errorf("topology: source %q not found in topology (sources: %v)",
			sourceName, d.topo.SourceNames())
	}
	return src.processWithErr(r)
}

// ReadOutput returns all Records collected by the named sink since the last
// ReadOutput call (or since TestDriver creation). The sink's buffer is cleared
// on each call, so successive calls return only new records.
//
// Returns an error if the sink name is not found in the topology.
func (d *TestDriver) ReadOutput(sinkName string) ([]Record, error) {
	buf, ok := d.buffers[sinkName]
	if !ok {
		return nil, fmt.Errorf("topology: sink %q not found in topology (sinks: %v)",
			sinkName, d.topo.SinkNames())
	}
	d.buffers[sinkName] = nil // drain the buffer
	return buf, nil
}

// PeekOutput returns the Records collected by the named sink WITHOUT clearing the
// buffer. Useful for intermediate assertions that want to inspect output without
// consuming it.
func (d *TestDriver) PeekOutput(sinkName string) ([]Record, error) {
	buf, ok := d.buffers[sinkName]
	if !ok {
		return nil, fmt.Errorf("topology: sink %q not found in topology (sinks: %v)",
			sinkName, d.topo.SinkNames())
	}
	// Return a copy so the caller cannot mutate the internal buffer.
	out := make([]Record, len(buf))
	copy(out, buf)
	return out, nil
}

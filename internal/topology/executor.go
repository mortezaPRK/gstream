package topology

import "fmt"

// Executor drives a sealed Topology for production (one per task/partition, §7).
//
// Unlike TestDriver, Executor does NOT permanently mutate node.processFn on the
// shared *Topology.  Concurrency safety is achieved as follows:
//
//   - Sink nodes are intercepted by name during DAG traversal via
//     processWithCtxAndHook.  The hook routes arriving records into per-sink
//     buffers that are owned exclusively by this Executor instance.
//   - The shared node graph (node.processFn / node.statefulFn / node.downstreams)
//     is never written after Build() returns, so multiple Executors over the same
//     *Topology read-only from the graph and write only to their own private
//     buffers — there is no cross-contamination.
//   - The stores map is also owned by the caller and supplied per-Executor, so two
//     Executors with different stores maps cannot affect each other's state.
//
// See TestExecutorConcurrencySafety in executor_test.go for a concrete proof.
type Executor struct {
	topo       *Topology
	stores     map[string]any
	buffers    map[string][]Record // keyed by sink name; private to this Executor
	streamTime *int64              // per-task stream-time pointer; nil for stateless paths
}

// NewExecutor creates an Executor for the given Topology. stores is the map of
// named state stores available to stateful processors (may be nil for stateless
// topologies). Each Executor maintains its own independent sink buffers; two
// Executors over the same *Topology do not share any mutable state.
//
// NewExecutor does not track stream-time; processors may call StreamTime() but
// will always receive 0. Use NewExecutorWithStreamTime when windowed operators or
// stream-time-aware processors are present.
func NewExecutor(topo *Topology, stores map[string]any) *Executor {
	buffers := make(map[string][]Record, len(topo.sinks))
	for name := range topo.sinks {
		buffers[name] = nil
	}
	return &Executor{topo: topo, stores: stores, buffers: buffers, streamTime: nil}
}

// NewExecutorWithStreamTime creates an Executor that threads the provided
// streamTime pointer into every ProcessorContext built during DAG traversal.
// All stateful processors in the topology share the same *int64, so any call to
// ctx.AdvanceStreamTime(ts) from any processor is immediately visible to the next
// processor in the same or subsequent Process calls.
//
// The caller owns streamTime and may read it between Process calls to observe the
// current watermark. Passing a nil pointer degrades to the same behaviour as
// NewExecutor (StreamTime() returns 0, AdvanceStreamTime is a no-op).
func NewExecutorWithStreamTime(topo *Topology, stores map[string]any, streamTime *int64) *Executor {
	buffers := make(map[string][]Record, len(topo.sinks))
	for name := range topo.sinks {
		buffers[name] = nil
	}
	return &Executor{topo: topo, stores: stores, buffers: buffers, streamTime: streamTime}
}

// Process injects a Record into the named source node and drives the topology
// synchronously to completion using processWithCtxAndHook. Sink records are
// captured in this Executor's private buffers without touching node.processFn.
//
// Returns an error if any processor returns an error, or if the source name is
// not found in the topology.
func (e *Executor) Process(sourceName string, r Record) error {
	src, ok := e.topo.sources[sourceName]
	if !ok {
		return fmt.Errorf("topology: source %q not found in topology (sources: %v)",
			sourceName, e.topo.SourceNames())
	}
	return src.processWithCtxAndHook(r, e.stores, e.streamTime, func(sinkName string, rec Record) {
		e.buffers[sinkName] = append(e.buffers[sinkName], rec)
	})
}

// DrainSink returns the buffered records for the named sink and clears the buffer.
// Intended for per-Process-call usage by the runtime: call after each Process to
// collect produced output before the next record is driven through the topology.
//
// Returns an error if the sink name is not found in the topology.
func (e *Executor) DrainSink(sinkName string) ([]Record, error) {
	buf, ok := e.buffers[sinkName]
	if !ok {
		return nil, fmt.Errorf("topology: sink %q not found in topology (sinks: %v)",
			sinkName, e.topo.SinkNames())
	}
	e.buffers[sinkName] = nil
	return buf, nil
}

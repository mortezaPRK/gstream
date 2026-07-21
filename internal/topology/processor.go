package topology

// Forwarder is given to a Processor so it can emit records downstream.
// Each call to Forward enqueues the record for the next nodes in the DAG.
// Multiple calls within one Process invocation are allowed (fan-out, flat-map).
type Forwarder func(r Record)

// ProcessFunc is the core processing function type.
// It receives an incoming Record, a Forwarder to emit zero or more downstream
// Records, and returns an error (nil = success). Returning an error halts the
// synchronous pipeline in the TestDriver and propagates to the caller of PipeInput.
type ProcessFunc func(r Record, forward Forwarder) error

// ProcessorContext is passed to stateful processors: forwarding + named store access.
type ProcessorContext interface {
	Forward(r Record)
	Store(name string) any // concrete store; caller type-asserts. nil if unregistered.
	// StreamTime returns the current per-task stream-time: the maximum observed
	// event timestamp (Unix ms) seen so far across all records processed by this
	// task. Returns 0 if no record has been processed yet.
	StreamTime() int64
	// AdvanceStreamTime advances the stream-time to max(current, ts). No-op when
	// ts is less than or equal to the current stream-time.
	AdvanceStreamTime(ts int64)
}

// StatefulProcessFunc is a processor with state access.
type StatefulProcessFunc func(r Record, ctx ProcessorContext) error

// node is the internal DAG node; it wraps a ProcessFunc with its downstream edges.
// Source and Sink nodes use special-cased ProcessFuncs (see source.go / sink.go).
// Exactly one of processFn or statefulFn is non-nil per node.
type node struct {
	name        string
	processFn   ProcessFunc
	statefulFn  StatefulProcessFunc
	storeNames  []string
	downstreams []*node
	isSink      bool // true for nodes registered via AddSink
}

// processorCtxImpl implements ProcessorContext. It holds the forward closure and
// a snapshot of the stores map for the current execution. It is created fresh for
// each stateful processor invocation and is not shared between invocations.
// streamTime is a pointer shared across all processorCtxImpl instances within the
// same task; nil means no stream-time tracking (safe no-op for stateless paths).
type processorCtxImpl struct {
	forwardFn  func(r Record)
	stores     map[string]any
	streamTime *int64
}

func (c *processorCtxImpl) Forward(r Record) { c.forwardFn(r) }

func (c *processorCtxImpl) Store(name string) any {
	if c.stores == nil {
		return nil
	}
	return c.stores[name] // nil if unregistered
}

// StreamTime returns the current stream-time value. Returns 0 when streamTime
// pointer is nil (stateless paths where stream-time tracking is unused).
func (c *processorCtxImpl) StreamTime() int64 {
	if c.streamTime == nil {
		return 0
	}
	return *c.streamTime
}

// AdvanceStreamTime sets stream-time to max(current, ts). No-op when streamTime
// pointer is nil or ts does not exceed the current value.
func (c *processorCtxImpl) AdvanceStreamTime(ts int64) {
	if c.streamTime == nil {
		return
	}
	if ts > *c.streamTime {
		*c.streamTime = ts
	}
}

// processWithCtx drives execution of this node with the given store map, then
// recursively drives all downstream nodes for each forwarded record.
//
// The named return (firstErr) accumulates the first error seen anywhere in the
// subtree: downstream errors win over the current-node error (first-error-wins,
// stop-on-error semantics are preserved from the original processWithErr).
//
// When stores is nil all Store() calls return nil; this is the normal path for
// stateless processors (processFn set, statefulFn nil).
// streamTime is a shared pointer owned by the Executor (or nil for the TestDriver
// path); the same pointer is threaded into every processorCtxImpl in one traversal.
func (n *node) processWithCtx(r Record, stores map[string]any, streamTime *int64) (firstErr error) {
	// Build the shared downstream-forwarding closure that both stateless and
	// stateful paths use.  It captures firstErr by reference so downstream
	// errors are accumulated in the outer named return.
	forward := func(out Record) {
		if firstErr != nil {
			return // stop forwarding once we have an error
		}
		for _, ds := range n.downstreams {
			if err := ds.processWithCtx(out, stores, streamTime); err != nil {
				firstErr = err
				return
			}
		}
	}

	if n.statefulFn != nil {
		// Stateful path: build a fresh ProcessorContext backed by the shared
		// forward closure, caller-supplied stores map, and shared stream-time pointer.
		ctx := &processorCtxImpl{forwardFn: forward, stores: stores, streamTime: streamTime}
		if fnErr := n.statefulFn(r, ctx); fnErr != nil && firstErr == nil {
			firstErr = fnErr
		}
	} else {
		// Stateless path: wrap the forward closure in the Forwarder type and
		// delegate to processFn exactly as the original processWithErr did.
		if fnErr := n.processFn(r, Forwarder(forward)); fnErr != nil && firstErr == nil {
			firstErr = fnErr
		}
	}
	return
}

// processWithCtxAndHook is identical to processWithCtx except that for nodes
// marked isSink==true it calls onSink(n.name, r) instead of executing the node's
// processFn. This allows the Executor to collect sink output into its own private
// buffers WITHOUT permanently mutating node.processFn on the shared *Topology.
//
// onSink must not be nil when the topology contains sink nodes.
// streamTime is a shared pointer owned by the Executor; nil is safe (no-op).
func (n *node) processWithCtxAndHook(r Record, stores map[string]any, streamTime *int64, onSink func(sinkName string, r Record)) (firstErr error) {
	// Sink nodes are leaf interceptors: hand off to the caller's hook instead
	// of running the placeholder processFn.
	if n.isSink {
		onSink(n.name, r)
		return nil
	}

	forward := func(out Record) {
		if firstErr != nil {
			return
		}
		for _, ds := range n.downstreams {
			if err := ds.processWithCtxAndHook(out, stores, streamTime, onSink); err != nil {
				firstErr = err
				return
			}
		}
	}

	if n.statefulFn != nil {
		ctx := &processorCtxImpl{forwardFn: forward, stores: stores, streamTime: streamTime}
		if fnErr := n.statefulFn(r, ctx); fnErr != nil && firstErr == nil {
			firstErr = fnErr
		}
	} else {
		if fnErr := n.processFn(r, Forwarder(forward)); fnErr != nil && firstErr == nil {
			firstErr = fnErr
		}
	}
	return
}

// processWithErr drives the node using processWithCtx with no stores (nil map)
// and no stream-time tracking (nil pointer). This keeps the TestDriver — which
// calls processWithErr directly — working identically for all stateless topologies.
// The error semantics (first error wins, stop-on-error, current-node error only if
// no downstream error) are unchanged.
func (n *node) processWithErr(r Record) error {
	return n.processWithCtx(r, nil, nil)
}

// Filter returns a ProcessFunc that passes the Record downstream only when
// predicate returns true. This is the simplest example of a stateless filter
// processor (§6.2: KStream[K,V].Filter).
func Filter(predicate func(key, value any) bool) ProcessFunc {
	return func(r Record, forward Forwarder) error {
		if predicate(r.Key, r.Value) {
			forward(r)
		}
		return nil
	}
}

// Mapper returns a ProcessFunc that transforms every Record 1-to-1 using
// mapFn. The result is forwarded downstream. This models KStream[K,V].Map (§6.2).
func Mapper(mapFn func(key, value any) (newKey, newValue any)) ProcessFunc {
	return func(r Record, forward Forwarder) error {
		k2, v2 := mapFn(r.Key, r.Value)
		forward(Record{Key: k2, Value: v2, Timestamp: r.Timestamp})
		return nil
	}
}

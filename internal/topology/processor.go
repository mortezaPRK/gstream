package topology

import "context"

// Forwarder is given to a Processor so it can emit records downstream.
// Each call to Forward enqueues the record for the next nodes in the DAG.
// Multiple calls within one Process invocation are allowed (fan-out, flat-map).
type Forwarder func(r Record)

// ProcessFunc is the core processing function type.
// ctx carries cancellation and deadlines for the current pipeline invocation.
// It receives an incoming Record, a Forwarder to emit zero or more downstream
// Records, and returns an error (nil = success). Returning an error halts the
// synchronous pipeline in the TestDriver and propagates to the caller of PipeInput.
type ProcessFunc func(ctx context.Context, r Record, forward Forwarder) error

// ProcessorContext is passed to stateful processors: forwarding + named store access.
// ctx (cancellation/deadlines) is a SEPARATE explicit parameter on StatefulProcessFunc,
// not embedded here. Do not stash context.Context in ProcessorContext values.
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
// ctx is the real context.Context for cancellation/deadlines — it is a separate
// first parameter and must NOT be stored inside pctx (ProcessorContext).
type StatefulProcessFunc func(ctx context.Context, r Record, pctx ProcessorContext) error

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
// The named return (firstErr) accumulates the first error seen anywhere in the
// subtree (first-error-wins, stop-on-error).
func (n *node) processWithCtx(ctx context.Context, r Record, stores map[string]any, streamTime *int64) (firstErr error) {
	forward := func(out Record) {
		if firstErr != nil {
			return // stop forwarding once we have an error
		}
		for _, ds := range n.downstreams {
			if err := ds.processWithCtx(ctx, out, stores, streamTime); err != nil {
				firstErr = err
				return
			}
		}
	}

	if n.statefulFn != nil {
		pctx := &processorCtxImpl{forwardFn: forward, stores: stores, streamTime: streamTime}
		if fnErr := n.statefulFn(ctx, r, pctx); fnErr != nil && firstErr == nil {
			firstErr = fnErr
		}
	} else {
		if fnErr := n.processFn(ctx, r, Forwarder(forward)); fnErr != nil && firstErr == nil {
			firstErr = fnErr
		}
	}
	return
}

// processWithCtxAndHook is identical to processWithCtx except that for nodes
// marked isSink==true it calls onSink(n.name, r) instead of executing the node's
// processFn. This allows the Executor to collect sink output into its own private
// buffers without permanently mutating node.processFn on the shared *Topology.
// onSink must not be nil when the topology contains sink nodes.
func (n *node) processWithCtxAndHook(ctx context.Context, r Record, stores map[string]any, streamTime *int64, onSink func(sinkName string, r Record)) (firstErr error) {
	if n.isSink {
		onSink(n.name, r)
		return nil
	}

	forward := func(out Record) {
		if firstErr != nil {
			return
		}
		for _, ds := range n.downstreams {
			if err := ds.processWithCtxAndHook(ctx, out, stores, streamTime, onSink); err != nil {
				firstErr = err
				return
			}
		}
	}

	if n.statefulFn != nil {
		pctx := &processorCtxImpl{forwardFn: forward, stores: stores, streamTime: streamTime}
		if fnErr := n.statefulFn(ctx, r, pctx); fnErr != nil && firstErr == nil {
			firstErr = fnErr
		}
	} else {
		if fnErr := n.processFn(ctx, r, Forwarder(forward)); fnErr != nil && firstErr == nil {
			firstErr = fnErr
		}
	}
	return
}

// processWithErr drives the node using processWithCtx with no stores, no
// stream-time tracking, and context.Background(). Used by TestDriver.
func (n *node) processWithErr(r Record) error {
	return n.processWithCtx(context.Background(), r, nil, nil)
}

// Filter returns a ProcessFunc that passes the Record downstream only when predicate returns true.
func Filter(predicate func(key, value any) bool) ProcessFunc {
	return func(_ context.Context, r Record, forward Forwarder) error {
		if predicate(r.Key, r.Value) {
			forward(r)
		}
		return nil
	}
}

// Mapper returns a ProcessFunc that transforms every Record 1-to-1 using mapFn.
func Mapper(mapFn func(key, value any) (newKey, newValue any)) ProcessFunc {
	return func(_ context.Context, r Record, forward Forwarder) error {
		k2, v2 := mapFn(r.Key, r.Value)
		forward(Record{Key: k2, Value: v2, Timestamp: r.Timestamp})
		return nil
	}
}

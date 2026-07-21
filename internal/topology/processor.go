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
type processorCtxImpl struct {
	forwardFn func(r Record)
	stores    map[string]any
}

func (c *processorCtxImpl) Forward(r Record) { c.forwardFn(r) }

func (c *processorCtxImpl) Store(name string) any {
	if c.stores == nil {
		return nil
	}
	return c.stores[name] // nil if unregistered
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
func (n *node) processWithCtx(r Record, stores map[string]any) (firstErr error) {
	// Build the shared downstream-forwarding closure that both stateless and
	// stateful paths use.  It captures firstErr by reference so downstream
	// errors are accumulated in the outer named return.
	forward := func(out Record) {
		if firstErr != nil {
			return // stop forwarding once we have an error
		}
		for _, ds := range n.downstreams {
			if err := ds.processWithCtx(out, stores); err != nil {
				firstErr = err
				return
			}
		}
	}

	if n.statefulFn != nil {
		// Stateful path: build a fresh ProcessorContext backed by the shared
		// forward closure and the caller-supplied stores map.
		ctx := &processorCtxImpl{forwardFn: forward, stores: stores}
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
func (n *node) processWithCtxAndHook(r Record, stores map[string]any, onSink func(sinkName string, r Record)) (firstErr error) {
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
			if err := ds.processWithCtxAndHook(out, stores, onSink); err != nil {
				firstErr = err
				return
			}
		}
	}

	if n.statefulFn != nil {
		ctx := &processorCtxImpl{forwardFn: forward, stores: stores}
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

// processWithErr drives the node using processWithCtx with no stores (nil map).
// This keeps the TestDriver — which calls processWithErr directly — working
// identically for all stateless topologies. The error semantics (first error wins,
// stop-on-error, current-node error only if no downstream error) are unchanged.
func (n *node) processWithErr(r Record) error {
	return n.processWithCtx(r, nil)
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

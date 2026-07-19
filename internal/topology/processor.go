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

// node is the internal DAG node; it wraps a ProcessFunc with its downstream edges.
// Source and Sink nodes use special-cased ProcessFuncs (see source.go / sink.go).
type node struct {
	name        string
	processFn   ProcessFunc
	downstreams []*node
}

// processWithErr is like process but propagates the first error seen in any
// downstream through the forwarder chain. It is used internally by the forwarding
// loop to stop early.
//
// The named return (firstErr) accumulates downstream errors encountered inside the
// Forwarder closure.  We must NOT use an explicit "return processFn(...)" here
// because that would override firstErr with the current-node result even when a
// downstream already set it.  Instead we assign processFn's own error only when
// no downstream error has already been captured.
func (n *node) processWithErr(r Record) (firstErr error) {
	if fnErr := n.processFn(r, func(out Record) {
		if firstErr != nil {
			return // stop forwarding once we have an error
		}
		for _, ds := range n.downstreams {
			if err := ds.processWithErr(out); err != nil {
				firstErr = err
				return
			}
		}
	}); fnErr != nil && firstErr == nil {
		// The current node itself errored (and no downstream error was captured).
		firstErr = fnErr
	}
	return
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

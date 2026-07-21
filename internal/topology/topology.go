package topology

import "fmt"

// Topology is an immutable, sealed processor DAG. It is produced by Builder.Build()
// and consumed by the runtime (one instance per assigned partition task, §7) or by
// TestDriver for broker-free testing (§16).
//
// The topology holds references to all named nodes (sources, processors, sinks) so
// that the driver/runtime can look them up by name to inject or collect records.
type Topology struct {
	sources map[string]*node // keyed by source name
	sinks   map[string]*node // keyed by sink name
	nodes   map[string]*node // all named nodes (sources ∪ processors ∪ sinks)
}

// SourceNames returns the names of all source nodes in the topology.
func (t *Topology) SourceNames() []string {
	names := make([]string, 0, len(t.sources))
	for name := range t.sources {
		names = append(names, name)
	}
	return names
}

// SinkNames returns the names of all sink nodes in the topology.
func (t *Topology) SinkNames() []string {
	names := make([]string, 0, len(t.sinks))
	for name := range t.sinks {
		names = append(names, name)
	}
	return names
}

// Builder assembles a Topology. It is not safe for concurrent use during
// construction; call Build() once all nodes are added.
//
// Typical usage:
//
//	b := topology.NewBuilder()
//	src  := b.AddSource("src")
//	filt := b.AddProcessor("filter", topology.Filter(myPred), src)
//	mapp := b.AddProcessor("map",    topology.Mapper(myMap),  filt)
//	b.AddSink("sink", mapp)
//	topo := b.Build()
type Builder struct {
	sources map[string]*node
	sinks   map[string]*node
	nodes   map[string]*node
}

// NewBuilder returns an empty Builder.
func NewBuilder() *Builder {
	return &Builder{
		sources: make(map[string]*node),
		sinks:   make(map[string]*node),
		nodes:   make(map[string]*node),
	}
}

// AddSource registers a named source node. The returned value is used as the
// parent argument to AddProcessor or AddSink.
func (b *Builder) AddSource(name string) string {
	if _, exists := b.nodes[name]; exists {
		panic(fmt.Sprintf("topology: node %q already exists", name))
	}
	n := &node{
		name: name,
		// Source's ProcessFunc simply forwards the record as-is; the TestDriver
		// calls processWithErr directly on the source node after this injection.
		processFn: func(r Record, forward Forwarder) error {
			forward(r)
			return nil
		},
	}
	b.nodes[name] = n
	b.sources[name] = n
	return name
}

// AddProcessor registers a named processor node with the given ProcessFunc.
// parents must be names of previously added nodes; their output edges are wired
// to this new processor. Returns the new node's name for chaining.
func (b *Builder) AddProcessor(name string, fn ProcessFunc, parents ...string) string {
	if _, exists := b.nodes[name]; exists {
		panic(fmt.Sprintf("topology: node %q already exists", name))
	}
	if len(parents) == 0 {
		panic(fmt.Sprintf("topology: processor %q must have at least one parent", name))
	}
	n := &node{name: name, processFn: fn}
	b.nodes[name] = n

	for _, parentName := range parents {
		p, ok := b.nodes[parentName]
		if !ok {
			panic(fmt.Sprintf("topology: parent %q not found for processor %q", parentName, name))
		}
		p.downstreams = append(p.downstreams, n)
	}
	return name
}

// AddStatefulProcessor registers a named stateful processor node with the given
// StatefulProcessFunc and an optional list of store names the processor will
// access via ProcessorContext.Store. It mirrors AddProcessor but sets node.statefulFn
// and node.storeNames instead of node.processFn.
// parents must be names of previously added nodes.
func (b *Builder) AddStatefulProcessor(name string, fn StatefulProcessFunc, storeNames []string, parents ...string) string {
	if _, exists := b.nodes[name]; exists {
		panic(fmt.Sprintf("topology: node %q already exists", name))
	}
	if len(parents) == 0 {
		panic(fmt.Sprintf("topology: processor %q must have at least one parent", name))
	}
	n := &node{name: name, statefulFn: fn, storeNames: storeNames}
	b.nodes[name] = n

	for _, parentName := range parents {
		p, ok := b.nodes[parentName]
		if !ok {
			panic(fmt.Sprintf("topology: parent %q not found for processor %q", parentName, name))
		}
		p.downstreams = append(p.downstreams, n)
	}
	return name
}

// AddSink registers a named sink node. Sink nodes have no downstream; records
// that reach a sink are captured for the runtime / TestDriver to collect.
// parents must be names of previously added nodes.
func (b *Builder) AddSink(name string, parents ...string) string {
	if _, exists := b.nodes[name]; exists {
		panic(fmt.Sprintf("topology: node %q already exists", name))
	}
	if len(parents) == 0 {
		panic(fmt.Sprintf("topology: sink %q must have at least one parent", name))
	}

	// The sink ProcessFunc is a stub; records are captured by the TestDriver by
	// replacing it with a collector-injecting function at Build() time. In
	// production runtime the sink ProcessFunc writes to Kafka.
	n := &node{
		name:      name,
		processFn: sinkPlaceholderFn,
		isSink:    true,
	}
	b.nodes[name] = n
	b.sinks[name] = n

	for _, parentName := range parents {
		p, ok := b.nodes[parentName]
		if !ok {
			panic(fmt.Sprintf("topology: parent %q not found for sink %q", parentName, name))
		}
		p.downstreams = append(p.downstreams, n)
	}
	return name
}

// sinkPlaceholderFn is replaced by the TestDriver before driving records.
func sinkPlaceholderFn(_ Record, _ Forwarder) error {
	return fmt.Errorf("topology: sink has no handler installed (use TestDriver or runtime)")
}

// Build seals the builder and returns an immutable Topology. The builder must
// not be used after Build() is called.
func (b *Builder) Build() *Topology {
	if len(b.sources) == 0 {
		panic("topology: topology must have at least one source")
	}
	if len(b.sinks) == 0 {
		panic("topology: topology must have at least one sink")
	}
	return &Topology{
		sources: b.sources,
		sinks:   b.sinks,
		nodes:   b.nodes,
	}
}

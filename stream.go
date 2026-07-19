package gstream

import (
	"fmt"

	"github.com/mortezaPRK/gstream/internal/topology"
)

// StreamBuilder assembles a typed topology by wrapping topology.Builder (§6.3).
//
// Usage:
//
//	b := gstream.NewStreamBuilder()
//	src := gstream.Stream[string, Order](b, "orders", "orders-src",
//	    gstream.JSONSerde[string]{}, gstream.JSONSerde[Order]{})
//	src.To("enriched-orders", "enriched-sink",
//	    gstream.JSONSerde[string]{}, gstream.JSONSerde[Order]{})
//	built := b.Build()
//
// Operator methods (Filter, Map, SelectKey, etc.) are defined in operators.go
// (same package).
type StreamBuilder struct {
	internal     *topology.Builder
	counter      int
	sources      map[string]SourceBinding
	sinks        map[string]SinkBinding
	repartitions map[string]bool
}

// NewStreamBuilder creates and returns a new StreamBuilder ready to accept
// source, processor, and sink registrations (§6.3).
func NewStreamBuilder() *StreamBuilder {
	return &StreamBuilder{
		internal:     topology.NewBuilder(),
		sources:      make(map[string]SourceBinding),
		sinks:        make(map[string]SinkBinding),
		repartitions: make(map[string]bool),
	}
}

// nextName returns a unique node name for the given prefix by appending and
// incrementing an internal counter. It is unexported and used by operators.go
// (same package) to obtain deterministic node names, e.g. "map-0", "filter-1".
func (b *StreamBuilder) nextName(prefix string) string {
	name := fmt.Sprintf("%s-%d", prefix, b.counter)
	b.counter++
	return name
}

// KStream is a typed, lazy stream that compiles to a topology DAG node (§6.3).
//
// K and V are the key and value types for records on this stream. KStream does
// not process data eagerly; it records DAG wiring that the runtime or
// TestDriver executes after Build().
//
// Operator methods (Filter, Map, MapValues, SelectKey, etc.) are defined in
// operators.go (same package).
type KStream[K, V any] struct {
	builder  *StreamBuilder
	nodeName string
}

// SourceBinding is the type-erased decode pair for a source node. It is stored
// in StreamBuilder and copied verbatim into BuiltTopology. The runtime uses
// DecodeKey/DecodeVal to decode raw Kafka bytes into typed values before feeding
// them into the processor DAG (§10, §6.3).
type SourceBinding struct {
	// Topic is the Kafka source topic name.
	Topic string

	// DecodeKey deserializes a raw Kafka key byte slice into an any value.
	// The underlying concrete type matches K from the originating Stream[K,V] call.
	DecodeKey func([]byte) (any, error)

	// DecodeVal deserializes a raw Kafka value byte slice into an any value.
	// The underlying concrete type matches V from the originating Stream[K,V] call.
	DecodeVal func([]byte) (any, error)
}

// SinkBinding is the type-erased encode pair for a sink node, bound to the
// output K,V types at the call site of KStream[K,V].To() (§6.3, §10).
//
// This corrects the original Adapter single-serde bug: each EncodeKey/EncodeVal
// closure captures the concrete Serde[K] and Serde[V] at the point where To() is
// called, so type-changing operators (Map, SelectKey) are handled correctly.
type SinkBinding struct {
	// Topic is the Kafka sink topic name.
	Topic string

	// EncodeKey serializes an any value (underlying concrete type K) into bytes.
	EncodeKey func(any) ([]byte, error)

	// EncodeVal serializes an any value (underlying concrete type V) into bytes.
	EncodeVal func(any) ([]byte, error)
}

// BuiltTopology is the immutable, sealed output of StreamBuilder.Build() (§6.3).
// It is consumed by the runtime (to spin up tasks) and by the TestDriver
// (for broker-free testing, §16). After Build() is called the StreamBuilder
// must not be used further.
type BuiltTopology struct {
	// Topology is the sealed processor DAG from internal/topology.
	Topology *topology.Topology

	// Sources maps source node names to their type-erased decode bindings.
	Sources map[string]SourceBinding

	// Sinks maps sink node names to their type-erased encode bindings.
	Sinks map[string]SinkBinding
}

// Stream registers a typed source node in the topology and returns a
// KStream[K,V] representing that source (§6.2, §10).
//
// topic is the Kafka topic to consume. sourceName is the unique node name for
// this source in the DAG. keySerde and valSerde are used to decode raw bytes
// into typed K and V values at the source boundary; the closures stored in
// SourceBinding erase the concrete types to any while preserving their
// behaviour.
func Stream[K, V any](b *StreamBuilder, topic, sourceName string, keySerde Serde[K], valSerde Serde[V]) KStream[K, V] {
	b.internal.AddSource(sourceName)

	b.sources[sourceName] = SourceBinding{
		Topic: topic,
		DecodeKey: func(raw []byte) (any, error) {
			return keySerde.Deserialize(raw)
		},
		DecodeVal: func(raw []byte) (any, error) {
			return valSerde.Deserialize(raw)
		},
	}

	return KStream[K, V]{builder: b, nodeName: sourceName}
}

// To registers a typed sink node in the topology, binding the encode closures
// to the current K,V types of this KStream (§6.2, §10).
//
// topic is the Kafka sink topic. sinkName is the unique node name in the DAG.
// keySerde and valSerde encode K/V values back to bytes at the sink boundary.
//
// Each encode closure performs a two-value type assertion so that a type mismatch
// (e.g. a key-changing operator that was not accounted for) surfaces as an
// explicit error rather than a silent panic.
func (s KStream[K, V]) To(topic, sinkName string, keySerde Serde[K], valSerde Serde[V]) {
	s.builder.internal.AddSink(sinkName, s.nodeName)

	s.builder.sinks[sinkName] = SinkBinding{
		Topic: topic,
		EncodeKey: func(x any) ([]byte, error) {
			v, ok := x.(K)
			if !ok {
				return nil, fmt.Errorf("gstream: sink %q EncodeKey: expected %T, got %T", sinkName, *new(K), x)
			}
			return keySerde.Serialize(v)
		},
		EncodeVal: func(x any) ([]byte, error) {
			v, ok := x.(V)
			if !ok {
				return nil, fmt.Errorf("gstream: sink %q EncodeVal: expected %T, got %T", sinkName, *new(V), x)
			}
			return valSerde.Serialize(v)
		},
	}
}

// Build seals the topology and returns an immutable BuiltTopology (§6.3).
//
// After Build() the StreamBuilder must not be used further; the underlying
// topology.Builder is consumed.
//
// Note: repartitions (marked by key-changing operators such as SelectKey) are
// tracked in b.repartitions but repartition topic creation/management is deferred
// to P4 (§6.3, §14). The field is preserved so P4 can iterate over it without
// API changes.
func (b *StreamBuilder) Build() *BuiltTopology {
	return &BuiltTopology{
		Topology: b.internal.Build(),
		Sources:  b.sources,
		Sinks:    b.sinks,
	}
}

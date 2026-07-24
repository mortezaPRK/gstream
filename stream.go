package gstream

import (
	"fmt"

	"github.com/mortezaPRK/gstream/internal/topology"
)

// StreamBuilder assembles a typed topology.
//
// Operator methods (Filter, Map, SelectKey, etc.) are defined in operators.go
// (same package).
type StreamBuilder struct {
	internal            *topology.Builder
	counter             int
	sources             map[string]SourceBinding
	sinks               map[string]SinkBinding
	repartitionBindings map[string]RepartitionBinding
	stores              map[string]StoreBinding
	windowStores        map[string]WindowStoreBinding
	sessionStores       map[string]SessionStoreBinding
	globalTables        map[string]GlobalTableBinding
}

// NewStreamBuilder creates and returns a new StreamBuilder.
func NewStreamBuilder() *StreamBuilder {
	return &StreamBuilder{
		internal:            topology.NewBuilder(),
		sources:             make(map[string]SourceBinding),
		sinks:               make(map[string]SinkBinding),
		repartitionBindings: make(map[string]RepartitionBinding),
		stores:              make(map[string]StoreBinding),
		windowStores:        make(map[string]WindowStoreBinding),
		sessionStores:       make(map[string]SessionStoreBinding),
		globalTables:        make(map[string]GlobalTableBinding),
	}
}

// nextName returns a unique node name for the given prefix, e.g. "map-0", "filter-1".
func (b *StreamBuilder) nextName(prefix string) string {
	name := fmt.Sprintf("%s-%d", prefix, b.counter)
	b.counter++
	return name
}

// KStream is a typed, lazy stream that compiles to a topology DAG node.
//
// K and V are the key and value types. KStream records DAG wiring executed
// after Build().
//
// Operator methods (Filter, Map, MapValues, SelectKey, etc.) are defined in
// operators.go (same package).
type KStream[K, V any] struct {
	builder  *StreamBuilder
	nodeName string
}

// SourceBinding is the type-erased decode pair for a source node. It is stored
// in StreamBuilder and copied verbatim into BuiltTopology.
type SourceBinding struct {
	// Topic is the Kafka source topic name.
	Topic string

	// DecodeKey deserializes a raw Kafka key byte slice. The underlying concrete
	// type matches K from the originating Stream[K,V] call.
	DecodeKey func([]byte) (any, error)

	// DecodeVal deserializes a raw Kafka value byte slice. The underlying concrete
	// type matches V from the originating Stream[K,V] call.
	DecodeVal func([]byte) (any, error)
}

// SinkBinding is the type-erased encode pair for a sink node, bound to the
// output K,V types at the call site of KStream[K,V].To(). Each EncodeKey/EncodeVal
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

// StoreBinding is the type-erased serde pair for a stateful store registered via
// Count or Aggregate. ChangelogTopic carries the bare store name; the runtime
// derives the full Kafka topic as <AppID>-<StoreName>-changelog.
type StoreBinding struct {
	// StoreName is the logical identifier of the state store.
	StoreName string

	// ChangelogTopic is the bare store name; the runtime prepends <AppID>- and
	// appends -changelog to form the actual Kafka topic name.
	ChangelogTopic string

	// EncodeKey serializes an any value (underlying concrete type K) to bytes.
	EncodeKey func(any) ([]byte, error)

	// DecodeKey deserializes bytes back into an any value whose underlying type is K.
	DecodeKey func([]byte) (any, error)

	// EncodeVal serializes an any value (underlying concrete accumulator type A) to bytes.
	EncodeVal func(any) ([]byte, error)

	// DecodeVal deserializes bytes back into an any value whose underlying type is A.
	DecodeVal func([]byte) (any, error)
}

// WindowStoreBinding extends StoreBinding with window-specific metadata.
// The runtime uses WindowDef.MaxSizeMs() together with GraceMs to compute retention.
type WindowStoreBinding struct {
	StoreBinding
	// WindowDef is the WindowDefinition used to assign windows.
	WindowDef WindowDefinition
	// GraceMs is the late-record grace period in milliseconds.
	GraceMs int64
}

// SessionStoreBinding extends StoreBinding with session-specific metadata.
// GapMs is the inactivity gap that closes a session; GraceMs is the late-record
// grace period. Both are in milliseconds.
type SessionStoreBinding struct {
	StoreBinding
	// GapMs is the session inactivity gap in milliseconds.
	GapMs int64
	// GraceMs is the late-record grace period in milliseconds.
	GraceMs int64
}

// GlobalTableBinding is the type-erased serde quad for a GlobalKTable. It
// mirrors the shape of RepartitionBinding (EncodeKey/DecodeKey/EncodeVal/DecodeVal)
// but carries no SinkName/SourceName/Partitions because the global table is not
// a DAG node — it is consumed by a dedicated all-partitions reader (C3) that
// uses Topic directly.
//
// StoreName is the logical store identifier. Topic is the Kafka source topic.
// The four closures are built from keySerde/valSerde at DSL time; their type
// assertions match GlobalTable[K,V]() call-site types.
type GlobalTableBinding struct {
	// StoreName is the logical identifier of the global state store.
	StoreName string

	// Topic is the Kafka topic that backs this global table.
	Topic string

	// EncodeKey serializes an any value (underlying concrete type K) to bytes.
	EncodeKey func(any) ([]byte, error)

	// DecodeKey deserializes bytes back into an any value whose underlying type is K.
	DecodeKey func([]byte) (any, error)

	// EncodeVal serializes an any value (underlying concrete type V) to bytes.
	EncodeVal func(any) ([]byte, error)

	// DecodeVal deserializes bytes back into an any value whose underlying type is V.
	DecodeVal func([]byte) (any, error)
}

// RepartitionBinding is the type-erased serde pair for a repartition topic that
// sits between a key-changing operator (Map, SelectKey) and its downstream
// consumer. It combines the encode shape of SinkBinding with the decode shape of
// SourceBinding so the C3 adapter resolver can register it into resolvedSinks and
// resolvedSources with zero special-casing.
//
// Name is the bare logical name; the runtime derives the full Kafka topic as
// <AppID>-<Name>-repartition. SinkName and SourceName are the DAG node names
// that the topology uses for the write and read sides respectively.
// Partitions is the desired partition count — must match any co-grouped or joined
// topics, enforced by C4 (admin validation).
type RepartitionBinding struct {
	// Name is the bare logical identifier; runtime forms <AppID>-<Name>-repartition.
	Name string

	// SinkName is the topology sink node name for the write side.
	SinkName string

	// SourceName is the topology source node name for the read side.
	SourceName string

	// Partitions is the desired partition count for the repartition topic.
	Partitions int32

	// EncodeKey serializes an any value (underlying concrete type K) into bytes.
	// Mirrors SinkBinding.EncodeKey exactly.
	EncodeKey func(any) ([]byte, error)

	// EncodeVal serializes an any value (underlying concrete type V) into bytes.
	// Mirrors SinkBinding.EncodeVal exactly.
	EncodeVal func(any) ([]byte, error)

	// DecodeKey deserializes a raw Kafka key byte slice.
	// Mirrors SourceBinding.DecodeKey exactly.
	DecodeKey func([]byte) (any, error)

	// DecodeVal deserializes a raw Kafka value byte slice.
	// Mirrors SourceBinding.DecodeVal exactly.
	DecodeVal func([]byte) (any, error)
}

// BuiltTopology is the immutable, sealed output of StreamBuilder.Build().
// After Build() the StreamBuilder must not be used further.
type BuiltTopology struct {
	// Topology is the sealed processor DAG from internal/topology.
	Topology *topology.Topology

	// Sources maps source node names to their type-erased decode bindings.
	Sources map[string]SourceBinding

	// Sinks maps sink node names to their type-erased encode bindings.
	Sinks map[string]SinkBinding

	// RepartitionBindings maps logical repartition names to their combined
	// encode+decode bindings. Populated by Build(); consumed by C3 (adapter).
	RepartitionBindings map[string]RepartitionBinding

	// StoreBindings maps store names to their type-erased serde bindings.
	StoreBindings map[string]StoreBinding

	// WindowStoreBindings maps window store names to their serde + window metadata bindings.
	WindowStoreBindings map[string]WindowStoreBinding

	// SessionStoreBindings maps session store names to their serde + session metadata bindings.
	SessionStoreBindings map[string]SessionStoreBinding

	// GlobalTableBindings maps store names to their type-erased serde bindings for
	// GlobalKTable instances. Populated by Build(); consumed by C3 (all-partitions
	// global table consumer) and C2 (JoinGlobal processor).
	GlobalTableBindings map[string]GlobalTableBinding
}

// Stream registers a typed source node in the topology and returns a KStream[K,V].
//
// topic is the Kafka topic to consume. sourceName is the unique node name in the DAG.
// keySerde and valSerde decode raw bytes into typed K and V values at the source boundary.
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
// to the current K,V types of this KStream.
//
// topic is the Kafka sink topic. sinkName is the unique node name in the DAG.
// keySerde and valSerde encode K/V values back to bytes at the sink boundary.
// Each encode closure type-asserts the value so a type mismatch surfaces as an
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

// Build seals the topology and returns an immutable BuiltTopology.
// After Build() the StreamBuilder must not be used further.
func (b *StreamBuilder) Build() *BuiltTopology {
	return &BuiltTopology{
		Topology:             b.internal.Build(),
		Sources:              b.sources,
		Sinks:                b.sinks,
		RepartitionBindings:  b.repartitionBindings,
		StoreBindings:        b.stores,
		WindowStoreBindings:  b.windowStores,
		SessionStoreBindings: b.sessionStores,
		GlobalTableBindings:  b.globalTables,
	}
}

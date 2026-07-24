package gstream

import "fmt"

// KGroupedStream[K,V] is a typed, lazy intermediate produced by
// KStream.GroupByKey and consumed by Count or Aggregate.
//
// The key distribution is assumed correct: all records with the same key must
// be routed to the same task (partition co-location). GroupByKey does NOT
// introduce a repartition boundary; repartition across key-changing operators
// is not yet supported.
type KGroupedStream[K, V any] struct {
	builder  *StreamBuilder
	nodeName string
	keySerde Serde[K]
	valSerde Serde[V]
}

// KTable[K,V] is a changelog-backed, key-partitioned table produced by a
// stateful aggregation (Count, Aggregate) on a KGroupedStream.
//
// KTable is currently opaque: it holds the topology node anchor and the store
// name for the runtime to wire up, but exposes no query or sink methods.
// storeName identifies the KeyValueStore registered in BuiltTopology.StoreBindings.
type KTable[K, V any] struct {
	builder   *StreamBuilder
	nodeName  string
	storeName string
	// sinkName is the name of the internal ktable-out DAG sink node created by
	// Aggregate. KTable.To() registers a SinkBinding under this name so the runtime
	// routes forwarded K/V records to the user-specified Kafka topic.
	sinkName string
	// keySerde is used by stream-table join to encode the lookup key.
	keySerde Serde[K]
	// valSerde is used by stream-table join to decode the stored value bytes
	// back into the concrete V type. Set by Aggregate (accSerde); nil for
	// windowed/session KTables (key is Windowed[K], not stream-joinable in P4a).
	valSerde Serde[V]
}

// To registers topic as the Kafka sink for the table's update stream.
//
// Every time the table is updated (a new aggregated value for a key) the
// updated key/value is emitted to topic via the normal sink/produce path.
// Under ALO the produce happens after the state-store write; under EOS the
// produce is inside the Kafka transaction, giving exactly-once semantics with
// no additional configuration.
//
// keySerde and valSerde encode the typed K/V at the sink boundary. The same
// serde types used to build the topology should be used here so the on-wire
// bytes are consistent.
//
// To() MAY be called at most once per KTable.  If To() is NOT called the
// table's internal output sink is left unregistered and drained+discarded by
// the runtime — behaviour is unchanged from a KTable without a sink.
func (t KTable[K, V]) To(topic string, keySerde Serde[K], valSerde Serde[V]) {
	t.builder.sinks[t.sinkName] = SinkBinding{
		Topic: topic,
		EncodeKey: func(x any) ([]byte, error) {
			v, ok := x.(K)
			if !ok {
				return nil, fmt.Errorf("gstream: ktable sink %q EncodeKey: expected %T, got %T", t.sinkName, *new(K), x)
			}
			return keySerde.Serialize(v)
		},
		EncodeVal: func(x any) ([]byte, error) {
			v, ok := x.(V)
			if !ok {
				return nil, fmt.Errorf("gstream: ktable sink %q EncodeVal: expected %T, got %T", t.sinkName, *new(V), x)
			}
			return valSerde.Serialize(v)
		},
	}
}

// GlobalKTable[K,V] is a fully-replicated table: every application instance
// reads all partitions of the backing topic from offset 0 at startup and
// materializes the latest value per key into a local key-value store. Because
// every instance holds the complete dataset, joins via KStream.JoinGlobal
// require no co-partitioning — the lookup key does not need to match the stream
// partition key.
//
// storeName identifies the store registered in BuiltTopology.GlobalTableBindings.
// GlobalKTable is NOT a DAG source node: it does not appear in
// Topology.SourceNames() and does not affect per-partition task assignment.
// The backing topic is consumed by a dedicated all-partitions consumer (C3).
type GlobalKTable[K, V any] struct {
	builder   *StreamBuilder
	nodeName  string
	storeName string
	// keySerde encodes the lookup key for store Get calls in JoinGlobal (C2).
	keySerde Serde[K]
	// valSerde decodes stored value bytes back into the concrete V type.
	valSerde Serde[V]
}

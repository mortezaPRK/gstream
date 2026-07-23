package gstream

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
	// keySerde is used by stream-table join to encode the lookup key.
	keySerde Serde[K]
	// valSerde is used by stream-table join to decode the stored value bytes
	// back into the concrete V type. Set by Aggregate (accSerde); nil for
	// windowed/session KTables (key is Windowed[K], not stream-joinable in P4a).
	valSerde Serde[V]
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

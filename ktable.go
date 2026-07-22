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
}

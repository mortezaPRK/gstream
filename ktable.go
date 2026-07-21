package gstream

// KGroupedStream[K,V] is a typed, lazy intermediate representation of a
// KStream that has been grouped by its current key (§6.2). It is produced by
// KStream.GroupByKey and consumed by Count or Aggregate.
//
// The key distribution is assumed correct: all records with the same key must
// be routed to the same task (partition co-location). GroupByKey does NOT
// introduce a repartition boundary in P2; repartition across key-changing
// operators is deferred to P4 (§9).
type KGroupedStream[K, V any] struct {
	builder  *StreamBuilder
	nodeName string
	keySerde Serde[K]
	valSerde Serde[V]
}

// KTable[K,V] is a changelog-backed, key-partitioned table produced by a
// stateful aggregation (Count, Aggregate) on a KGroupedStream (§6.2).
//
// In P2, KTable is opaque: it holds the topology node anchor and the store
// name for the runtime to wire up, but exposes no query or sink methods.
// Interactive table reads and KTable.To() are deferred to §12 (P5+).
//
// The storeName identifies the KeyValueStore registered in
// BuiltTopology.StoreBindings. The runtime uses that binding to open, write
// to, and recover the state store from its changelog topic.
type KTable[K, V any] struct {
	builder   *StreamBuilder
	nodeName  string
	storeName string
}

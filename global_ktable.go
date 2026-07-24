package gstream

import "fmt"

// GlobalTable registers a fully-replicated global table backed by topic.
//
// nodeName is the logical node identifier stored in the returned GlobalKTable
// (used by C2/JoinGlobal to name its processor). storeName is derived
// automatically via b.nextName so callers need not supply it.
//
// GlobalTable does NOT call b.internal.AddSource: the global topic is NOT a
// DAG source and does not appear in Topology.SourceNames(). It is consumed by a
// dedicated all-partitions consumer (C3) that reads GlobalTableBindings
// directly from BuiltTopology. This keeps the global topic out of the
// per-partition task DAG and prevents a phantom source node.
//
// The four Encode/Decode closures mirror the pattern used by Repartition() and
// To(): EncodeKey/EncodeVal type-assert the any argument to K/V and delegate to
// the concrete Serde; DecodeKey/DecodeVal call Deserialize and return as any.
func GlobalTable[K, V any](b *StreamBuilder, topic, nodeName string, keySerde Serde[K], valSerde Serde[V]) GlobalKTable[K, V] {
	storeName := b.nextName("global-store")

	b.globalTables[storeName] = GlobalTableBinding{
		StoreName: storeName,
		Topic:     topic,

		// EncodeKey mirrors SinkBinding.EncodeKey from To(): type-assert any to K,
		// then delegate to keySerde.Serialize.
		EncodeKey: func(x any) ([]byte, error) {
			v, ok := x.(K)
			if !ok {
				return nil, fmt.Errorf("gstream: GlobalTable %q EncodeKey: expected %T, got %T", storeName, *new(K), x)
			}
			return keySerde.Serialize(v)
		},

		// DecodeKey mirrors SourceBinding.DecodeKey from Stream(): delegate to
		// keySerde.Deserialize and return as any.
		DecodeKey: func(raw []byte) (any, error) {
			return keySerde.Deserialize(raw)
		},

		// EncodeVal mirrors SinkBinding.EncodeVal from To(): type-assert any to V,
		// then delegate to valSerde.Serialize.
		EncodeVal: func(x any) ([]byte, error) {
			v, ok := x.(V)
			if !ok {
				return nil, fmt.Errorf("gstream: GlobalTable %q EncodeVal: expected %T, got %T", storeName, *new(V), x)
			}
			return valSerde.Serialize(v)
		},

		// DecodeVal mirrors SourceBinding.DecodeVal from Stream(): delegate to
		// valSerde.Deserialize and return as any.
		DecodeVal: func(raw []byte) (any, error) {
			return valSerde.Deserialize(raw)
		},
	}

	return GlobalKTable[K, V]{
		builder:   b,
		nodeName:  nodeName,
		storeName: storeName,
		keySerde:  keySerde,
		valSerde:  valSerde,
	}
}

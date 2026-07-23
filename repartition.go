package gstream

import "fmt"

// Repartition inserts a round-trip through an internal Kafka topic of the form
// <appID>-<name>-repartition, forcing a shuffle of the stream by its current
// key.  The write side is a sink node (sinkName) that consumes from the upstream
// node; the read side is a new source node (sourceName) that acts as the root for
// all downstream operators.  This severs the in-process execution path so the
// runtime can route records through a correctly partitioned Kafka topic before
// they reach downstream processors.
//
// partitions must equal the partition count of every topic in the same
// co-partition group (e.g. all inputs to a downstream join or aggregate).
// Co-partitioning is the caller's responsibility — the same as vanilla Kafka
// Streams.  Keys with degenerate distributions will not spread records evenly
// regardless of the partition count; the runtime applies murmur2(key) to pick a
// partition.
//
// keySerde and valSerde are used to build the four type-erased Encode/Decode
// closures stored in the RepartitionBinding.  They mirror how Stream[K,V]() and
// KStream[K,V].To() build SourceBinding/SinkBinding closures.
func (s KStream[K, V]) Repartition(name string, partitions int32, keySerde Serde[K], valSerde Serde[V]) KStream[K, V] {
	sinkName := s.builder.nextName("repartition-sink")
	sourceName := s.builder.nextName("repartition-source")

	// Wire the write side: terminal sink that consumes from the upstream node.
	// Mirrors KStream[K,V].To() — AddSink(sinkName, parentNode).
	s.builder.internal.AddSink(sinkName, s.nodeName)

	// Wire the read side: new root source.
	// Mirrors Stream[K,V]() — AddSource(sourceName).
	s.builder.internal.AddSource(sourceName)

	rb := RepartitionBinding{
		Name:       name,
		SinkName:   sinkName,
		SourceName: sourceName,
		Partitions: partitions,

		// EncodeKey mirrors SinkBinding.EncodeKey from To(): type-assert any to K,
		// then delegate to keySerde.Serialize.
		EncodeKey: func(x any) ([]byte, error) {
			v, ok := x.(K)
			if !ok {
				return nil, fmt.Errorf("gstream: repartition %q EncodeKey: expected %T, got %T", name, *new(K), x)
			}
			return keySerde.Serialize(v)
		},

		// EncodeVal mirrors SinkBinding.EncodeVal from To(): type-assert any to V,
		// then delegate to valSerde.Serialize.
		EncodeVal: func(x any) ([]byte, error) {
			v, ok := x.(V)
			if !ok {
				return nil, fmt.Errorf("gstream: repartition %q EncodeVal: expected %T, got %T", name, *new(V), x)
			}
			return valSerde.Serialize(v)
		},

		// DecodeKey mirrors SourceBinding.DecodeKey from Stream(): delegate to
		// keySerde.Deserialize and return as any.
		DecodeKey: func(raw []byte) (any, error) {
			return keySerde.Deserialize(raw)
		},

		// DecodeVal mirrors SourceBinding.DecodeVal from Stream(): delegate to
		// valSerde.Deserialize and return as any.
		DecodeVal: func(raw []byte) (any, error) {
			return valSerde.Deserialize(raw)
		},
	}

	s.builder.repartitionBindings[name] = rb

	// Return a fresh KStream rooted at the new source so downstream operators
	// (.Filter, .GroupByKey, .To, …) chain from the re-partitioned stream.
	return KStream[K, V]{
		builder:  s.builder,
		nodeName: sourceName,
	}
}

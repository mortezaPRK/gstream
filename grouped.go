package gstream

import (
	"context"
	"fmt"

	"github.com/mortezaPRK/gstream/internal/topology"
)

// kvBytesStore is the interface stateful operators assert against at runtime.
// The runtime supplies a *state.KeyValueStore[[]byte,[]byte] in the stores map,
// which satisfies this interface. Using a bytes interface avoids the type-erasure
// boundary bug: the DSL processor captures the concrete serdes at build time and
// encodes/decodes itself, so the store only ever sees raw bytes — it does not need
// to know the concrete K or A types.
//
// This is Option A of the P2-S7 type-erasure fix: typed at the DSL edge, bytes
// through storage.
type kvBytesStore interface {
	Get(key []byte) ([]byte, bool, error)
	Put(key []byte, value []byte) error
}

// GroupByKey groups the stream by its current key, returning a KGroupedStream
// ready for stateful aggregation (Count, Aggregate).
//
// GroupByKey does NOT introduce a repartition boundary: it assumes records with
// the same key are already co-located on the same partition. Key-changing operators
// (Map, SelectKey) before GroupByKey without a repartition may yield incorrect results.
func (s KStream[K, V]) GroupByKey(keySerde Serde[K], valSerde Serde[V]) KGroupedStream[K, V] {
	return KGroupedStream[K, V]{
		builder:  s.builder,
		nodeName: s.nodeName,
		keySerde: keySerde,
		valSerde: valSerde,
	}
}

// Count aggregates the number of records per key, persisting counts in the
// state store named storeName.
//
// Under at-least-once semantics a crash between the store write and the offset
// commit may replay the batch, causing a double-count. ExactlyOnce eliminates
// this gap. Count delegates to g.Aggregate.
func (g KGroupedStream[K, V]) Count(storeName string) KTable[K, int64] {
	return g.Aggregate[int64](
		storeName,
		func() int64 { return 0 },
		func(_ K, _ V, acc int64) int64 { return acc + 1 },
		JSONSerde[int64]{},
	)
}

// Aggregate aggregates records per key using a user-supplied accumulator.
//
// storeName identifies the KeyValueStore[[]byte,[]byte] wired into the topology
// executor by the runtime. The processor captures keySerde and accSerde at
// DSL-build time and performs all encoding/decoding itself — the underlying
// store only ever sees raw bytes.
//
// initFn returns the zero accumulator for a key not yet seen. aggFn combines the
// current key, incoming value, and existing accumulator into the next accumulator.
// accSerde serializes/deserializes the accumulator for Pebble and the changelog topic.
//
// topology.Builder.Build() panics when no sink nodes exist. Aggregate adds an
// internal "ktable-out-N" sink to satisfy that invariant; because the processor
// never calls ctx.Forward, the buffer stays nil (O(1), zero unbounded growth).
//
// Under at-least-once semantics a crash between the store write and the offset
// commit may replay the batch, causing aggFn to be applied again. ExactlyOnce
// eliminates this gap.
func (g KGroupedStream[K, V]) Aggregate[A any](
	storeName string,
	initFn func() A,
	aggFn func(K, V, A) A,
	accSerde Serde[A],
) KTable[K, A] {
	name := g.builder.nextName("aggregate")

	keySerde := g.keySerde

	g.builder.internal.AddStatefulProcessor(name, func(_ context.Context, r topology.Record, ctx topology.ProcessorContext) error {
		k, ok := r.Key.(K)
		if !ok {
			return fmt.Errorf("aggregate %q: key type mismatch: got %T, want %T", storeName, r.Key, *new(K))
		}
		v, ok := r.Value.(V)
		if !ok {
			return fmt.Errorf("aggregate %q: value type mismatch: got %T, want %T", storeName, r.Value, *new(V))
		}

		raw := ctx.Store(storeName)
		if raw == nil {
			return fmt.Errorf("aggregate %q: store not wired", storeName)
		}
		store, ok := raw.(kvBytesStore)
		if !ok {
			return fmt.Errorf("aggregate %q: store type mismatch: got %T, want kvBytesStore ([]byte/[]byte store)", storeName, raw)
		}

		keyBytes, err := keySerde.Serialize(k)
		if err != nil {
			return fmt.Errorf("aggregate %q: encode key: %w", storeName, err)
		}

		var cur A
		valBytes, found, err := store.Get(keyBytes)
		if err != nil {
			return fmt.Errorf("aggregate %q: store Get: %w", storeName, err)
		}
		if found {
			cur, err = accSerde.Deserialize(valBytes)
			if err != nil {
				return fmt.Errorf("aggregate %q: decode accumulator: %w", storeName, err)
			}
		} else {
			cur = initFn()
		}

		next := aggFn(k, v, cur)

		nextBytes, err := accSerde.Serialize(next)
		if err != nil {
			return fmt.Errorf("aggregate %q: encode accumulator: %w", storeName, err)
		}
		if err := store.Put(keyBytes, nextBytes); err != nil {
			return fmt.Errorf("aggregate %q: store Put: %w", storeName, err)
		}

		// KTable has no downstream consumer; ctx.Forward is intentionally omitted.
		// When KTable.To() is introduced, Forward will be re-enabled here.
		return nil
	}, []string{storeName}, g.nodeName)

	// Internal sink to satisfy topology.Builder.Build()'s >=1 sink invariant.
	// Not registered in BuiltTopology.Sinks; invisible to the runtime output path.
	sinkName := g.builder.nextName("ktable-out")
	g.builder.internal.AddSink(sinkName, name)

	// Register a StoreBinding so the runtime can open and recover this store.
	// ChangelogTopic is the bare store name; the runtime derives the full Kafka topic
	// as <AppID>-<storeName>-changelog. EncodeKey/DecodeKey/EncodeVal/DecodeVal are
	// present for future restore-and-decode use; the active processing path does its
	// own serde inline and does not use them.
	g.builder.stores[storeName] = StoreBinding{
		StoreName:      storeName,
		ChangelogTopic: storeName,
		EncodeKey: func(x any) ([]byte, error) {
			k, ok := x.(K)
			if !ok {
				return nil, fmt.Errorf("StoreBinding %q EncodeKey: expected %T, got %T", storeName, *new(K), x)
			}
			return keySerde.Serialize(k)
		},
		DecodeKey: func(b []byte) (any, error) {
			return keySerde.Deserialize(b)
		},
		EncodeVal: func(x any) ([]byte, error) {
			a, ok := x.(A)
			if !ok {
				return nil, fmt.Errorf("StoreBinding %q EncodeVal: expected %T, got %T", storeName, *new(A), x)
			}
			return accSerde.Serialize(a)
		},
		DecodeVal: func(b []byte) (any, error) {
			return accSerde.Deserialize(b)
		},
	}

	return KTable[K, A]{
		builder:   g.builder,
		nodeName:  name,
		storeName: storeName,
	}
}

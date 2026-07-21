package gstream

import (
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
// GroupByKey does NOT introduce a repartition boundary: it assumes that records
// with the same key K are already co-located on the same partition. Key-changing
// operators (Map, SelectKey) produce a repartition boundary; calling GroupByKey
// after such an operator without a preceding repartition (P4) may yield incorrect
// aggregation results.
//
// keySerde and valSerde are forwarded to the resulting KGroupedStream so that
// Count/Aggregate can register StoreBindings with the correct serialization.
func (s KStream[K, V]) GroupByKey(keySerde Serde[K], valSerde Serde[V]) KGroupedStream[K, V] {
	return KGroupedStream[K, V]{
		builder:  s.builder,
		nodeName: s.nodeName,
		keySerde: keySerde,
		valSerde: valSerde,
	}
}

// Count aggregates the number of records per key, persisting counts in the
// state store named storeName. It returns a KTable[K,int64] whose value type
// is the running count.
//
// The underlying StatefulProcessFunc performs an at-least-once (ALO) increment:
// it reads the current count, increments it, writes it to the store, and does
// NOT forward the result in P2 (KTable has no downstream consumer until P4/P5).
// If the task crashes between the store write and the downstream offset commit,
// the reprocessed batch may over-count by up to the batch size. ExactlyOnce
// (§4.2 / P5) eliminates this gap by making the store write and offset commit
// atomic.
//
// Count delegates to g.Aggregate.
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
// store only ever sees raw bytes (Option A: typed at the DSL edge, bytes through
// storage).
//
// initFn returns the zero accumulator value for a key that has not yet been seen.
// aggFn combines the current key, incoming value, and existing accumulator into
// the next accumulator value.
// accSerde serializes/deserializes the accumulator for Pebble and the changelog
// topic. The runtime derives the full topic name as <AppID>-<storeName>-changelog;
// the StoreBinding carries only the bare storeName because the AppID is a runtime
// concern, not a topology concern.
//
// The StatefulProcessFunc asserts key and value types at runtime; a type mismatch
// or a missing/incorrectly-typed store returns a descriptive error rather than
// panicking.
//
// # Sink invariant and memory safety
//
// topology.Builder.Build() panics when no sink nodes exist. Aggregate adds an
// internal "ktable-out-N" sink to satisfy that invariant. The StatefulProcessFunc
// intentionally does NOT call ctx.Forward in P2: KTable has no downstream
// consumer until KTable.To() is introduced in P4. Because Forward is never
// called, no records ever reach "ktable-out-N" and its Executor buffer stays nil
// (O(1) map entry, zero unbounded growth). When KTable.To() is introduced,
// Forward will be re-enabled and the internal sink replaced by a real one.
//
// # ALO caveat
//
// A crash between the store write and the offset commit may replay the batch,
// causing aggFn to be applied again to already-processed records.
// ExactlyOnce (§4.2 / P5) eliminates this gap.
func (g KGroupedStream[K, V]) Aggregate[A any](
	storeName string,
	initFn func() A,
	aggFn func(K, V, A) A,
	accSerde Serde[A],
) KTable[K, A] {
	name := g.builder.nextName("aggregate")

	// Capture serdes at DSL-build time. The StatefulProcessFunc will encode K→bytes
	// via keySerde and A→bytes via accSerde on every call, then store/retrieve raw
	// bytes from a kvBytesStore. This avoids the type-erasure boundary bug where the
	// runtime can only supply a *state.KeyValueStore[[]byte,[]byte] (it has no
	// concrete K,A at runtime), and the old kvStoreI[K,A] assertion on an
	// erasedStore[any,any] would fail at runtime on the first real record.
	keySerde := g.keySerde

	g.builder.internal.AddStatefulProcessor(name, func(r topology.Record, ctx topology.ProcessorContext) error {
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

		// Encode the record key K → []byte using the DSL-captured keySerde.
		keyBytes, err := keySerde.Serialize(k)
		if err != nil {
			return fmt.Errorf("aggregate %q: encode key: %w", storeName, err)
		}

		// Retrieve the current accumulator value (or initialise it).
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

		// Apply the aggregation function.
		next := aggFn(k, v, cur)

		// Encode the new accumulator A → []byte and write it back.
		nextBytes, err := accSerde.Serialize(next)
		if err != nil {
			return fmt.Errorf("aggregate %q: encode accumulator: %w", storeName, err)
		}
		if err := store.Put(keyBytes, nextBytes); err != nil {
			return fmt.Errorf("aggregate %q: store Put: %w", storeName, err)
		}

		// P2: KTable has no downstream consumer; ctx.Forward is intentionally
		// omitted. Forwarding would accumulate records in the ktable-out buffer
		// indefinitely because no consumer ever calls DrainSink on it.
		// When KTable.To() is introduced in P4, Forward will be re-enabled here.
		return nil
	}, []string{storeName}, g.nodeName)

	// ktable-out is an internal terminal sink registered solely to satisfy
	// topology.Builder.Build()'s "at least one sink" invariant (Build() panics
	// when len(sinks)==0). Because the StatefulProcessFunc above never calls
	// ctx.Forward, no records ever reach this sink and its Executor buffer stays
	// nil — a fixed O(1) map entry with no unbounded growth.
	//
	// This sink is intentionally not registered in BuiltTopology.Sinks (no Kafka
	// topic mapping); it is invisible to the runtime's output path.
	sinkName := g.builder.nextName("ktable-out")
	g.builder.internal.AddSink(sinkName, name)

	// Register a StoreBinding so the runtime knows how to open and recover this
	// store. ChangelogTopic is the bare store name; the runtime prepends <AppID>-
	// and appends -changelog to derive the full Kafka topic name.
	//
	// EncodeKey/DecodeKey and EncodeVal/DecodeVal are still present in the binding
	// for the changelog restore path: state.RestoreFromChangelog reads raw bytes
	// from Kafka and writes them directly to Pebble without decoding, so these
	// closures are for any future restore-and-decode use case. The active processing
	// path (Aggregate's StatefulProcessFunc) does NOT use them — it does its own
	// serde inline.
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

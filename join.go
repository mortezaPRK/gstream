package gstream

import (
	"context"
	"fmt"

	"github.com/mortezaPRK/gstream/internal/topology"
)

// JoinGlobal performs a stream × GlobalKTable inner join.
//
// For each stream record, keyMapper(k, v) derives the lookup key GK — the stream key
// K need not equal the table key GK, and no co-partitioning is required because the
// global table is fully replicated across all instances. The derived key is serialized
// with gkt.keySerde and used to query the global store (injected by the runtime under
// gkt.storeName). On a hit, joiner(v, gv) produces the output value VR which is
// forwarded downstream. On a miss (derived key absent), the record is silently dropped
// (inner-join semantics).
//
// The global store is fully replicated and bootstrapped before task processing starts;
// readiness gate is handled by the runtime (C5). JoinGlobal does NOT create a new
// store binding — it reads the existing global store registered by GlobalTable().
//
// keyMapper must not be nil; a nil keyMapper panics at topology build time (programming
// error, not a runtime condition).
func (s KStream[K, V]) JoinGlobal[GK, GV, VR any](
	gkt GlobalKTable[GK, GV],
	keyMapper func(K, V) GK,
	joiner func(V, GV) VR,
	outValSerde Serde[VR],
) KStream[K, VR] {
	if keyMapper == nil {
		panic("gstream: JoinGlobal keyMapper must not be nil")
	}

	name := s.builder.nextName("join-global")

	gktStoreName := gkt.storeName
	gktKeySerde := gkt.keySerde
	gktValSerde := gkt.valSerde

	s.builder.internal.AddStatefulProcessor(name, func(_ context.Context, r topology.Record, ctx topology.ProcessorContext) error {
		k, ok := r.Key.(K)
		if !ok {
			return fmt.Errorf("join-global %q: key type mismatch: got %T, want %T", gktStoreName, r.Key, *new(K))
		}
		v, ok := r.Value.(V)
		if !ok {
			return fmt.Errorf("join-global %q: value type mismatch: got %T, want %T", gktStoreName, r.Value, *new(V))
		}

		raw := ctx.Store(gktStoreName)
		if raw == nil {
			return fmt.Errorf("join-global %q: store not wired", gktStoreName)
		}
		store, ok := raw.(kvBytesStore)
		if !ok {
			return fmt.Errorf("join-global %q: store type mismatch: got %T, want kvBytesStore", gktStoreName, raw)
		}

		gk := keyMapper(k, v)
		gkBytes, err := gktKeySerde.Serialize(gk)
		if err != nil {
			return fmt.Errorf("join-global %q: encode derived key: %w", gktStoreName, err)
		}

		valBytes, found, err := store.Get(gkBytes)
		if err != nil {
			return fmt.Errorf("join-global %q: store Get: %w", gktStoreName, err)
		}
		if !found {
			// inner-join miss — drop silently
			return nil
		}

		gv, err := gktValSerde.Deserialize(valBytes)
		if err != nil {
			return fmt.Errorf("join-global %q: decode global table value: %w", gktStoreName, err)
		}

		ctx.Forward(topology.Record{
			Key:       k,
			Value:     joiner(v, gv),
			Timestamp: r.Timestamp,
		})
		return nil
	}, []string{gktStoreName}, s.nodeName)

	return KStream[K, VR]{
		builder:             s.builder,
		nodeName:            name,
		repartitionRequired: s.repartitionRequired,
	}
}

// JoinTable performs a stream-table inner join between s (KStream[K,V]) and
// table (KTable[K,VT]).
//
// For each stream record, the processor looks up the current table value for the
// record's key using table.keySerde to encode the lookup key and table.valSerde
// to decode the stored bytes. On a hit, joiner(v, vt) produces the output value
// VR which is forwarded downstream. On a miss (key absent in table), the record
// is silently dropped (inner-join semantics).
//
// The join processor is wired as a child of s's topology node and reads from the
// table's existing store. It does NOT create a new store binding. The table's
// aggregation sub-graph (source → aggregate → store) is already wired and
// continues to run independently.
//
// streamValSerde serializes V when a preceding key-changing operator requires
// automatic repartitioning. outValSerde serializes VR output values so returned
// KStream can be sunk via .To().
//
// Constraint: table must carry a non-nil valSerde (produced by Aggregate or Count
// on a flat KGroupedStream, not a windowed/session KTable whose key is Windowed[K]).
func (s KStream[K, V]) JoinTable[VT, VR any](
	table KTable[K, VT],
	joiner func(V, VT) VR,
	streamValSerde Serde[V],
	outValSerde Serde[VR],
) KStream[K, VR] {
	s = s.ensureRepartition(table.keySerde, streamValSerde)
	name := s.builder.nextName("join-table")

	tableStoreName := table.storeName
	tableKeySerde := table.keySerde
	tableValSerde := table.valSerde

	s.builder.internal.AddStatefulProcessor(name, func(_ context.Context, r topology.Record, ctx topology.ProcessorContext) error {
		k, ok := r.Key.(K)
		if !ok {
			return fmt.Errorf("join-table %q: key type mismatch: got %T, want %T", tableStoreName, r.Key, *new(K))
		}
		v, ok := r.Value.(V)
		if !ok {
			return fmt.Errorf("join-table %q: value type mismatch: got %T, want %T", tableStoreName, r.Value, *new(V))
		}

		raw := ctx.Store(tableStoreName)
		if raw == nil {
			return fmt.Errorf("join-table %q: store not wired", tableStoreName)
		}
		store, ok := raw.(kvBytesStore)
		if !ok {
			return fmt.Errorf("join-table %q: store type mismatch: got %T, want kvBytesStore", tableStoreName, raw)
		}

		kBytes, err := tableKeySerde.Serialize(k)
		if err != nil {
			return fmt.Errorf("join-table %q: encode key: %w", tableStoreName, err)
		}

		valBytes, found, err := store.Get(kBytes)
		if err != nil {
			return fmt.Errorf("join-table %q: store Get: %w", tableStoreName, err)
		}
		if !found {
			// inner-join miss — drop silently
			return nil
		}

		vt, err := tableValSerde.Deserialize(valBytes)
		if err != nil {
			return fmt.Errorf("join-table %q: decode table value: %w", tableStoreName, err)
		}

		ctx.Forward(topology.Record{
			Key:       k,
			Value:     joiner(v, vt),
			Timestamp: r.Timestamp,
		})
		return nil
	}, []string{tableStoreName}, s.nodeName)

	return KStream[K, VR]{
		builder:  s.builder,
		nodeName: name,
	}
}

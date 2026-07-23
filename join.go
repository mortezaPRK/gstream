package gstream

import (
	"context"
	"fmt"

	"github.com/mortezaPRK/gstream/internal/topology"
)

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
// outValSerde serializes VR output values so the returned KStream can be sunk
// via .To().
//
// Constraint: table must carry a non-nil valSerde (produced by Aggregate or Count
// on a flat KGroupedStream, not a windowed/session KTable whose key is Windowed[K]).
func (s KStream[K, V]) JoinTable[VT, VR any](
	table KTable[K, VT],
	joiner func(V, VT) VR,
	outValSerde Serde[VR],
) KStream[K, VR] {
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

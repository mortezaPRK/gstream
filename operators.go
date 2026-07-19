package gstream

import (
	"fmt"

	"github.com/mortezaPRK/gstream/internal/topology"
)

// Filter keeps only records where fn(k, v) returns true. The key and value
// types remain unchanged. No repartition boundary is introduced — the
// downstream node sees the same key distribution as the upstream (§6.2).
//
// The returned KStream shares the same key K and value V types as the receiver.
func (s KStream[K, V]) Filter(fn func(K, V) bool) KStream[K, V] {
	name := s.builder.nextName("filter")
	s.builder.internal.AddProcessor(name, func(r topology.Record, forward topology.Forwarder) error {
		k, ok := r.Key.(K)
		if !ok {
			return fmt.Errorf("filter: key type mismatch: got %T, want %T", r.Key, *new(K))
		}
		v, ok := r.Value.(V)
		if !ok {
			return fmt.Errorf("filter: value type mismatch: got %T, want %T", r.Value, *new(V))
		}
		if fn(k, v) {
			forward(r)
		}
		return nil
	}, s.nodeName)
	return KStream[K, V]{builder: s.builder, nodeName: name}
}

// MapValues transforms the value of each record from V to V2 using fn. The key
// K is passed through unchanged; no repartition boundary is introduced because
// the key distribution is not affected (§6.2).
//
// The returned KStream has value type V2 and the same key type K.
func (s KStream[K, V]) MapValues[V2 any](fn func(V) V2) KStream[K, V2] {
	name := s.builder.nextName("mapvalues")
	s.builder.internal.AddProcessor(name, func(r topology.Record, forward topology.Forwarder) error {
		v, ok := r.Value.(V)
		if !ok {
			return fmt.Errorf("mapvalues: value type mismatch: got %T, want %T", r.Value, *new(V))
		}
		forward(topology.Record{
			Key:       r.Key,
			Value:     fn(v),
			Timestamp: r.Timestamp,
		})
		return nil
	}, s.nodeName)
	return KStream[K, V2]{builder: s.builder, nodeName: name}
}

// Map transforms each record's key and value from (K, V) to (K2, V2) using fn.
//
// WARNING: Map marks a repartition boundary in the topology because the key
// distribution changes. The boundary is recorded in the builder
// (builder.repartitions[name] = true) but the repartition topic is NOT wired
// until P4 (§6.3, §9). Downstream operators that require co-partitioning
// (joins, aggregations) must not be used across this boundary until P4.
//
// The returned KStream has key type K2 and value type V2.
func (s KStream[K, V]) Map[K2, V2 any](fn func(K, V) (K2, V2)) KStream[K2, V2] {
	name := s.builder.nextName("map")
	s.builder.repartitions[name] = true
	s.builder.internal.AddProcessor(name, func(r topology.Record, forward topology.Forwarder) error {
		k, ok := r.Key.(K)
		if !ok {
			return fmt.Errorf("map: key type mismatch: got %T, want %T", r.Key, *new(K))
		}
		v, ok := r.Value.(V)
		if !ok {
			return fmt.Errorf("map: value type mismatch: got %T, want %T", r.Value, *new(V))
		}
		newKey, newVal := fn(k, v)
		forward(topology.Record{
			Key:       newKey,
			Value:     newVal,
			Timestamp: r.Timestamp,
		})
		return nil
	}, s.nodeName)
	return KStream[K2, V2]{builder: s.builder, nodeName: name}
}

// SelectKey replaces the key of each record using fn(k, v) → K2. The value V
// is passed through unchanged.
//
// WARNING: SelectKey marks a repartition boundary in the topology because the
// key distribution changes. The boundary is recorded in the builder
// (builder.repartitions[name] = true) but the repartition topic is NOT wired
// until P4 (§6.3, §9). Downstream operators that require co-partitioning
// (joins, aggregations) must not be used across this boundary until P4.
//
// The returned KStream has key type K2 and the same value type V.
func (s KStream[K, V]) SelectKey[K2 any](fn func(K, V) K2) KStream[K2, V] {
	name := s.builder.nextName("selectkey")
	s.builder.repartitions[name] = true
	s.builder.internal.AddProcessor(name, func(r topology.Record, forward topology.Forwarder) error {
		k, ok := r.Key.(K)
		if !ok {
			return fmt.Errorf("selectkey: key type mismatch: got %T, want %T", r.Key, *new(K))
		}
		v, ok := r.Value.(V)
		if !ok {
			return fmt.Errorf("selectkey: value type mismatch: got %T, want %T", r.Value, *new(V))
		}
		forward(topology.Record{
			Key:       fn(k, v),
			Value:     v,
			Timestamp: r.Timestamp,
		})
		return nil
	}, s.nodeName)
	return KStream[K2, V]{builder: s.builder, nodeName: name}
}

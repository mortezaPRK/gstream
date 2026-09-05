package gstream

import (
	"context"
	"fmt"

	"mortz.dev/go/gstream/internal/topology"
)

// Filter keeps only records where fn(k, v) returns true. The key and value
// types remain unchanged. No repartition boundary is introduced.
func (s KStream[K, V]) Filter(fn func(K, V) bool) KStream[K, V] {
	name := s.builder.nextName("filter")
	s.builder.internal.AddProcessor(name, func(_ context.Context, r topology.Record, forward topology.Forwarder) error {
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
	return KStream[K, V]{builder: s.builder, nodeName: name, repartitionRequired: s.repartitionRequired}
}

// MapValues transforms the value of each record from V to V2 using fn. The key
// is forwarded unchanged so no repartition boundary is introduced.
func (s KStream[K, V]) MapValues[V2 any](fn func(K, V) V2) KStream[K, V2] {
	name := s.builder.nextName("mapvalues")
	s.builder.internal.AddProcessor(name, func(_ context.Context, r topology.Record, forward topology.Forwarder) error {
		k, ok := r.Key.(K)
		if !ok {
			return fmt.Errorf("mapvalues: key type mismatch: got %T, want %T", r.Key, *new(K))
		}
		v, ok := r.Value.(V)
		if !ok {
			return fmt.Errorf("mapvalues: value type mismatch: got %T, want %T", r.Value, *new(V))
		}
		forward(topology.Record{
			Key:       k,
			Value:     fn(k, v),
			Timestamp: r.Timestamp,
		})
		return nil
	}, s.nodeName)
	return KStream[K, V2]{builder: s.builder, nodeName: name, repartitionRequired: s.repartitionRequired}
}

// Map transforms each record's key and value from (K, V) to (K2, V2) using fn.
//
// Map introduces a repartition boundary because the key distribution changes.
// Register a RepartitionBinding (C2 DSL) to wire the intermediate topic before
// using downstream operators that require co-partitioning (joins, aggregations).
func (s KStream[K, V]) Map[K2, V2 any](fn func(K, V) (K2, V2)) KStream[K2, V2] {
	name := s.builder.nextName("map")
	s.builder.internal.AddProcessor(name, func(_ context.Context, r topology.Record, forward topology.Forwarder) error {
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
	return KStream[K2, V2]{builder: s.builder, nodeName: name, repartitionRequired: true}
}

// SelectKey replaces the key of each record using fn(k, v) → K2. The value V
// is passed through unchanged.
//
// SelectKey introduces a repartition boundary because the key distribution changes.
// Register a RepartitionBinding (C2 DSL) to wire the intermediate topic before
// using downstream operators that require co-partitioning (joins, aggregations).
func (s KStream[K, V]) SelectKey[K2 any](fn func(K, V) K2) KStream[K2, V] {
	name := s.builder.nextName("selectkey")
	s.builder.internal.AddProcessor(name, func(_ context.Context, r topology.Record, forward topology.Forwarder) error {
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
	return KStream[K2, V]{builder: s.builder, nodeName: name, repartitionRequired: true}
}

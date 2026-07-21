package state

import "sync"

// Mutation is a single captured state change — a Put or Delete — expressed as
// pre-encoded bytes that match the Pebble key/value layout used by KeyValueStore.
//
// Key is always the full Pebble key (store prefix + separator + serialized key),
// exactly as written to Pebble, so changelog consumers can reconstruct the store
// state without needing to know the serialization format.
//
// For a Put, Value holds the serialized value bytes. For a Delete, Value is nil
// and IsDelete is true.
type Mutation struct {
	Key      []byte // full Pebble key: <store-name> 0x00 <encoded-key>
	Value    []byte // serialized value bytes; nil for deletions
	IsDelete bool   // true = Delete; false = Put
}

// MutationCollector accumulates Mutations produced by a KeyValueStore during
// processing of a single input record. At the end of processing, the runtime
// drains the collector and produces the mutations as changelog records.
//
// MutationCollector is safe for concurrent use.
type MutationCollector struct {
	mu        sync.Mutex
	mutations []Mutation
}

// Append adds m to the collector. It is called by KeyValueStore.Put and
// KeyValueStore.Delete when a collector is attached.
func (c *MutationCollector) Append(m Mutation) {
	c.mu.Lock()
	c.mutations = append(c.mutations, m)
	c.mu.Unlock()
}

// Drain returns all accumulated mutations and resets the collector to empty.
// The returned slice is owned by the caller; subsequent Appends do not affect it.
// Returns nil (not an empty slice) when there are no mutations.
func (c *MutationCollector) Drain() []Mutation {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.mutations) == 0 {
		return nil
	}
	out := c.mutations
	c.mutations = nil
	return out
}

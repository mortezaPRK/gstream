package state

import "sync"

// Mutation is the sealed union for state-change operations recorded by a
// KeyValueStore. The only concrete implementations are Put and Delete.
//
// The interface has an unexported method so that callers outside this package
// cannot implement it, approximating a sealed union in Go.
type Mutation interface {
	isMutation()
}

// Put records a key-value insertion or update.
// Key is the full Pebble key (store prefix + separator + serialised key),
// exactly as written to Pebble.
// Value is the serialised value bytes.
type Put struct {
	Key   []byte // full Pebble key: <store-name> 0x00 <encoded-key>
	Value []byte // serialised value bytes
}

// Delete records a key deletion (tombstone).
// Key is the full Pebble key (store prefix + separator + serialised key),
// exactly as written to Pebble.
type Delete struct {
	Key []byte // full Pebble key: <store-name> 0x00 <encoded-key>
}

func (Put) isMutation()    {}
func (Delete) isMutation() {}

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

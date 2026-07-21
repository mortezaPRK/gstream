package kafka

import "time"

// InRecord is the internal representation of a record consumed from Kafka.
// It holds only the fields the processing layer needs; kgo.Record is not exposed.
type InRecord struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Timestamp time.Time
}

// OutRecord is the internal representation of a record to be produced to Kafka.
// The caller supplies Topic, Key, Value, and optionally Partition.
// Timestamp is always set by the Kafka broker (broker-side default).
//
// # Partition semantics
//
//   - nil (zero value): unpinned — the producer hashes Key using Kafka-compatible
//     murmur2 to select a partition. This is the correct value for sink records.
//     All existing OutRecords constructed without a Partition field automatically
//     follow this path because the Go zero value for a pointer is nil.
//
//   - non-nil: pinned — the record is routed to exactly *Partition, including
//     partition 0. Used by changelog writes (P2+) that must co-locate state
//     mutations with the source input partition.
type OutRecord struct {
	Topic     string
	Key       []byte
	Value     []byte
	Partition *int32 // nil = key-hash (sink); non-nil = pinned partition (changelog)
}

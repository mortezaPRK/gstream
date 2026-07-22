package kafka

import (
	"time"

	"github.com/mortezaPRK/gstream/xtypes"
)

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
//
// # Partition semantics
//
//   - IsValid=false (zero value): unpinned — the producer hashes Key using
//     Kafka-compatible murmur2. This is the correct value for sink records.
//
//   - IsValid=true: pinned — the record is routed to exactly Partition.Value,
//     including partition 0. Used by changelog writes that must co-locate state
//     mutations with the source input partition.
type OutRecord struct {
	Topic     string
	Key       []byte
	Value     []byte
	Partition xtypes.Nil[int32] // IsValid=false = key-hash (sink); IsValid=true = pinned partition = Value
}

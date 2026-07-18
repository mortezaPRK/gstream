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
// The caller supplies Topic, Key, and Value; Partition and Timestamp are set by
// the producer (partition by Kafka key hash, timestamp by Kafka broker default).
type OutRecord struct {
	Topic string
	Key   []byte
	Value []byte
}

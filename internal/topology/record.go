package topology

// Record is the internal data unit flowing through the processor DAG.
//
// Key and Value carry any Go value; the type-safe generic DSL layer (KStream[K,V],
// planned for a later phase) encodes and decodes at source/sink boundaries so the
// DAG itself remains type-agnostic. Timestamp is a Unix millisecond event-time
// timestamp (§8 — stream-time semantics will be layered on top of this field).
type Record struct {
	Key       any
	Value     any
	Timestamp int64 // Unix ms; see §8 for full event-time semantics (P3+)
}

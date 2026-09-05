package gstream

// Serde is the serialization/deserialization interface for a value of type T.
// It is the single extension point for encoding. Users implement this interface
// or use an implementation from mortz.dev/go/gstream/serdes.
//
// Serdes are attached at source, sink, and store creation; a store's changelog
// reuses that store's serde so encoding is consistent between local state
// and the changelog topic.
type Serde[T any] interface {
	// Serialize encodes v into a byte slice for Kafka or local storage.
	Serialize(T) ([]byte, error)

	// Deserialize decodes a byte slice previously produced by Serialize back into T.
	Deserialize([]byte) (T, error)
}

package gstream

// Serde is the serialization/deserialization interface for a value of type T.
// It is the single extension point for encoding; users implement this interface
// or use one of the built-in implementations.
//
// Built-in implementations:
//   - BytesSerde   — identity pass-through for raw []byte.
//   - JSONSerde[T] — marshals/unmarshals via encoding/json/v2.
//   - Protobuf     — see module github.com/mortezaPRK/gstream/serde/proto
//     (protoserde.Serde[T, PT]).
//
// Serdes are attached at source, sink, and store creation; a store's changelog
// reuses that store's serde so encoding is consistent between local Pebble state
// and the changelog topic.
type Serde[T any] interface {
	// Serialize encodes v into a byte slice for Kafka or Pebble storage.
	Serialize(T) ([]byte, error)

	// Deserialize decodes a byte slice previously produced by Serialize back into T.
	Deserialize([]byte) (T, error)
}

// BytesSerde is an identity Serde[[]byte] that passes byte slices through
// unchanged. It is used by the runtime to create raw-bytes KeyValueStore
// instances for stateful processors that perform their own serialization at
// the DSL edge.
type BytesSerde struct{}

func (BytesSerde) Serialize(b []byte) ([]byte, error) { return b, nil }

func (BytesSerde) Deserialize(b []byte) ([]byte, error) { return b, nil }

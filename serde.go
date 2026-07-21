package gstream

// Serde is the serialization/deserialization interface for a value of type T.
// It is the single extension point for encoding; users implement this interface
// or use one of the built-in implementations (see §10 of the design document).
//
// Two built-in implementations are delivered in a later change:
//   - JSONSerde[T]   — marshals/unmarshals via encoding/json.
//   - ProtoSerde[T, PT] — marshals/unmarshals via google.golang.org/protobuf/proto,
//     requiring a two-parameter form because T must be paired with a pointer type PT
//     that satisfies proto.Message.
//
// Serdes are attached at source, sink, and store creation; a store's changelog
// reuses that store's serde so encoding is consistent between local Pebble state
// and the changelog topic (§10).
type Serde[T any] interface {
	// Serialize encodes v into a byte slice for Kafka or Pebble storage.
	Serialize(T) ([]byte, error)

	// Deserialize decodes a byte slice previously produced by Serialize back into T.
	Deserialize([]byte) (T, error)
}

// BytesSerde is an identity Serde[[]byte] that passes byte slices through
// unchanged. It is used by the runtime to create raw-bytes KeyValueStore
// instances for stateful processors that perform their own serialization at
// the DSL edge (Option A: typed at DSL boundary, bytes through storage).
type BytesSerde struct{}

// Serialize returns b unchanged.
func (BytesSerde) Serialize(b []byte) ([]byte, error) { return b, nil }

// Deserialize returns b unchanged.
func (BytesSerde) Deserialize(b []byte) ([]byte, error) { return b, nil }

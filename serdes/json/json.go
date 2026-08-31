package jsonserde

import "encoding/json/v2"

// Serde is a generic JSON serializer/deserializer for values of type T,
// implementing Serde[T] via encoding/json.
//
// The zero value is ready to use; no configuration is required.
//
// Usage:
//
//	jsonserde.Serde[Order]{}  →  gstream.Serde[Order]
type Serde[T any] struct{}

// Serialize encodes v as JSON.
func (Serde[T]) Serialize(v T) ([]byte, error) {
	return json.Marshal(v)
}

// Deserialize decodes JSON bytes into a value of type T.
func (Serde[T]) Deserialize(b []byte) (T, error) {
	var v T
	err := json.Unmarshal(b, &v)
	return v, err
}

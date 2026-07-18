package gstream

import "encoding/json"

// JSONSerde is a generic JSON serializer/deserializer for values of type T,
// implementing Serde[T] via encoding/json (§10.1 of the design document).
//
// It marshals the domain value T to a JSON byte slice and unmarshals it back,
// making it the simplest built-in serde for human-readable messages.
// The zero value is ready to use; no configuration is required.
//
// Usage:
//
//	gstream.JSONSerde[Order]{}  →  Serde[Order]
type JSONSerde[T any] struct{}

// Serialize encodes v as JSON.
func (JSONSerde[T]) Serialize(v T) ([]byte, error) {
	return json.Marshal(v)
}

// Deserialize decodes JSON bytes into a value of type T.
func (JSONSerde[T]) Deserialize(b []byte) (T, error) {
	var v T
	err := json.Unmarshal(b, &v)
	return v, err
}

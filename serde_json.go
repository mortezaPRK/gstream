package gstream

import "encoding/json/v2"

// JSONSerde is a generic JSON serializer/deserializer for values of type T,
// implementing Serde[T] via encoding/json.
//
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

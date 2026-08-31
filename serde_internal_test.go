package gstream

import "encoding/json/v2"

// JSONSerde exists only for white-box tests. Public implementations live in
// separate modules under serdes/.
type JSONSerde[T any] struct{}

func (JSONSerde[T]) Serialize(value T) ([]byte, error) { return json.Marshal(value) }

func (JSONSerde[T]) Deserialize(data []byte) (T, error) {
	var value T
	err := json.Unmarshal(data, &value)
	return value, err
}

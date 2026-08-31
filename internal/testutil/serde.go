package testutil

import "encoding/json/v2"

type JSONSerde[T any] struct{}

func (JSONSerde[T]) Serialize(value T) ([]byte, error) { return json.Marshal(value) }

func (JSONSerde[T]) Deserialize(data []byte) (T, error) {
	var value T
	err := json.Unmarshal(data, &value)
	return value, err
}

type BytesSerde struct{}

func (BytesSerde) Serialize(value []byte) ([]byte, error) { return value, nil }

func (BytesSerde) Deserialize(value []byte) ([]byte, error) { return value, nil }

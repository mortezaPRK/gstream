// Package protoserde provides a Protobuf implementation of gstream.Serde.
package protoserde

import (
	"fmt"
	"reflect"

	"google.golang.org/protobuf/proto"
)

// Serde is a generic Protobuf serializer/deserializer implementing
// gstream.Serde[T]. T must be a pointer to a generated protobuf message.
//
// The zero value is ready to use.
//
// Usage:
//
//	protoserde.Serde[*pb.Order]{}  →  gstream.Serde[*pb.Order]
type Serde[T proto.Message] struct{}

// Serialize encodes v as a protobuf wire-format byte slice.
func (Serde[T]) Serialize(v T) ([]byte, error) {
	return proto.Marshal(v)
}

// Deserialize decodes protobuf wire-format bytes into a value of type T.
func (Serde[T]) Deserialize(b []byte) (T, error) {
	var zero T
	typeOfT := reflect.TypeOf(zero)
	if typeOfT == nil || typeOfT.Kind() != reflect.Pointer {
		return zero, fmt.Errorf("protoserde: message type must be a pointer")
	}
	value, ok := reflect.New(typeOfT.Elem()).Interface().(T)
	if !ok {
		return zero, fmt.Errorf("protoserde: cannot allocate %v", typeOfT)
	}
	if err := proto.Unmarshal(b, value); err != nil {
		return zero, err
	}
	return value, nil
}

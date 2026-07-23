// Package protoserde provides a Protobuf implementation of gstream.Serde.
package protoserde

import (
	"google.golang.org/protobuf/proto"
)

// ProtoMessage is a constraint that asserts *T implements proto.Message.
// It bridges the value type T and the pointer type PT required by the protobuf runtime.
type ProtoMessage[T any] interface {
	*T
	proto.Message
}

// Serde is a generic Protobuf serializer/deserializer implementing gstream.Serde[T].
//
// Two type parameters are needed because proto.Marshal/Unmarshal operate on
// proto.Message, which is satisfied by *T not T. ProtoMessage[T] binds both T (the
// value type) and PT (its pointer satisfying proto.Message).
//
//   - T is the value type (e.g. pb.Order).
//   - PT is the pointer to T satisfying proto.Message (e.g. *pb.Order).
//
// The zero value is ready to use.
//
// Usage:
//
//	protoserde.Serde[pb.Order, *pb.Order]{}  →  gstream.Serde[pb.Order]
type Serde[T any, PT ProtoMessage[T]] struct{}

// Serialize encodes v as a protobuf wire-format byte slice.
func (Serde[T, PT]) Serialize(v T) ([]byte, error) {
	return proto.Marshal(PT(&v))
}

// Deserialize decodes protobuf wire-format bytes into a value of type T.
func (Serde[T, PT]) Deserialize(b []byte) (T, error) {
	var v T
	err := proto.Unmarshal(b, PT(&v))
	return v, err
}

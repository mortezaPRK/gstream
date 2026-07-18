package gstream

import "google.golang.org/protobuf/proto"

// ProtoMessage is a constraint that asserts *T implements proto.Message.
// It is the bridge between the value type T and the pointer type PT expected by the
// protobuf runtime.
type ProtoMessage[T any] interface {
	*T
	proto.Message
}

// ProtoSerde is a generic Protobuf serializer/deserializer implementing Serde[T]
// via google.golang.org/protobuf/proto (§10.2 of the design document).
//
// # Why two type parameters?
//
// Protobuf's proto.Marshal and proto.Unmarshal operate on proto.Message, which is
// an interface satisfied by *SomeProtoType, not SomeProtoType itself. When the generic
// code holds a plain T (value, not pointer), it must take the address to get a *T that
// implements proto.Message — but the compiler cannot verify *T satisfies proto.Message
// from a single type constraint [T any]. The ProtoMessage[T] constraint binds both T
// (the value type the caller stores and passes around) and PT (its pointer, which must
// satisfy proto.Message), letting the compiler enforce the relationship statically.
//
//   - T is the value type (e.g. pb.Order).
//   - PT is the pointer to T that satisfies proto.Message (e.g. *pb.Order).
//
// The zero value is ready to use; no configuration is required.
//
// Usage:
//
//	gstream.ProtoSerde[pb.Order, *pb.Order]{}  →  Serde[pb.Order]
type ProtoSerde[T any, PT ProtoMessage[T]] struct{}

// Serialize encodes v as a protobuf wire-format byte slice.
// It takes the address of v to obtain the PT pointer required by proto.Marshal.
func (ProtoSerde[T, PT]) Serialize(v T) ([]byte, error) {
	return proto.Marshal(PT(&v))
}

// Deserialize decodes protobuf wire-format bytes into a value of type T.
// It takes the address of the zero T value to obtain the PT pointer required by
// proto.Unmarshal, which allocates any nested message fields during decoding.
func (ProtoSerde[T, PT]) Deserialize(b []byte) (T, error) {
	var v T
	err := proto.Unmarshal(b, PT(&v))
	return v, err
}

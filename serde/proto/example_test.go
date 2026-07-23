package protoserde_test

import (
	"fmt"

	protoserde "github.com/mortezaPRK/gstream/serde/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// ExampleSerde shows a full serialize → deserialize round-trip using a
// wrapperspb.StringValue as the proto message type.
func ExampleSerde() {
	s := protoserde.Serde[wrapperspb.StringValue, *wrapperspb.StringValue]{}

	original := wrapperspb.StringValue{Value: "gstream"}

	b, _ := s.Serialize(original)
	decoded, _ := s.Deserialize(b)

	fmt.Println(decoded.Value)
	// Output: gstream
}

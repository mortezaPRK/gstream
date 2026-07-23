package protoserde_test

import (
	"testing"

	gstream "github.com/mortezaPRK/gstream"
	protoserde "github.com/mortezaPRK/gstream/serde/proto"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// Compile-time assertion: Serde satisfies gstream.Serde for wrapperspb.StringValue.
var _ gstream.Serde[wrapperspb.StringValue] = protoserde.Serde[wrapperspb.StringValue, *wrapperspb.StringValue]{}

// testRoundTrip performs the full serialize→deserialize cycle and checks proto.Equal.
func testRoundTrip[T any, PT interface {
	*T
	proto.Message
}](t *testing.T, s protoserde.Serde[T, PT], input *T) {
	t.Helper()

	b, err := s.Serialize(*input)
	if err != nil {
		t.Fatalf("Serialize: %v", err)
	}
	if len(b) == 0 {
		t.Fatal("Serialize returned empty bytes")
	}

	out, err := s.Deserialize(b)
	if err != nil {
		t.Fatalf("Deserialize: %v", err)
	}

	if !proto.Equal(PT(&out), PT(input)) {
		t.Errorf("round-trip mismatch: got %v, want %v", PT(&out), PT(input))
	}
}

func TestSerde_RoundTrip(t *testing.T) {
	t.Parallel()

	t.Run("StringValue", func(t *testing.T) {
		t.Parallel()
		in := &wrapperspb.StringValue{Value: "hello proto"}
		testRoundTrip(t, protoserde.Serde[wrapperspb.StringValue, *wrapperspb.StringValue]{}, in)
	})

	t.Run("Int64Value", func(t *testing.T) {
		t.Parallel()
		in := &wrapperspb.Int64Value{Value: 123456789}
		testRoundTrip(t, protoserde.Serde[wrapperspb.Int64Value, *wrapperspb.Int64Value]{}, in)
	})

	t.Run("BoolValue", func(t *testing.T) {
		t.Parallel()
		in := &wrapperspb.BoolValue{Value: true}
		testRoundTrip(t, protoserde.Serde[wrapperspb.BoolValue, *wrapperspb.BoolValue]{}, in)
	})
}

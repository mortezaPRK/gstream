package protoserde_test

import (
	"testing"

	protoserde "mortz.dev/go/gstream/serdes/proto"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// testRoundTrip performs the full serialize→deserialize cycle and checks proto.Equal.
func testRoundTrip[T proto.Message](t *testing.T, s protoserde.Serde[T], input T) {
	t.Helper()

	b, err := s.Serialize(input)
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

	if !proto.Equal(out, input) {
		t.Errorf("round-trip mismatch: got %v, want %v", out, input)
	}
}

func TestSerde_RoundTrip(t *testing.T) {
	t.Parallel()

	t.Run("StringValue", func(t *testing.T) {
		t.Parallel()
		in := &wrapperspb.StringValue{Value: "hello proto"}
		testRoundTrip(t, protoserde.Serde[*wrapperspb.StringValue]{}, in)
	})

	t.Run("Int64Value", func(t *testing.T) {
		t.Parallel()
		in := &wrapperspb.Int64Value{Value: 123456789}
		testRoundTrip(t, protoserde.Serde[*wrapperspb.Int64Value]{}, in)
	})

	t.Run("BoolValue", func(t *testing.T) {
		t.Parallel()
		in := &wrapperspb.BoolValue{Value: true}
		testRoundTrip(t, protoserde.Serde[*wrapperspb.BoolValue]{}, in)
	})
}

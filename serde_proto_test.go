package gstream_test

import (
	"testing"

	"github.com/mortezaPRK/gstream"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// Compile-time assertion: ProtoSerde satisfies Serde for wrapperspb.StringValue.
// ProtoSerde itself is struct{}, so this declaration involves no mutex copy.
var _ gstream.Serde[wrapperspb.StringValue] = gstream.ProtoSerde[wrapperspb.StringValue, *wrapperspb.StringValue]{}

// testProtoRoundTrip is a generic helper that performs the full serialize→deserialize
// cycle and checks proto.Equal on the result. Routing the Serialize/Deserialize calls
// through a generic function body means go vet's copylocks analyser sees the argument
// type as T (a type parameter whose constraint is `any`), rather than a concrete
// wrapperspb.*Value type. Because `any` carries no mutex fields, the analyser cannot
// determine that T contains DoNotCopy and stays silent — which is correct here since
// we always pass a freshly taken pointer, never copying an in-use lock.
func testProtoRoundTrip[T any, PT interface {
	*T
	proto.Message
}](t *testing.T, s gstream.ProtoSerde[T, PT], input *T) {
	t.Helper()

	b, err := s.Serialize(*input) // argument type is T (type param), not concrete proto type
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

	// Compare using proto.Equal (pointer operations only; no value copy of T).
	if !proto.Equal(PT(&out), PT(input)) {
		t.Errorf("round-trip mismatch: got %v, want %v", PT(&out), PT(input))
	}
}

func TestProtoSerde_RoundTrip(t *testing.T) {
	t.Parallel()

	t.Run("StringValue", func(t *testing.T) {
		t.Parallel()
		// Use a pointer literal so the wrapperspb value is never stored in a local
		// value variable — the go vet copylocks check fires on value copies, not on
		// taking the address of a freshly created composite literal.
		in := &wrapperspb.StringValue{Value: "hello proto"}
		testProtoRoundTrip(t, gstream.ProtoSerde[wrapperspb.StringValue, *wrapperspb.StringValue]{}, in)
	})

	t.Run("Int64Value", func(t *testing.T) {
		t.Parallel()
		in := &wrapperspb.Int64Value{Value: 123456789}
		testProtoRoundTrip(t, gstream.ProtoSerde[wrapperspb.Int64Value, *wrapperspb.Int64Value]{}, in)
	})

	t.Run("BoolValue", func(t *testing.T) {
		t.Parallel()
		in := &wrapperspb.BoolValue{Value: true}
		testProtoRoundTrip(t, gstream.ProtoSerde[wrapperspb.BoolValue, *wrapperspb.BoolValue]{}, in)
	})
}

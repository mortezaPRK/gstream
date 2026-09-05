package bytesserde_test

import (
	"bytes"
	"testing"

	bytesserde "mortz.dev/go/gstream/serdes/bytes"
)

func TestRoundTrip(t *testing.T) {
	input := []byte("gstream")
	encoded, err := (bytesserde.Serde{}).Serialize(input)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := (bytesserde.Serde{}).Deserialize(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded, input) {
		t.Fatalf("decoded = %q, want %q", decoded, input)
	}
}

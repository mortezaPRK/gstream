package gstream_test

import (
	"testing"

	"github.com/mortezaPRK/gstream"
)

// Compile-time assertion: JSONSerde[someStruct] must satisfy Serde[someStruct].
type someStruct struct {
	Name  string
	Value int
}

var _ gstream.Serde[someStruct] = gstream.JSONSerde[someStruct]{}

func TestJSONSerde_RoundTrip(t *testing.T) {
	t.Parallel()

	t.Run("struct", func(t *testing.T) {
		t.Parallel()
		s := gstream.JSONSerde[someStruct]{}
		in := someStruct{Name: "hello", Value: 42}

		b, err := s.Serialize(in)
		if err != nil {
			t.Fatalf("Serialize: %v", err)
		}

		out, err := s.Deserialize(b)
		if err != nil {
			t.Fatalf("Deserialize: %v", err)
		}
		if out != in {
			t.Errorf("round-trip mismatch: got %+v, want %+v", out, in)
		}
	})

	t.Run("map", func(t *testing.T) {
		t.Parallel()
		s := gstream.JSONSerde[map[string]int]{}
		in := map[string]int{"a": 1, "b": 2}

		b, err := s.Serialize(in)
		if err != nil {
			t.Fatalf("Serialize: %v", err)
		}

		out, err := s.Deserialize(b)
		if err != nil {
			t.Fatalf("Deserialize: %v", err)
		}
		if len(out) != len(in) {
			t.Errorf("length mismatch: got %d, want %d", len(out), len(in))
		}
		for k, v := range in {
			if out[k] != v {
				t.Errorf("key %q: got %d, want %d", k, out[k], v)
			}
		}
	})

	t.Run("slice", func(t *testing.T) {
		t.Parallel()
		s := gstream.JSONSerde[[]string]{}
		in := []string{"foo", "bar", "baz"}

		b, err := s.Serialize(in)
		if err != nil {
			t.Fatalf("Serialize: %v", err)
		}

		out, err := s.Deserialize(b)
		if err != nil {
			t.Fatalf("Deserialize: %v", err)
		}
		if len(out) != len(in) {
			t.Errorf("length mismatch: got %d, want %d", len(out), len(in))
		}
		for i := range in {
			if out[i] != in[i] {
				t.Errorf("index %d: got %q, want %q", i, out[i], in[i])
			}
		}
	})

	t.Run("int", func(t *testing.T) {
		t.Parallel()
		s := gstream.JSONSerde[int]{}
		in := 99

		b, err := s.Serialize(in)
		if err != nil {
			t.Fatalf("Serialize: %v", err)
		}

		out, err := s.Deserialize(b)
		if err != nil {
			t.Fatalf("Deserialize: %v", err)
		}
		if out != in {
			t.Errorf("got %d, want %d", out, in)
		}
	})
}

func TestJSONSerde_DeserializeMalformed(t *testing.T) {
	t.Parallel()

	s := gstream.JSONSerde[someStruct]{}
	_, err := s.Deserialize([]byte(`{not valid json`))
	if err == nil {
		t.Fatal("expected error on malformed JSON, got nil")
	}
}

package xtypes_test

import (
	"encoding/json/v2"
	"testing"

	"mortz.dev/go/gstream/xtypes"
)

// Compile-time interface checks.
var _ json.Marshaler = xtypes.Nil[int32]{}
var _ json.Unmarshaler = (*xtypes.Nil[int32])(nil)

// ---------------------------------------------------------------------------
// int32 round-trips
// ---------------------------------------------------------------------------

func TestNil_Int32_MarshalPresent(t *testing.T) {
	v := xtypes.NilOf(int32(7))
	got, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "7" {
		t.Fatalf("want %q, got %q", "7", got)
	}
}

func TestNil_Int32_MarshalAbsent(t *testing.T) {
	var v xtypes.Nil[int32]
	got, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "null" {
		t.Fatalf("want %q, got %q", "null", got)
	}
}

func TestNil_Int32_UnmarshalPresent(t *testing.T) {
	var v xtypes.Nil[int32]
	if err := json.Unmarshal([]byte("7"), &v); err != nil {
		t.Fatal(err)
	}
	if !v.IsValid {
		t.Fatal("expected IsValid=true")
	}
	if v.Value != 7 {
		t.Fatalf("want Value=7, got %d", v.Value)
	}
}

func TestNil_Int32_UnmarshalNull(t *testing.T) {
	// Start with a present value to ensure null resets it.
	v := xtypes.NilOf(int32(7))
	if err := json.Unmarshal([]byte("null"), &v); err != nil {
		t.Fatal(err)
	}
	if v.IsValid {
		t.Fatal("expected IsValid=false after unmarshal null")
	}
	if v.Value != 0 {
		t.Fatalf("want Value=0, got %d", v.Value)
	}
}

func TestNil_Int32_RoundTrip(t *testing.T) {
	original := xtypes.NilOf(int32(42))
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	var decoded xtypes.Nil[int32]
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Value != original.Value || decoded.IsValid != original.IsValid {
		t.Fatalf("round-trip mismatch: got {%v,%v}, want {%v,%v}",
			decoded.Value, decoded.IsValid, original.Value, original.IsValid)
	}
}

// ---------------------------------------------------------------------------
// string round-trips
// ---------------------------------------------------------------------------

func TestNil_String_MarshalPresent(t *testing.T) {
	v := xtypes.NilOf("hello")
	got, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != `"hello"` {
		t.Fatalf("want %q, got %q", `"hello"`, got)
	}
}

func TestNil_String_MarshalAbsent(t *testing.T) {
	var v xtypes.Nil[string]
	got, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "null" {
		t.Fatalf("want %q, got %q", "null", got)
	}
}

func TestNil_String_UnmarshalNull(t *testing.T) {
	v := xtypes.NilOf("before")
	if err := json.Unmarshal([]byte("null"), &v); err != nil {
		t.Fatal(err)
	}
	if v.IsValid {
		t.Fatal("expected IsValid=false after unmarshal null")
	}
}

// ---------------------------------------------------------------------------
// IsZero / String
// ---------------------------------------------------------------------------

func TestNil_IsZero(t *testing.T) {
	absent := xtypes.Nil[int32]{}
	if !absent.IsZero() {
		t.Fatal("expected zero-value IsZero()=true")
	}
	present := xtypes.NilOf(int32(0))
	if present.IsZero() {
		t.Fatal("expected NilOf(0).IsZero()=false")
	}
}

func TestNil_String_Output(t *testing.T) {
	absent := xtypes.Nil[int32]{}
	if absent.String() != "<nil>" {
		t.Fatalf("want <nil>, got %q", absent.String())
	}
	present := xtypes.NilOf(int32(5))
	if present.String() != "5" {
		t.Fatalf("want 5, got %q", present.String())
	}
}

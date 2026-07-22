// Package xtypes provides generic utility types for gstream.
package xtypes

import (
	"bytes"
	"encoding/json"
	"fmt"
)

var nullAsBytes = []byte("null")

// Nil[T] distinguishes a zero value from an absent value without using a pointer.
// IsValid=true means Value is present; IsValid=false means absent (no value set).
type Nil[T any] struct {
	Value   T
	IsValid bool
}

// NilOf returns a Nil[T] with Value set and IsValid=true.
func NilOf[T any](value T) Nil[T] {
	return Nil[T]{Value: value, IsValid: true}
}

// IsZero reports whether the value is absent (IsValid=false).
func (t Nil[T]) IsZero() bool {
	return !t.IsValid
}

// String implements fmt.Stringer. Returns the formatted Value when present,
// or "<nil>" when absent.
func (t Nil[T]) String() string {
	if !t.IsValid {
		return "<nil>"
	}
	return fmt.Sprintf("%v", t.Value)
}

// MarshalJSON implements json.Marshaler. Absent values marshal as JSON null.
func (t Nil[T]) MarshalJSON() ([]byte, error) {
	if !t.IsValid {
		return nullAsBytes, nil
	}
	return json.Marshal(t.Value)
}

// UnmarshalJSON implements json.Unmarshaler. JSON null produces IsValid=false;
// any other value is unmarshaled into Value with IsValid=true.
func (t *Nil[T]) UnmarshalJSON(data []byte) error {
	var out Nil[T]
	if !bytes.Equal(data, nullAsBytes) {
		if err := json.Unmarshal(data, &out.Value); err != nil {
			return err
		}
		out.IsValid = true
	}
	*t = out
	return nil
}

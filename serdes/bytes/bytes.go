// Package bytesserde provides identity serialization for raw bytes.
package bytesserde

// Serde passes byte slices through unchanged.
type Serde struct{}

func (Serde) Serialize(value []byte) ([]byte, error) { return value, nil }

func (Serde) Deserialize(value []byte) ([]byte, error) { return value, nil }

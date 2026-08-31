package pebble_test

import (
	"bytes"
	"encoding/binary"
	"testing"

	state "github.com/mortezaPRK/gstream/stores/pebble"
)

// TestWindowCompositeKey_RoundTrip verifies that DecodeWindowCompositeKey reverses
// WindowCompositeKey for a variety of inputs including kBytes containing 0x00.
func TestWindowCompositeKey_RoundTrip(t *testing.T) {
	cases := []struct {
		name        string
		kBytes      []byte
		windowStart int64
	}{
		{"empty key, zero ts", []byte{}, 0},
		{"simple key", []byte("alice"), 10000},
		{"key with 0x00", []byte{0x61, 0x00, 0x62}, 99999},
		{"large ts", []byte("bob"), 1_700_000_000_000},
		{"zero-length key, large ts", []byte{}, 1_234_567_890_123},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw := state.WindowCompositeKey(tc.kBytes, tc.windowStart)
			gotK, gotTS, err := state.DecodeWindowCompositeKey(raw)
			if err != nil {
				t.Fatalf("DecodeWindowCompositeKey: %v", err)
			}
			if !bytes.Equal(gotK, tc.kBytes) {
				t.Errorf("kBytes mismatch: got %v, want %v", gotK, tc.kBytes)
			}
			if gotTS != tc.windowStart {
				t.Errorf("windowStart mismatch: got %d, want %d", gotTS, tc.windowStart)
			}
		})
	}
}

// TestDecodeWindowCompositeKey_Errors verifies that malformed input returns errors.
func TestDecodeWindowCompositeKey_Errors(t *testing.T) {
	cases := []struct {
		name string
		raw  []byte
	}{
		{"nil", nil},
		{"too short (3 bytes)", []byte{0x00, 0x00, 0x00}},
		{"only 4 bytes (missing kBytes+ts)", []byte{0x00, 0x00, 0x00, 0x03}},
		{"declared kLen too large", func() []byte {
			b := make([]byte, 4+2+8) // declares 3 but only 2 present
			binary.BigEndian.PutUint32(b[0:4], 3)
			return b
		}()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := state.DecodeWindowCompositeKey(tc.raw)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}

// TestWindowKeyOrdering writes entries for "alice" at three timestamps, "bob" at
// one timestamp, and "alice2" at one timestamp using a real in-memory store via
// RangeBytes. It verifies that scanning with WindowKeyLowerBound/UpperBound for
// "alice" returns exactly alice's three entries in ascending order and excludes
// bob and alice2.
func TestWindowKeyOrdering(t *testing.T) {
	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	store := state.NewKeyValueStore("win-order", db, stringSerde{}, stringSerde{})
	defer store.Close()

	// We use RangeBytes directly so we don't need a typed window store.
	// First, write raw bytes via the store using the composite key as the K serde key.
	// Easiest: use a bytes store with []byte K.
	rawStore := state.NewKeyValueStore("win-order-raw", db, bytesSerde{}, bytesSerde{})
	defer rawStore.Close()

	aliceKey := []byte("alice")
	alice2Key := []byte("alice2")
	bobKey := []byte("bob")

	ts0 := int64(0)
	ts1 := int64(10000)
	ts2 := int64(20000)
	ts3 := int64(5000)

	// Write alice@0, alice@10000, alice@20000
	for _, ts := range []int64{ts0, ts1, ts2} {
		ck := state.WindowCompositeKey(aliceKey, ts)
		if err := rawStore.Put(ck, ck); err != nil { // value = key for easy verification
			t.Fatalf("Put alice@%d: %v", ts, err)
		}
	}
	// Write alice2@5000
	if err := rawStore.Put(
		state.WindowCompositeKey(alice2Key, ts3),
		state.WindowCompositeKey(alice2Key, ts3),
	); err != nil {
		t.Fatalf("Put alice2@5000: %v", err)
	}
	// Write bob@0
	if err := rawStore.Put(
		state.WindowCompositeKey(bobKey, ts0),
		state.WindowCompositeKey(bobKey, ts0),
	); err != nil {
		t.Fatalf("Put bob@0: %v", err)
	}

	// Scan for all windows of "alice"
	lb := state.WindowKeyLowerBound(aliceKey)
	ub := state.WindowKeyUpperBound(aliceKey)

	var results [][]byte
	if err := rawStore.RangeBytes(lb, ub, func(key, _ []byte) bool {
		// key is the full Pebble key; strip the store prefix to get the composite key
		results = append(results, key)
		return true
	}); err != nil {
		t.Fatalf("RangeBytes: %v", err)
	}

	if len(results) != 3 {
		t.Fatalf("expected 3 results for alice, got %d", len(results))
	}

	// Verify ordering: ts0 < ts1 < ts2 ascending
	wantTSOrder := []int64{ts0, ts1, ts2}
	for i, raw := range results {
		// raw is the full Pebble key; the store prefix is "win-order-raw\x00"
		prefix := append([]byte("win-order-raw"), 0x00)
		if !bytes.HasPrefix(raw, prefix) {
			t.Fatalf("result[%d]: unexpected prefix in %x", i, raw)
		}
		compositeKey := raw[len(prefix):]
		gotK, gotTS, err := state.DecodeWindowCompositeKey(compositeKey)
		if err != nil {
			t.Fatalf("result[%d] decode: %v", i, err)
		}
		if !bytes.Equal(gotK, aliceKey) {
			t.Errorf("result[%d]: expected kBytes=alice, got %q", i, gotK)
		}
		if gotTS != wantTSOrder[i] {
			t.Errorf("result[%d]: expected ts=%d, got %d", i, wantTSOrder[i], gotTS)
		}
	}
}

// bytesSerde is a trivial Serde[[]byte] for raw-byte round-trips in tests.
type bytesSerde struct{}

func (bytesSerde) Serialize(b []byte) ([]byte, error)   { return b, nil }
func (bytesSerde) Deserialize(b []byte) ([]byte, error) { return b, nil }

// TestWindowKeyLowerBound_NotMinInt64 confirms that the lower bound encodes
// int64(0) (8 zero bytes) and NOT MinInt64 (0x80... which sorts above 0x00... in
// unsigned byte order).
func TestWindowKeyLowerBound_NotMinInt64(t *testing.T) {
	lb := state.WindowKeyLowerBound([]byte("k"))
	// Last 8 bytes must be 0x00_00_00_00_00_00_00_00 (int64(0))
	ts := lb[len(lb)-8:]
	for i, b := range ts {
		if b != 0x00 {
			t.Errorf("lower bound byte[%d] = 0x%02x, want 0x00 (int64(0) not MinInt64)", i, b)
		}
	}
}

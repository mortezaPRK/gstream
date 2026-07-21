package state

import (
	"encoding/binary"
	"fmt"
)

// WindowCompositeKey builds the per-store byte key for a windowed entry:
//
//	uint32(len(kBytes)) big-endian ‖ kBytes ‖ int64(windowStart) big-endian
//
// Length-prefixing makes the format self-delimiting even when kBytes contains
// 0x00 bytes. The result is used as the "per-store key portion" passed to
// RangeBytes / DeleteRangeBytes (the store prefix is added by those methods).
func WindowCompositeKey(kBytes []byte, windowStart int64) []byte {
	// 4 bytes for uint32 length + len(kBytes) + 8 bytes for int64 timestamp
	out := make([]byte, 4+len(kBytes)+8)
	binary.BigEndian.PutUint32(out[0:4], uint32(len(kBytes)))
	copy(out[4:], kBytes)
	binary.BigEndian.PutUint64(out[4+len(kBytes):], uint64(windowStart))
	return out
}

// DecodeWindowCompositeKey reverses WindowCompositeKey.
// Returns an error on malformed input (too short, or declared length inconsistent
// with the total buffer size).
func DecodeWindowCompositeKey(raw []byte) (kBytes []byte, windowStart int64, err error) {
	if len(raw) < 4 {
		return nil, 0, fmt.Errorf("state: WindowCompositeKey too short (%d bytes, need >= 4)", len(raw))
	}
	kLen := int(binary.BigEndian.Uint32(raw[0:4]))
	if len(raw) != 4+kLen+8 {
		return nil, 0, fmt.Errorf(
			"state: WindowCompositeKey malformed: declared kLen=%d but total length=%d (expected %d)",
			kLen, len(raw), 4+kLen+8,
		)
	}
	kBytes = make([]byte, kLen)
	copy(kBytes, raw[4:4+kLen])
	windowStart = int64(binary.BigEndian.Uint64(raw[4+kLen:]))
	return kBytes, windowStart, nil
}

// WindowKeyLowerBound returns the inclusive lower bound to scan ALL windows of
// kBytes:
//
//	uint32(len(kBytes)) big-endian ‖ kBytes ‖ int64(0) big-endian
//
// The windowStart floor is int64(0) because Unix millisecond timestamps are always
// >= 0. Do NOT change this to math.MinInt64: big-endian encoding puts negative
// int64 values ABOVE positive ones in unsigned byte order, so MinInt64 encodes as
// 0x80_00_00_00_00_00_00_00 which is numerically greater than int64(0)'s encoding
// 0x00_00_00_00_00_00_00_00. A MinInt64 lower bound would therefore return zero
// records. int64(0) is the correct floor for Unix-ms window starts.
func WindowKeyLowerBound(kBytes []byte) []byte {
	// timestamp = int64(0) → 8 zero bytes; WindowCompositeKey with windowStart=0
	return WindowCompositeKey(kBytes, 0)
}

// WindowKeyUpperBound returns the exclusive upper bound to scan ALL windows of
// kBytes. It is prefixUpperBound applied to the (uint32(len)‖kBytes) portion —
// i.e. the key WITHOUT the timestamp suffix. This is tighter than appending
// MaxInt64 and reuses the already-tested prefixUpperBound logic.
func WindowKeyUpperBound(kBytes []byte) []byte {
	// Build the prefix portion: uint32(len) ‖ kBytes
	prefix := make([]byte, 4+len(kBytes))
	binary.BigEndian.PutUint32(prefix[0:4], uint32(len(kBytes)))
	copy(prefix[4:], kBytes)
	return prefixUpperBound(prefix)
}

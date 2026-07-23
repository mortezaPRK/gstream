// Package winkey owns the single canonical composite-key format used by all
// windowed and stream-stream-join stores.  It has zero dependencies outside the
// standard library, which means both internal/state and gstream (which cannot
// import each other due to a Serde[T] import cycle) can safely import it.
//
// Format: uint32(len(kBytes)) big-endian | kBytes | int64(windowStart) big-endian
//
// Length-prefixing makes the format self-delimiting even when kBytes contains
// 0x00 bytes.  big-endian int64 encodes positive timestamps below 0x80... so
// lexicographic order matches numeric order for non-negative Unix-ms values.
// (Negative int64 values sort ABOVE positive ones in unsigned byte order — that
// property is intentionally exploited by the hiMs+1 overflow in ssJoinScanBounds.)
package winkey

import "encoding/binary"

// CompositeKey builds the per-store byte key for a windowed entry:
//
//	uint32(len(kBytes)) big-endian | kBytes | int64(windowStart) big-endian
func CompositeKey(kBytes []byte, windowStart int64) []byte {
	out := make([]byte, 4+len(kBytes)+8)
	binary.BigEndian.PutUint32(out[0:4], uint32(len(kBytes)))
	copy(out[4:], kBytes)
	binary.BigEndian.PutUint64(out[4+len(kBytes):], uint64(windowStart))
	return out
}

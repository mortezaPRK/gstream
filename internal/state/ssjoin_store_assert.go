package state

// ssJoinStoreAssert mirrors the gstream.ssJoinStore interface (unexported) for the
// sole purpose of proving at compile time that *KeyValueStore[[]byte,[]byte]
// satisfies it.  gstream cannot import internal/state (cycle), so the assertion
// lives here instead.  This file contains no logic; it is a build-time safety net
// for P4b-F2-C1 (contract freeze).
type ssJoinStoreIface interface {
	WindowPut(kBytes []byte, windowStart int64, val []byte) error
	RangeCompositeBytes(lower, upper []byte, fn func(compositeKey, val []byte) bool) error
}

var _ ssJoinStoreIface = (*KeyValueStore[[]byte, []byte])(nil)

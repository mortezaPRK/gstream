package state

import (
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/pebble"
)

// Checkpoint key scheme
//
// Store keys in Pebble have the form:
//
//	<storeName-bytes> 0x00 <serialized-key-bytes>
//
// The documented contract in keyvalue.go requires storeName to contain no 0x00
// bytes. Therefore the first byte of every store key is the first byte of the
// store name — which is NEVER 0x00.
//
// Checkpoint keys are constructed as:
//
//	0x00 "__gstream_ckpt__" 0x00 <storeName-bytes>
//
// The leading 0x00 byte (checkpointPrefix[0]) guarantees that a checkpoint key
// can never equal or be confused with any store key, because their first bytes
// differ by construction. The marker string "__gstream_ckpt__" further
// distinguishes checkpoints from any hypothetical future key type that might
// also use a leading-NUL convention. The trailing 0x00 (the same separator
// constant as keyvalue.go) ensures that two different store names produce
// distinct checkpoint keys without ambiguity.

// checkpointPrefix is the fixed byte sequence prepended to every checkpoint key.
// The leading 0x00 is the collision-prevention byte; no valid store name may
// start with 0x00, so no store key can share this prefix.
const checkpointPrefix = "\x00__gstream_ckpt__\x00"

// checkpointKey returns the Pebble key for the changelog offset checkpoint of
// the named store.
func checkpointKey(storeName string) []byte {
	key := make([]byte, len(checkpointPrefix)+len(storeName))
	copy(key, checkpointPrefix)
	copy(key[len(checkpointPrefix):], storeName)
	return key
}

// WriteCheckpoint writes the 8-byte big-endian encoding of offset into batch b
// under the checkpoint key for storeName. The caller owns the batch and is
// responsible for calling b.Commit(pebble.Sync) — this function intentionally
// does NOT commit, so the checkpoint write can be made atomic with other state
// writes in the same batch.
func WriteCheckpoint(b *pebble.Batch, storeName string, offset int64) error {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(offset))
	if err := b.Set(checkpointKey(storeName), buf, nil); err != nil {
		return fmt.Errorf("state: WriteCheckpoint: %w", err)
	}
	return nil
}

// ReadCheckpoint reads the changelog offset checkpoint for storeName from db.
//
//   - Returns (offset, true, nil) when a checkpoint is present.
//   - Returns (0, false, nil) when no checkpoint has been written yet.
//   - Returns (0, false, err) on any Pebble I/O error.
func ReadCheckpoint(db *pebble.DB, storeName string) (offset int64, found bool, err error) {
	val, closer, err := db.Get(checkpointKey(storeName))
	if err != nil {
		if err == pebble.ErrNotFound {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("state: ReadCheckpoint: %w", err)
	}
	// Copy bytes before closing the reader: the slice returned by Get is only
	// valid until closer.Close() is called.
	buf := make([]byte, len(val))
	copy(buf, val)
	if err := closer.Close(); err != nil {
		return 0, false, fmt.Errorf("state: ReadCheckpoint close: %w", err)
	}

	if len(buf) != 8 {
		return 0, false, fmt.Errorf("state: ReadCheckpoint: corrupt value: expected 8 bytes, got %d", len(buf))
	}
	return int64(binary.BigEndian.Uint64(buf)), true, nil
}

// WriteCheckpointSync is a thin convenience wrapper that creates a new batch,
// calls WriteCheckpoint, and commits synchronously with pebble.Sync. Useful for
// the final checkpoint write during restore where no other writes need to be
// batched atomically. For atomic multi-write scenarios use WriteCheckpoint
// directly with a caller-owned batch.
func WriteCheckpointSync(db *pebble.DB, storeName string, offset int64) error {
	b := db.NewBatch()
	defer func() { _ = b.Close() }()
	if err := WriteCheckpoint(b, storeName, offset); err != nil {
		return err
	}
	if err := b.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("state: WriteCheckpointSync commit: %w", err)
	}
	return nil
}

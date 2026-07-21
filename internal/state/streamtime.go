package state

import (
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/pebble"
)

// Stream-time key scheme
//
// Stream-time is a single scalar (int64 Unix ms) persisted per task/partition.
// The key follows the same collision-prevention convention as checkpoint keys:
//
//	0x00 "__gstream_streamtime__" 0x00
//
// The leading 0x00 byte guarantees it cannot collide with any store key (which
// must start with the first byte of a store name, never 0x00). The distinct
// marker string "__gstream_streamtime__" distinguishes it from checkpoint keys
// ("\x00__gstream_ckpt__\x00<name>"), which always carry a trailing store name.

// streamTimeKey is the single fixed Pebble key used to persist stream-time.
const streamTimeKey = "\x00__gstream_streamtime__\x00"

// WriteStreamTime writes the 8-byte big-endian encoding of ts to db under the
// stream-time key, committing synchronously with pebble.Sync.
func WriteStreamTime(db *pebble.DB, ts int64) error {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(ts))
	if err := db.Set([]byte(streamTimeKey), buf, pebble.Sync); err != nil {
		return fmt.Errorf("state: WriteStreamTime: %w", err)
	}
	return nil
}

// WriteStreamTimeBatch writes the 8-byte big-endian encoding of ts into batch b
// under the stream-time key. The caller owns the batch and is responsible for
// committing it — this function intentionally does NOT commit, so the stream-time
// write can be made atomic with other state writes in the same batch.
func WriteStreamTimeBatch(b *pebble.Batch, ts int64) error {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(ts))
	if err := b.Set([]byte(streamTimeKey), buf, nil); err != nil {
		return fmt.Errorf("state: WriteStreamTimeBatch: %w", err)
	}
	return nil
}

// ReadStreamTime reads the persisted stream-time from db.
//
//   - Returns (ts, true, nil) when a stream-time has been persisted.
//   - Returns (0, false, nil) when no stream-time has been written yet.
//   - Returns (0, false, err) on any Pebble I/O error.
func ReadStreamTime(db *pebble.DB) (ts int64, found bool, err error) {
	val, closer, err := db.Get([]byte(streamTimeKey))
	if err != nil {
		if err == pebble.ErrNotFound {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("state: ReadStreamTime: %w", err)
	}
	// Copy bytes before closing the reader: the slice returned by Get is only
	// valid until closer.Close() is called.
	buf := make([]byte, len(val))
	copy(buf, val)
	closer.Close()

	if len(buf) != 8 {
		return 0, false, fmt.Errorf("state: ReadStreamTime: corrupt value: expected 8 bytes, got %d", len(buf))
	}
	return int64(binary.BigEndian.Uint64(buf)), true, nil
}

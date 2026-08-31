package pebble_test

import (
	"os"
	"testing"

	"github.com/cockroachdb/pebble"
	state "github.com/mortezaPRK/gstream/stores/pebble"
)

// TestReadCheckpoint_Missing verifies that ReadCheckpoint on a fresh database
// returns (0, false, nil) — no error, no offset, not found.
func TestReadCheckpoint_Missing(t *testing.T) {
	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	offset, found, err := state.ReadCheckpoint(db, "my-store")
	if err != nil {
		t.Fatalf("ReadCheckpoint: unexpected error: %v", err)
	}
	if found {
		t.Fatalf("ReadCheckpoint: expected found=false, got offset=%d", offset)
	}
	if offset != 0 {
		t.Fatalf("ReadCheckpoint: expected offset=0, got %d", offset)
	}
}

// TestWriteReadCheckpoint_RoundTrip verifies that WriteCheckpoint + batch Commit
// followed by ReadCheckpoint returns the exact offset that was written.
func TestWriteReadCheckpoint_RoundTrip(t *testing.T) {
	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	offsets := []int64{0, 1, 42, 1 << 40}

	for _, want := range offsets {
		b := db.NewBatch()
		if err := state.WriteCheckpoint(b, "round-trip-store", want); err != nil {
			b.Close()
			t.Fatalf("WriteCheckpoint(%d): %v", want, err)
		}
		if err := b.Commit(pebble.Sync); err != nil {
			b.Close()
			t.Fatalf("batch.Commit(%d): %v", want, err)
		}
		b.Close()

		got, found, err := state.ReadCheckpoint(db, "round-trip-store")
		if err != nil {
			t.Fatalf("ReadCheckpoint after write(%d): %v", want, err)
		}
		if !found {
			t.Fatalf("ReadCheckpoint after write(%d): expected found=true", want)
		}
		if got != want {
			t.Fatalf("ReadCheckpoint after write(%d): got %d", want, got)
		}
	}
}

// TestCheckpoint_TwoStoresIsolated verifies that:
//  1. Checkpoints for two different stores in the same DB are independent.
//  2. A checkpoint key does NOT collide with a real KeyValueStore entry: creating
//     a store whose name resembles the checkpoint marker string still does not
//     produce a Pebble key equal to the checkpoint key, and Range over that
//     store does not return the checkpoint entry.
func TestCheckpoint_TwoStoresIsolated(t *testing.T) {
	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	// Write distinct offsets for two store names.
	for _, tc := range []struct {
		name   string
		offset int64
	}{
		{"store-alpha", 100},
		{"store-beta", 200},
	} {
		b := db.NewBatch()
		if err := state.WriteCheckpoint(b, tc.name, tc.offset); err != nil {
			b.Close()
			t.Fatalf("WriteCheckpoint %s: %v", tc.name, err)
		}
		if err := b.Commit(pebble.Sync); err != nil {
			b.Close()
			t.Fatalf("Commit %s: %v", tc.name, err)
		}
		b.Close()
	}

	// Confirm each store has its own offset.
	got1, found1, err := state.ReadCheckpoint(db, "store-alpha")
	if err != nil || !found1 || got1 != 100 {
		t.Fatalf("store-alpha: got=%d found=%v err=%v", got1, found1, err)
	}
	got2, found2, err := state.ReadCheckpoint(db, "store-beta")
	if err != nil || !found2 || got2 != 200 {
		t.Fatalf("store-beta: got=%d found=%v err=%v", got2, found2, err)
	}

	// Collision test: create a KeyValueStore with a name that is textually similar
	// to the checkpoint prefix (without the leading NUL) to stress-test uniqueness.
	// Store keys are <name>\x00<encodedKey>; checkpoint keys start with \x00 so
	// even if the store name is "__gstream_ckpt__..." the first byte differs.
	collidingStoreName := "__gstream_ckpt__lookalike"
	kvStore := state.NewKeyValueStore(collidingStoreName, db, stringSerde{}, stringSerde{})
	defer kvStore.Close()

	if err := kvStore.Put("some-key", "some-value"); err != nil {
		t.Fatalf("Put into colliding store: %v", err)
	}

	// ReadCheckpoint for the colliding store name should return not-found because
	// no checkpoint was written for it (and the store key is not a checkpoint key).
	offset3, found3, err := state.ReadCheckpoint(db, collidingStoreName)
	if err != nil {
		t.Fatalf("ReadCheckpoint colliding store: %v", err)
	}
	if found3 {
		t.Fatalf("ReadCheckpoint colliding store: found=%v offset=%d; collision detected!", found3, offset3)
	}

	// The checkpoint keys for store-alpha / store-beta must NOT appear in the
	// colliding store's Range — only the directly-inserted key should surface.
	var rangeKeys []string
	if err := kvStore.Range(func(k string, _ string) bool {
		rangeKeys = append(rangeKeys, k)
		return true
	}); err != nil {
		t.Fatalf("Range over colliding store: %v", err)
	}
	if len(rangeKeys) != 1 || rangeKeys[0] != "some-key" {
		t.Fatalf("Range: expected only 'some-key', got %v", rangeKeys)
	}
}

// TestCheckpoint_SurvivesReopen verifies that:
//  1. A checkpoint written with Sync survives a Pebble Close + reopen.
//  2. An atomic batch containing both a store-style key and a checkpoint key
//     persists both entries after close/reopen.
func TestCheckpoint_SurvivesReopen(t *testing.T) {
	dir, err := os.MkdirTemp("", "gstream-ckpt-test-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	defer os.RemoveAll(dir)

	const storeName = "reopen-store"
	const wantOffset int64 = 999999

	// storeKey encodes a key in the same layout as KeyValueStore: <name>\x00<rawKey>.
	// We build it by hand here to write it directly into a batch together with the
	// checkpoint, proving atomicity across close/reopen for both key types.
	storeKey := append([]byte(storeName+"\x00"), []byte("persist-key")...)

	// --- First open: write checkpoint and a store-style key atomically. ---
	db1, err := state.OpenDB(dir)
	if err != nil {
		t.Fatalf("OpenDB (first): %v", err)
	}

	b := db1.NewBatch()
	if setErr := b.Set(storeKey, []byte("persist-value"), nil); setErr != nil {
		b.Close()
		db1.Close()
		t.Fatalf("batch.Set store key: %v", setErr)
	}
	if writeErr := state.WriteCheckpoint(b, storeName, wantOffset); writeErr != nil {
		b.Close()
		db1.Close()
		t.Fatalf("WriteCheckpoint in batch: %v", writeErr)
	}
	if commitErr := b.Commit(pebble.Sync); commitErr != nil {
		b.Close()
		db1.Close()
		t.Fatalf("batch.Commit: %v", commitErr)
	}
	b.Close()
	if closeErr := db1.Close(); closeErr != nil {
		t.Fatalf("db1.Close: %v", closeErr)
	}

	// --- Second open: verify both entries are present. ---
	db2, err := state.OpenDB(dir)
	if err != nil {
		t.Fatalf("OpenDB (second): %v", err)
	}
	defer db2.Close()

	// Check checkpoint survived.
	gotOffset, found, err := state.ReadCheckpoint(db2, storeName)
	if err != nil {
		t.Fatalf("ReadCheckpoint after reopen: %v", err)
	}
	if !found {
		t.Fatal("ReadCheckpoint after reopen: expected found=true")
	}
	if gotOffset != wantOffset {
		t.Fatalf("ReadCheckpoint after reopen: got %d, want %d", gotOffset, wantOffset)
	}

	// Check store-style key also survived (atomic batch guarantee).
	valBytes, closer, getErr := db2.Get(storeKey)
	if getErr != nil {
		t.Fatalf("db2.Get store key after reopen: %v", getErr)
	}
	gotVal := string(valBytes)
	closer.Close()
	if gotVal != "persist-value" {
		t.Fatalf("store key after reopen: got %q, want %q", gotVal, "persist-value")
	}

	// Also exercise WriteCheckpointSync as a convenience path.
	const storeName2 = "sync-store"
	if err := state.WriteCheckpointSync(db2, storeName2, 77); err != nil {
		t.Fatalf("WriteCheckpointSync: %v", err)
	}
	got77, found77, err77 := state.ReadCheckpoint(db2, storeName2)
	if err77 != nil || !found77 || got77 != 77 {
		t.Fatalf("ReadCheckpoint after WriteCheckpointSync: got=%d found=%v err=%v", got77, found77, err77)
	}
}

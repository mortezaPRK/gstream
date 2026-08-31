package pebble_test

import (
	"os"
	"testing"

	"github.com/cockroachdb/pebble"
	state "github.com/mortezaPRK/gstream/store/pebble"
)

// TestStreamTime_MissingReturnsZero verifies that ReadStreamTime on a fresh
// database returns (0, false, nil) — no error, zero timestamp, not found.
func TestStreamTime_MissingReturnsZero(t *testing.T) {
	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	ts, found, err := state.ReadStreamTime(db)
	if err != nil {
		t.Fatalf("ReadStreamTime: unexpected error: %v", err)
	}
	if found {
		t.Fatalf("ReadStreamTime: expected found=false, got ts=%d", ts)
	}
	if ts != 0 {
		t.Fatalf("ReadStreamTime: expected ts=0, got %d", ts)
	}
}

// TestStreamTime_RoundTrip verifies that WriteStreamTime followed by
// ReadStreamTime returns the exact value that was written.
func TestStreamTime_RoundTrip(t *testing.T) {
	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	cases := []int64{0, 1, 42, 1_000_000_000, 1<<40 + 7}
	for _, want := range cases {
		if writeErr := state.WriteStreamTime(db, want); writeErr != nil {
			t.Fatalf("WriteStreamTime(%d): %v", want, writeErr)
		}
		got, found, readErr := state.ReadStreamTime(db)
		if readErr != nil {
			t.Fatalf("ReadStreamTime after write(%d): %v", want, readErr)
		}
		if !found {
			t.Fatalf("ReadStreamTime after write(%d): expected found=true", want)
		}
		if got != want {
			t.Fatalf("ReadStreamTime after write(%d): got %d", want, got)
		}
	}
}

// TestStreamTime_BatchRoundTrip verifies that WriteStreamTimeBatch committed in
// a caller-owned batch persists correctly.
func TestStreamTime_BatchRoundTrip(t *testing.T) {
	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	const want int64 = 99999

	b := db.NewBatch()
	if writeErr := state.WriteStreamTimeBatch(b, want); writeErr != nil {
		b.Close()
		t.Fatalf("WriteStreamTimeBatch: %v", writeErr)
	}
	if commitErr := b.Commit(pebble.Sync); commitErr != nil {
		b.Close()
		t.Fatalf("batch.Commit: %v", commitErr)
	}
	b.Close()

	got, found, err := state.ReadStreamTime(db)
	if err != nil {
		t.Fatalf("ReadStreamTime: %v", err)
	}
	if !found {
		t.Fatal("ReadStreamTime: expected found=true after batch write")
	}
	if got != want {
		t.Fatalf("ReadStreamTime: got %d, want %d", got, want)
	}
}

// TestStreamTime_SurvivesReopen verifies that a stream-time written with
// WriteStreamTime (Sync) survives a Pebble Close + reopen, and that an atomic
// batch write also survives close/reopen.
func TestStreamTime_SurvivesReopen(t *testing.T) {
	dir, err := os.MkdirTemp("", "gstream-streamtime-test-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	defer os.RemoveAll(dir)

	const want int64 = 1_700_000_000_000 // representative Unix ms

	// First open: write via WriteStreamTime (Sync).
	db1, err := state.OpenDB(dir)
	if err != nil {
		t.Fatalf("OpenDB (first): %v", err)
	}
	if writeErr := state.WriteStreamTime(db1, want); writeErr != nil {
		db1.Close()
		t.Fatalf("WriteStreamTime: %v", writeErr)
	}
	if closeErr := db1.Close(); closeErr != nil {
		t.Fatalf("db1.Close: %v", closeErr)
	}

	// Second open: verify stream-time survived, then write a new value via batch.
	db2, err := state.OpenDB(dir)
	if err != nil {
		t.Fatalf("OpenDB (second): %v", err)
	}

	got, found, readErr := state.ReadStreamTime(db2)
	if readErr != nil {
		db2.Close()
		t.Fatalf("ReadStreamTime after reopen: %v", readErr)
	}
	if !found {
		db2.Close()
		t.Fatal("ReadStreamTime after reopen: expected found=true")
	}
	if got != want {
		db2.Close()
		t.Fatalf("ReadStreamTime after reopen: got %d, want %d", got, want)
	}

	// Also test batch path survives via WriteStreamTimeBatch.
	const want2 int64 = 1_700_000_001_000
	b := db2.NewBatch()
	if writeErr := state.WriteStreamTimeBatch(b, want2); writeErr != nil {
		b.Close()
		db2.Close()
		t.Fatalf("WriteStreamTimeBatch: %v", writeErr)
	}
	if commitErr := b.Commit(pebble.Sync); commitErr != nil {
		b.Close()
		db2.Close()
		t.Fatalf("batch.Commit: %v", commitErr)
	}
	b.Close()
	if closeErr := db2.Close(); closeErr != nil {
		t.Fatalf("db2.Close: %v", closeErr)
	}

	// Third open: verify batch write survived.
	db3, err := state.OpenDB(dir)
	if err != nil {
		t.Fatalf("OpenDB (third): %v", err)
	}
	defer db3.Close()

	got2, found2, readErr2 := state.ReadStreamTime(db3)
	if readErr2 != nil {
		t.Fatalf("ReadStreamTime after batch reopen: %v", readErr2)
	}
	if !found2 {
		t.Fatal("ReadStreamTime after batch reopen: expected found=true")
	}
	if got2 != want2 {
		t.Fatalf("ReadStreamTime after batch reopen: got %d, want %d", got2, want2)
	}
}

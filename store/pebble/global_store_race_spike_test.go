package pebble_test

// Spike: P4c-S2 — verify that one KeyValueStore[[]byte,[]byte] (no collector,
// global-store form) is data-race-free when shared between one background writer
// (Put + Delete, simulating tail-consume applying GlobalKTable updates) and N
// concurrent readers (Get, simulating per-partition task join lookups).
//
// Primary signal: run with -race.
// Secondary: readers must never see garbage (empty value when found=true).
//
//	go test -race ./store/pebble/ -run 'GlobalStoreRace|StoreRace' -v -count=1

import (
	"fmt"
	"sync"
	"testing"

	state "github.com/mortezaPRK/gstream/store/pebble"
)

func TestGlobalStoreRace(t *testing.T) {
	const (
		numReaders      = 8
		numKeys         = 16
		writerIters     = 3000
		readerItersEach = 3000
	)

	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	// NewKeyValueStore — no collector, global-store form.
	// rawBytesSerde is defined in keyvalue_test.go (same package state_test).
	store := state.NewKeyValueStore[[]byte, []byte](
		"global-race-test", db, rawBytesSerde{}, rawBytesSerde{},
	)
	defer store.Close()

	// Pre-populate so readers see actual values from the start.
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		val := []byte(fmt.Sprintf("val-%04d-init", i))
		if err := store.Put(key, val); err != nil {
			t.Fatalf("initial Put key-%d: %v", i, err)
		}
	}

	var wg sync.WaitGroup

	// Writer: simulates tail-consume goroutine applying global-topic updates.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < writerIters; i++ {
			keyIdx := i % numKeys
			key := []byte(fmt.Sprintf("key-%04d", keyIdx))
			if i%10 == 0 {
				// Occasional delete — same pattern as changelog-apply tombstone.
				if err := store.Delete(key); err != nil {
					t.Errorf("writer Delete i=%d: %v", i, err)
					return
				}
			} else {
				val := []byte(fmt.Sprintf("val-%04d-iter-%d", keyIdx, i))
				if err := store.Put(key, val); err != nil {
					t.Errorf("writer Put i=%d: %v", i, err)
					return
				}
			}
		}
	}()

	// Readers: simulate per-partition task executors doing JoinGlobal point lookups.
	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			for i := 0; i < readerItersEach; i++ {
				keyIdx := (readerID*readerItersEach + i) % numKeys
				key := []byte(fmt.Sprintf("key-%04d", keyIdx))
				val, found, err := store.Get(key)
				if err != nil {
					t.Errorf("reader %d Get i=%d: %v", readerID, i, err)
					return
				}
				// Eventual consistency is expected (old/new/not-found all valid).
				// A found=true result with an empty value would be garbage — assert
				// our writes always produce non-empty values.
				if found && len(val) == 0 {
					t.Errorf("reader %d Get i=%d: found=true but empty value (garbage)", readerID, i)
					return
				}
			}
		}(r)
	}

	wg.Wait()
	// The race detector fails the test automatically if any concurrent access
	// to shared memory is unsynchronized.  Reaching PASS here means race-free.
}

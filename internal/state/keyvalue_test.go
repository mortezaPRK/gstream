package state_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/mortezaPRK/gstream/internal/state"
)

// --- Minimal test serdes (independent of any sibling-agent serde implementation) ---

// stringSerde serializes strings as their raw UTF-8 bytes.
type stringSerde struct{}

func (stringSerde) Serialize(s string) ([]byte, error)   { return []byte(s), nil }
func (stringSerde) Deserialize(b []byte) (string, error) { return string(b), nil }

// int64Serde serializes int64 values as 8-byte big-endian.
// Big-endian ensures that lexicographic byte order equals numeric order,
// which is important for Range tests.
type int64Serde struct{}

func (int64Serde) Serialize(n int64) ([]byte, error) {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(n))
	return b, nil
}

func (int64Serde) Deserialize(b []byte) (int64, error) {
	if len(b) != 8 {
		return 0, fmt.Errorf("int64Serde: expected 8 bytes, got %d", len(b))
	}
	return int64(binary.BigEndian.Uint64(b)), nil
}

// errSerde always returns an error from Serialize and Deserialize.
type errSerde[T any] struct{}

func (errSerde[T]) Serialize(_ T) ([]byte, error) { return nil, fmt.Errorf("serialize error") }
func (errSerde[T]) Deserialize(_ []byte) (T, error) {
	var z T
	return z, fmt.Errorf("deserialize error")
}

// errValueSerde returns errors only from value methods; key serde is normal string.
type errValueSerialize struct{}

func (errValueSerialize) Serialize(_ string) ([]byte, error) {
	return nil, fmt.Errorf("value serialize error")
}
func (errValueSerialize) Deserialize(b []byte) (string, error) { return string(b), nil }

// --- Helper to open an in-memory DB for a test ---

func mustOpenMemDB(t *testing.T) interface{ Close() error } {
	t.Helper()
	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	return db
}

// --- Tests ---

// TestPutGetRoundTrip verifies that a Put followed by a Get returns the same value.
func TestPutGetRoundTrip(t *testing.T) {
	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	store := state.NewKeyValueStore("test-store", db, stringSerde{}, stringSerde{})
	defer store.Close()

	if err := store.Put("hello", "world"); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, found, err := store.Get("hello")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found {
		t.Fatal("Get: expected found=true")
	}
	if got != "world" {
		t.Fatalf("Get: got %q, want %q", got, "world")
	}
}

// TestGetMissingKey verifies that Get on a non-existent key returns found=false and no error.
func TestGetMissingKey(t *testing.T) {
	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	store := state.NewKeyValueStore("test-store", db, stringSerde{}, stringSerde{})
	defer store.Close()

	got, found, err := store.Get("nosuchkey")
	if err != nil {
		t.Fatalf("Get: unexpected error: %v", err)
	}
	if found {
		t.Fatalf("Get: expected found=false, got value %q", got)
	}
}

// TestDeleteThenGet verifies that after a Delete the key is gone.
func TestDeleteThenGet(t *testing.T) {
	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	store := state.NewKeyValueStore("test-store", db, stringSerde{}, stringSerde{})
	defer store.Close()

	if err := store.Put("key", "value"); err != nil {
		t.Fatalf("Put: %v", err)
	}

	if err := store.Delete("key"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, found, err := store.Get("key")
	if err != nil {
		t.Fatalf("Get after Delete: %v", err)
	}
	if found {
		t.Fatal("Get after Delete: expected found=false")
	}
}

// TestDeleteMissingKeyIsNoop verifies deleting a non-existent key does not error.
func TestDeleteMissingKeyIsNoop(t *testing.T) {
	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	store := state.NewKeyValueStore("test-store", db, stringSerde{}, stringSerde{})
	defer store.Close()

	if err := store.Delete("ghost"); err != nil {
		t.Fatalf("Delete of missing key: unexpected error: %v", err)
	}
}

// TestRangeInOrder verifies that Range yields keys in ascending byte order.
func TestRangeInOrder(t *testing.T) {
	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	store := state.NewKeyValueStore("ordered", db, int64Serde{}, stringSerde{})
	defer store.Close()

	// Insert in non-ascending order.
	inserts := []int64{5, 2, 8, 1, 3}
	for _, k := range inserts {
		if err := store.Put(k, fmt.Sprintf("val-%d", k)); err != nil {
			t.Fatalf("Put %d: %v", k, err)
		}
	}

	var gotKeys []int64
	if err := store.Range(func(k int64, _ string) bool {
		gotKeys = append(gotKeys, k)
		return true
	}); err != nil {
		t.Fatalf("Range: %v", err)
	}

	expected := []int64{1, 2, 3, 5, 8}
	if len(gotKeys) != len(expected) {
		t.Fatalf("Range: got %v, want %v", gotKeys, expected)
	}
	for i := range expected {
		if gotKeys[i] != expected[i] {
			t.Fatalf("Range[%d]: got %d, want %d", i, gotKeys[i], expected[i])
		}
	}
}

// TestRangeStopsOnFalse verifies early termination when fn returns false.
func TestRangeStopsOnFalse(t *testing.T) {
	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	store := state.NewKeyValueStore("stop-test", db, int64Serde{}, stringSerde{})
	defer store.Close()

	for _, k := range []int64{1, 2, 3, 4, 5} {
		if err := store.Put(k, "x"); err != nil {
			t.Fatalf("Put %d: %v", k, err)
		}
	}

	var count int
	if err := store.Range(func(_ int64, _ string) bool {
		count++
		return count < 3 // stop after 3
	}); err != nil {
		t.Fatalf("Range: %v", err)
	}

	if count != 3 {
		t.Fatalf("Range: expected 3 calls, got %d", count)
	}
}

// TestNoCrossTalkBetweenStores verifies that two stores sharing a single Pebble DB
// do not see each other's keys during Range.
func TestNoCrossTalkBetweenStores(t *testing.T) {
	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	storeA := state.NewKeyValueStore("store-a", db, stringSerde{}, stringSerde{})
	storeB := state.NewKeyValueStore("store-b", db, stringSerde{}, stringSerde{})
	defer storeA.Close()
	defer storeB.Close()

	// Insert distinct keys in each store.
	if err := storeA.Put("alpha", "A"); err != nil {
		t.Fatalf("storeA Put: %v", err)
	}
	if err := storeA.Put("beta", "A"); err != nil {
		t.Fatalf("storeA Put: %v", err)
	}
	if err := storeB.Put("gamma", "B"); err != nil {
		t.Fatalf("storeB Put: %v", err)
	}
	if err := storeB.Put("delta", "B"); err != nil {
		t.Fatalf("storeB Put: %v", err)
	}

	// Range over storeA — should only see A's keys.
	var aKeys []string
	if err := storeA.Range(func(k string, v string) bool {
		if v != "A" {
			t.Errorf("storeA.Range: saw value %q from storeB", v)
		}
		aKeys = append(aKeys, k)
		return true
	}); err != nil {
		t.Fatalf("storeA Range: %v", err)
	}
	if len(aKeys) != 2 {
		t.Fatalf("storeA Range: got %d keys, want 2: %v", len(aKeys), aKeys)
	}

	// Range over storeB — should only see B's keys.
	var bKeys []string
	if err := storeB.Range(func(k string, v string) bool {
		if v != "B" {
			t.Errorf("storeB.Range: saw value %q from storeA", v)
		}
		bKeys = append(bKeys, k)
		return true
	}); err != nil {
		t.Fatalf("storeB Range: %v", err)
	}
	if len(bKeys) != 2 {
		t.Fatalf("storeB Range: got %d keys, want 2: %v", len(bKeys), bKeys)
	}

	// Cross-check: storeA cannot Get a key that only exists in storeB.
	_, found, err := storeA.Get("gamma")
	if err != nil {
		t.Fatalf("storeA.Get(gamma): %v", err)
	}
	if found {
		t.Fatal("storeA.Get(gamma): found a key that belongs to storeB")
	}

	// Cross-check: storeB cannot Get a key that only exists in storeA.
	_, found, err = storeB.Get("alpha")
	if err != nil {
		t.Fatalf("storeB.Get(alpha): %v", err)
	}
	if found {
		t.Fatal("storeB.Get(alpha): found a key that belongs to storeA")
	}
}

// TestSerdeErrorOnPut verifies that a Serialize error during Put is propagated.
func TestSerdeErrorOnPut(t *testing.T) {
	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	// Key serde that errors: any Put will fail at key serialization.
	store := state.NewKeyValueStore("err-store", db, errSerde[string]{}, stringSerde{})
	defer store.Close()

	if err := store.Put("k", "v"); err == nil {
		t.Fatal("Put: expected error from errSerde, got nil")
	}
}

// TestSerdeErrorOnValuePut verifies that a value Serialize error is propagated.
func TestSerdeErrorOnValuePut(t *testing.T) {
	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	store := state.NewKeyValueStore("err-val-store", db, stringSerde{}, errValueSerialize{})
	defer store.Close()

	if err := store.Put("k", "v"); err == nil {
		t.Fatal("Put: expected error from errValueSerialize, got nil")
	}
}

// TestSerdeErrorOnGet verifies that a Deserialize error during Get is propagated.
func TestSerdeErrorOnGet(t *testing.T) {
	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	// First, write a value using a normal value serde.
	normalStore := state.NewKeyValueStore("err-get", db, stringSerde{}, stringSerde{})
	if err := normalStore.Put("k", "v"); err != nil {
		t.Fatalf("Put via normal store: %v", err)
	}
	normalStore.Close()

	// Now read with a broken value serde — Deserialize should error.
	// (Key serde must still work to form the lookup key.)
	errValDeStore := state.NewKeyValueStore("err-get", db, stringSerde{}, errSerde[string]{})
	defer errValDeStore.Close()

	_, _, err = errValDeStore.Get("k")
	if err == nil {
		t.Fatal("Get: expected deserialize error, got nil")
	}
}

// TestCloseRejectsOperations verifies that operations on a closed store return errors.
func TestCloseRejectsOperations(t *testing.T) {
	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	store := state.NewKeyValueStore("closed-store", db, stringSerde{}, stringSerde{})
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if err := store.Put("k", "v"); err == nil {
		t.Error("Put on closed store: expected error")
	}
	if _, _, err := store.Get("k"); err == nil {
		t.Error("Get on closed store: expected error")
	}
	if err := store.Delete("k"); err == nil {
		t.Error("Delete on closed store: expected error")
	}
	if err := store.Range(func(string, string) bool { return true }); err == nil {
		t.Error("Range on closed store: expected error")
	}
}

// TestMultiplePutsOverwrite verifies that a subsequent Put overwrites the previous value.
func TestMultiplePutsOverwrite(t *testing.T) {
	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	store := state.NewKeyValueStore("overwrite-store", db, stringSerde{}, stringSerde{})
	defer store.Close()

	if err := store.Put("key", "first"); err != nil {
		t.Fatalf("first Put: %v", err)
	}
	if err := store.Put("key", "second"); err != nil {
		t.Fatalf("second Put: %v", err)
	}

	got, found, err := store.Get("key")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found {
		t.Fatal("Get: expected found=true")
	}
	if got != "second" {
		t.Fatalf("Get: got %q, want %q", got, "second")
	}
}

// TestRangeEmptyStore verifies Range on an empty store calls fn zero times.
func TestRangeEmptyStore(t *testing.T) {
	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	store := state.NewKeyValueStore("empty-store", db, stringSerde{}, stringSerde{})
	defer store.Close()

	var count int
	if err := store.Range(func(string, string) bool {
		count++
		return true
	}); err != nil {
		t.Fatalf("Range on empty store: %v", err)
	}
	if count != 0 {
		t.Fatalf("Range on empty store: expected 0 calls, got %d", count)
	}
}

// ---------------------------------------------------------------------------
// MutationCollector tests
// ---------------------------------------------------------------------------

// TestMutationCollector_DrainEmpty verifies that Drain on a fresh collector returns nil.
func TestMutationCollector_DrainEmpty(t *testing.T) {
	c := &state.MutationCollector{}
	got := c.Drain()
	if got != nil {
		t.Fatalf("Drain on empty collector: expected nil, got %v", got)
	}
}

// TestMutationCollector_PutDeleteMutations verifies that Put and Delete on a store with
// a MutationCollector produce correctly encoded Mutation entries:
//   - two Put mutations with matching encoded key/value bytes
//   - one Delete mutation with IsDelete=true and nil Value
func TestMutationCollector_PutDeleteMutations(t *testing.T) {
	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	c := &state.MutationCollector{}
	store := state.NewKeyValueStoreWithChangelog("mut-store", db, stringSerde{}, stringSerde{}, c)
	defer store.Close()

	if err := store.Put("alpha", "A"); err != nil {
		t.Fatalf("Put alpha: %v", err)
	}
	if err := store.Put("beta", "B"); err != nil {
		t.Fatalf("Put beta: %v", err)
	}
	if err := store.Delete("alpha"); err != nil {
		t.Fatalf("Delete alpha: %v", err)
	}

	mutations := c.Drain()
	if len(mutations) != 3 {
		t.Fatalf("expected 3 mutations, got %d", len(mutations))
	}

	// Mutation 0: Put("alpha","A")
	m0 := mutations[0]
	if m0.IsDelete {
		t.Error("mutation[0]: expected IsDelete=false for Put")
	}
	if string(m0.Value) != "A" {
		t.Errorf("mutation[0]: expected value='A', got %q", m0.Value)
	}
	// Key must be the Pebble-encoded key: "mut-store" + 0x00 + "alpha"
	wantKey0 := append([]byte("mut-store\x00"), []byte("alpha")...)
	if string(m0.Key) != string(wantKey0) {
		t.Errorf("mutation[0]: key mismatch: got %q, want %q", m0.Key, wantKey0)
	}

	// Mutation 1: Put("beta","B")
	m1 := mutations[1]
	if m1.IsDelete {
		t.Error("mutation[1]: expected IsDelete=false for Put")
	}
	if string(m1.Value) != "B" {
		t.Errorf("mutation[1]: expected value='B', got %q", m1.Value)
	}
	wantKey1 := append([]byte("mut-store\x00"), []byte("beta")...)
	if string(m1.Key) != string(wantKey1) {
		t.Errorf("mutation[1]: key mismatch: got %q, want %q", m1.Key, wantKey1)
	}

	// Mutation 2: Delete("alpha")
	m2 := mutations[2]
	if !m2.IsDelete {
		t.Error("mutation[2]: expected IsDelete=true for Delete")
	}
	if m2.Value != nil {
		t.Errorf("mutation[2]: expected nil Value for Delete, got %q", m2.Value)
	}
	if string(m2.Key) != string(wantKey0) {
		t.Errorf("mutation[2]: key mismatch: got %q, want %q", m2.Key, wantKey0)
	}
}

// TestMutationCollector_DrainClearsCollector verifies that Drain resets the collector
// so a subsequent Drain returns nil.
func TestMutationCollector_DrainClearsCollector(t *testing.T) {
	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	c := &state.MutationCollector{}
	store := state.NewKeyValueStoreWithChangelog("drain-store", db, stringSerde{}, stringSerde{}, c)
	defer store.Close()

	if err := store.Put("k", "v"); err != nil {
		t.Fatalf("Put: %v", err)
	}

	first := c.Drain()
	if len(first) != 1 {
		t.Fatalf("first Drain: expected 1 mutation, got %d", len(first))
	}

	second := c.Drain()
	if second != nil {
		t.Fatalf("second Drain: expected nil after reset, got %v", second)
	}
}

// ---------------------------------------------------------------------------
// RangeBytes tests
// ---------------------------------------------------------------------------

// TestRangeBytes_BasicRange verifies that RangeBytes returns only keys in [lower, upper).
func TestRangeBytes_BasicRange(t *testing.T) {
	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	// Use int64Serde so keys are big-endian and therefore ordered numerically.
	store := state.NewKeyValueStore("rb-basic", db, int64Serde{}, stringSerde{})
	defer store.Close()

	for _, k := range []int64{1, 2, 3, 4, 5} {
		if err := store.Put(k, fmt.Sprintf("v%d", k)); err != nil {
			t.Fatalf("Put %d: %v", k, err)
		}
	}

	// lower = serialized int64(2), upper = serialized int64(4) → expect {2, 3}
	lower := make([]byte, 8)
	upper := make([]byte, 8)
	binary.BigEndian.PutUint64(lower, uint64(2))
	binary.BigEndian.PutUint64(upper, uint64(4))

	var gotKeys []int64
	if err := store.RangeBytes(lower, upper, func(key, _ []byte) bool {
		// strip prefix
		prefix := append([]byte("rb-basic"), 0x00)
		raw := key[len(prefix):]
		k := int64(binary.BigEndian.Uint64(raw))
		gotKeys = append(gotKeys, k)
		return true
	}); err != nil {
		t.Fatalf("RangeBytes: %v", err)
	}

	if len(gotKeys) != 2 || gotKeys[0] != 2 || gotKeys[1] != 3 {
		t.Fatalf("RangeBytes: got %v, want [2 3]", gotKeys)
	}
}

// TestRangeBytes_EarlyStop verifies that returning false from fn stops iteration.
func TestRangeBytes_EarlyStop(t *testing.T) {
	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	store := state.NewKeyValueStore("rb-stop", db, int64Serde{}, stringSerde{})
	defer store.Close()

	for _, k := range []int64{1, 2, 3, 4, 5} {
		if err := store.Put(k, "x"); err != nil {
			t.Fatalf("Put %d: %v", k, err)
		}
	}

	lower := make([]byte, 8)
	upper := make([]byte, 8)
	binary.BigEndian.PutUint64(lower, uint64(1))
	binary.BigEndian.PutUint64(upper, uint64(6))

	var count int
	if err := store.RangeBytes(lower, upper, func(_, _ []byte) bool {
		count++
		return count < 2
	}); err != nil {
		t.Fatalf("RangeBytes: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 calls before stop, got %d", count)
	}
}

// TestRangeBytes_ClosedStoreError verifies that RangeBytes on a closed store errors.
func TestRangeBytes_ClosedStoreError(t *testing.T) {
	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	store := state.NewKeyValueStore("rb-closed", db, stringSerde{}, stringSerde{})
	store.Close()

	if err := store.RangeBytes([]byte("a"), []byte("z"), func(_, _ []byte) bool { return true }); err == nil {
		t.Fatal("expected error on closed store, got nil")
	}
}

// ---------------------------------------------------------------------------
// DeleteRangeBytes tests
// ---------------------------------------------------------------------------

// TestDeleteRangeBytes_DeletesKeysInRange verifies that after DeleteRangeBytes the
// targeted keys are gone and keys outside the range remain.
func TestDeleteRangeBytes_DeletesKeysInRange(t *testing.T) {
	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	store := state.NewKeyValueStore("drb-basic", db, int64Serde{}, stringSerde{})
	defer store.Close()

	for _, k := range []int64{1, 2, 3, 4, 5} {
		if err := store.Put(k, fmt.Sprintf("v%d", k)); err != nil {
			t.Fatalf("Put %d: %v", k, err)
		}
	}

	// Delete [2, 4) → keys 2, 3 deleted; 1, 4, 5 remain.
	lower := make([]byte, 8)
	upper := make([]byte, 8)
	binary.BigEndian.PutUint64(lower, uint64(2))
	binary.BigEndian.PutUint64(upper, uint64(4))

	if err := store.DeleteRangeBytes(lower, upper); err != nil {
		t.Fatalf("DeleteRangeBytes: %v", err)
	}

	// Keys 2 and 3 must be gone.
	for _, k := range []int64{2, 3} {
		_, found, err := store.Get(k)
		if err != nil {
			t.Fatalf("Get(%d): %v", k, err)
		}
		if found {
			t.Errorf("key %d still present after DeleteRangeBytes", k)
		}
	}

	// Keys 1, 4, 5 must still exist.
	for _, k := range []int64{1, 4, 5} {
		_, found, err := store.Get(k)
		if err != nil {
			t.Fatalf("Get(%d): %v", k, err)
		}
		if !found {
			t.Errorf("key %d missing but should not have been deleted", k)
		}
	}
}

// TestDeleteRangeBytes_MutationCollector verifies that each deleted key produces
// one IsDelete tombstone in the MutationCollector, and that the Mutation.Key
// matches the full Pebble key form used by Put (prefix + serialized-key).
func TestDeleteRangeBytes_MutationCollector(t *testing.T) {
	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	c := &state.MutationCollector{}
	store := state.NewKeyValueStoreWithChangelog("drb-mut", db, int64Serde{}, stringSerde{}, c)
	defer store.Close()

	for _, k := range []int64{10, 20, 30} {
		if err := store.Put(k, "v"); err != nil {
			t.Fatalf("Put %d: %v", k, err)
		}
	}
	// Drain Put mutations so we start clean.
	c.Drain()

	// Delete all three keys [10, 31).
	lower := make([]byte, 8)
	upper := make([]byte, 8)
	binary.BigEndian.PutUint64(lower, uint64(10))
	binary.BigEndian.PutUint64(upper, uint64(31))

	if err := store.DeleteRangeBytes(lower, upper); err != nil {
		t.Fatalf("DeleteRangeBytes: %v", err)
	}

	mutations := c.Drain()
	if len(mutations) != 3 {
		t.Fatalf("expected 3 tombstone mutations, got %d", len(mutations))
	}

	// Every mutation must be a deletion.
	for i, m := range mutations {
		if !m.IsDelete {
			t.Errorf("mutation[%d]: expected IsDelete=true", i)
		}
		if m.Value != nil {
			t.Errorf("mutation[%d]: expected nil Value, got %v", i, m.Value)
		}
	}

	// Verify Mutation.Key form: full Pebble key = "drb-mut\x00" + serialized int64.
	prefix := append([]byte("drb-mut"), 0x00)
	wantKeys := []int64{10, 20, 30}
	for i, m := range mutations {
		if !bytes.HasPrefix(m.Key, prefix) {
			t.Errorf("mutation[%d]: key %x does not start with prefix %x", i, m.Key, prefix)
			continue
		}
		raw := m.Key[len(prefix):]
		if len(raw) != 8 {
			t.Errorf("mutation[%d]: key suffix length %d, want 8", i, len(raw))
			continue
		}
		gotK := int64(binary.BigEndian.Uint64(raw))
		if gotK != wantKeys[i] {
			t.Errorf("mutation[%d]: key value %d, want %d", i, gotK, wantKeys[i])
		}
	}
}

// TestDeleteRangeBytes_ClosedStoreError verifies that DeleteRangeBytes on a closed
// store returns an error.
func TestDeleteRangeBytes_ClosedStoreError(t *testing.T) {
	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	store := state.NewKeyValueStore("drb-closed", db, stringSerde{}, stringSerde{})
	store.Close()

	if err := store.DeleteRangeBytes([]byte("a"), []byte("z")); err == nil {
		t.Fatal("expected error on closed store, got nil")
	}
}

// TestKeyValueStore_WithoutCollectorUnchanged verifies that a store created with
// NewKeyValueStore (no collector) still operates correctly — Put/Get/Delete/Range
// all work as before. This confirms the nil-collector path does not regress.
func TestKeyValueStore_WithoutCollectorUnchanged(t *testing.T) {
	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	store := state.NewKeyValueStore("no-collector", db, stringSerde{}, stringSerde{})
	defer store.Close()

	if err := store.Put("x", "1"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, found, err := store.Get("x")
	if err != nil || !found || got != "1" {
		t.Fatalf("Get after Put: got=%q found=%v err=%v", got, found, err)
	}
	if err := store.Delete("x"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, found, err = store.Get("x")
	if err != nil || found {
		t.Fatalf("Get after Delete: expected missing, got found=%v err=%v", found, err)
	}
}

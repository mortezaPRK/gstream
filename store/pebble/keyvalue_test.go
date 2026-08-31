package pebble_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	state "github.com/mortezaPRK/gstream/store/pebble"
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
// a MutationCollector produce correctly typed Mutation values:
//   - two state.Put mutations with matching encoded key/value bytes
//   - one state.Delete mutation with a nil Value (tombstone)
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
	wantKey0 := append([]byte("mut-store\x00"), []byte("alpha")...)
	switch m0 := mutations[0].(type) {
	case state.Put:
		if string(m0.Value) != "A" {
			t.Errorf("mutation[0]: expected value='A', got %q", m0.Value)
		}
		// Key must be the Pebble-encoded key: "mut-store" + 0x00 + "alpha"
		if string(m0.Key) != string(wantKey0) {
			t.Errorf("mutation[0]: key mismatch: got %q, want %q", m0.Key, wantKey0)
		}
	default:
		t.Errorf("mutation[0]: expected state.Put, got %T", mutations[0])
	}

	// Mutation 1: Put("beta","B")
	wantKey1 := append([]byte("mut-store\x00"), []byte("beta")...)
	switch m1 := mutations[1].(type) {
	case state.Put:
		if string(m1.Value) != "B" {
			t.Errorf("mutation[1]: expected value='B', got %q", m1.Value)
		}
		if string(m1.Key) != string(wantKey1) {
			t.Errorf("mutation[1]: key mismatch: got %q, want %q", m1.Key, wantKey1)
		}
	default:
		t.Errorf("mutation[1]: expected state.Put, got %T", mutations[1])
	}

	// Mutation 2: Delete("alpha")
	switch m2 := mutations[2].(type) {
	case state.Delete:
		if string(m2.Key) != string(wantKey0) {
			t.Errorf("mutation[2]: key mismatch: got %q, want %q", m2.Key, wantKey0)
		}
	default:
		t.Errorf("mutation[2]: expected state.Delete, got %T", mutations[2])
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

	// Every mutation must be a state.Delete (tombstone).
	prefix := append([]byte("drb-mut"), 0x00)
	wantKeys := []int64{10, 20, 30}
	for i, m := range mutations {
		del, ok := m.(state.Delete)
		if !ok {
			t.Errorf("mutation[%d]: expected state.Delete, got %T", i, m)
			continue
		}
		// Verify Delete.Key form: full Pebble key = "drb-mut\x00" + serialized int64.
		if !bytes.HasPrefix(del.Key, prefix) {
			t.Errorf("mutation[%d]: key %x does not start with prefix %x", i, del.Key, prefix)
			continue
		}
		raw := del.Key[len(prefix):]
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

// ---------------------------------------------------------------------------
// Iter / IterBytes tests
// ---------------------------------------------------------------------------

// TestIter verifies that Iter yields all key-value pairs and that early
// termination via a range-loop break stops iteration correctly.
func TestIter(t *testing.T) {
	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	store := state.NewKeyValueStore("iter-store", db, int64Serde{}, stringSerde{})
	defer store.Close()

	// Insert 3 key-value pairs.
	pairs := [][2]interface{}{{int64(1), "a"}, {int64(2), "b"}, {int64(3), "c"}}
	for _, p := range pairs {
		if err := store.Put(p[0].(int64), p[1].(string)); err != nil {
			t.Fatalf("Put %v: %v", p[0], err)
		}
	}

	// Collect all via Iter.
	var gotKeys []int64
	var gotVals []string
	for k, v := range store.Iter() {
		gotKeys = append(gotKeys, k)
		gotVals = append(gotVals, v)
	}
	if len(gotKeys) != 3 {
		t.Fatalf("Iter: expected 3 pairs, got %d", len(gotKeys))
	}
	expected := []int64{1, 2, 3}
	for i, k := range gotKeys {
		if k != expected[i] {
			t.Errorf("Iter[%d]: got key %d, want %d", i, k, expected[i])
		}
	}

	// Early-stop: break after first element.
	var earlyCount int
	for range store.Iter() {
		earlyCount++
		break
	}
	if earlyCount != 1 {
		t.Fatalf("Iter early-stop: expected 1 iteration, got %d", earlyCount)
	}
}

// ---------------------------------------------------------------------------
// RangeCompositeBytes tests
// ---------------------------------------------------------------------------

// TestRangeCompositeBytes verifies that RangeCompositeBytes:
//   - returns only entries for the queried kBytes (kA vs kB isolation)
//   - fn receives the per-store composite key (prefix stripped, decodable via
//     DecodeWindowCompositeKey) and the correct value
//   - keys come back in ascending order
//   - early-stop via return false works
func TestRangeCompositeBytes(t *testing.T) {
	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	store := state.NewKeyValueStore[[]byte, []byte]("rcb-store", db, rawBytesSerde{}, rawBytesSerde{})
	defer store.Close()

	kA := []byte("userA")
	kB := []byte("userB")

	// Write three windows for kA and one for kB.
	startsA := []int64{100, 200, 300}
	for _, s := range startsA {
		val := []byte(fmt.Sprintf("val-A-%d", s))
		if err := store.WindowPut(kA, s, val); err != nil {
			t.Fatalf("WindowPut kA %d: %v", s, err)
		}
	}
	if err := store.WindowPut(kB, 150, []byte("val-B-150")); err != nil {
		t.Fatalf("WindowPut kB: %v", err)
	}

	// RangeCompositeBytes over kA's full range.
	lower := state.WindowKeyLowerBound(kA)
	upper := state.WindowKeyUpperBound(kA)

	var gotKeys [][]byte
	var gotVals [][]byte
	if err := store.RangeCompositeBytes(lower, upper, func(compositeKey, val []byte) bool {
		gotKeys = append(gotKeys, compositeKey)
		gotVals = append(gotVals, val)
		return true
	}); err != nil {
		t.Fatalf("RangeCompositeBytes: %v", err)
	}

	// Expect exactly the three kA entries.
	if len(gotKeys) != 3 {
		t.Fatalf("expected 3 entries for kA, got %d", len(gotKeys))
	}

	// Each composite key must decode to kA and the corresponding windowStart.
	for i, ck := range gotKeys {
		decodedK, decodedStart, err := state.DecodeWindowCompositeKey(ck)
		if err != nil {
			t.Fatalf("entry[%d] DecodeWindowCompositeKey: %v", i, err)
		}
		if string(decodedK) != string(kA) {
			t.Errorf("entry[%d]: decoded key %q, want %q", i, decodedK, kA)
		}
		if decodedStart != startsA[i] {
			t.Errorf("entry[%d]: decoded windowStart %d, want %d", i, decodedStart, startsA[i])
		}
		wantVal := fmt.Sprintf("val-A-%d", startsA[i])
		if string(gotVals[i]) != wantVal {
			t.Errorf("entry[%d]: value %q, want %q", i, gotVals[i], wantVal)
		}
	}

	// Early-stop: fn returns false after first entry.
	var stopCount int
	if err := store.RangeCompositeBytes(lower, upper, func(_, _ []byte) bool {
		stopCount++
		return false // stop immediately
	}); err != nil {
		t.Fatalf("RangeCompositeBytes early-stop: %v", err)
	}
	if stopCount != 1 {
		t.Fatalf("early-stop: expected 1 call, got %d", stopCount)
	}
}

// TestRangeCompositeBytes_ClosedStoreError verifies error on closed store.
func TestRangeCompositeBytes_ClosedStoreError(t *testing.T) {
	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	store := state.NewKeyValueStore[[]byte, []byte]("rcb-closed", db, rawBytesSerde{}, rawBytesSerde{})
	store.Close()

	if err := store.RangeCompositeBytes([]byte("a"), []byte("z"), func(_, _ []byte) bool { return true }); err == nil {
		t.Fatal("expected error on closed store, got nil")
	}
}

// ---------------------------------------------------------------------------
// WindowDelete tests
// ---------------------------------------------------------------------------

// TestWindowDelete verifies that WindowDelete removes the entry and that the
// MutationCollector receives a Delete with the full prefixed Pebble key.
func TestWindowDelete(t *testing.T) {
	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	c := &state.MutationCollector{}
	store := state.NewKeyValueStoreWithChangelog[[]byte, []byte]("wd-store", db, rawBytesSerde{}, rawBytesSerde{}, c)
	defer store.Close()

	kBytes := []byte("mykey")
	sessionStart := int64(1000)
	val := []byte("myval")

	// Put then delete.
	if err := store.WindowPut(kBytes, sessionStart, val); err != nil {
		t.Fatalf("WindowPut: %v", err)
	}
	c.Drain() // clear Put mutation

	if err := store.WindowDelete(kBytes, sessionStart); err != nil {
		t.Fatalf("WindowDelete: %v", err)
	}

	// Key must be gone.
	got, found, err := store.WindowGet(kBytes, sessionStart)
	if err != nil {
		t.Fatalf("WindowGet after WindowDelete: %v", err)
	}
	if found {
		t.Fatalf("WindowGet: expected found=false after delete, got value %q", got)
	}

	// Collector must have one Delete with the full prefixed key.
	mutations := c.Drain()
	if len(mutations) != 1 {
		t.Fatalf("expected 1 mutation, got %d", len(mutations))
	}
	del, ok := mutations[0].(state.Delete)
	if !ok {
		t.Fatalf("expected state.Delete, got %T", mutations[0])
	}

	// Verify full key: "wd-store\x00" + WindowCompositeKey(kBytes, sessionStart)
	prefix := append([]byte("wd-store"), 0x00)
	ck := state.WindowCompositeKey(kBytes, sessionStart)
	wantKey := append(prefix, ck...)
	if string(del.Key) != string(wantKey) {
		t.Errorf("Delete.Key = %x, want %x", del.Key, wantKey)
	}
}

// TestWindowDelete_MissingKeyNoError verifies that WindowDelete on a missing key is a no-op.
func TestWindowDelete_MissingKeyNoError(t *testing.T) {
	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	store := state.NewKeyValueStore[[]byte, []byte]("wd-noop", db, rawBytesSerde{}, rawBytesSerde{})
	defer store.Close()

	if err := store.WindowDelete([]byte("ghost"), 999); err != nil {
		t.Fatalf("WindowDelete on missing key: unexpected error: %v", err)
	}
}

// TestWindowDelete_NilCollectorSafe verifies no panic when collector is nil.
func TestWindowDelete_NilCollectorSafe(t *testing.T) {
	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	store := state.NewKeyValueStore[[]byte, []byte]("wd-nilcol", db, rawBytesSerde{}, rawBytesSerde{})
	defer store.Close()

	if err := store.WindowPut([]byte("k"), 42, []byte("v")); err != nil {
		t.Fatalf("WindowPut: %v", err)
	}
	if err := store.WindowDelete([]byte("k"), 42); err != nil {
		t.Fatalf("WindowDelete with nil collector: %v", err)
	}
}

// TestWindowDelete_ClosedStoreError verifies error on closed store.
func TestWindowDelete_ClosedStoreError(t *testing.T) {
	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	store := state.NewKeyValueStore[[]byte, []byte]("wd-closed", db, rawBytesSerde{}, rawBytesSerde{})
	store.Close()

	if err := store.WindowDelete([]byte("k"), 0); err == nil {
		t.Fatal("expected error on closed store, got nil")
	}
}

// rawBytesSerde is a Serde[[]byte] that passes bytes through unchanged.
type rawBytesSerde struct{}

func (rawBytesSerde) Serialize(b []byte) ([]byte, error)   { return b, nil }
func (rawBytesSerde) Deserialize(b []byte) ([]byte, error) { return b, nil }

// TestIterBytes verifies that IterBytes yields raw key/value pairs in [lower, upper)
// and that early termination via break stops iteration correctly.
func TestIterBytes(t *testing.T) {
	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	store := state.NewKeyValueStore("iterbytes-store", db, int64Serde{}, stringSerde{})
	defer store.Close()

	for _, k := range []int64{1, 2, 3, 4, 5} {
		if err := store.Put(k, fmt.Sprintf("v%d", k)); err != nil {
			t.Fatalf("Put %d: %v", k, err)
		}
	}

	// lower = serialized int64(2), upper = serialized int64(5) → expect keys 2, 3, 4.
	lower := make([]byte, 8)
	upper := make([]byte, 8)
	binary.BigEndian.PutUint64(lower, uint64(2))
	binary.BigEndian.PutUint64(upper, uint64(5))

	var gotKeys []int64
	prefix := append([]byte("iterbytes-store"), 0x00)
	for k, _ := range store.IterBytes(lower, upper) {
		raw := k[len(prefix):]
		gotKeys = append(gotKeys, int64(binary.BigEndian.Uint64(raw)))
	}
	if len(gotKeys) != 3 {
		t.Fatalf("IterBytes: expected 3 keys, got %v", gotKeys)
	}
	wantKeys := []int64{2, 3, 4}
	for i, k := range gotKeys {
		if k != wantKeys[i] {
			t.Errorf("IterBytes[%d]: got %d, want %d", i, k, wantKeys[i])
		}
	}

	// Early-stop: break after first element.
	var earlyCount int
	for range store.IterBytes(lower, upper) {
		earlyCount++
		break
	}
	if earlyCount != 1 {
		t.Fatalf("IterBytes early-stop: expected 1 iteration, got %d", earlyCount)
	}
}

// ---------------------------------------------------------------------------
// RangeForKey tests  [T1-amendment]
// ---------------------------------------------------------------------------

// TestRangeForKey verifies that RangeForKey:
//   - calls fn with the correct sessionStart (ascending) and intact value bytes
//   - isolates entries for the queried kBytes from those of another key
//   - early-stop via return false works
func TestRangeForKey(t *testing.T) {
	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	store := state.NewKeyValueStore[[]byte, []byte]("rfk-store", db, rawBytesSerde{}, rawBytesSerde{})
	defer store.Close()

	kA := []byte("alice")
	kB := []byte("bob")

	// Write three sessions for kA and one for kB.
	startsA := []int64{1000, 2000, 3000}
	for _, s := range startsA {
		val := []byte(fmt.Sprintf("val-A-%d", s))
		if err := store.WindowPut(kA, s, val); err != nil {
			t.Fatalf("WindowPut kA %d: %v", s, err)
		}
	}
	if err := store.WindowPut(kB, 1500, []byte("val-B-1500")); err != nil {
		t.Fatalf("WindowPut kB: %v", err)
	}

	// RangeForKey over kA — expect exactly the three kA entries in ascending order.
	var gotStarts []int64
	var gotVals [][]byte
	if err := store.RangeForKey(kA, func(sStart int64, val []byte) bool {
		gotStarts = append(gotStarts, sStart)
		gotVals = append(gotVals, val)
		return true
	}); err != nil {
		t.Fatalf("RangeForKey: %v", err)
	}

	if len(gotStarts) != 3 {
		t.Fatalf("expected 3 entries for kA, got %d", len(gotStarts))
	}
	for i, s := range startsA {
		if gotStarts[i] != s {
			t.Errorf("entry[%d]: sessionStart got %d, want %d", i, gotStarts[i], s)
		}
		wantVal := fmt.Sprintf("val-A-%d", s)
		if string(gotVals[i]) != wantVal {
			t.Errorf("entry[%d]: value %q, want %q", i, gotVals[i], wantVal)
		}
	}

	// kB entry must NOT appear in kA's range.
	for _, s := range gotStarts {
		if s == 1500 {
			t.Error("kB entry at sStart=1500 appeared in kA's RangeForKey — isolation violated")
		}
	}

	// Early-stop: fn returns false after first entry.
	var stopCount int
	if err := store.RangeForKey(kA, func(_ int64, _ []byte) bool {
		stopCount++
		return false // stop immediately
	}); err != nil {
		t.Fatalf("RangeForKey early-stop: %v", err)
	}
	if stopCount != 1 {
		t.Fatalf("early-stop: expected 1 call, got %d", stopCount)
	}
}

// TestRangeForKey_EmptyKey verifies that RangeForKey on a key with no sessions calls fn zero times.
func TestRangeForKey_EmptyKey(t *testing.T) {
	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	store := state.NewKeyValueStore[[]byte, []byte]("rfk-empty", db, rawBytesSerde{}, rawBytesSerde{})
	defer store.Close()

	var count int
	if err := store.RangeForKey([]byte("ghost"), func(_ int64, _ []byte) bool {
		count++
		return true
	}); err != nil {
		t.Fatalf("RangeForKey on empty key: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 calls for key with no sessions, got %d", count)
	}
}

// TestRangeForKey_ClosedStoreError verifies error on closed store.
func TestRangeForKey_ClosedStoreError(t *testing.T) {
	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close()

	store := state.NewKeyValueStore[[]byte, []byte]("rfk-closed", db, rawBytesSerde{}, rawBytesSerde{})
	store.Close()

	if err := store.RangeForKey([]byte("k"), func(_ int64, _ []byte) bool { return true }); err == nil {
		t.Fatal("expected error on closed store, got nil")
	}
}

package memory_test

import (
	"testing"

	"mortz.dev/go/gstream/stores/memory"
)

type bytesSerde struct{}

func (bytesSerde) Serialize(value []byte) ([]byte, error)   { return value, nil }
func (bytesSerde) Deserialize(value []byte) ([]byte, error) { return value, nil }

func TestKeyValueAndWindowOperations(t *testing.T) {
	db, err := memory.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	store := memory.NewKeyValueStore[[]byte, []byte](
		"test", db, bytesSerde{}, bytesSerde{},
	)

	if err := store.Put([]byte("plain"), []byte("value")); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	value, found, err := store.Get([]byte("plain"))
	if err != nil || !found || string(value) != "value" {
		t.Fatalf("Get() = %q, %v, %v", value, found, err)
	}
	if err := store.WindowPut([]byte("window"), 10, []byte("first")); err != nil {
		t.Fatalf("WindowPut(first) error = %v", err)
	}
	if err := store.WindowPut([]byte("window"), 20, []byte("second")); err != nil {
		t.Fatalf("WindowPut(second) error = %v", err)
	}
	var starts []int64
	if err := store.RangeForKey([]byte("window"), func(start int64, _ []byte) bool {
		starts = append(starts, start)
		return true
	}); err != nil {
		t.Fatalf("RangeForKey() error = %v", err)
	}
	if len(starts) != 2 || starts[0] != 10 || starts[1] != 20 {
		t.Fatalf("RangeForKey() starts = %v, want [10 20]", starts)
	}
}

func TestClosedStoreRejectsOperations(t *testing.T) {
	db, _ := memory.OpenMemDB()
	store := memory.NewKeyValueStore[[]byte, []byte](
		"test", db, bytesSerde{}, bytesSerde{},
	)
	if err := store.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := store.Put([]byte("key"), []byte("value")); err == nil {
		t.Fatal("Put() error = nil after Close")
	}
}

func TestClosedDatabaseRejectsOperations(t *testing.T) {
	db, _ := memory.OpenMemDB()
	store := memory.NewKeyValueStore[[]byte, []byte](
		"test", db, bytesSerde{}, bytesSerde{},
	)
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if _, _, err := store.Get([]byte("key")); err == nil {
		t.Fatal("Get() error = nil after DB Close")
	}
}

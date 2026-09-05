package runtime

import (
	"context"
	"testing"

	gstream "mortz.dev/go/gstream"
	state "mortz.dev/go/gstream/internal/testutil"
)

// dummyBinding returns a minimal GlobalTableBinding for tests that do not
// exercise the serde closures.
func dummyBinding(storeName, topic string) gstream.GlobalTableBinding {
	encode := func(v any) ([]byte, error) { return v.([]byte), nil }
	identity := func(b []byte) (any, error) { return b, nil }
	return gstream.GlobalTableBinding{
		StoreName: storeName,
		Topic:     topic,
		EncodeKey: encode,
		DecodeKey: identity,
		EncodeVal: encode,
		DecodeVal: identity,
	}
}

func TestNewGlobalConsumer_OpensConfiguredStore(t *testing.T) {
	dir := t.TempDir()
	cfg := gstream.Config{
		ApplicationID: "test-app",
		Brokers:       []string{"localhost:9092"}, // not contacted in this test
		StateDir:      dir,
		StoreProvider: state.MemoryProvider{},
	}
	binding := dummyBinding("my-store", "my-topic")

	gc, err := NewGlobalConsumer(cfg, binding, nil)
	if err != nil {
		t.Fatalf("NewGlobalConsumer: %v", err)
	}
	defer gc.Close() //nolint:errcheck

	// DB must be open: a Put round-trip succeeds.
	if err := gc.store.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("store.Put: %v", err)
	}
	got, ok, err := gc.store.Get([]byte("k"))
	if err != nil || !ok || string(got) != "v" {
		t.Fatalf("store.Get: got=%q ok=%v err=%v", got, ok, err)
	}

}

// TestGlobalConsumer_StoreRoundTrip verifies that Store() returns the live
// *state.KeyValueStore[[]byte,[]byte] and that Put/Get/Delete work via it.
func TestGlobalConsumer_StoreRoundTrip(t *testing.T) {
	dir := t.TempDir()
	cfg := gstream.Config{
		ApplicationID: "rt-app",
		Brokers:       []string{"localhost:9092"},
		StateDir:      dir,
		StoreProvider: state.MemoryProvider{},
	}
	gc, err := NewGlobalConsumer(cfg, dummyBinding("rt-store", "rt-topic"), nil)
	if err != nil {
		t.Fatalf("NewGlobalConsumer: %v", err)
	}
	defer gc.Close() //nolint:errcheck

	raw := gc.Store()
	if raw == nil {
		t.Fatal("Store() returned nil")
	}
	kvStore, ok := raw.(gstream.Store)
	if !ok {
		t.Fatalf("Store() returned %T, want gstream.Store", raw)
	}

	if err := kvStore.Put([]byte("hello"), []byte("world")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, ok, err := kvStore.Get([]byte("hello"))
	if err != nil || !ok || string(got) != "world" {
		t.Errorf("Get: got=%q ok=%v err=%v", got, ok, err)
	}

	if err := kvStore.Delete([]byte("hello")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, ok, _ = kvStore.Get([]byte("hello"))
	if ok {
		t.Error("key present after Delete")
	}
}

// TestGlobalConsumer_ApplyKV verifies the tombstone/put dispatch in applyKV
// without a broker.
func TestGlobalConsumer_ApplyKV(t *testing.T) {
	db, err := state.OpenMemDB()
	if err != nil {
		t.Fatalf("OpenMemDB: %v", err)
	}
	defer db.Close() //nolint:errcheck

	store := state.NewKeyValueStore[[]byte, []byte](
		"kv-test", db,
		state.BytesSerde{}, state.BytesSerde{},
	)
	gc := &GlobalConsumer{store: store, backend: db, storeName: "kv-test"}

	t.Run("put_non_tombstone", func(t *testing.T) {
		if err := gc.applyKV([]byte("k1"), []byte("v1"), 0, 0); err != nil {
			t.Fatalf("applyKV put: %v", err)
		}
		got, ok, err := store.Get([]byte("k1"))
		if err != nil || !ok || string(got) != "v1" {
			t.Errorf("Get: got=%q ok=%v err=%v", got, ok, err)
		}
	})

	t.Run("tombstone_deletes", func(t *testing.T) {
		_ = store.Put([]byte("k2"), []byte("v2"))
		// nil value is a Kafka tombstone.
		if err := gc.applyKV([]byte("k2"), nil, 1, 0); err != nil {
			t.Fatalf("applyKV tombstone: %v", err)
		}
		_, ok, err := store.Get([]byte("k2"))
		if err != nil {
			t.Fatalf("Get after tombstone: %v", err)
		}
		if ok {
			t.Error("key present after tombstone")
		}
	})

	t.Run("empty_value_is_tombstone", func(t *testing.T) {
		_ = store.Put([]byte("k3"), []byte("v3"))
		// Empty (not nil) value: also treated as tombstone (len==0).
		if err := gc.applyKV([]byte("k3"), []byte{}, 2, 0); err != nil {
			t.Fatalf("applyKV empty-value: %v", err)
		}
		_, ok, _ := store.Get([]byte("k3"))
		if ok {
			t.Error("key present after empty-value tombstone")
		}
	})
}

// TestGlobalConsumer_CloseWithoutTail verifies Close does not panic when
// called before TailConsume.
func TestGlobalConsumer_CloseWithoutTail(t *testing.T) {
	dir := t.TempDir()
	cfg := gstream.Config{
		ApplicationID: "close-app",
		Brokers:       []string{"localhost:9092"},
		StateDir:      dir,
		StoreProvider: state.MemoryProvider{},
	}
	gc, err := NewGlobalConsumer(cfg, dummyBinding("cs", "ct"), nil)
	if err != nil {
		t.Fatalf("NewGlobalConsumer: %v", err)
	}
	if err := gc.Close(); err != nil {
		t.Logf("Close (no-tail): %v (non-fatal)", err)
	}
}

// TestGlobalConsumer_TailConsume_NilClientGuard verifies that TailConsume returns
// an error if Bootstrap has not been called (gc.client == nil).
func TestGlobalConsumer_TailConsume_NilClientGuard(t *testing.T) {
	dir := t.TempDir()
	cfg := gstream.Config{
		ApplicationID: "nil-client-app",
		Brokers:       []string{"localhost:9092"},
		StateDir:      dir,
		StoreProvider: state.MemoryProvider{},
	}
	gc, err := NewGlobalConsumer(cfg, dummyBinding("nc", "nt"), nil)
	if err != nil {
		t.Fatalf("NewGlobalConsumer: %v", err)
	}
	defer gc.Close() //nolint:errcheck

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = gc.TailConsume(ctx)
	if err == nil {
		t.Error("TailConsume should return an error when Bootstrap was not called")
	}
}

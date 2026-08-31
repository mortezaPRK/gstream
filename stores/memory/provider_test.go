package memory_test

import (
	"testing"

	gstream "github.com/mortezaPRK/gstream"
	"github.com/mortezaPRK/gstream/stores/memory"
)

func TestProviderRestoreRoundTrip(t *testing.T) {
	backend, err := memory.NewProvider().Open("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = backend.Close() })

	store, err := backend.OpenStore("counts", true)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	mutations := store.DrainMutations()
	if len(mutations) != 1 {
		t.Fatalf("DrainMutations() = %d, want 1", len(mutations))
	}

	restored, err := memory.NewProvider().Open("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restored.Close() })
	if err := restored.Restore("counts", mutations, 7); err != nil {
		t.Fatal(err)
	}
	restoredStore, err := restored.OpenStore("counts", false)
	if err != nil {
		t.Fatal(err)
	}
	value, found, err := restoredStore.Get([]byte("key"))
	if err != nil || !found || string(value) != "value" {
		t.Fatalf("Get() = %q, %t, %v", value, found, err)
	}
	offset, found, err := restored.ReadCheckpoint("counts")
	if err != nil || !found || offset != 7 {
		t.Fatalf("ReadCheckpoint() = %d, %t, %v", offset, found, err)
	}
}

var _ gstream.StoreProvider = memory.Provider{}

package pebble

import (
	"fmt"

	pebbledb "github.com/cockroachdb/pebble"
	gstream "github.com/mortezaPRK/gstream"
)

// Provider opens Pebble-backed state backends.
type Provider struct{}

// NewProvider creates Pebble StoreProvider.
func NewProvider() Provider { return Provider{} }

// Open opens persistent backend at path.
func (Provider) Open(path string) (gstream.StoreBackend, error) {
	db, err := OpenDB(path)
	if err != nil {
		return nil, err
	}
	return &Backend{db: db}, nil
}

// Backend owns one Pebble database shared by named stores.
type Backend struct{ db *pebbledb.DB }

// NewMemoryBackend creates in-memory Pebble backend for tests.
func NewMemoryBackend() (*Backend, error) {
	db, err := OpenMemDB()
	if err != nil {
		return nil, err
	}
	return &Backend{db: db}, nil
}

type bytesSerde struct{}

func (bytesSerde) Serialize(value []byte) ([]byte, error) { return value, nil }

func (bytesSerde) Deserialize(value []byte) ([]byte, error) { return value, nil }

func (backend *Backend) OpenStore(name string, collectMutations bool) (gstream.Store, error) {
	if backend == nil || backend.db == nil {
		return nil, fmt.Errorf("pebble: nil backend")
	}
	var collector *MutationCollector
	if collectMutations {
		collector = &MutationCollector{}
	}
	return NewKeyValueStoreWithChangelog[[]byte, []byte](
		name, backend.db, bytesSerde{}, bytesSerde{}, collector,
	), nil
}

func (backend *Backend) Restore(storeName string, mutations []gstream.StoreMutation, checkpoint int64) error {
	batch := backend.db.NewBatch()
	defer func() { _ = batch.Close() }()
	for _, mutation := range mutations {
		var err error
		if mutation.Value == nil {
			err = batch.Delete(mutation.Key, nil)
		} else {
			err = batch.Set(mutation.Key, mutation.Value, nil)
		}
		if err != nil {
			return gstream.ErrStoreWrite{Op: "Restore", Err: err}
		}
	}
	if checkpoint >= 0 {
		if err := WriteCheckpoint(batch, storeName, checkpoint); err != nil {
			return err
		}
	}
	if err := batch.Commit(pebbledb.Sync); err != nil {
		return gstream.ErrStoreWrite{Op: "Restore", Err: err}
	}
	return nil
}

func (backend *Backend) ReadCheckpoint(storeName string) (int64, bool, error) {
	return ReadCheckpoint(backend.db, storeName)
}

func (backend *Backend) WriteCheckpoint(storeName string, offset int64) error {
	return WriteCheckpointSync(backend.db, storeName, offset)
}

func (backend *Backend) ReadStreamTime() (int64, bool, error) {
	return ReadStreamTime(backend.db)
}

func (backend *Backend) WriteStreamTime(timestamp int64) error {
	return WriteStreamTime(backend.db, timestamp)
}

func (backend *Backend) Close() error { return backend.db.Close() }

var _ gstream.StoreProvider = Provider{}
var _ gstream.StoreBackend = (*Backend)(nil)

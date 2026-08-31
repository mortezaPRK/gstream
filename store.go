package gstream

import (
	"errors"
	"fmt"
)

// StoreMutation is one opaque storage mutation used for Kafka changelogs.
// A nil Value represents deletion.
type StoreMutation struct {
	Key   []byte
	Value []byte
}

// Store is raw byte-oriented state used by the runtime and stateful DSL.
// Implementations own key layout and persistence details.
type Store interface {
	Get(key []byte) ([]byte, bool, error)
	Put(key, value []byte) error
	Delete(key []byte) error
	Range(visit func(key, value []byte) bool) error
	WindowGet(key []byte, windowStart int64) ([]byte, bool, error)
	WindowPut(key []byte, windowStart int64, value []byte) error
	WindowDelete(key []byte, windowStart int64) error
	RangeCompositeBytes(lower, upper []byte, visit func(compositeKey, value []byte) bool) error
	RangeForKey(key []byte, visit func(sessionStart int64, value []byte) bool) error
	DrainMutations() []StoreMutation
	Close() error
}

// StoreBackend owns all named stores for one runtime task or global table.
type StoreBackend interface {
	OpenStore(name string, collectMutations bool) (Store, error)
	Restore(storeName string, mutations []StoreMutation, checkpoint int64) error
	ReadCheckpoint(storeName string) (offset int64, found bool, err error)
	WriteCheckpoint(storeName string, offset int64) error
	ReadStreamTime() (timestamp int64, found bool, err error)
	WriteStreamTime(timestamp int64) error
	Close() error
}

// StoreProvider opens one backend at path. Root runtime depends only on this
// contract; concrete implementations live under github.com/mortezaPRK/gstream/stores.
type StoreProvider interface {
	Open(path string) (StoreBackend, error)
}

// ErrStoreWriteSentinel identifies fatal local-state write failures.
var ErrStoreWriteSentinel = errors.New("store write failed")

// ErrStoreWrite wraps a failed mutating operation.
type ErrStoreWrite struct {
	Op  string
	Err error
}

func (e ErrStoreWrite) Error() string { return fmt.Sprintf("store %s: %v", e.Op, e.Err) }

func (e ErrStoreWrite) Unwrap() error { return e.Err }

func (ErrStoreWrite) Is(target error) bool { return target == ErrStoreWriteSentinel }

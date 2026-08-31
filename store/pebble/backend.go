package pebble

import (
	"io"

	"github.com/cockroachdb/pebble"
)

// StorageBackend is the minimal surface of *pebble.DB used by this package.
// It exists as a future extraction seam: once the Pebble dependency is large
// enough to warrant its own submodule, KeyValueStore and the checkpoint/restore
// helpers will be rewired to accept StorageBackend instead of *pebble.DB
// directly. For now the interface documents the used surface without changing
// any behaviour.
//
// Note: because NewBatch returns *pebble.Batch and NewIter returns
// *pebble.Iterator, this interface cannot decouple store/pebble from the
// pebble import yet — that is expected and acceptable for a seam-only change.
type StorageBackend interface {
	// Get retrieves the value for key. The returned closer must be called to
	// release the underlying buffer; the value slice is only valid until Close.
	Get(key []byte) (value []byte, closer io.Closer, err error)

	// Set writes key→value with the given write options.
	Set(key, value []byte, opts *pebble.WriteOptions) error

	// Delete removes key with the given write options.
	Delete(key []byte, opts *pebble.WriteOptions) error

	// NewIter returns an iterator bounded by opts.
	NewIter(opts *pebble.IterOptions) (*pebble.Iterator, error)

	// NewBatch returns a new write batch.
	NewBatch() *pebble.Batch
}

// Compile-time assertion: *pebble.DB satisfies StorageBackend.
var _ StorageBackend = (*pebble.DB)(nil)

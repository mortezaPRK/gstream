package pebble

import (
	"bytes"
	"fmt"
	"iter"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"

	gstream "github.com/mortezaPRK/gstream"
)

// ErrStoreWrite preserves implementation-package compatibility while the
// contract and sentinel live in root gstream module.
type ErrStoreWrite = gstream.ErrStoreWrite

var ErrStoreWriteSentinel = gstream.ErrStoreWriteSentinel

// separator is the byte used to separate the store name prefix from the encoded key.
// 0x00 is chosen because a serialized key should never be empty; we also ensure no
// serialized key can start with 0x00 by construction (the prefix ends with 0x00 and
// the next byte is always the encoded key payload, which is at least 1 byte).
const separator byte = 0x00

// KeyValueStore is a generic key-value store backed by Pebble.
//
// # Key-prefixing scheme
//
// Each store is created with a name string. Keys stored in Pebble are prefixed as:
//
//	<name-bytes> 0x00 <serialized-key-bytes>
//
// The 0x00 separator terminates the name so that a name "foo" does not share a prefix
// with a name "foobar". All Range/iteration is bounded to keys that start with this
// prefix, so two KeyValueStore instances sharing the same *pebble.DB cannot interfere
// with each other.
//
// # DB-ownership / Close semantics
//
// KeyValueStore does NOT own the *pebble.DB it receives. The caller is responsible for
// opening and closing the database. Calling KeyValueStore.Close() is a no-op from the
// Pebble DB perspective — it simply marks the store as closed and refuses further
// operations. This design lets many logical stores share a single Pebble DB (one DB per
// task/partition is the intended use-case), with the task owning the DB lifetime.
//
// For tests or standalone use, call OpenDB / OpenMemDB to get a caller-owned *pebble.DB,
// then pass it to NewKeyValueStore; defer db.Close() after all stores using it are done.
//
// # Write durability
//
// Writes use pebble.Sync (fsync after every write). This is the correct default for a
// state store that must be durable for local crash recovery. Callers that want higher
// throughput at the cost of durability can pass custom WriteOptions, but the public
// API intentionally does not expose that knob yet — correctness first.
//
// # Changelog capture
//
// When a *MutationCollector is attached (via NewKeyValueStoreWithChangelog), every
// Put and Delete appends a Mutation to the collector after the Pebble write succeeds.
// Mutation keys/values are the same pre-encoded bytes written to Pebble, so changelog
// consumers can restore store state exactly. A nil collector disables capture and
// leaves existing behaviour unchanged.
type KeyValueStore[K, V any] struct {
	db        *pebble.DB
	prefix    []byte // name + separator
	kSerde    gstream.Serde[K]
	vSerde    gstream.Serde[V]
	collector *MutationCollector // nil = no changelog capture
	closed    bool
}

// OpenDB opens a Pebble database at dir with default options.
// The caller owns the returned *pebble.DB and must call db.Close().
func OpenDB(dir string) (*pebble.DB, error) {
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("state: open pebble at %q: %w", dir, err)
	}
	return db, nil
}

// OpenMemDB opens an in-memory Pebble database (no disk I/O). Intended for tests.
// The caller owns the returned *pebble.DB and must call db.Close().
func OpenMemDB() (*pebble.DB, error) {
	db, err := pebble.Open("", &pebble.Options{FS: vfs.NewMem()})
	if err != nil {
		return nil, fmt.Errorf("state: open in-memory pebble: %w", err)
	}
	return db, nil
}

// NewKeyValueStore creates a new KeyValueStore for the given store name.
//
//   - name: logical store identifier; used as a key prefix. Must not contain 0x00 bytes
//     (names are plain ASCII/UTF-8 identifiers in practice).
//   - db: an already-open Pebble database. The store does NOT close it.
//   - kSerde: serializer/deserializer for keys of type K.
//   - vSerde: serializer/deserializer for values of type V.
func NewKeyValueStore[K, V any](
	name string,
	db *pebble.DB,
	kSerde gstream.Serde[K],
	vSerde gstream.Serde[V],
) *KeyValueStore[K, V] {
	return NewKeyValueStoreWithChangelog[K, V](name, db, kSerde, vSerde, nil)
}

// NewKeyValueStoreWithChangelog creates a KeyValueStore that additionally records
// every Put and Delete to collector, producing Mutation entries with the same
// pre-encoded key/value bytes that are written to Pebble. This enables changelog
// capture for stateful partition recovery.
//
// Passing collector == nil is identical to calling NewKeyValueStore — no mutations
// are captured and existing behaviour is fully preserved.
//
//   - name: logical store identifier; used as a key prefix.
//   - db: an already-open Pebble database. The store does NOT close it.
//   - kSerde: serializer/deserializer for keys of type K.
//   - vSerde: serializer/deserializer for values of type V.
//   - collector: mutation sink; nil disables changelog capture.
func NewKeyValueStoreWithChangelog[K, V any](
	name string,
	db *pebble.DB,
	kSerde gstream.Serde[K],
	vSerde gstream.Serde[V],
	collector *MutationCollector,
) *KeyValueStore[K, V] {
	prefix := append([]byte(name), separator)
	return &KeyValueStore[K, V]{
		db:        db,
		prefix:    prefix,
		kSerde:    kSerde,
		vSerde:    vSerde,
		collector: collector,
	}
}

// Close marks the store as closed. It does NOT close the underlying Pebble DB
// (ownership stays with the caller). After Close, all operations return an error.
func (s *KeyValueStore[K, V]) Close() error {
	s.closed = true
	return nil
}

// DrainMutations returns collected changelog mutations in root contract form.
func (s *KeyValueStore[K, V]) DrainMutations() []gstream.StoreMutation {
	if s.collector == nil {
		return nil
	}
	mutations := s.collector.Drain()
	result := make([]gstream.StoreMutation, 0, len(mutations))
	for _, mutation := range mutations {
		switch value := mutation.(type) {
		case Put:
			result = append(result, gstream.StoreMutation{Key: value.Key, Value: value.Value})
		case Delete:
			result = append(result, gstream.StoreMutation{Key: value.Key})
		}
	}
	return result
}

// Get retrieves the value for key k.
// Returns (value, true, nil) if found, (zero, false, nil) if not found,
// or (zero, false, err) if an error occurs.
func (s *KeyValueStore[K, V]) Get(k K) (V, bool, error) {
	var zero V
	if err := s.checkOpen(); err != nil {
		return zero, false, err
	}

	pk, err := s.encodeKey(k)
	if err != nil {
		return zero, false, fmt.Errorf("state: Get encode key: %w", err)
	}

	valBytes, closer, err := s.db.Get(pk)
	if err != nil {
		if err == pebble.ErrNotFound {
			return zero, false, nil
		}
		return zero, false, fmt.Errorf("state: Get pebble: %w", err)
	}
	defer func() { _ = closer.Close() }()

	v, err := s.vSerde.Deserialize(valBytes)
	if err != nil {
		return zero, false, fmt.Errorf("state: Get deserialize value: %w", err)
	}
	return v, true, nil
}

// Put stores the mapping k -> v, overwriting any existing value for k.
// If a MutationCollector is attached, a Put{Key, Value} mutation
// is appended after the Pebble write succeeds.
func (s *KeyValueStore[K, V]) Put(k K, v V) error {
	if err := s.checkOpen(); err != nil {
		return err
	}

	pk, err := s.encodeKey(k)
	if err != nil {
		return fmt.Errorf("state: Put encode key: %w", err)
	}

	pv, err := s.vSerde.Serialize(v)
	if err != nil {
		return fmt.Errorf("state: Put serialize value: %w", err)
	}

	if err := s.db.Set(pk, pv, pebble.Sync); err != nil {
		return ErrStoreWrite{Op: "Put", Err: err}
	}

	if s.collector != nil {
		keyCopy := make([]byte, len(pk))
		copy(keyCopy, pk)
		valCopy := make([]byte, len(pv))
		copy(valCopy, pv)
		s.collector.Append(Put{Key: keyCopy, Value: valCopy})
	}
	return nil
}

// Delete removes the entry for key k. It is not an error to delete a key that
// does not exist. If a MutationCollector is attached, a Delete{Key} mutation
// is appended after the Pebble write succeeds.
func (s *KeyValueStore[K, V]) Delete(k K) error {
	if err := s.checkOpen(); err != nil {
		return err
	}

	pk, err := s.encodeKey(k)
	if err != nil {
		return fmt.Errorf("state: Delete encode key: %w", err)
	}

	if err := s.db.Delete(pk, pebble.Sync); err != nil {
		return ErrStoreWrite{Op: "Delete", Err: err}
	}

	if s.collector != nil {
		keyCopy := make([]byte, len(pk))
		copy(keyCopy, pk)
		s.collector.Append(Delete{Key: keyCopy})
	}
	return nil
}

// Range iterates over all key-value pairs in this store in key order (ascending
// byte order of the serialized key). It calls fn for each pair; if fn returns
// false the iteration stops early. Range does not cross into another store's
// key prefix.
func (s *KeyValueStore[K, V]) Range(fn func(K, V) bool) error {
	if err := s.checkOpen(); err != nil {
		return err
	}

	// Upper bound: increment the last byte of prefix to form an exclusive upper key.
	// This works because all keys in this store are >= prefix and we want strictly
	// less than the next prefix value.
	upperBound := prefixUpperBound(s.prefix)

	iterOpts := &pebble.IterOptions{
		LowerBound: s.prefix,
		UpperBound: upperBound,
	}

	iter, err := s.db.NewIter(iterOpts)
	if err != nil {
		return fmt.Errorf("state: Range new iter: %w", err)
	}
	defer func() {
		_ = iter.Close()
	}()

	for valid := iter.First(); valid; valid = iter.Next() {
		rawKey := iter.Key()

		// Strip prefix (name + separator)
		if !bytes.HasPrefix(rawKey, s.prefix) {
			// Should not happen given iterator bounds, but be defensive.
			break
		}
		encodedKey := rawKey[len(s.prefix):]

		k, err := s.kSerde.Deserialize(encodedKey)
		if err != nil {
			return fmt.Errorf("state: Range deserialize key: %w", err)
		}

		v, err := s.vSerde.Deserialize(iter.Value())
		if err != nil {
			return fmt.Errorf("state: Range deserialize value: %w", err)
		}

		if !fn(k, v) {
			break
		}
	}

	if err := iter.Error(); err != nil {
		return fmt.Errorf("state: Range iter error: %w", err)
	}
	return nil
}

// RangeBytes iterates keys in [lower, upper) within this store's prefix, WITHOUT
// K/V serde — raw bytes. lower and upper are per-store key portions; the store
// prefix is added internally. fn receives copies of key and value bytes (safe to
// retain after fn returns). Return false from fn to stop early.
// Mirrors Range's iterator setup and byte-copy semantics.
func (s *KeyValueStore[K, V]) RangeBytes(lower, upper []byte, fn func(key, val []byte) bool) error {
	if err := s.checkOpen(); err != nil {
		return err
	}

	lowerBound := make([]byte, len(s.prefix)+len(lower))
	copy(lowerBound, s.prefix)
	copy(lowerBound[len(s.prefix):], lower)

	upperBound := make([]byte, len(s.prefix)+len(upper))
	copy(upperBound, s.prefix)
	copy(upperBound[len(s.prefix):], upper)

	iterOpts := &pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	}

	iter, err := s.db.NewIter(iterOpts)
	if err != nil {
		return fmt.Errorf("state: RangeBytes new iter: %w", err)
	}
	defer func() {
		_ = iter.Close()
	}()

	for valid := iter.First(); valid; valid = iter.Next() {
		rawKey := iter.Key()
		rawVal := iter.Value()

		// Copy both before handing to fn — Pebble reuses its buffers.
		keyCopy := make([]byte, len(rawKey))
		copy(keyCopy, rawKey)
		valCopy := make([]byte, len(rawVal))
		copy(valCopy, rawVal)

		if !fn(keyCopy, valCopy) {
			break
		}
	}

	if err := iter.Error(); err != nil {
		return fmt.Errorf("state: RangeBytes iter error: %w", err)
	}
	return nil
}

// DeleteRangeBytes deletes all keys in [lower, upper) within the store prefix.
// lower and upper are per-store key portions; the store prefix is added internally.
//
// It uses iterate-then-delete (NOT pebble.DeleteRange) so that the MutationCollector
// receives one tombstone per deleted key — pebble.DeleteRange emits no per-key events
// and would diverge changelog restore.
//
// The Mutation.Key appended to the collector is the full Pebble key
// (prefix + per-store key), matching exactly what Put and Delete append, so
// changelog and restore remain consistent.
func (s *KeyValueStore[K, V]) DeleteRangeBytes(lower, upper []byte) error {
	if err := s.checkOpen(); err != nil {
		return err
	}

	// Collect keys to delete first; do not mutate while iterating.
	var keys [][]byte
	if err := s.RangeBytes(lower, upper, func(key, _ []byte) bool {
		keys = append(keys, key) // key is already a copy from RangeBytes
		return true
	}); err != nil {
		return fmt.Errorf("state: DeleteRangeBytes scan: %w", err)
	}

	for _, fullKey := range keys {
		if err := s.db.Delete(fullKey, pebble.Sync); err != nil {
			return ErrStoreWrite{Op: "DeleteRangeBytes", Err: err}
		}
		if s.collector != nil {
			// Delete.Key must be the full Pebble key to match what Put/Delete append.
			keyCopy := make([]byte, len(fullKey))
			copy(keyCopy, fullKey)
			s.collector.Append(Delete{Key: keyCopy})
		}
	}
	return nil
}

// WindowGet retrieves the raw value bytes for the composite (kBytes, windowStart)
// key. The composite key is built with WindowCompositeKey so the format is
// owned in exactly one place; this avoids duplicating the encoding in callers.
//
// WindowGet and WindowPut bypass the K/V serdes — they interact with Pebble
// directly using the store prefix, mirroring the raw-bytes pattern of
// RangeBytes and DeleteRangeBytes. They are designed for use with
// KeyValueStore[[]byte,[]byte] where the windowed DSL handles value
// serialisation outside the store.
//
// Returns (nil, false, nil) when the key is not found.
func (s *KeyValueStore[K, V]) WindowGet(kBytes []byte, windowStart int64) ([]byte, bool, error) {
	if err := s.checkOpen(); err != nil {
		return nil, false, err
	}
	ck := WindowCompositeKey(kBytes, windowStart)
	pk := make([]byte, len(s.prefix)+len(ck))
	copy(pk, s.prefix)
	copy(pk[len(s.prefix):], ck)

	valBytes, closer, err := s.db.Get(pk)
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("state: WindowGet pebble: %w", err)
	}
	defer func() { _ = closer.Close() }()

	// Copy bytes out of Pebble's buffer before the closer is called.
	result := make([]byte, len(valBytes))
	copy(result, valBytes)
	return result, true, nil
}

// WindowPut stores val under the composite (kBytes, windowStart) key.
// The composite key is built with WindowCompositeKey; the store prefix is
// prepended before writing to Pebble. If a MutationCollector is attached, a
// Mutation is appended after the Pebble write succeeds (matching Put semantics
// for changelog capture).
func (s *KeyValueStore[K, V]) WindowPut(kBytes []byte, windowStart int64, val []byte) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	ck := WindowCompositeKey(kBytes, windowStart)
	pk := make([]byte, len(s.prefix)+len(ck))
	copy(pk, s.prefix)
	copy(pk[len(s.prefix):], ck)

	if err := s.db.Set(pk, val, pebble.Sync); err != nil {
		return ErrStoreWrite{Op: "WindowPut", Err: err}
	}

	if s.collector != nil {
		keyCopy := make([]byte, len(pk))
		copy(keyCopy, pk)
		valCopy := make([]byte, len(val))
		copy(valCopy, val)
		s.collector.Append(Put{Key: keyCopy, Value: valCopy})
	}
	return nil
}

// RangeCompositeBytes iterates keys in [lower, upper) within this store's prefix,
// WITHOUT K/V serde — raw bytes. lower and upper are per-store composite key
// portions; the store prefix is added internally for iteration bounds.
// fn receives copies of the per-store composite key (store prefix stripped) and
// value bytes (safe to retain after fn returns). The composite key is decodable by
// DecodeWindowCompositeKey. Return false from fn to stop early.
// Mirrors RangeBytes's iterator setup and byte-copy semantics.
func (s *KeyValueStore[K, V]) RangeCompositeBytes(lower, upper []byte, fn func(compositeKey, val []byte) bool) error {
	if err := s.checkOpen(); err != nil {
		return err
	}

	lowerBound := make([]byte, len(s.prefix)+len(lower))
	copy(lowerBound, s.prefix)
	copy(lowerBound[len(s.prefix):], lower)

	upperBound := make([]byte, len(s.prefix)+len(upper))
	copy(upperBound, s.prefix)
	copy(upperBound[len(s.prefix):], upper)

	iterOpts := &pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	}

	iter, err := s.db.NewIter(iterOpts)
	if err != nil {
		return fmt.Errorf("state: RangeCompositeBytes new iter: %w", err)
	}
	defer func() {
		_ = iter.Close()
	}()

	for valid := iter.First(); valid; valid = iter.Next() {
		rawKey := iter.Key()
		rawVal := iter.Value()

		// Strip the store prefix; fn receives the per-store composite key only.
		compositeKey := rawKey[len(s.prefix):]

		// Copy both before handing to fn — Pebble reuses its buffers.
		keyCopy := make([]byte, len(compositeKey))
		copy(keyCopy, compositeKey)
		valCopy := make([]byte, len(rawVal))
		copy(valCopy, rawVal)

		if !fn(keyCopy, valCopy) {
			break
		}
	}

	if err := iter.Error(); err != nil {
		return fmt.Errorf("state: RangeCompositeBytes iter error: %w", err)
	}
	return nil
}

// WindowDelete removes the entry for the composite (kBytes, windowStart) key.
// It is not an error to delete a key that does not exist. If a MutationCollector
// is attached, a Delete{Key} mutation is appended after the Pebble write succeeds.
// The Mutation.Key is the full Pebble key (prefix + composite), matching the key
// form used by WindowPut's Put mutation and Delete's Delete mutation.
func (s *KeyValueStore[K, V]) WindowDelete(kBytes []byte, windowStart int64) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	ck := WindowCompositeKey(kBytes, windowStart)
	pk := make([]byte, len(s.prefix)+len(ck))
	copy(pk, s.prefix)
	copy(pk[len(s.prefix):], ck)

	if err := s.db.Delete(pk, pebble.Sync); err != nil {
		return ErrStoreWrite{Op: "WindowDelete", Err: err}
	}

	if s.collector != nil {
		keyCopy := make([]byte, len(pk))
		copy(keyCopy, pk)
		s.collector.Append(Delete{Key: keyCopy})
	}
	return nil
}

// RangeForKey iterates all windowed / session entries stored under kBytes, calling
// fn with each entry's sessionStart (decoded from the composite key) and raw value
// bytes (safe to retain after fn returns). Return false from fn to stop early.
//
// The composite-key format (WindowCompositeKey) is owned by stores/pebble; this
// method decodes it so callers outside the package do not need to import stores/pebble
// just to enumerate sessions. (gstream cannot import stores/pebble: stores/pebble
// imports gstream via Serde[T], which would create a cycle.)
//
// [T1-amendment] Added additively for the session-windows DSL.
func (s *KeyValueStore[K, V]) RangeForKey(kBytes []byte, fn func(sessionStart int64, val []byte) bool) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	lower := WindowKeyLowerBound(kBytes)
	upper := WindowKeyUpperBound(kBytes)

	var decodeErr error
	err := s.RangeCompositeBytes(lower, upper, func(compositeKey, val []byte) bool {
		_, sessionStart, dErr := DecodeWindowCompositeKey(compositeKey)
		if dErr != nil {
			decodeErr = dErr
			return false // stop on malformed key
		}
		return fn(sessionStart, val)
	})
	if err != nil {
		return err
	}
	return decodeErr
}

// Iter returns an iter.Seq2 iterator over all key-value pairs in ascending order.
// It wraps Range; early-termination from yield propagates to Range's fn.
func (s *KeyValueStore[K, V]) Iter() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		_ = s.Range(func(k K, v V) bool { return yield(k, v) })
	}
}

// IterBytes returns an iter.Seq2 iterator over raw key/value bytes in [lower, upper).
// lower and upper are per-store key portions; the store prefix is added internally.
// It wraps RangeBytes; early-termination from yield propagates to RangeBytes's fn.
func (s *KeyValueStore[K, V]) IterBytes(lower, upper []byte) iter.Seq2[[]byte, []byte] {
	return func(yield func([]byte, []byte) bool) {
		_ = s.RangeBytes(lower, upper, func(k, v []byte) bool { return yield(k, v) })
	}
}

// encodeKey serializes k and prepends the store prefix.
func (s *KeyValueStore[K, V]) encodeKey(k K) ([]byte, error) {
	kb, err := s.kSerde.Serialize(k)
	if err != nil {
		return nil, err
	}
	pk := make([]byte, len(s.prefix)+len(kb))
	copy(pk, s.prefix)
	copy(pk[len(s.prefix):], kb)
	return pk, nil
}

// checkOpen returns an error if the store has been closed.
func (s *KeyValueStore[K, V]) checkOpen() error {
	if s.closed {
		return fmt.Errorf("state: store is closed")
	}
	return nil
}

// prefixUpperBound returns the smallest key strictly greater than all keys with
// the given prefix. It increments the last non-0xff byte; if all bytes are 0xff
// (extremely unlikely for named prefixes) it returns nil, which Pebble treats as
// no upper bound — safe because the prefix still filters via LowerBound.
func prefixUpperBound(prefix []byte) []byte {
	ub := make([]byte, len(prefix))
	copy(ub, prefix)
	for i := len(ub) - 1; i >= 0; i-- {
		ub[i]++
		if ub[i] != 0x00 { // no overflow
			return ub[:i+1]
		}
	}
	return nil // all bytes were 0xff; no upper bound needed
}

// Compile-time assertion: KeyValueStore satisfies io.Closer.
// This keeps Close's signature honest without importing io at call sites.
type _ interface{ Close() error }

var _ interface{ Close() error } = (*KeyValueStore[int, int])(nil)
var _ gstream.Store = (*KeyValueStore[[]byte, []byte])(nil)

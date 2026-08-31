// Package memory provides broker-free, in-memory state stores.
package memory

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	gstream "github.com/mortezaPRK/gstream"
	"github.com/mortezaPRK/gstream/internal/winkey"
)

// DB owns shared in-memory data. Multiple named stores may share one DB.
type DB struct {
	mu     sync.RWMutex
	data   map[string][]byte
	closed bool
}

// OpenMemDB creates an empty in-memory database.
func OpenMemDB() (*DB, error) {
	return &DB{data: make(map[string][]byte)}, nil
}

// Close releases DB and rejects later operations.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.closed = true
	clear(db.data)
	return nil
}

// KeyValueStore is a named in-memory key-value store.
type KeyValueStore[K, V any] struct {
	db     *DB
	prefix []byte
	kSerde gstream.Serde[K]
	vSerde gstream.Serde[V]
	closed atomic.Bool
}

// NewKeyValueStore creates a named store in db.
func NewKeyValueStore[K, V any](
	name string,
	db *DB,
	kSerde gstream.Serde[K],
	vSerde gstream.Serde[V],
) *KeyValueStore[K, V] {
	return &KeyValueStore[K, V]{
		db: db, prefix: append([]byte(name), 0), kSerde: kSerde, vSerde: vSerde,
	}
}

// Close closes this logical store without closing shared DB.
func (s *KeyValueStore[K, V]) Close() error {
	s.closed.Store(true)
	return nil
}

// Get returns value stored for key.
func (s *KeyValueStore[K, V]) Get(key K) (V, bool, error) {
	var zero V
	encoded, err := s.encodeKey(key)
	if err != nil {
		return zero, false, err
	}
	value, found, err := s.get(encoded)
	if err != nil || !found {
		return zero, found, err
	}
	decoded, err := s.vSerde.Deserialize(value)
	if err != nil {
		return zero, false, fmt.Errorf("memory: deserialize value: %w", err)
	}
	return decoded, true, nil
}

// Put stores value for key.
func (s *KeyValueStore[K, V]) Put(key K, value V) error {
	encodedKey, err := s.encodeKey(key)
	if err != nil {
		return err
	}
	encodedValue, err := s.vSerde.Serialize(value)
	if err != nil {
		return fmt.Errorf("memory: serialize value: %w", err)
	}
	return s.put(encodedKey, encodedValue)
}

// Delete removes key.
func (s *KeyValueStore[K, V]) Delete(key K) error {
	encoded, err := s.encodeKey(key)
	if err != nil {
		return err
	}
	return s.delete(encoded)
}

// WindowGet reads raw value stored under key and window start.
func (s *KeyValueStore[K, V]) WindowGet(key []byte, windowStart int64) ([]byte, bool, error) {
	return s.get(s.prefixed(winkey.CompositeKey(key, windowStart)))
}

// WindowPut stores raw value under key and window start.
func (s *KeyValueStore[K, V]) WindowPut(key []byte, windowStart int64, value []byte) error {
	return s.put(s.prefixed(winkey.CompositeKey(key, windowStart)), value)
}

// WindowDelete removes value stored under key and window start.
func (s *KeyValueStore[K, V]) WindowDelete(key []byte, windowStart int64) error {
	return s.delete(s.prefixed(winkey.CompositeKey(key, windowStart)))
}

// RangeCompositeBytes visits raw composite keys in byte order.
func (s *KeyValueStore[K, V]) RangeCompositeBytes(
	lower, upper []byte,
	visit func(compositeKey, value []byte) bool,
) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	lower = s.prefixed(lower)
	upper = s.prefixed(upper)
	s.db.mu.RLock()
	if s.db.closed {
		s.db.mu.RUnlock()
		return fmt.Errorf("memory: database closed")
	}
	keys := make([][]byte, 0)
	for raw := range s.db.data {
		key := []byte(raw)
		if bytes.Compare(key, lower) >= 0 && bytes.Compare(key, upper) < 0 {
			keys = append(keys, append([]byte(nil), key...))
		}
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i], keys[j]) < 0 })
	entries := make([][]byte, len(keys))
	for index, key := range keys {
		entries[index] = append([]byte(nil), s.db.data[string(key)]...)
	}
	s.db.mu.RUnlock()
	for index, key := range keys {
		if !visit(key[len(s.prefix):], entries[index]) {
			break
		}
	}
	return nil
}

// RangeForKey visits every window or session entry for key.
func (s *KeyValueStore[K, V]) RangeForKey(
	key []byte,
	visit func(sessionStart int64, value []byte) bool,
) error {
	lower := winkey.CompositeKey(key, 0)
	prefix := make([]byte, 4+len(key))
	binary.BigEndian.PutUint32(prefix, uint32(len(key)))
	copy(prefix[4:], key)
	upper := prefixUpperBound(prefix)
	var decodeErr error
	err := s.RangeCompositeBytes(lower, upper, func(compositeKey, value []byte) bool {
		_, start, err := decodeCompositeKey(compositeKey)
		if err != nil {
			decodeErr = err
			return false
		}
		return visit(start, value)
	})
	if err != nil {
		return err
	}
	return decodeErr
}

func (s *KeyValueStore[K, V]) encodeKey(key K) ([]byte, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	encoded, err := s.kSerde.Serialize(key)
	if err != nil {
		return nil, fmt.Errorf("memory: serialize key: %w", err)
	}
	return s.prefixed(encoded), nil
}

func (s *KeyValueStore[K, V]) prefixed(key []byte) []byte {
	result := make([]byte, len(s.prefix)+len(key))
	copy(result, s.prefix)
	copy(result[len(s.prefix):], key)
	return result
}

func (s *KeyValueStore[K, V]) get(key []byte) ([]byte, bool, error) {
	if err := s.checkOpen(); err != nil {
		return nil, false, err
	}
	s.db.mu.RLock()
	defer s.db.mu.RUnlock()
	if s.db.closed {
		return nil, false, fmt.Errorf("memory: database closed")
	}
	value, found := s.db.data[string(key)]
	return append([]byte(nil), value...), found, nil
}

func (s *KeyValueStore[K, V]) put(key, value []byte) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	s.db.mu.Lock()
	defer s.db.mu.Unlock()
	if s.db.closed {
		return fmt.Errorf("memory: database closed")
	}
	s.db.data[string(key)] = append([]byte(nil), value...)
	return nil
}

func (s *KeyValueStore[K, V]) delete(key []byte) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	s.db.mu.Lock()
	defer s.db.mu.Unlock()
	if s.db.closed {
		return fmt.Errorf("memory: database closed")
	}
	delete(s.db.data, string(key))
	return nil
}

func (s *KeyValueStore[K, V]) checkOpen() error {
	if s == nil || s.db == nil {
		return fmt.Errorf("memory: nil store")
	}
	if s.closed.Load() {
		return fmt.Errorf("memory: store closed")
	}
	s.db.mu.RLock()
	defer s.db.mu.RUnlock()
	if s.db.closed {
		return fmt.Errorf("memory: database closed")
	}
	return nil
}

func prefixUpperBound(prefix []byte) []byte {
	upper := append([]byte(nil), prefix...)
	for index := len(upper) - 1; index >= 0; index-- {
		if upper[index] != 0xff {
			upper[index]++
			return upper[:index+1]
		}
	}
	return nil
}

func decodeCompositeKey(raw []byte) ([]byte, int64, error) {
	if len(raw) < 12 {
		return nil, 0, fmt.Errorf("memory: composite key too short")
	}
	keyLength := int(binary.BigEndian.Uint32(raw[:4]))
	if len(raw) != 4+keyLength+8 {
		return nil, 0, fmt.Errorf("memory: malformed composite key")
	}
	key := append([]byte(nil), raw[4:4+keyLength]...)
	start := int64(binary.BigEndian.Uint64(raw[4+keyLength:]))
	return key, start, nil
}

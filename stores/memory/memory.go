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
	mu            sync.RWMutex
	data          map[string][]byte
	checkpoints   map[string]int64
	streamTime    int64
	hasStreamTime bool
	closed        bool
}

// OpenMemDB creates an empty in-memory database.
func OpenMemDB() (*DB, error) {
	return &DB{data: make(map[string][]byte), checkpoints: make(map[string]int64)}, nil
}

// Provider opens in-memory backends. Path is intentionally ignored.
type Provider struct{}

// NewProvider creates in-memory StoreProvider.
func NewProvider() Provider { return Provider{} }

// Open creates empty in-memory backend.
func (Provider) Open(_ string) (gstream.StoreBackend, error) { return OpenMemDB() }

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
	db               *DB
	prefix           []byte
	kSerde           gstream.Serde[K]
	vSerde           gstream.Serde[V]
	collectMutations bool
	mutationsMu      sync.Mutex
	mutations        []gstream.StoreMutation
	closed           atomic.Bool
}

type bytesSerde struct{}

func (bytesSerde) Serialize(value []byte) ([]byte, error) { return value, nil }

func (bytesSerde) Deserialize(value []byte) ([]byte, error) { return value, nil }

// OpenStore opens raw byte store within backend.
func (db *DB) OpenStore(name string, collectMutations bool) (gstream.Store, error) {
	if name == "" {
		return nil, fmt.Errorf("memory: store name must not be empty")
	}
	store := NewKeyValueStore[[]byte, []byte](name, db, bytesSerde{}, bytesSerde{})
	store.collectMutations = collectMutations
	return store, nil
}

// Restore atomically applies opaque changelog mutations and checkpoint.
func (db *DB) Restore(storeName string, mutations []gstream.StoreMutation, checkpoint int64) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return fmt.Errorf("memory: database closed")
	}
	for _, mutation := range mutations {
		if mutation.Value == nil {
			delete(db.data, string(mutation.Key))
		} else {
			db.data[string(mutation.Key)] = append([]byte(nil), mutation.Value...)
		}
	}
	if checkpoint >= 0 {
		db.checkpoints[storeName] = checkpoint
	}
	return nil
}

func (db *DB) ReadCheckpoint(storeName string) (int64, bool, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return 0, false, fmt.Errorf("memory: database closed")
	}
	offset, found := db.checkpoints[storeName]
	return offset, found, nil
}

func (db *DB) WriteCheckpoint(storeName string, offset int64) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return fmt.Errorf("memory: database closed")
	}
	db.checkpoints[storeName] = offset
	return nil
}

func (db *DB) ReadStreamTime() (int64, bool, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return 0, false, fmt.Errorf("memory: database closed")
	}
	return db.streamTime, db.hasStreamTime, nil
}

func (db *DB) WriteStreamTime(timestamp int64) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return fmt.Errorf("memory: database closed")
	}
	db.streamTime = timestamp
	db.hasStreamTime = true
	return nil
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

// Range visits typed entries in encoded-key order.
func (s *KeyValueStore[K, V]) Range(visit func(K, V) bool) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	upper := prefixUpperBound(s.prefix)
	s.db.mu.RLock()
	keys := make([][]byte, 0)
	for raw := range s.db.data {
		key := []byte(raw)
		if bytes.Compare(key, s.prefix) >= 0 && (upper == nil || bytes.Compare(key, upper) < 0) {
			keys = append(keys, append([]byte(nil), key...))
		}
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i], keys[j]) < 0 })
	values := make([][]byte, len(keys))
	for index, key := range keys {
		values[index] = append([]byte(nil), s.db.data[string(key)]...)
	}
	s.db.mu.RUnlock()
	for index, key := range keys {
		decodedKey, err := s.kSerde.Deserialize(key[len(s.prefix):])
		if err != nil {
			return fmt.Errorf("memory: deserialize key: %w", err)
		}
		decodedValue, err := s.vSerde.Deserialize(values[index])
		if err != nil {
			return fmt.Errorf("memory: deserialize value: %w", err)
		}
		if !visit(decodedKey, decodedValue) {
			break
		}
	}
	return nil
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

// DrainMutations returns and clears collected mutations.
func (s *KeyValueStore[K, V]) DrainMutations() []gstream.StoreMutation {
	s.mutationsMu.Lock()
	defer s.mutationsMu.Unlock()
	mutations := s.mutations
	s.mutations = nil
	return mutations
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
	if s.db.closed {
		s.db.mu.Unlock()
		return fmt.Errorf("memory: database closed")
	}
	s.db.data[string(key)] = append([]byte(nil), value...)
	s.db.mu.Unlock()
	s.recordMutation(key, value)
	return nil
}

func (s *KeyValueStore[K, V]) delete(key []byte) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	s.db.mu.Lock()
	if s.db.closed {
		s.db.mu.Unlock()
		return fmt.Errorf("memory: database closed")
	}
	delete(s.db.data, string(key))
	s.db.mu.Unlock()
	s.recordMutation(key, nil)
	return nil
}

func (s *KeyValueStore[K, V]) recordMutation(key, value []byte) {
	if !s.collectMutations {
		return
	}
	s.mutationsMu.Lock()
	defer s.mutationsMu.Unlock()
	s.mutations = append(s.mutations, gstream.StoreMutation{
		Key: append([]byte(nil), key...), Value: append([]byte(nil), value...),
	})
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

var _ gstream.StoreProvider = Provider{}
var _ gstream.StoreBackend = (*DB)(nil)
var _ gstream.Store = (*KeyValueStore[[]byte, []byte])(nil)

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

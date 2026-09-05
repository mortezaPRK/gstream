package testutil

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"

	gstream "mortz.dev/go/gstream"
)

type MemoryProvider struct{}

func (MemoryProvider) Open(_ string) (gstream.StoreBackend, error) { return NewMemoryBackend(), nil }

type MemoryBackend struct {
	mu            sync.RWMutex
	data          map[string][]byte
	checkpoints   map[string]int64
	streamTime    int64
	hasStreamTime bool
	closed        bool
}

func NewMemoryBackend() *MemoryBackend {
	return &MemoryBackend{data: make(map[string][]byte), checkpoints: make(map[string]int64)}
}

func OpenMemDB() (*MemoryBackend, error) { return NewMemoryBackend(), nil }

func (backend *MemoryBackend) OpenStore(name string, collect bool) (gstream.Store, error) {
	if name == "" {
		return nil, fmt.Errorf("test store: empty name")
	}
	return &MemoryStore{backend: backend, prefix: append([]byte(name), 0), collect: collect}, nil
}

func (backend *MemoryBackend) Restore(storeName string, mutations []gstream.StoreMutation, checkpoint int64) error {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	for _, mutation := range mutations {
		if mutation.Value == nil {
			delete(backend.data, string(mutation.Key))
		} else {
			backend.data[string(mutation.Key)] = append([]byte(nil), mutation.Value...)
		}
	}
	if checkpoint >= 0 {
		backend.checkpoints[storeName] = checkpoint
	}
	return nil
}

func (backend *MemoryBackend) ReadCheckpoint(name string) (int64, bool, error) {
	backend.mu.RLock()
	defer backend.mu.RUnlock()
	offset, found := backend.checkpoints[name]
	return offset, found, nil
}

func (backend *MemoryBackend) WriteCheckpoint(name string, offset int64) error {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	backend.checkpoints[name] = offset
	return nil
}

func (backend *MemoryBackend) ReadStreamTime() (int64, bool, error) {
	backend.mu.RLock()
	defer backend.mu.RUnlock()
	return backend.streamTime, backend.hasStreamTime, nil
}

func (backend *MemoryBackend) WriteStreamTime(timestamp int64) error {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	backend.streamTime = timestamp
	backend.hasStreamTime = true
	return nil
}

func (backend *MemoryBackend) Close() error {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	backend.closed = true
	return nil
}

type MemoryStore struct {
	backend   *MemoryBackend
	prefix    []byte
	collect   bool
	mutations []gstream.StoreMutation
	collector *MutationCollector
	closed    bool
}

func NewMemoryStore(name string, collect bool) (*MemoryBackend, *MemoryStore) {
	backend := NewMemoryBackend()
	store, _ := backend.OpenStore(name, collect)
	return backend, store.(*MemoryStore)
}

func (store *MemoryStore) full(key []byte) []byte {
	result := make([]byte, len(store.prefix)+len(key))
	copy(result, store.prefix)
	copy(result[len(store.prefix):], key)
	return result
}

func (store *MemoryStore) Get(key []byte) ([]byte, bool, error) {
	store.backend.mu.RLock()
	defer store.backend.mu.RUnlock()
	value, found := store.backend.data[string(store.full(key))]
	return append([]byte(nil), value...), found, nil
}

func (store *MemoryStore) Put(key, value []byte) error { return store.putFull(store.full(key), value) }

func (store *MemoryStore) Delete(key []byte) error { return store.deleteFull(store.full(key)) }

func (store *MemoryStore) Range(visit func(key, value []byte) bool) error {
	return store.rangeFull(store.prefix, prefixUpperBound(store.prefix), func(key, value []byte) bool {
		return visit(key[len(store.prefix):], value)
	})
}

func (store *MemoryStore) WindowGet(key []byte, start int64) ([]byte, bool, error) {
	return store.getFull(store.full(compositeKey(key, start)))
}

func (store *MemoryStore) WindowPut(key []byte, start int64, value []byte) error {
	return store.putFull(store.full(compositeKey(key, start)), value)
}

func (store *MemoryStore) WindowDelete(key []byte, start int64) error {
	return store.deleteFull(store.full(compositeKey(key, start)))
}

func (store *MemoryStore) RangeCompositeBytes(lower, upper []byte, visit func([]byte, []byte) bool) error {
	lowerFull := store.full(lower)
	var upperFull []byte
	if upper == nil {
		upperFull = prefixUpperBound(store.prefix)
	} else {
		upperFull = store.full(upper)
	}
	return store.rangeFull(lowerFull, upperFull, func(key, value []byte) bool {
		return visit(key[len(store.prefix):], value)
	})
}

func (store *MemoryStore) RangeForKey(key []byte, visit func(int64, []byte) bool) error {
	prefix := make([]byte, 4+len(key))
	binary.BigEndian.PutUint32(prefix, uint32(len(key)))
	copy(prefix[4:], key)
	return store.RangeCompositeBytes(prefix, prefixUpperBound(prefix), func(raw, value []byte) bool {
		_, start, err := decodeCompositeKey(raw)
		return err == nil && visit(start, value)
	})
}

func (store *MemoryStore) DrainMutations() []gstream.StoreMutation {
	mutations := store.mutations
	store.mutations = nil
	return mutations
}

func (store *MemoryStore) Close() error { store.closed = true; return nil }

func (store *MemoryStore) getFull(key []byte) ([]byte, bool, error) {
	store.backend.mu.RLock()
	defer store.backend.mu.RUnlock()
	value, found := store.backend.data[string(key)]
	return append([]byte(nil), value...), found, nil
}

func (store *MemoryStore) putFull(key, value []byte) error {
	store.backend.mu.Lock()
	store.backend.data[string(key)] = append([]byte(nil), value...)
	store.backend.mu.Unlock()
	if store.collect {
		store.mutations = append(store.mutations, gstream.StoreMutation{Key: append([]byte(nil), key...), Value: append([]byte(nil), value...)})
	}
	if store.collector != nil {
		store.collector.Append(Put{Key: append([]byte(nil), key...), Value: append([]byte(nil), value...)})
	}
	return nil
}

func (store *MemoryStore) deleteFull(key []byte) error {
	store.backend.mu.Lock()
	delete(store.backend.data, string(key))
	store.backend.mu.Unlock()
	if store.collect {
		store.mutations = append(store.mutations, gstream.StoreMutation{Key: append([]byte(nil), key...)})
	}
	if store.collector != nil {
		store.collector.Append(Delete{Key: append([]byte(nil), key...)})
	}
	return nil
}

func (store *MemoryStore) rangeFull(lower, upper []byte, visit func([]byte, []byte) bool) error {
	store.backend.mu.RLock()
	keys := make([][]byte, 0)
	for raw := range store.backend.data {
		key := []byte(raw)
		if bytes.Compare(key, lower) >= 0 && (upper == nil || bytes.Compare(key, upper) < 0) {
			keys = append(keys, append([]byte(nil), key...))
		}
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i], keys[j]) < 0 })
	values := make([][]byte, len(keys))
	for index, key := range keys {
		values[index] = append([]byte(nil), store.backend.data[string(key)]...)
	}
	store.backend.mu.RUnlock()
	for index, key := range keys {
		if !visit(key, values[index]) {
			break
		}
	}
	return nil
}

func compositeKey(key []byte, start int64) []byte {
	result := make([]byte, 4+len(key)+8)
	binary.BigEndian.PutUint32(result, uint32(len(key)))
	copy(result[4:], key)
	binary.BigEndian.PutUint64(result[4+len(key):], uint64(start))
	return result
}

func decodeCompositeKey(raw []byte) ([]byte, int64, error) {
	if len(raw) < 12 {
		return nil, 0, fmt.Errorf("short composite key")
	}
	length := int(binary.BigEndian.Uint32(raw[:4]))
	if len(raw) != 4+length+8 {
		return nil, 0, fmt.Errorf("invalid composite key")
	}
	return raw[4 : 4+length], int64(binary.BigEndian.Uint64(raw[4+length:])), nil
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

var _ gstream.StoreProvider = MemoryProvider{}
var _ gstream.StoreBackend = (*MemoryBackend)(nil)
var _ gstream.Store = (*MemoryStore)(nil)

type KeyValueStore[K, V any] struct {
	raw   *MemoryStore
	key   gstream.Serde[K]
	value gstream.Serde[V]
}

func NewKeyValueStore[K, V any](name string, backend *MemoryBackend, key gstream.Serde[K], value gstream.Serde[V]) *KeyValueStore[K, V] {
	raw, _ := backend.OpenStore(name, false)
	return &KeyValueStore[K, V]{raw: raw.(*MemoryStore), key: key, value: value}
}

func NewKeyValueStoreWithChangelog[K, V any](name string, backend *MemoryBackend, key gstream.Serde[K], value gstream.Serde[V], collector *MutationCollector) *KeyValueStore[K, V] {
	store := NewKeyValueStore(name, backend, key, value)
	store.raw.collect = true
	store.raw.collector = collector
	return store
}

func (store *KeyValueStore[K, V]) Get(key K) (V, bool, error) {
	var zero V
	encoded, err := store.key.Serialize(key)
	if err != nil {
		return zero, false, err
	}
	value, found, err := store.raw.Get(encoded)
	if err != nil || !found {
		return zero, found, err
	}
	decoded, err := store.value.Deserialize(value)
	return decoded, true, err
}

func (store *KeyValueStore[K, V]) Put(key K, value V) error {
	encodedKey, err := store.key.Serialize(key)
	if err != nil {
		return err
	}
	encodedValue, err := store.value.Serialize(value)
	if err != nil {
		return err
	}
	return store.raw.Put(encodedKey, encodedValue)
}

func (store *KeyValueStore[K, V]) Delete(key K) error {
	encoded, err := store.key.Serialize(key)
	if err != nil {
		return err
	}
	return store.raw.Delete(encoded)
}

func (store *KeyValueStore[K, V]) WindowGet(key []byte, start int64) ([]byte, bool, error) {
	return store.raw.WindowGet(key, start)
}

func (store *KeyValueStore[K, V]) WindowPut(key []byte, start int64, value []byte) error {
	return store.raw.WindowPut(key, start, value)
}

func (store *KeyValueStore[K, V]) WindowDelete(key []byte, start int64) error {
	return store.raw.WindowDelete(key, start)
}

func (store *KeyValueStore[K, V]) RangeForKey(key []byte, visit func(int64, []byte) bool) error {
	return store.raw.RangeForKey(key, visit)
}

func (store *KeyValueStore[K, V]) RangeCompositeBytes(lower, upper []byte, visit func([]byte, []byte) bool) error {
	return store.raw.RangeCompositeBytes(lower, upper, visit)
}

func (store *KeyValueStore[K, V]) DrainMutations() []gstream.StoreMutation {
	return store.raw.DrainMutations()
}

func (store *KeyValueStore[K, V]) Range(visit func(K, V) bool) error {
	return store.raw.Range(func(key, value []byte) bool {
		decodedKey, keyErr := store.key.Deserialize(key)
		decodedValue, valueErr := store.value.Deserialize(value)
		return keyErr == nil && valueErr == nil && visit(decodedKey, decodedValue)
	})
}

func (store *KeyValueStore[K, V]) Close() error { return store.raw.Close() }

type Mutation interface{ isMutation() }

type Put struct{ Key, Value []byte }

func (Put) isMutation() {}

type Delete struct{ Key []byte }

func (Delete) isMutation() {}

type MutationCollector struct {
	mu        sync.Mutex
	mutations []Mutation
}

func (collector *MutationCollector) Append(mutation Mutation) {
	collector.mu.Lock()
	defer collector.mu.Unlock()
	collector.mutations = append(collector.mutations, mutation)
}

func (collector *MutationCollector) Drain() []Mutation {
	collector.mu.Lock()
	defer collector.mu.Unlock()
	mutations := collector.mutations
	collector.mutations = nil
	return mutations
}

func WriteStreamTime(backend *MemoryBackend, timestamp int64) error {
	return backend.WriteStreamTime(timestamp)
}

func ReadStreamTime(backend *MemoryBackend) (int64, bool, error) { return backend.ReadStreamTime() }

func DecodeWindowCompositeKey(raw []byte) ([]byte, int64, error) { return decodeCompositeKey(raw) }

type ErrStoreWrite = gstream.ErrStoreWrite

var ErrStoreWriteSentinel = gstream.ErrStoreWriteSentinel

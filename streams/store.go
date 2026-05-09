package streams

import "sync"

// StateStore is a key-value store keyed by record key. Stage 8 ships
// only an in-memory implementation; a future stage backs stores with a
// compacted internal topic so state survives restarts (the Kafka Streams
// trick — already on TODO.md).
//
// Implementations must be safe for concurrent use.
type StateStore interface {
	Get(key []byte) ([]byte, bool)
	Put(key, value []byte)
	Delete(key []byte)
	Range(fn func(key, value []byte) bool)
}

// MemoryStore is a map-backed StateStore.
type MemoryStore struct {
	mu      sync.RWMutex
	entries map[string][]byte
}

// NewMemoryStore returns an empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{entries: make(map[string][]byte)}
}

// Get returns the value for key, or (nil, false) if absent.
func (s *MemoryStore) Get(key []byte) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.entries[string(key)]
	if !ok {
		return nil, false
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out, true
}

// Put records value under key, copying the bytes.
func (s *MemoryStore) Put(key, value []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stored := make([]byte, len(value))
	copy(stored, value)
	s.entries[string(key)] = stored
}

// Delete removes key from the store. No-op if absent.
func (s *MemoryStore) Delete(key []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.entries, string(key))
}

// Range iterates every (key, value) pair, stopping when fn returns false.
// Iteration order is undefined.
func (s *MemoryStore) Range(fn func(key, value []byte) bool) {
	s.mu.RLock()
	snapshot := make([][2][]byte, 0, len(s.entries))
	for k, v := range s.entries {
		snapshot = append(snapshot, [2][]byte{[]byte(k), append([]byte(nil), v...)})
	}
	s.mu.RUnlock()
	for _, kv := range snapshot {
		if !fn(kv[0], kv[1]) {
			return
		}
	}
}

// StoreFactory builds a fresh StateStore for a given partition.
// PartitionedStore invokes its factory once per distinct partition
// observed, caching the returned substore for subsequent For()
// calls. The partition argument lets factories that depend on
// per-partition resources (e.g. a per-partition changelog topic)
// scope themselves correctly; factories that don't need it can
// simply ignore the parameter.
type StoreFactory func(partition int) StateStore

// NewMemoryStoreFactory returns a StoreFactory that builds a fresh
// MemoryStore on each invocation, ignoring the partition argument.
// Pair with NewPartitionedStore to give each partition its own
// in-memory state.
func NewMemoryStoreFactory() StoreFactory {
	return func(_ int) StateStore { return NewMemoryStore() }
}

// PartitionedStore scopes state to the partition currently being
// processed. Operators call For(partition) to access the substore that
// holds state for that partition's records; the wrapper lazily creates
// substores via the configured factory.
//
// External callers that don't know which partition holds a key (test
// inspection, debug tools) can call Get and Range on the wrapper
// itself: Get scans every substore, Range walks every (key, value)
// pair across every substore. Iteration order across partitions is
// undefined.
//
// Safe for concurrent use.
type PartitionedStore struct {
	factory StoreFactory

	mu    sync.RWMutex
	parts map[int]StateStore
}

// NewPartitionedStore constructs a PartitionedStore that uses factory to
// build a substore on first For(partition) for a given partition.
func NewPartitionedStore(factory StoreFactory) *PartitionedStore {
	if factory == nil {
		factory = NewMemoryStoreFactory()
	}
	return &PartitionedStore{
		factory: factory,
		parts:   make(map[int]StateStore),
	}
}

// For returns the substore for partition, lazily creating it via the
// factory the first time. Subsequent calls with the same partition
// return the same instance.
func (s *PartitionedStore) For(partition int) StateStore {
	s.mu.RLock()
	if sub, ok := s.parts[partition]; ok {
		s.mu.RUnlock()
		return sub
	}
	s.mu.RUnlock()

	s.mu.Lock()
	defer s.mu.Unlock()
	if sub, ok := s.parts[partition]; ok {
		return sub
	}
	sub := s.factory(partition)
	s.parts[partition] = sub
	return sub
}

// Get returns the value for key from the first substore that contains
// it, or (nil, false) if no substore has the key. Designed for
// inspection — pipelines that know the partition should call
// For(partition).Get instead.
func (s *PartitionedStore) Get(key []byte) ([]byte, bool) {
	for _, sub := range s.snapshot() {
		if v, ok := sub.Get(key); ok {
			return v, true
		}
	}
	return nil, false
}

// Range iterates every (key, value) pair across every substore,
// stopping when fn returns false. Order is undefined both within and
// across partitions.
func (s *PartitionedStore) Range(fn func(key, value []byte) bool) {
	for _, sub := range s.snapshot() {
		stop := false
		sub.Range(func(k, v []byte) bool {
			if !fn(k, v) {
				stop = true
				return false
			}
			return true
		})
		if stop {
			return
		}
	}
}

// Partitions returns the indices of every partition that has had a
// substore created via For(). Order is undefined. Use to iterate
// explicitly partition-by-partition (e.g. windowed-operator
// punctuators that must walk every partition's state on each tick).
func (s *PartitionedStore) Partitions() []int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]int, 0, len(s.parts))
	for p := range s.parts {
		out = append(out, p)
	}
	return out
}

// snapshot returns the current set of substores under a brief read
// lock so callers can iterate without holding the lock during the
// underlying StateStore calls (which would risk deadlock if a substore
// implementation itself takes locks).
func (s *PartitionedStore) snapshot() []StateStore {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]StateStore, 0, len(s.parts))
	for _, sub := range s.parts {
		out = append(out, sub)
	}
	return out
}

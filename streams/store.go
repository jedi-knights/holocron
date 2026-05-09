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

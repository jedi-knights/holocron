package groups

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// NoOffset is the sentinel returned by Lookup when a (group, topic,
// partition) tuple has never been committed.
const NoOffset = int64(-1)

// OffsetStore is the durability boundary for committed consumer-group
// offsets. Stage 4 ships JSONOffsetStore (a JSON file in the data
// directory) and a no-op MemoryOffsetStore for the in-memory broker.
// A later stage may swap in an internal-topic-backed store, the way Kafka
// uses `__consumer_offsets`.
type OffsetStore interface {
	Commit(group, topic string, partition int32, offset int64) error
	Lookup(group, topic string, partition int32) (int64, error)
	Close() error
}

// offsetKey is the disk representation of one commit entry. Group, topic,
// and partition together uniquely identify a slot.
type offsetKey struct {
	Group     string `json:"group"`
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
}

// MemoryOffsetStore holds commits in RAM. Used by NewMemory brokers and
// by tests.
type MemoryOffsetStore struct {
	mu      sync.RWMutex
	entries map[string]int64 // group/topic/partition → offset
}

// NewMemoryOffsetStore returns an empty in-memory store.
func NewMemoryOffsetStore() *MemoryOffsetStore {
	return &MemoryOffsetStore{entries: make(map[string]int64)}
}

// Commit records a (group, topic, partition) → offset entry.
func (m *MemoryOffsetStore) Commit(group, topic string, partition int32, offset int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries[joinKey(group, topic, partition)] = offset
	return nil
}

// Lookup returns the committed offset for a key, or NoOffset if unset.
func (m *MemoryOffsetStore) Lookup(group, topic string, partition int32) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if v, ok := m.entries[joinKey(group, topic, partition)]; ok {
		return v, nil
	}
	return NoOffset, nil
}

// Close is a no-op for the in-memory store.
func (m *MemoryOffsetStore) Close() error { return nil }

// JSONOffsetStore persists commits to a single JSON file. Every commit
// rewrites the file atomically (temp + rename). Acceptable for a learning
// project; production would use the Kafka internal-topic trick.
type JSONOffsetStore struct {
	path string

	mu      sync.RWMutex
	entries map[string]int64
}

// OpenJSONOffsetStore loads (or creates) the JSON offset file at path.
func OpenJSONOffsetStore(path string) (*JSONOffsetStore, error) {
	s := &JSONOffsetStore{path: path, entries: make(map[string]int64)}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *JSONOffsetStore) load() error {
	b, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("groups: read %s: %w", s.path, err)
	}
	if len(b) == 0 {
		return nil
	}
	var entries []offsetKey
	if err := json.Unmarshal(b, &entries); err != nil {
		return fmt.Errorf("groups: parse %s: %w", s.path, err)
	}
	for _, e := range entries {
		s.entries[joinKey(e.Group, e.Topic, e.Partition)] = e.Offset
	}
	return nil
}

// Commit records a (group, topic, partition) → offset entry and rewrites
// the JSON file.
func (s *JSONOffsetStore) Commit(group, topic string, partition int32, offset int64) error {
	s.mu.Lock()
	s.entries[joinKey(group, topic, partition)] = offset
	snapshot := s.snapshotLocked()
	s.mu.Unlock()
	return s.persist(snapshot)
}

// Lookup returns the committed offset for a key, or NoOffset if unset.
func (s *JSONOffsetStore) Lookup(group, topic string, partition int32) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if v, ok := s.entries[joinKey(group, topic, partition)]; ok {
		return v, nil
	}
	return NoOffset, nil
}

// Close is a no-op; commits are flushed synchronously.
func (s *JSONOffsetStore) Close() error { return nil }

func (s *JSONOffsetStore) snapshotLocked() []offsetKey {
	out := make([]offsetKey, 0, len(s.entries))
	for k, off := range s.entries {
		group, topic, partition := splitKey(k)
		out = append(out, offsetKey{Group: group, Topic: topic, Partition: partition, Offset: off})
	}
	return out
}

func (s *JSONOffsetStore) persist(snapshot []offsetKey) error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

func joinKey(group, topic string, partition int32) string {
	return fmt.Sprintf("%s\x00%s\x00%d", group, topic, partition)
}

func splitKey(k string) (string, string, int32) {
	var group, topic string
	var partition int32
	_, _ = fmt.Sscanf(k, "%s\x00%s\x00%d", &group, &topic, &partition)
	// Sscanf with NUL separators is fragile; use a manual split as fallback.
	parts := splitNUL(k)
	if len(parts) == 3 {
		var p int32
		_, _ = fmt.Sscanf(parts[2], "%d", &p)
		return parts[0], parts[1], p
	}
	return group, topic, partition
}

func splitNUL(s string) []string {
	var out []string
	start := 0
	for i, c := range s {
		if c == 0 {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	out = append(out, s[start:])
	return out
}

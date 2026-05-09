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
	List(group string) []OffsetEntry
	DeleteGroup(group string) error
	Close() error
}

// OffsetEntry is one (topic, partition, offset) tuple committed under
// a particular consumer group. Returned by OffsetStore.List so an
// operator-facing API can enumerate the partitions a group has touched
// without needing to know the topic/partition layout up front.
type OffsetEntry struct {
	Topic     string
	Partition int32
	Offset    int64
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

// List returns every (topic, partition, offset) entry committed under
// group. Order is unspecified — callers that need stable order sort
// the result.
func (m *MemoryOffsetStore) List(group string) []OffsetEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return listEntries(m.entries, group)
}

// DeleteGroup drops every commit recorded under group. Used by
// operator-driven group cleanup; pairs with Manager.Delete.
func (m *MemoryOffsetStore) DeleteGroup(group string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	deleteGroupEntries(m.entries, group)
	return nil
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

// List returns every (topic, partition, offset) entry committed under
// group. Order is unspecified.
func (s *JSONOffsetStore) List(group string) []OffsetEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return listEntries(s.entries, group)
}

// DeleteGroup drops every commit recorded under group and rewrites
// the JSON file so the change survives a restart.
func (s *JSONOffsetStore) DeleteGroup(group string) error {
	s.mu.Lock()
	deleteGroupEntries(s.entries, group)
	snapshot := s.snapshotLocked()
	s.mu.Unlock()
	return s.persist(snapshot)
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

// deleteGroupEntries removes every entry keyed under group from the
// shared (joinKey-keyed) map. Shared by Memory/JSON stores so the
// matching logic lives in one place.
func deleteGroupEntries(entries map[string]int64, group string) {
	for k := range entries {
		if g, _, _ := splitKey(k); g == group {
			delete(entries, k)
		}
	}
}

// listEntries projects entries (keyed by joinKey) down to the subset
// belonging to the named group. Shared by Memory/JSON/Topic stores so
// the enumeration logic lives in one place.
func listEntries(entries map[string]int64, group string) []OffsetEntry {
	out := make([]OffsetEntry, 0)
	for k, off := range entries {
		g, topic, partition := splitKey(k)
		if g != group {
			continue
		}
		out = append(out, OffsetEntry{Topic: topic, Partition: partition, Offset: off})
	}
	return out
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

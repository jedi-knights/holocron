package storage

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jedi-knights/holocron/proto"
)

// MemoryStore is the Stage 1 in-process implementation of Store. It holds
// all records in RAM and makes no durability guarantees. Useful for tests
// and the in-memory pub/sub broker.
type MemoryStore struct {
	mu         sync.RWMutex
	partitions map[proto.PartitionRef]*memPartition
}

type memPartition struct {
	mu      sync.RWMutex
	records []proto.Record
}

// NewMemoryStore returns an empty MemoryStore. Partitions are auto-created
// on first append; topic existence is enforced upstream by the topic
// registry, not here.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		partitions: make(map[proto.PartitionRef]*memPartition),
	}
}

// Append assigns r the next offset in the partition and stores it.
func (s *MemoryStore) Append(ctx context.Context, p proto.PartitionRef, r proto.Record) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	part := s.getOrCreate(p)
	part.mu.Lock()
	defer part.mu.Unlock()
	r.Offset = int64(len(part.records))
	if r.Timestamp == 0 {
		r.Timestamp = time.Now().UnixNano()
	}
	part.records = append(part.records, r)
	return r.Offset, nil
}

// Read returns up to maxRecords records starting at fromOffset (inclusive).
// An empty slice is returned at end-of-log; an error is returned only for
// invalid input.
func (s *MemoryStore) Read(ctx context.Context, p proto.PartitionRef, fromOffset int64, maxRecords int) ([]proto.Record, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if fromOffset < 0 {
		return nil, fmt.Errorf("storage: negative offset %d", fromOffset)
	}
	if maxRecords <= 0 {
		return nil, nil
	}
	part, ok := s.get(p)
	if !ok {
		return nil, nil
	}
	part.mu.RLock()
	defer part.mu.RUnlock()
	if fromOffset >= int64(len(part.records)) {
		return nil, nil
	}
	end := min(fromOffset+int64(maxRecords), int64(len(part.records)))
	out := make([]proto.Record, end-fromOffset)
	copy(out, part.records[fromOffset:end])
	return out, nil
}

// HighWater returns the offset of the next record to be appended. For an
// empty partition, this is 0.
func (s *MemoryStore) HighWater(ctx context.Context, p proto.PartitionRef) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	part, ok := s.get(p)
	if !ok {
		return 0, nil
	}
	part.mu.RLock()
	defer part.mu.RUnlock()
	return int64(len(part.records)), nil
}

// Sync is a no-op for the in-memory store: nothing to flush.
func (s *MemoryStore) Sync(ctx context.Context, _ proto.PartitionRef) error {
	return ctx.Err()
}

// Close is a no-op for the in-memory store.
func (s *MemoryStore) Close() error { return nil }

func (s *MemoryStore) get(p proto.PartitionRef) (*memPartition, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	part, ok := s.partitions[p]
	return part, ok
}

func (s *MemoryStore) getOrCreate(p proto.PartitionRef) *memPartition {
	s.mu.RLock()
	if part, ok := s.partitions[p]; ok {
		s.mu.RUnlock()
		return part
	}
	s.mu.RUnlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if part, ok := s.partitions[p]; ok {
		return part
	}
	part := &memPartition{}
	s.partitions[p] = part
	return part
}

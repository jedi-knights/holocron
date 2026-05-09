package sdk

import (
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jedi-knights/holocron/proto"
)

// Partitioner picks a partition index for a record. Producers hold one and
// consult it on every Send. It is a Strategy — callers may swap in a
// custom routing scheme by implementing the interface.
type Partitioner interface {
	Partition(r proto.Record, numPartitions int32) int32
}

// DefaultPartitioner implements the standard producer routing rules:
//   - If r.Key is non-empty, route by FNV-1a(Key) mod numPartitions.
//   - Otherwise round-robin across partitions.
//
// FNV-1a is a placeholder hash. The wire-stable choice for Stage 3 is
// tracked in docs/data-model.md "open questions".
type DefaultPartitioner struct {
	counter atomic.Uint32
}

// Partition implements Partitioner.
func (p *DefaultPartitioner) Partition(r proto.Record, numPartitions int32) int32 {
	if numPartitions <= 0 {
		return 0
	}
	if len(r.Key) > 0 {
		h := fnv.New32a()
		_, _ = h.Write(r.Key)
		return int32(h.Sum32() % uint32(numPartitions))
	}
	n := p.counter.Add(1) - 1
	return int32(n % uint32(numPartitions))
}

// StickyPartitioner wraps another Partitioner and, for keyless records,
// holds the most recently chosen partition for a fixed window. Within
// the window, every keyless record goes to the same partition; this
// fills batches faster than the underlying round-robin would. Records
// with non-empty Key fall through to the underlying Partitioner so
// key-based routing semantics are preserved.
//
// The Producer auto-wraps its partitioner in StickyPartitioner when
// WithLinger is set, with stickWindow = the linger duration.
type StickyPartitioner struct {
	underlying  Partitioner
	stickWindow time.Duration
	now         func() time.Time

	mu    sync.Mutex
	last  int32
	until time.Time
}

// NewStickyPartitioner wraps underlying with sticky-window behavior.
// If stickWindow is zero, behavior is identical to underlying.
func NewStickyPartitioner(underlying Partitioner, stickWindow time.Duration) *StickyPartitioner {
	return &StickyPartitioner{
		underlying:  underlying,
		stickWindow: stickWindow,
		now:         time.Now,
	}
}

// Partition implements Partitioner.
func (s *StickyPartitioner) Partition(r proto.Record, numPartitions int32) int32 {
	if len(r.Key) > 0 {
		return s.underlying.Partition(r, numPartitions)
	}
	if s.stickWindow <= 0 {
		return s.underlying.Partition(r, numPartitions)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	if now.Before(s.until) {
		return s.last
	}
	p := s.underlying.Partition(r, numPartitions)
	s.last = p
	s.until = now.Add(s.stickWindow)
	return p
}

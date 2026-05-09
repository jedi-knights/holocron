package storage

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/jedi-knights/holocron/broker/internal/log"
	"github.com/jedi-knights/holocron/proto"
)

// FileStore is the Stage 2 disk-backed implementation of Store. Each
// partition's records live in <root>/<topic>/<index>/, segmented into
// .log + .idx file pairs by broker/internal/log.
//
// Partition logs are opened lazily on first access and cached. On startup
// the FileStore does not eagerly walk the directory tree — directories
// are picked up only as their partitions are addressed.
type FileStore struct {
	root        string
	segmentSize int64

	mu   sync.RWMutex
	logs map[proto.PartitionRef]*log.PartitionLog
}

// FileStoreOption configures a FileStore.
type FileStoreOption func(*FileStore)

// WithSegmentBytes overrides the default segment-rollover threshold.
func WithSegmentBytes(n int64) FileStoreOption {
	return func(s *FileStore) { s.segmentSize = n }
}

// NewFileStore returns a FileStore rooted at dir. The directory is created
// if it does not exist.
func NewFileStore(dir string, opts ...FileStoreOption) (*FileStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("storage: mkdir %s: %w", dir, err)
	}
	s := &FileStore{
		root:        dir,
		segmentSize: log.DefaultSegmentBytes,
		logs:        make(map[proto.PartitionRef]*log.PartitionLog),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s, nil
}

// Append assigns the next offset in the partition and persists r.
func (s *FileStore) Append(ctx context.Context, p proto.PartitionRef, r proto.Record) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	plog, err := s.openLog(p)
	if err != nil {
		return 0, err
	}
	r.Offset = plog.HighWater()
	if r.Timestamp == 0 {
		r.Timestamp = time.Now().UnixNano()
	}
	return plog.Append(r)
}

// Read returns up to maxRecords records starting at fromOffset.
func (s *FileStore) Read(ctx context.Context, p proto.PartitionRef, fromOffset int64, maxRecords int) ([]proto.Record, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if fromOffset < 0 {
		return nil, fmt.Errorf("storage: negative offset %d", fromOffset)
	}
	plog, err := s.openLog(p)
	if err != nil {
		return nil, err
	}
	return plog.Read(fromOffset, maxRecords)
}

// HighWater returns the offset of the next record to be appended.
func (s *FileStore) HighWater(ctx context.Context, p proto.PartitionRef) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	plog, err := s.openLog(p)
	if err != nil {
		return 0, err
	}
	return plog.HighWater(), nil
}

// EnforceRetention applies time-based retention to every open partition.
// Callers (the broker's retention sweeper) drive this on a schedule.
func (s *FileStore) EnforceRetention(retention time.Duration) error {
	if retention <= 0 {
		return nil
	}
	cutoff := time.Now().Add(-retention).UnixNano()
	for _, l := range s.snapshotLogs() {
		if _, err := l.EnforceTimeRetention(cutoff); err != nil {
			return err
		}
	}
	return nil
}

// EnforceSizeRetention applies size-based retention to every open
// partition. Each partition is independently capped at maxBytes; the
// active segment is never removed.
func (s *FileStore) EnforceSizeRetention(maxBytes int64) error {
	if maxBytes <= 0 {
		return nil
	}
	for _, l := range s.snapshotLogs() {
		if _, err := l.EnforceSizeRetention(maxBytes); err != nil {
			return err
		}
	}
	return nil
}

// Compact runs log compaction on every open partition. Callers schedule
// this from the retention sweeper; for V1 every partition is compacted
// with the same policy.
func (s *FileStore) Compact() error {
	for _, l := range s.snapshotLogs() {
		if err := l.Compact(); err != nil {
			return err
		}
	}
	return nil
}

func (s *FileStore) snapshotLogs() []*log.PartitionLog {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*log.PartitionLog, 0, len(s.logs))
	for _, l := range s.logs {
		out = append(out, l)
	}
	return out
}

// Sync fsyncs the active segment of the partition to disk. The
// PartitionLog opens lazily; if nothing has been written to this
// partition yet, Sync is a no-op.
func (s *FileStore) Sync(ctx context.Context, p proto.PartitionRef) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.RLock()
	plog, ok := s.logs[p]
	s.mu.RUnlock()
	if !ok {
		return nil
	}
	return plog.Sync()
}

// Close flushes and closes every open partition log.
func (s *FileStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var firstErr error
	for _, l := range s.logs {
		if err := l.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	s.logs = nil
	return firstErr
}

func (s *FileStore) openLog(p proto.PartitionRef) (*log.PartitionLog, error) {
	s.mu.RLock()
	if l, ok := s.logs[p]; ok {
		s.mu.RUnlock()
		return l, nil
	}
	s.mu.RUnlock()

	s.mu.Lock()
	defer s.mu.Unlock()
	if l, ok := s.logs[p]; ok {
		return l, nil
	}
	dir := log.PartitionDir(s.root, p)
	l, err := log.OpenPartition(dir, s.segmentSize)
	if err != nil {
		return nil, fmt.Errorf("storage: open partition %v: %w", p, err)
	}
	s.logs[p] = l
	return l, nil
}

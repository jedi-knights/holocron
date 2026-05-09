package log

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"

	"github.com/jedi-knights/holocron/proto"
)

// Defaults match the data-model spec.
const (
	DefaultSegmentBytes = 1 << 30 // 1 GiB
)

// PartitionLog is the on-disk append-only log for a single partition. It
// is composed of an ordered list of segments. Exactly one segment is
// "active" (open for writes); older ones are sealed and read-only.
//
// PartitionLog is safe for concurrent reads. Appends must be serialized
// by the caller — the broker holds a per-partition mutex around each
// publish, which gives this property naturally.
type PartitionLog struct {
	dir      string
	maxBytes int64

	mu       sync.RWMutex
	segments []*segment // ordered by base offset, ascending
}

// OpenPartition opens the partition log rooted at dir. If dir does not
// exist it is created and a fresh first segment is opened. If dir holds
// segment files they are loaded; the highest-base segment is reopened for
// append, all others are reopened read-only.
func OpenPartition(dir string, maxBytes int64) (*PartitionLog, error) {
	if maxBytes <= 0 {
		maxBytes = DefaultSegmentBytes
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("log: mkdir %s: %w", dir, err)
	}

	if err := recoverCompactingSegments(dir); err != nil {
		return nil, fmt.Errorf("log: recover compaction state: %w", err)
	}

	bases, err := discoverSegments(dir)
	if err != nil {
		return nil, err
	}

	p := &PartitionLog{dir: dir, maxBytes: maxBytes}

	if len(bases) == 0 {
		first, err := createSegment(dir, 0, maxBytes)
		if err != nil {
			return nil, err
		}
		p.segments = []*segment{first}
		return p, nil
	}

	for i, base := range bases {
		forAppend := i == len(bases)-1
		s, err := openSegment(dir, base, maxBytes, forAppend)
		if err != nil {
			return nil, err
		}
		p.segments = append(p.segments, s)
	}
	return p, nil
}

// HighWater returns the offset of the next record to be appended.
func (p *PartitionLog) HighWater() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if len(p.segments) == 0 {
		return 0
	}
	return p.segments[len(p.segments)-1].highWater
}

// SegmentSnapshot describes one segment's state at a moment in
// time: its base offset and the current sizes of the .log and .idx
// files. Snapshots are taken under the partition's mutex so a
// downstream reader can fetch byte ranges up to the listed sizes
// and observe a self-consistent prefix of every segment — including
// the currently active segment that may be growing concurrently.
type SegmentSnapshot struct {
	Base    int64
	LogSize int64
	IdxSize int64
}

// Snapshot returns SegmentSnapshot for every segment in the
// partition, ordered by ascending base offset. The last entry
// describes the currently active segment.
//
// Before reporting sizes, every segment is flushed and its
// in-memory index is persisted so the on-disk file matches the
// reported size. A follower can then read each segment's bytes up
// to the listed sizes and observe a self-consistent prefix —
// including bytes from a still-active segment whose buffer would
// otherwise sit in memory.
//
// Returns an error if the flush or persist fails. The partition's
// mutex is held write-locked across the flush so concurrent
// appenders can't extend the file mid-snapshot.
func (p *PartitionLog) Snapshot() ([]SegmentSnapshot, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, seg := range p.segments {
		if err := seg.snapshotSync(); err != nil {
			return nil, fmt.Errorf("log: snapshot sync segment %d: %w", seg.baseOffset, err)
		}
	}
	out := make([]SegmentSnapshot, 0, len(p.segments))
	for _, seg := range p.segments {
		out = append(out, SegmentSnapshot{
			Base:    seg.baseOffset,
			LogSize: seg.size,
			IdxSize: seg.idx.sizeBytes(),
		})
	}
	return out, nil
}

// SegmentKind selects which file of a segment ReadSegmentBytes
// reads. SegmentLog is the records, SegmentIdx is the sparse index.
type SegmentKind uint8

const (
	// SegmentLog addresses the segment's .log file.
	SegmentLog SegmentKind = 0
	// SegmentIdx addresses the segment's .idx file.
	SegmentIdx SegmentKind = 1
)

// ReadSegmentBytes returns up to maxBytes from the addressed
// segment file starting at offset. The byte range is read directly
// from disk — appended-to active segments are safe to read because
// the on-disk file grows monotonically and reads of [0, listedSize]
// always return committed bytes.
//
// Returns nil bytes when offset is at or past the file's current
// size; that's the end-of-stream signal for the chunked bootstrap
// loop. Returns an error if the segment base is unknown.
func (p *PartitionLog) ReadSegmentBytes(base int64, kind SegmentKind, offset int64, maxBytes int) ([]byte, error) {
	if maxBytes <= 0 {
		return nil, nil
	}
	p.mu.RLock()
	var path string
	found := false
	for _, seg := range p.segments {
		if seg.baseOffset == base {
			if kind == SegmentIdx {
				path = seg.idxPath()
			} else {
				path = seg.logPath()
			}
			found = true
			break
		}
	}
	p.mu.RUnlock()
	if !found {
		return nil, fmt.Errorf("log: unknown segment base %d", base)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if _, err := f.Seek(offset, 0); err != nil {
		return nil, err
	}
	buf := make([]byte, maxBytes)
	n, err := f.Read(buf)
	if err != nil && err.Error() != "EOF" {
		return nil, err
	}
	return buf[:n], nil
}

// Append serializes r and writes it to the active segment. The broker
// supplies r.Offset; PartitionLog only persists what it is given.
// Rollover happens when the active segment reaches maxBytes.
func (p *PartitionLog) Append(r proto.Record) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.segments) == 0 {
		return 0, errors.New("log: no active segment")
	}
	active := p.segments[len(p.segments)-1]
	off, err := active.append(r)
	if err != nil {
		return 0, err
	}
	if active.shouldRoll() {
		if err := p.rollLocked(); err != nil {
			return off, fmt.Errorf("log: rollover: %w", err)
		}
	}
	return off, nil
}

// Read returns up to maxRecords records starting at fromOffset. Reads may
// span segments transparently.
func (p *PartitionLog) Read(fromOffset int64, maxRecords int) ([]proto.Record, error) {
	if maxRecords <= 0 {
		return nil, nil
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if len(p.segments) == 0 {
		return nil, nil
	}
	idx := max(sort.Search(len(p.segments), func(i int) bool {
		return p.segments[i].baseOffset > fromOffset
	})-1, 0)

	var out []proto.Record
	for ; idx < len(p.segments) && len(out) < maxRecords; idx++ {
		records, err := p.segments[idx].readFrom(fromOffset, maxRecords-len(out))
		if err != nil {
			return nil, err
		}
		out = append(out, records...)
		if len(records) > 0 {
			fromOffset = records[len(records)-1].Offset + 1
		}
	}
	return out, nil
}

// Sync flushes the active segment to disk. Used by acks=durable in later
// stages; for now exposed for test determinism.
func (p *PartitionLog) Sync() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.segments) == 0 {
		return nil
	}
	active := p.segments[len(p.segments)-1]
	if err := active.flushIfActive(); err != nil {
		return err
	}
	return active.logFile.Sync()
}

// Close flushes, seals, and closes all open segments. The PartitionLog is
// not usable after Close.
func (p *PartitionLog) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	var firstErr error
	for i, s := range p.segments {
		if i == len(p.segments)-1 {
			if err := s.seal(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		if err := s.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// EnforceTimeRetention removes sealed segments whose newest record is
// older than retainNanos before the cutoff. The active segment is never
// removed. Returns the number of segments deleted.
//
// Callers run this periodically (e.g. from a sweeper goroutine).
func (p *PartitionLog) EnforceTimeRetention(cutoffNanos int64) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.segments) <= 1 {
		return 0, nil
	}

	deleted := 0
	for len(p.segments) > 1 {
		oldest := p.segments[0]
		// The newest record in a sealed segment is at offset highWater - 1.
		recs, err := oldest.readFrom(oldest.highWater-1, 1)
		if err != nil {
			return deleted, err
		}
		if len(recs) == 0 || recs[0].Timestamp >= cutoffNanos {
			break
		}
		if err := oldest.close(); err != nil {
			return deleted, err
		}
		if err := oldest.remove(); err != nil {
			return deleted, err
		}
		p.segments = p.segments[1:]
		deleted++
	}
	return deleted, nil
}

// EnforceSizeRetention removes oldest sealed segments while the total
// on-disk size of the partition exceeds maxBytes. The active segment is
// never removed even if it alone exceeds maxBytes. Returns the number
// of segments deleted.
func (p *PartitionLog) EnforceSizeRetention(maxBytes int64) (int, error) {
	if maxBytes <= 0 {
		return 0, nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.segments) <= 1 {
		return 0, nil
	}

	total := int64(0)
	for _, s := range p.segments {
		total += s.size
	}

	deleted := 0
	for total > maxBytes && len(p.segments) > 1 {
		oldest := p.segments[0]
		dropped := oldest.size
		if err := oldest.close(); err != nil {
			return deleted, err
		}
		if err := oldest.remove(); err != nil {
			return deleted, err
		}
		p.segments = p.segments[1:]
		total -= dropped
		deleted++
	}
	return deleted, nil
}

// rollLocked seals the active segment and opens a new one starting at the
// current high water. The caller must hold p.mu.
func (p *PartitionLog) rollLocked() error {
	active := p.segments[len(p.segments)-1]
	if err := active.seal(); err != nil {
		return err
	}
	next, err := createSegment(p.dir, active.highWater, p.maxBytes)
	if err != nil {
		return err
	}
	p.segments = append(p.segments, next)
	return nil
}

// discoverSegments lists segment base offsets in dir, ascending.
func discoverSegments(dir string) ([]int64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var bases []int64
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".log") {
			continue
		}
		base, err := parseBaseOffset(e.Name())
		if err != nil {
			continue
		}
		bases = append(bases, base)
	}
	slices.Sort(bases)
	return bases, nil
}

// PartitionDir is the conventional directory layout helper:
// <root>/<topic>/<partition-index>/.
func PartitionDir(root string, p proto.PartitionRef) string {
	return filepath.Join(root, p.Topic, fmt.Sprintf("%d", p.Index))
}

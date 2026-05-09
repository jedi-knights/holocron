package log

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"

	"github.com/jedi-knights/holocron/proto"
)

// indexIntervalBytes is the granularity of the sparse index — write an
// index entry once each segment has accumulated this many bytes since the
// last entry. 4 KiB matches Kafka's default and the data-model spec.
const indexIntervalBytes = 4096

// segmentNameWidth is the zero-padded width of segment filenames so
// lexical sort matches numeric sort.
const segmentNameWidth = 20

// segment is a single .log + .idx file pair holding a contiguous range of
// a partition's records. Exactly one segment per partition is open for
// writing at any time (the active segment); older segments are closed and
// immutable.
type segment struct {
	dir        string
	baseOffset int64
	maxBytes   int64
	// nameSuffix lives between the standard segment file extension and
	// EOF when set, e.g. ".compacting" yields "<base>.log.compacting"
	// and "<base>.idx.compacting". A non-empty suffix means the segment
	// is in a transitional state (e.g., compaction in progress) and
	// must not be picked up by discoverSegments. finalize() renames it
	// to the canonical files.
	nameSuffix string

	logFile *os.File
	logBuf  *bufio.Writer
	idx     *index

	size            int64 // bytes written to .log
	bytesSinceIndex int64 // for sparse-index spacing
	highWater       int64 // offset of the next record to be appended
}

// logPath returns the on-disk path to this segment's .log file,
// including any pending name suffix.
func (s *segment) logPath() string {
	return filepath.Join(s.dir, segmentLogName(s.baseOffset)+s.nameSuffix)
}

// idxPath returns the on-disk path to this segment's .idx file,
// including any pending name suffix.
func (s *segment) idxPath() string {
	return filepath.Join(s.dir, segmentIndexName(s.baseOffset)+s.nameSuffix)
}

// segmentLogName returns the .log filename for a base offset.
func segmentLogName(baseOffset int64) string {
	return fmt.Sprintf("%0*d.log", segmentNameWidth, baseOffset)
}

// segmentIndexName returns the .idx filename for a base offset.
func segmentIndexName(baseOffset int64) string {
	return fmt.Sprintf("%0*d.idx", segmentNameWidth, baseOffset)
}

// parseBaseOffset extracts the base offset encoded in a segment filename
// like "00000000000000000123.log".
func parseBaseOffset(name string) (int64, error) {
	stem := name
	if ext := filepath.Ext(stem); ext != "" {
		stem = stem[:len(stem)-len(ext)]
	}
	if len(stem) != segmentNameWidth {
		return 0, fmt.Errorf("log: bad segment name %q", name)
	}
	return strconv.ParseInt(stem, 10, 64)
}

// createSegment opens a new (or truncates an existing) segment in dir at
// the given base offset, ready for appends.
func createSegment(dir string, baseOffset, maxBytes int64) (*segment, error) {
	return createSegmentWithSuffix(dir, baseOffset, maxBytes, "")
}

// createSegmentWithSuffix is the suffix-aware constructor. A non-empty
// suffix produces a segment whose files live alongside the canonical
// names but with the suffix appended (e.g. ".compacting"). Callers
// finalize the segment with finalize() once it's durable.
func createSegmentWithSuffix(dir string, baseOffset, maxBytes int64, suffix string) (*segment, error) {
	s := &segment{
		dir:        dir,
		baseOffset: baseOffset,
		maxBytes:   maxBytes,
		nameSuffix: suffix,
		idx:        newIndex(),
		highWater:  baseOffset,
	}
	f, err := os.OpenFile(s.logPath(), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("log: open %s: %w", s.logPath(), err)
	}
	s.logFile = f
	s.logBuf = bufio.NewWriter(f)
	return s, nil
}

// openSegment reopens an existing segment for reading, recovering its
// in-memory state by replaying the log file. Trailing torn frames are
// truncated; the index is rebuilt from scratch (cheap and always correct).
//
// If forAppend is true the segment is opened read-write so further records
// can be written; otherwise it is opened read-only.
func openSegment(dir string, baseOffset, maxBytes int64, forAppend bool) (*segment, error) {
	logPath := filepath.Join(dir, segmentLogName(baseOffset))

	flag := os.O_RDONLY
	if forAppend {
		flag = os.O_RDWR
	}
	f, err := os.OpenFile(logPath, flag, 0o644)
	if err != nil {
		return nil, fmt.Errorf("log: open %s: %w", logPath, err)
	}

	s := &segment{
		dir:        dir,
		baseOffset: baseOffset,
		maxBytes:   maxBytes,
		logFile:    f,
		idx:        newIndex(),
		highWater:  baseOffset,
	}
	if forAppend {
		s.logBuf = bufio.NewWriter(f)
	}

	if err := s.recover(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return s, nil
}

// recover replays the log to rebuild high-water, size, and index state.
// On a torn (CRC-failing) frame, it truncates the file at that position.
func (s *segment) recover() error {
	if _, err := s.logFile.Seek(0, io.SeekStart); err != nil {
		return err
	}
	r := bufio.NewReader(s.logFile)

	var pos int64
	var bytesSinceIndex int64
	for {
		rec, n, err := readRecordFrom(r)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, errTornFrame) {
				if err := s.logFile.Truncate(pos); err != nil {
					return fmt.Errorf("log: truncate torn tail: %w", err)
				}
				if _, err := s.logFile.Seek(pos, io.SeekStart); err != nil {
					return err
				}
				break
			}
			return err
		}
		if pos == 0 || bytesSinceIndex >= indexIntervalBytes {
			s.idx.add(uint32(rec.Offset-s.baseOffset), uint32(pos))
			bytesSinceIndex = 0
		}
		pos += int64(n)
		bytesSinceIndex += int64(n)
		s.highWater = rec.Offset + 1
	}
	s.size = pos
	s.bytesSinceIndex = bytesSinceIndex
	return nil
}

// append serializes r and writes it to the segment, returning the offset
// the broker may report. The caller is responsible for setting r.Offset
// (the segment trusts and uses it).
func (s *segment) append(r proto.Record) (int64, error) {
	if s.logBuf == nil {
		return 0, errors.New("log: segment is read-only")
	}
	frame := encodeRecord(nil, r)
	if s.bytesSinceIndex == 0 || s.bytesSinceIndex >= indexIntervalBytes {
		s.idx.add(uint32(r.Offset-s.baseOffset), uint32(s.size))
		s.bytesSinceIndex = 0
	}
	if _, err := s.logBuf.Write(frame); err != nil {
		return 0, err
	}
	s.size += int64(len(frame))
	s.bytesSinceIndex += int64(len(frame))
	s.highWater = r.Offset + 1
	return r.Offset, nil
}

// readFrom returns up to maxRecords records starting at fromOffset
// (absolute, partition-relative offsets are computed internally).
func (s *segment) readFrom(fromOffset int64, maxRecords int) ([]proto.Record, error) {
	if maxRecords <= 0 {
		return nil, nil
	}
	if fromOffset >= s.highWater {
		return nil, nil
	}
	if fromOffset < s.baseOffset {
		fromOffset = s.baseOffset
	}

	if err := s.flushIfActive(); err != nil {
		return nil, err
	}

	startPos := s.idx.lookup(uint32(fromOffset - s.baseOffset))
	if _, err := s.logFile.Seek(int64(startPos), io.SeekStart); err != nil {
		return nil, err
	}
	r := bufio.NewReader(s.logFile)

	out := make([]proto.Record, 0, maxRecords)
	for len(out) < maxRecords {
		rec, _, err := readRecordFrom(r)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return nil, err
		}
		if rec.Offset < fromOffset {
			continue
		}
		out = append(out, rec)
	}
	return out, nil
}

// flushIfActive ensures buffered writes are visible to readers. Read-only
// segments have no buffer and skip the flush.
func (s *segment) flushIfActive() error {
	if s.logBuf == nil {
		return nil
	}
	return s.logBuf.Flush()
}

// shouldRoll reports whether the segment has reached its size cap and
// should be sealed in favor of a fresh active segment.
func (s *segment) shouldRoll() bool {
	return s.size >= s.maxBytes
}

// seal flushes pending writes, persists the index, and converts the
// segment to read-only state. After seal the segment may still be read
// but not appended to.
func (s *segment) seal() error {
	if err := s.flushIfActive(); err != nil {
		return err
	}
	if err := s.logFile.Sync(); err != nil {
		return err
	}
	if err := s.persistIndex(); err != nil {
		return err
	}
	s.logBuf = nil
	return nil
}

// persistIndex writes the in-memory index to disk atomically (temp file +
// rename) so a crash mid-write cannot leave a partial index.
func (s *segment) persistIndex() error {
	path := s.idxPath()
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if err := s.idx.writeTo(f); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// close releases file handles. Active segments should be sealed first;
// callers do this explicitly so close is safe to call from defers.
func (s *segment) close() error {
	if err := s.flushIfActive(); err != nil {
		_ = s.logFile.Close()
		return err
	}
	return s.logFile.Close()
}

// finalize renames a suffixed segment's .log and .idx files to their
// canonical names and clears the suffix. The segment must already be
// sealed and synced. Used by compaction to atomically swap a freshly-
// written compacted segment over its placeholder name.
//
// On error, the segment is left in an inconsistent state with one file
// possibly renamed and the other not — the caller is expected to fail
// the partition and rely on OpenPartition's recovery on next start.
func (s *segment) finalize() error {
	if s.nameSuffix == "" {
		return nil
	}
	oldLog := s.logPath()
	oldIdx := s.idxPath()
	s.nameSuffix = ""
	if err := os.Rename(oldLog, s.logPath()); err != nil {
		return fmt.Errorf("log: finalize log %s: %w", oldLog, err)
	}
	if err := os.Rename(oldIdx, s.idxPath()); err != nil {
		return fmt.Errorf("log: finalize idx %s: %w", oldIdx, err)
	}
	return nil
}

// remove deletes the segment files from disk. The segment must be closed
// first.
func (s *segment) remove() error {
	errLog := os.Remove(s.logPath())
	errIdx := os.Remove(s.idxPath())
	if errLog != nil && !os.IsNotExist(errLog) {
		return errLog
	}
	if errIdx != nil && !os.IsNotExist(errIdx) {
		return errIdx
	}
	return nil
}

package log

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
)

// indexEntrySize is the on-disk size of a single index entry:
// 4-byte relative offset + 4-byte byte position.
const indexEntrySize = 8

// indexEntry maps a partition-relative offset to the byte position of that
// record's frame in the .log file. The index is sparse — only some records
// have entries — so a lookup finds the largest entry whose relative offset
// is <= the target, then scans forward in the .log.
type indexEntry struct {
	RelativeOffset uint32
	Position       uint32
}

// index holds an in-memory copy of a segment's sparse index. It is built
// up as records are written and is rebuilt from disk on segment open.
type index struct {
	entries []indexEntry
}

// newIndex returns an empty index.
func newIndex() *index { return &index{} }

// sizeBytes is the on-disk byte size the index would serialize to —
// indexEntrySize per logged entry. Used by the data-dir bootstrap
// path to capture a consistent listed size at snapshot time.
func (i *index) sizeBytes() int64 {
	return int64(len(i.entries)) * indexEntrySize
}

// add appends an entry. Entries must be added in monotonically increasing
// offset order; this is enforced by the caller (segment write path).
func (i *index) add(relOffset, pos uint32) {
	i.entries = append(i.entries, indexEntry{RelativeOffset: relOffset, Position: pos})
}

// lookup returns the byte position of the largest indexed record whose
// relative offset is <= relOffset, or 0 if no such entry exists (start of
// the segment).
func (i *index) lookup(relOffset uint32) uint32 {
	if len(i.entries) == 0 {
		return 0
	}
	idx := sort.Search(len(i.entries), func(j int) bool {
		return i.entries[j].RelativeOffset > relOffset
	})
	if idx == 0 {
		return 0
	}
	return i.entries[idx-1].Position
}

// writeTo serializes the index to w in big-endian on-disk format.
func (i *index) writeTo(w io.Writer) error {
	buf := make([]byte, indexEntrySize)
	for _, e := range i.entries {
		binary.BigEndian.PutUint32(buf[0:4], e.RelativeOffset)
		binary.BigEndian.PutUint32(buf[4:8], e.Position)
		if _, err := w.Write(buf); err != nil {
			return err
		}
	}
	return nil
}

// readIndexFrom loads an on-disk index file from path. Missing or empty
// files yield an empty index without error.
func readIndexFrom(path string) (*index, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return newIndex(), nil
		}
		return nil, err
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if stat.Size()%indexEntrySize != 0 {
		return nil, fmt.Errorf("log: index %s has trailing %d bytes", path, stat.Size()%indexEntrySize)
	}

	count := stat.Size() / indexEntrySize
	idx := &index{entries: make([]indexEntry, 0, count)}
	buf := make([]byte, indexEntrySize)
	for range count {
		if _, err := io.ReadFull(f, buf); err != nil {
			return nil, err
		}
		idx.entries = append(idx.entries, indexEntry{
			RelativeOffset: binary.BigEndian.Uint32(buf[0:4]),
			Position:       binary.BigEndian.Uint32(buf[4:8]),
		})
	}
	return idx, nil
}

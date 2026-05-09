package log

import (
	"fmt"
	"os"

	"github.com/jedi-knights/holocron/proto"
)

// compactBatchSize is how many records each readFrom call returns
// during streaming compaction. Bounded so memory use scales with
// distinct keys, not total record count.
const compactBatchSize = 256

// streamSealedRecords walks every record across sealed in offset
// order, calling fn once per record. Reads are batched so neither
// pass holds a full segment in memory at once.
func streamSealedRecords(sealed []*segment, fn func(proto.Record)) error {
	for _, s := range sealed {
		cursor := s.baseOffset
		for {
			batch, err := s.readFrom(cursor, compactBatchSize)
			if err != nil {
				return fmt.Errorf("log: compact read segment %d: %w", s.baseOffset, err)
			}
			if len(batch) == 0 {
				break
			}
			for _, r := range batch {
				fn(r)
			}
			cursor = batch[len(batch)-1].Offset + 1
		}
	}
	return nil
}

// Compact rewrites all sealed segments into a single new sealed segment
// containing only the latest record per key. Tombstone records (those
// with a nil Value) remove the key entirely. Records without a key are
// dropped — keys are required for compaction semantics.
//
// Offsets of retained records are preserved verbatim. The compacted log
// therefore has gaps (offsets where compacted-out records used to live);
// readers seek by index lookup or fall through to the next record, so
// gaps are invisible at the API level.
//
// The active segment is never touched. Compaction holds the partition's
// write lock for the duration; concurrent reads block until it returns.
//
// compactingSuffix marks a segment that compaction has produced but has
// not yet swapped over its placeholder name. OpenPartition recovers
// these on startup: rename → final if no canonical file exists (the
// rename was interrupted), delete → kept-original otherwise (the
// compaction never finished durably).
const compactingSuffix = ".compacting"

// Compact rewrites all sealed segments into a single new sealed
// segment containing only the latest record per key. Memory footprint
// is bounded by the number of distinct keys, not the total record
// count: pass 1 walks each segment in fixed-size batches and records
// only (key, offset) pairs; pass 2 walks the segments again and writes
// out the records whose offset matches the captured "latest" offset.
//
// Tombstone records (nil Value) remove their key from the keep set —
// pass 1 deletes the entry; pass 2 sees no match for that record and
// drops it.
func (p *PartitionLog) Compact() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.segments) <= 1 {
		return nil
	}
	sealed := p.segments[:len(p.segments)-1]
	active := p.segments[len(p.segments)-1]

	// Pass 1: build key → latest-offset, streamed.
	latest := make(map[string]int64)
	if err := streamSealedRecords(sealed, func(r proto.Record) {
		if len(r.Key) == 0 {
			return
		}
		if r.Value == nil {
			delete(latest, string(r.Key))
			return
		}
		latest[string(r.Key)] = r.Offset
	}); err != nil {
		return err
	}

	// All sealed records compacted out: drop them and keep only the
	// active segment. Crash here is safe — every old record was either
	// tombstoned or shadowed by a record that itself was tombstoned.
	if len(latest) == 0 {
		for _, s := range sealed {
			_ = s.close()
			_ = s.remove()
		}
		p.segments = []*segment{active}
		return nil
	}

	// New segment's base offset is the lowest retained offset. Pass 2
	// walks segments in offset order so the first kept record we see
	// names the base.
	var newBase int64 = -1
	for _, off := range latest {
		if newBase < 0 || off < newBase {
			newBase = off
		}
	}

	// Pass 2: stream sealed records again, appending those whose
	// offset matches the keep map. We never accumulate more than one
	// batch of records in memory.
	newSeg, err := createSegmentWithSuffix(p.dir, newBase, p.maxBytes, compactingSuffix)
	if err != nil {
		return fmt.Errorf("log: compact create placeholder: %w", err)
	}
	if err := streamSealedRecords(sealed, func(r proto.Record) {
		if len(r.Key) == 0 {
			return
		}
		if want, ok := latest[string(r.Key)]; !ok || want != r.Offset {
			return
		}
		// append errors get surfaced after the loop via newSeg state;
		// the closure can't return an error directly.
		if _, appendErr := newSeg.append(r); appendErr != nil {
			err = appendErr
		}
	}); err != nil {
		_ = newSeg.close()
		_ = newSeg.remove()
		return err
	}
	if err != nil {
		_ = newSeg.close()
		_ = newSeg.remove()
		return fmt.Errorf("log: compact append: %w", err)
	}
	if err := newSeg.seal(); err != nil {
		_ = newSeg.close()
		_ = newSeg.remove()
		return fmt.Errorf("log: compact seal: %w", err)
	}
	if err := syncDir(p.dir); err != nil {
		_ = newSeg.close()
		_ = newSeg.remove()
		return fmt.Errorf("log: compact sync placeholder: %w", err)
	}

	// Pass 3: drop the old sealed segments. After this point the only
	// sealed segment on disk is the placeholder; a crash now means
	// recovery must rename it to its final name.
	for _, s := range sealed {
		if err := s.close(); err != nil {
			return fmt.Errorf("log: compact close segment %d: %w", s.baseOffset, err)
		}
		if err := s.remove(); err != nil {
			return fmt.Errorf("log: compact remove segment %d: %w", s.baseOffset, err)
		}
	}
	if err := syncDir(p.dir); err != nil {
		return fmt.Errorf("log: compact sync after delete: %w", err)
	}

	// Pass 4: swap placeholder → final names.
	if err := newSeg.finalize(); err != nil {
		return err
	}
	if err := syncDir(p.dir); err != nil {
		return fmt.Errorf("log: compact sync after finalize: %w", err)
	}

	p.segments = []*segment{newSeg, active}
	return nil
}

// syncDir fsyncs the directory so a rename or unlink is durable.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// recoverCompactingSegments handles `.compacting` files left behind
// when a process crashed mid-compaction. Two recovery cases:
//
//   - The canonical file already exists (compaction crashed before
//     deleting old segments). The placeholder is incomplete or
//     redundant; delete it.
//   - No canonical file exists (compaction crashed after deleting old
//     segments but before renaming). The placeholder is the only copy
//     of the compacted data; rename it to the canonical name.
//
// Either way, the on-disk state ends consistent: every base offset has
// at most one .log/.idx pair and no leftover .compacting files.
func recoverCompactingSegments(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, e := range entries {
		name := e.Name()
		if !hasCompactingSuffix(name) {
			continue
		}
		final := name[:len(name)-len(compactingSuffix)]
		tmpPath := dir + string(os.PathSeparator) + name
		finalPath := dir + string(os.PathSeparator) + final
		if _, err := os.Stat(finalPath); err == nil {
			if err := os.Remove(tmpPath); err != nil {
				return fmt.Errorf("log: remove stale %s: %w", tmpPath, err)
			}
			continue
		}
		if err := os.Rename(tmpPath, finalPath); err != nil {
			return fmt.Errorf("log: finalize stale %s: %w", tmpPath, err)
		}
	}
	return nil
}

func hasCompactingSuffix(name string) bool {
	return len(name) > len(compactingSuffix) &&
		name[len(name)-len(compactingSuffix):] == compactingSuffix
}

package embed

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/jedi-knights/holocron/broker/internal/broker"
	"github.com/jedi-knights/holocron/broker/internal/log"
	"github.com/jedi-knights/holocron/broker/internal/storage"
	"github.com/jedi-knights/holocron/proto"
)

// hydrateDedupFromTopic reads every record on the persistent
// dedup topic and replays it into the broker's in-memory dedup
// table so retries that span a restart still dedup. Reads in
// 256-record batches; later records for the same (producer-id,
// partition) overwrite earlier ones, mirroring the on-disk
// "latest write wins" semantic.
//
// A missing topic is not an error — it just means the disk broker
// is opening for the first time and there's nothing to hydrate.
func hydrateDedupFromTopic(store storage.Store, core *broker.Broker) error {
	pref := proto.PartitionRef{Topic: dedupTopic, Index: 0}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cursor := int64(0)
	for {
		records, err := store.Read(ctx, pref, cursor, 256)
		if err != nil {
			return err
		}
		if len(records) == 0 {
			return nil
		}
		for _, r := range records {
			producerID, p, ok := broker.DecodeDedupKey(r.Key)
			if !ok {
				continue
			}
			seq, off, lastSeen, ok := broker.DecodeDedupValue(r.Value)
			if !ok {
				continue
			}
			core.HydrateDedup(producerID, p, seq, off, lastSeen)
		}
		cursor = records[len(records)-1].Offset + 1
	}
}

// PartitionSnapshotter is the minimal capability
// BootstrapPartitionFromPeer needs from a peer: list the partition's
// segment manifest, then fetch byte ranges of each segment file. The
// network transport (`sdk/net.Transport`) implements both methods;
// custom transports may also satisfy the interface.
type PartitionSnapshotter interface {
	ListSegments(ctx context.Context, p proto.PartitionRef) ([]proto.SegmentInfo, error)
	FetchSegmentChunk(ctx context.Context, p proto.PartitionRef, base int64, kind proto.SegmentKind, offset int64, maxBytes int32) ([]byte, error)
}

// bootstrapChunkSize is the maximum bytes per FetchSegmentChunk call
// during bootstrap. 1 MiB balances round-trip overhead against
// memory pressure on both ends; the broker independently caps the
// payload at segmentChunkServerCap.
const bootstrapChunkSize int32 = 1 << 20

// BootstrapPartitionFromPeer seeds dataDir with every segment file
// for partition p — sealed and active alike — by chunking each file
// over the wire from peer. Returns the number of segment files
// written.
//
// Active-segment safety: the donor captures each segment's .log and
// .idx sizes under the partition's mutex when responding to
// ListSegments, then the recipient reads up to those listed sizes.
// Records that the donor appends after the snapshot do not
// transfer; bootstrap is a point-in-time copy of the donor's
// durable state, not ongoing replication.
//
// Call this on a brand-new broker before opening its FileStore: the
// recipient's Open path picks up the seeded files and serves reads
// from offset zero up to the donor's snapshot-time high-water mark.
//
// dataDir must be writable but need not exist; the canonical
// <dataDir>/<topic>/<partition>/ layout is created as needed.
// Existing files at the same paths are overwritten.
func BootstrapPartitionFromPeer(ctx context.Context, peer PartitionSnapshotter, dataDir string, p proto.PartitionRef) (int, error) {
	if peer == nil {
		return 0, fmt.Errorf("embed: BootstrapPartitionFromPeer needs a non-nil peer")
	}
	infos, err := peer.ListSegments(ctx, p)
	if err != nil {
		return 0, fmt.Errorf("embed: list segments for %v: %w", p, err)
	}
	if len(infos) == 0 {
		return 0, nil
	}

	dir := storage.PartitionDir(dataDir, p)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return 0, fmt.Errorf("embed: mkdir %s: %w", dir, err)
	}

	written := 0
	for _, info := range infos {
		// Skip empty active segments that the donor will recreate on
		// open — writing an empty .log would create a stale segment
		// file that the recipient's OpenPartition picks up but adds
		// nothing to the recipient's high-water mark.
		if info.LogSize == 0 && info.IdxSize == 0 {
			continue
		}
		if err := fetchSegmentFile(ctx, peer, dir, p, info.Base, log.SegmentLog, info.LogSize); err != nil {
			return written, err
		}
		written++
		if err := fetchSegmentFile(ctx, peer, dir, p, info.Base, log.SegmentIdx, info.IdxSize); err != nil {
			return written, err
		}
		written++
	}
	return written, nil
}

// fetchSegmentFile drives the chunked-read loop for one segment
// file: reads bounded chunks from peer, writes each to the local
// file, stops when the listed size is reached or the donor returns
// fewer bytes than requested (end of file).
func fetchSegmentFile(ctx context.Context, peer PartitionSnapshotter, dir string, p proto.PartitionRef, base int64, kind log.SegmentKind, listedSize int64) error {
	path := filepath.Join(dir, storage.SegmentFileName(base, kind))
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("embed: create %s: %w", path, err)
	}
	defer f.Close()

	wireKind := proto.SegmentLog
	if kind == log.SegmentIdx {
		wireKind = proto.SegmentIdx
	}

	var offset int64
	for offset < listedSize {
		want := listedSize - offset
		if want > int64(bootstrapChunkSize) {
			want = int64(bootstrapChunkSize)
		}
		chunk, err := peer.FetchSegmentChunk(ctx, p, base, wireKind, offset, int32(want))
		if err != nil {
			return fmt.Errorf("embed: fetch chunk @%d of %s: %w", offset, path, err)
		}
		if len(chunk) == 0 {
			// Donor reached EOF before listedSize. Stop without
			// erroring — the recipient sees a slightly shorter
			// prefix than the listing claimed, which is harmless if
			// the donor's segment was truncated by retention or
			// compaction between list and fetch.
			break
		}
		if _, err := f.Write(chunk); err != nil {
			return fmt.Errorf("embed: write %s: %w", path, err)
		}
		offset += int64(len(chunk))
	}
	return f.Sync()
}

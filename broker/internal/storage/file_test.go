package storage

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jedi-knights/holocron/broker/internal/log"
	"github.com/jedi-knights/holocron/proto"
)

func TestFileStore_AppendThenReadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	ctx := context.Background()
	p := proto.PartitionRef{Topic: "t", Index: 0}
	for i := range 5 {
		off, err := s.Append(ctx, p, proto.Record{Value: []byte{byte('a' + i)}})
		if err != nil {
			t.Fatal(err)
		}
		if off != int64(i) {
			t.Fatalf("offset %d: got %d", i, off)
		}
	}

	got, err := s.Read(ctx, p, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 5 {
		t.Fatalf("got %d records, want 5", len(got))
	}
}

func TestFileStore_PersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	p := proto.PartitionRef{Topic: "t", Index: 0}

	{
		s, _ := NewFileStore(dir)
		for i := range 10 {
			_, _ = s.Append(ctx, p, proto.Record{Value: []byte{byte('a' + i)}})
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}

	s, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	hw, err := s.HighWater(ctx, p)
	if err != nil {
		t.Fatal(err)
	}
	if hw != 10 {
		t.Fatalf("hw after reopen: got %d want 10", hw)
	}
	got, err := s.Read(ctx, p, 5, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 5 {
		t.Fatalf("got %d records, want 5", len(got))
	}
	if got[0].Offset != 5 {
		t.Fatalf("first offset: got %d want 5", got[0].Offset)
	}
}

func TestFileStore_PartitionsAreIsolatedOnDisk(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewFileStore(dir)
	defer s.Close()
	ctx := context.Background()

	a := proto.PartitionRef{Topic: "t", Index: 0}
	b := proto.PartitionRef{Topic: "t", Index: 1}
	if _, err := s.Append(ctx, a, proto.Record{Value: []byte("a")}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append(ctx, b, proto.Record{Value: []byte("b")}); err != nil {
		t.Fatal(err)
	}
	hwA, _ := s.HighWater(ctx, a)
	hwB, _ := s.HighWater(ctx, b)
	if hwA != 1 || hwB != 1 {
		t.Fatalf("hw a=%d b=%d", hwA, hwB)
	}
}

// TestFileStore_ListAndFetchSegmentChunksRoundTrip proves a
// FileStore can hand off the file-level state of one partition to a
// fresh, empty FileStore via the chunked List+Fetch API the wire
// bootstrap is built on. The recipient reads the listed sizes,
// pulls each segment in chunks, and serves reads from offset zero
// — including records still in the donor's active segment.
func TestFileStore_ListAndFetchSegmentChunksRoundTrip(t *testing.T) {
	// Arrange — donor with small segmentSize so we definitely roll.
	donorDir := t.TempDir()
	donor, err := NewFileStore(donorDir, WithSegmentBytes(256))
	if err != nil {
		t.Fatal(err)
	}
	defer donor.Close()
	ctx := context.Background()
	p := proto.PartitionRef{Topic: "t", Index: 0}
	for i := range 103 {
		if _, err := donor.Append(ctx, p, proto.Record{
			Value: []byte("xxxxxxxxxxxxxxxxxxxxxxxxxx"),
			Key:   []byte{byte(i)},
		}); err != nil {
			t.Fatal(err)
		}
	}

	// Act — list the donor's segments, then chunk-fetch each file.
	infos, err := donor.ListSegments(ctx, p)
	if err != nil {
		t.Fatalf("ListSegments: %v", err)
	}
	if len(infos) < 2 {
		t.Fatalf("segments: got %d, want >= 2 (segments did not roll)", len(infos))
	}

	recipientDir := t.TempDir()
	dir := PartitionDir(recipientDir, p)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, info := range infos {
		for _, kind := range []log.SegmentKind{log.SegmentLog, log.SegmentIdx} {
			size := info.LogSize
			if kind == log.SegmentIdx {
				size = info.IdxSize
			}
			fpath := filepath.Join(dir, SegmentFileName(info.Base, kind))
			f, err := os.Create(fpath)
			if err != nil {
				t.Fatal(err)
			}
			var off int64
			for off < size {
				chunk, err := donor.FetchSegmentChunk(ctx, p, info.Base, kind, off, 64)
				if err != nil {
					t.Fatal(err)
				}
				if len(chunk) == 0 {
					break
				}
				if _, err := f.Write(chunk); err != nil {
					t.Fatal(err)
				}
				off += int64(len(chunk))
			}
			_ = f.Close()
		}
	}

	// Assert — fresh recipient reads every record (active included).
	recipient, err := NewFileStore(recipientDir, WithSegmentBytes(256))
	if err != nil {
		t.Fatal(err)
	}
	defer recipient.Close()
	got, err := recipient.Read(ctx, p, 0, 200)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != 103 {
		t.Fatalf("recipient saw %d records, want 103 — chunked transfer dropped records", len(got))
	}
	for i, r := range got {
		if r.Offset != int64(i) {
			t.Fatalf("record %d: offset=%d, want %d", i, r.Offset, i)
		}
	}
}

func TestFileStore_EnforceRetentionIsBestEffort(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewFileStore(dir, WithSegmentBytes(256))
	defer s.Close()
	ctx := context.Background()

	p := proto.PartitionRef{Topic: "t", Index: 0}
	for i := range 30 {
		_, err := s.Append(ctx, p, proto.Record{
			Timestamp: time.Now().Add(-2 * time.Hour).UnixNano(),
			Value:     []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"),
			Headers:   nil,
			Key:       nil,
			Offset:    int64(i),
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := s.EnforceRetention(time.Hour); err != nil {
		t.Fatal(err)
	}
}

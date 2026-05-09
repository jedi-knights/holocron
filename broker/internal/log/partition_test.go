package log

import (
	"bytes"
	"testing"

	"github.com/jedi-knights/holocron/proto"
)

func TestPartitionLog_AppendThenRead(t *testing.T) {
	dir := t.TempDir()
	p, err := OpenPartition(dir, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	for i := range 10 {
		if _, err := p.Append(proto.Record{Offset: int64(i), Timestamp: int64(i + 1), Value: []byte("x")}); err != nil {
			t.Fatal(err)
		}
	}
	if hw := p.HighWater(); hw != 10 {
		t.Fatalf("high water: got %d want 10", hw)
	}

	got, err := p.Read(0, 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 10 {
		t.Fatalf("got %d records, want 10", len(got))
	}
}

func TestPartitionLog_RollsAtSegmentCap(t *testing.T) {
	dir := t.TempDir()
	const segCap = 256
	p, err := OpenPartition(dir, segCap)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	body := bytes.Repeat([]byte("x"), 64)
	for i := range 50 {
		if _, err := p.Append(proto.Record{Offset: int64(i), Value: body}); err != nil {
			t.Fatal(err)
		}
	}
	bases, err := discoverSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(bases) < 2 {
		t.Fatalf("expected segment rollover, only %d segments", len(bases))
	}
}

func TestPartitionLog_ReadSpansSegments(t *testing.T) {
	dir := t.TempDir()
	p, _ := OpenPartition(dir, 256)
	defer p.Close()

	for i := range 30 {
		_, _ = p.Append(proto.Record{Offset: int64(i), Value: bytes.Repeat([]byte("y"), 64)})
	}
	got, err := p.Read(0, 30)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 30 {
		t.Fatalf("got %d, want 30", len(got))
	}
	for i, r := range got {
		if r.Offset != int64(i) {
			t.Fatalf("position %d: offset %d", i, r.Offset)
		}
	}
}

func TestPartitionLog_ReopensFromDisk(t *testing.T) {
	dir := t.TempDir()
	{
		p, _ := OpenPartition(dir, 256)
		for i := range 20 {
			_, _ = p.Append(proto.Record{Offset: int64(i), Timestamp: int64(i + 1), Value: bytes.Repeat([]byte("z"), 64)})
		}
		if err := p.Close(); err != nil {
			t.Fatal(err)
		}
	}

	p2, err := OpenPartition(dir, 256)
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close()

	if hw := p2.HighWater(); hw != 20 {
		t.Fatalf("high water after reopen: got %d want 20", hw)
	}
	got, err := p2.Read(15, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 5 {
		t.Fatalf("got %d records, want 5", len(got))
	}
}

func TestPartitionLog_TimeRetentionDropsOldSegments(t *testing.T) {
	dir := t.TempDir()
	p, _ := OpenPartition(dir, 256)
	defer p.Close()

	body := bytes.Repeat([]byte("x"), 64)
	for i := range 30 {
		_, _ = p.Append(proto.Record{Offset: int64(i), Timestamp: int64(i + 1), Value: body})
	}

	bases, _ := discoverSegments(dir)
	totalBefore := len(bases)
	if totalBefore < 2 {
		t.Fatalf("need at least 2 segments to test retention, got %d", totalBefore)
	}

	deleted, err := p.EnforceTimeRetention(1_000_000_000)
	if err != nil {
		t.Fatal(err)
	}
	if deleted == 0 {
		t.Fatal("expected at least one segment dropped")
	}
	if deleted >= totalBefore {
		t.Fatalf("dropped %d of %d segments — active segment must be retained", deleted, totalBefore)
	}
}

func TestPartitionLog_SizeRetentionDropsOldestSegments(t *testing.T) {
	// Arrange
	dir := t.TempDir()
	p, _ := OpenPartition(dir, 256)
	defer p.Close()

	body := bytes.Repeat([]byte("x"), 64)
	for i := range 30 {
		if _, err := p.Append(proto.Record{Offset: int64(i), Value: body}); err != nil {
			t.Fatal(err)
		}
	}
	bases, _ := discoverSegments(dir)
	if len(bases) < 3 {
		t.Fatalf("test needs at least 3 segments, got %d", len(bases))
	}

	// Act: cap the partition at the size of one segment plus the active segment.
	deleted, err := p.EnforceSizeRetention(512)
	if err != nil {
		t.Fatal(err)
	}

	// Assert
	if deleted == 0 {
		t.Fatal("expected at least one segment dropped")
	}
	if len(p.segments) < 1 {
		t.Fatal("active segment must be retained")
	}
}

func TestPartitionLog_SizeRetentionRespectsActiveSegment(t *testing.T) {
	// Arrange
	dir := t.TempDir()
	p, _ := OpenPartition(dir, 1<<20)
	defer p.Close()

	body := bytes.Repeat([]byte("y"), 64)
	for i := range 5 {
		if _, err := p.Append(proto.Record{Offset: int64(i), Value: body}); err != nil {
			t.Fatal(err)
		}
	}

	// Act: cap below the partition's actual size. Only sealed segments
	// can be deleted; with one (active) segment, none qualify.
	deleted, err := p.EnforceSizeRetention(1)
	if err != nil {
		t.Fatal(err)
	}

	// Assert
	if deleted != 0 {
		t.Fatalf("dropped %d segments; expected 0 because the active segment is unprunable", deleted)
	}
}

func TestPartitionLog_TimeRetentionKeepsRecentSegments(t *testing.T) {
	dir := t.TempDir()
	p, _ := OpenPartition(dir, 256)
	defer p.Close()

	body := bytes.Repeat([]byte("x"), 64)
	for i := range 20 {
		_, _ = p.Append(proto.Record{Offset: int64(i), Timestamp: int64(1_000_000 * (i + 1)), Value: body})
	}

	deleted, err := p.EnforceTimeRetention(0)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 0 {
		t.Fatalf("dropped %d segments with cutoff=0; expected 0", deleted)
	}
}

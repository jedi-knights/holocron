package log

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/jedi-knights/holocron/proto"
)

func TestSegment_AppendThenReadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s, err := createSegment(dir, 0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()

	for i := range 5 {
		_, err := s.append(proto.Record{Offset: int64(i), Timestamp: int64(i + 1), Value: []byte{byte('a' + i)}})
		if err != nil {
			t.Fatal(err)
		}
	}

	got, err := s.readFrom(0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 5 {
		t.Fatalf("got %d records, want 5", len(got))
	}
	for i, r := range got {
		if r.Offset != int64(i) || !bytes.Equal(r.Value, []byte{byte('a' + i)}) {
			t.Errorf("record %d mismatch: %+v", i, r)
		}
	}
}

func TestSegment_ReadFromMidOffset(t *testing.T) {
	dir := t.TempDir()
	s, _ := createSegment(dir, 100, 1<<20)
	defer s.close()
	for i := range 5 {
		_, _ = s.append(proto.Record{Offset: int64(100 + i), Value: []byte("x")})
	}

	got, err := s.readFrom(102, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d, want 3", len(got))
	}
	if got[0].Offset != 102 {
		t.Fatalf("first offset %d, want 102", got[0].Offset)
	}
}

func TestSegment_ReadBeyondHighWaterReturnsEmpty(t *testing.T) {
	dir := t.TempDir()
	s, _ := createSegment(dir, 0, 1<<20)
	defer s.close()
	_, _ = s.append(proto.Record{Offset: 0, Value: []byte("x")})

	got, err := s.readFrom(10, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("got %d, want 0", len(got))
	}
}

func TestSegment_ShouldRollAtSizeCap(t *testing.T) {
	dir := t.TempDir()
	const cap = 256
	s, _ := createSegment(dir, 0, cap)
	defer s.close()

	body := bytes.Repeat([]byte("x"), 64)
	for i := 0; !s.shouldRoll(); i++ {
		_, err := s.append(proto.Record{Offset: int64(i), Value: body})
		if err != nil {
			t.Fatal(err)
		}
		if i > 100 {
			t.Fatal("never rolled")
		}
	}
}

func TestSegment_SealPersistsIndex(t *testing.T) {
	dir := t.TempDir()
	s, _ := createSegment(dir, 0, 1<<20)

	for i := range 100 {
		_, _ = s.append(proto.Record{Offset: int64(i), Value: bytes.Repeat([]byte{byte('a' + i%26)}, 100)})
	}
	if err := s.seal(); err != nil {
		t.Fatal(err)
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}

	idxPath := filepath.Join(dir, segmentIndexName(0))
	stat, err := os.Stat(idxPath)
	if err != nil {
		t.Fatalf("index not persisted: %v", err)
	}
	if stat.Size() == 0 {
		t.Fatal("index file is empty")
	}
}

func TestSegment_RecoveryRebuildsState(t *testing.T) {
	dir := t.TempDir()

	{
		s, _ := createSegment(dir, 0, 1<<20)
		for i := range 50 {
			_, _ = s.append(proto.Record{Offset: int64(i), Timestamp: int64(i + 1), Value: []byte("x")})
		}
		if err := s.seal(); err != nil {
			t.Fatal(err)
		}
		_ = s.close()
	}

	got, err := openSegment(dir, 0, 1<<20, true)
	if err != nil {
		t.Fatal(err)
	}
	defer got.close()
	if got.highWater != 50 {
		t.Fatalf("high water: got %d want 50", got.highWater)
	}
	records, err := got.readFrom(25, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 10 {
		t.Fatalf("got %d records, want 10", len(records))
	}
	if records[0].Offset != 25 {
		t.Fatalf("first offset: got %d want 25", records[0].Offset)
	}
}

func TestSegment_RecoveryTruncatesTornTail(t *testing.T) {
	dir := t.TempDir()

	{
		s, _ := createSegment(dir, 0, 1<<20)
		for i := range 5 {
			_, _ = s.append(proto.Record{Offset: int64(i), Value: []byte("hello")})
		}
		if err := s.seal(); err != nil {
			t.Fatal(err)
		}
		_ = s.close()
	}

	logPath := filepath.Join(dir, segmentLogName(0))
	f, err := os.OpenFile(logPath, os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0, 0, 0, 8, 1, 2, 3, 4, 5, 6, 7, 8, 99, 99, 99, 99}); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	stat, _ := os.Stat(logPath)
	pretorn := stat.Size()

	got, err := openSegment(dir, 0, 1<<20, true)
	if err != nil {
		t.Fatal(err)
	}
	defer got.close()

	stat, _ = os.Stat(logPath)
	if stat.Size() >= pretorn {
		t.Fatalf("file size %d not truncated below %d", stat.Size(), pretorn)
	}
	if got.highWater != 5 {
		t.Fatalf("high water: got %d want 5", got.highWater)
	}
}

func TestSegment_RemoveDeletesFiles(t *testing.T) {
	dir := t.TempDir()
	s, _ := createSegment(dir, 0, 1<<20)
	_, _ = s.append(proto.Record{Offset: 0, Value: []byte("x")})
	_ = s.seal()
	_ = s.close()

	if err := s.remove(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, segmentLogName(0))); !os.IsNotExist(err) {
		t.Fatal(".log not deleted")
	}
	if _, err := os.Stat(filepath.Join(dir, segmentIndexName(0))); !os.IsNotExist(err) {
		t.Fatal(".idx not deleted")
	}
}

func TestParseBaseOffset(t *testing.T) {
	got, err := parseBaseOffset("00000000000000000123.log")
	if err != nil {
		t.Fatal(err)
	}
	if got != 123 {
		t.Fatalf("got %d want 123", got)
	}
}

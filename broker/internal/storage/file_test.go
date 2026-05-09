package storage

import (
	"context"
	"testing"
	"time"

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

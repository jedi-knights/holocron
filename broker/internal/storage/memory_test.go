package storage

import (
	"context"
	"errors"
	"testing"

	"github.com/jedi-knights/holocron/proto"
)

func TestMemoryStore_AppendAssignsDenseOffsets(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()
	p := proto.PartitionRef{Topic: "t", Index: 0}

	for i := range 5 {
		got, err := s.Append(ctx, p, proto.Record{Value: []byte("x")})
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		if got != int64(i) {
			t.Fatalf("offset %d: got %d", i, got)
		}
	}

	hw, err := s.HighWater(ctx, p)
	if err != nil {
		t.Fatal(err)
	}
	if hw != 5 {
		t.Fatalf("high water: got %d, want 5", hw)
	}
}

func TestMemoryStore_AppendStampsTimestampWhenZero(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()
	p := proto.PartitionRef{Topic: "t", Index: 0}

	if _, err := s.Append(ctx, p, proto.Record{}); err != nil {
		t.Fatal(err)
	}
	got, err := s.Read(ctx, p, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Timestamp == 0 {
		t.Fatalf("expected stamped timestamp, got %+v", got)
	}
}

func TestMemoryStore_ReadBeyondHighWaterReturnsEmpty(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()
	p := proto.PartitionRef{Topic: "t", Index: 0}

	if _, err := s.Append(ctx, p, proto.Record{Value: []byte("a")}); err != nil {
		t.Fatal(err)
	}
	got, err := s.Read(ctx, p, 5, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty, got %d records", len(got))
	}
}

func TestMemoryStore_ReadNegativeOffsetIsAnError(t *testing.T) {
	s := NewMemoryStore()
	if _, err := s.Read(context.Background(), proto.PartitionRef{Topic: "t"}, -1, 10); err == nil {
		t.Fatal("expected error on negative offset")
	}
}

func TestMemoryStore_PartitionsAreIndependent(t *testing.T) {
	s := NewMemoryStore()
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
		t.Fatalf("partition water: a=%d b=%d", hwA, hwB)
	}
}

func TestMemoryStore_RespectsContext(t *testing.T) {
	s := NewMemoryStore()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Append(ctx, proto.PartitionRef{Topic: "t"}, proto.Record{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

package topic

import (
	"errors"
	"testing"
)

func TestRegistry_CreateAndGet(t *testing.T) {
	r := NewRegistry()
	if err := r.Create(Spec{Name: "orders.placed", PartitionCount: 4}); err != nil {
		t.Fatal(err)
	}
	cfg, err := r.Get("orders.placed")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Name != "orders.placed" || cfg.PartitionCount != 4 {
		t.Fatalf("unexpected config: %+v", cfg)
	}
}

func TestRegistry_DuplicateRejected(t *testing.T) {
	r := NewRegistry()
	if err := r.Create(Spec{Name: "t", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	err := r.Create(Spec{Name: "t", PartitionCount: 1})
	if !errors.Is(err, ErrTopicExists) {
		t.Fatalf("expected ErrTopicExists, got %v", err)
	}
}

func TestRegistry_GetMissingReturnsSentinel(t *testing.T) {
	r := NewRegistry()
	_, err := r.Get("nope")
	if !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("expected ErrTopicNotFound, got %v", err)
	}
}

func TestRegistry_RejectsInvalidNames(t *testing.T) {
	r := NewRegistry()
	cases := []string{"", "has space", "bad/slash", string(make([]byte, 250))}
	for _, name := range cases {
		err := r.Create(Spec{Name: name, PartitionCount: 1})
		if !errors.Is(err, ErrInvalidName) {
			t.Errorf("name %q: expected ErrInvalidName, got %v", name, err)
		}
	}
}

func TestRegistry_RejectsZeroPartitions(t *testing.T) {
	r := NewRegistry()
	err := r.Create(Spec{Name: "t", PartitionCount: 0})
	if !errors.Is(err, ErrInvalidSpec) {
		t.Fatalf("expected ErrInvalidSpec, got %v", err)
	}
}

func TestRegistry_PartitionsForReturnsCount(t *testing.T) {
	r := NewRegistry()
	if err := r.Create(Spec{Name: "t", PartitionCount: 8}); err != nil {
		t.Fatal(err)
	}
	n, err := r.PartitionsFor("t")
	if err != nil {
		t.Fatal(err)
	}
	if n != 8 {
		t.Fatalf("got %d, want 8", n)
	}
}

// TestRegistry_UpdateConfigChangesRetentionAndSegment proves
// UpdateConfig modifies the soft knobs (retention, segment size)
// of an existing topic without affecting the partition count
// (which would break ordering invariants if changed).
func TestRegistry_UpdateConfigChangesRetentionAndSegment(t *testing.T) {
	// Arrange
	r := NewRegistry()
	if err := r.Create(Spec{
		Name:           "mut",
		PartitionCount: 4,
		RetentionMs:    1000,
		SegmentBytes:   512,
	}); err != nil {
		t.Fatal(err)
	}

	// Act — bump retention; leave segment alone via 0 sentinel.
	if err := r.UpdateConfig("mut", 5000, 0); err != nil {
		t.Fatalf("UpdateConfig: %v", err)
	}

	// Assert
	cfg, err := r.Get("mut")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RetentionMs != 5000 {
		t.Errorf("retention: got %d, want 5000", cfg.RetentionMs)
	}
	if cfg.SegmentBytes != 512 {
		t.Errorf("segment: got %d, want 512 (unchanged)", cfg.SegmentBytes)
	}
	if cfg.PartitionCount != 4 {
		t.Errorf("partitions: got %d, want 4 (immutable)", cfg.PartitionCount)
	}
}

// TestRegistry_UpdateConfigMissingReturnsSentinel proves the
// update path mirrors Get's not-found contract.
func TestRegistry_UpdateConfigMissingReturnsSentinel(t *testing.T) {
	r := NewRegistry()
	err := r.UpdateConfig("never-existed", 1000, 0)
	if !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("got %v, want ErrTopicNotFound", err)
	}
}

// TestRegistry_DeleteRemovesTopic proves Delete makes a previously
// created topic invisible to subsequent Get / PartitionsFor calls.
func TestRegistry_DeleteRemovesTopic(t *testing.T) {
	// Arrange
	r := NewRegistry()
	if err := r.Create(Spec{Name: "ephemeral", PartitionCount: 2}); err != nil {
		t.Fatal(err)
	}

	// Act
	if err := r.Delete("ephemeral"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// Assert
	if _, err := r.Get("ephemeral"); !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("after Delete, Get returned %v, want ErrTopicNotFound", err)
	}
	if err := r.Create(Spec{Name: "ephemeral", PartitionCount: 1}); err != nil {
		t.Errorf("recreate after Delete failed: %v", err)
	}
}

// TestRegistry_DeleteMissingReturnsSentinel proves deleting an
// unknown topic returns ErrTopicNotFound rather than silently
// succeeding — mirrors the Get/PartitionsFor convention.
func TestRegistry_DeleteMissingReturnsSentinel(t *testing.T) {
	r := NewRegistry()
	err := r.Delete("never-existed")
	if !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("got %v, want ErrTopicNotFound", err)
	}
}

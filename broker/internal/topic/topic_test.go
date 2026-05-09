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

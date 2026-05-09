package streams_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/jedi-knights/holocron/broker/embed"
	"github.com/jedi-knights/holocron/proto"
	"github.com/jedi-knights/holocron/sdk"
	"github.com/jedi-knights/holocron/streams"
)

// TestPartitionedStore_PerPartitionIsolation proves that each partition
// has its own substore — putting under the same key on different
// partitions creates two distinct entries that don't collide.
//
// This is the foundation of per-partition state stores: cross-partition
// operations on a shared key must not see each other's writes.
func TestPartitionedStore_PerPartitionIsolation(t *testing.T) {
	// Arrange
	ps := streams.NewPartitionedStore(streams.NewMemoryStoreFactory())

	// Act
	ps.For(0).Put([]byte("k"), []byte("v0"))
	ps.For(1).Put([]byte("k"), []byte("v1"))

	// Assert
	v0, ok0 := ps.For(0).Get([]byte("k"))
	if !ok0 || !bytes.Equal(v0, []byte("v0")) {
		t.Errorf("partition 0: got (%q, %v), want (v0, true)", v0, ok0)
	}
	v1, ok1 := ps.For(1).Get([]byte("k"))
	if !ok1 || !bytes.Equal(v1, []byte("v1")) {
		t.Errorf("partition 1: got (%q, %v), want (v1, true)", v1, ok1)
	}
}

// TestPartitionedStore_AggregatedGet proves the inspection-side Get
// surfaces a value from any substore, supporting external callers that
// don't know which partition holds a key (typical for tests and
// debug-only inspection).
func TestPartitionedStore_AggregatedGet(t *testing.T) {
	// Arrange
	ps := streams.NewPartitionedStore(streams.NewMemoryStoreFactory())
	ps.For(2).Put([]byte("alpha"), []byte("A"))
	ps.For(5).Put([]byte("bravo"), []byte("B"))

	// Act — aggregated lookup; partition unknown to caller.
	a, okA := ps.Get([]byte("alpha"))
	b, okB := ps.Get([]byte("bravo"))
	missing, okMissing := ps.Get([]byte("none"))

	// Assert
	if !okA || !bytes.Equal(a, []byte("A")) {
		t.Errorf("alpha: got (%q, %v), want (A, true)", a, okA)
	}
	if !okB || !bytes.Equal(b, []byte("B")) {
		t.Errorf("bravo: got (%q, %v), want (B, true)", b, okB)
	}
	if okMissing {
		t.Errorf("missing key: got %q, want absent", missing)
	}
}

// TestPartitionedStore_AggregatedRange iterates every (key, value) pair
// across every substore. Order across partitions is undefined, so the
// test gathers into a set and checks membership.
func TestPartitionedStore_AggregatedRange(t *testing.T) {
	// Arrange
	ps := streams.NewPartitionedStore(streams.NewMemoryStoreFactory())
	ps.For(0).Put([]byte("a"), []byte("1"))
	ps.For(0).Put([]byte("b"), []byte("2"))
	ps.For(1).Put([]byte("c"), []byte("3"))

	// Act
	got := map[string]string{}
	ps.Range(func(k, v []byte) bool {
		got[string(k)] = string(v)
		return true
	})

	// Assert
	want := map[string]string{"a": "1", "b": "2", "c": "3"}
	if len(got) != len(want) {
		t.Fatalf("got %d entries, want %d (got=%v)", len(got), len(want), got)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("key %q: got %q, want %q", k, got[k], v)
		}
	}
}

// TestChangelogStore_PartitionsAreIsolated proves a per-partition
// changelog: opening one store on partition 0 of a multi-partition
// changelog topic and another on partition 1 means writes to either
// don't surface in the other's replay. Without this, a topology's
// per-partition state shares one global changelog and rebalancing a
// partition between members causes state from other partitions to
// leak into the new owner's view.
func TestChangelogStore_PartitionsAreIsolated(t *testing.T) {
	// Arrange — broker with a 2-partition changelog topic.
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "counts-changelog", PartitionCount: 2}); err != nil {
		t.Fatal(err)
	}
	tr := b.Transport()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Donor 0 writes only on partition 0; donor 1 only on partition 1.
	donor0, err := streams.OpenChangelogStorePartition(ctx, tr, "counts", 0)
	if err != nil {
		t.Fatal(err)
	}
	donor0.Put([]byte("alpha"), []byte("0"))
	donor1, err := streams.OpenChangelogStorePartition(ctx, tr, "counts", 1)
	if err != nil {
		t.Fatal(err)
	}
	donor1.Put([]byte("bravo"), []byte("1"))
	_ = donor0.Close()
	_ = donor1.Close()

	// Act — fresh stores for each partition replay just their own
	// partition's history.
	r0, err := streams.OpenChangelogStorePartition(ctx, tr, "counts", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer r0.Close()
	r1, err := streams.OpenChangelogStorePartition(ctx, tr, "counts", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer r1.Close()

	// Assert — each partition sees only its own writes.
	if v, ok := r0.Get([]byte("alpha")); !ok || !bytes.Equal(v, []byte("0")) {
		t.Errorf("partition 0: alpha=(%q,%v), want (0,true)", v, ok)
	}
	if _, ok := r0.Get([]byte("bravo")); ok {
		t.Errorf("partition 0 saw bravo — partitions are not isolated")
	}
	if v, ok := r1.Get([]byte("bravo")); !ok || !bytes.Equal(v, []byte("1")) {
		t.Errorf("partition 1: bravo=(%q,%v), want (1,true)", v, ok)
	}
	if _, ok := r1.Get([]byte("alpha")); ok {
		t.Errorf("partition 1 saw alpha — partitions are not isolated")
	}
}

// TestTopology_StateIsPartitionScoped proves a multi-partition topology
// places state in per-partition substores rather than a single shared
// store. With 4 partitions and 4 keys partitioned by hash, the
// PartitionedStore must end up with at least 2 distinct substores —
// the smoking-gun signal that state is actually partition-scoped.
//
// Without this, every key would land in a single shared MemoryStore,
// and the test would observe a single substore (Partitions() len 1).
func TestTopology_StateIsPartitionScoped(t *testing.T) {
	// Arrange
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "input", PartitionCount: 4}); err != nil {
		t.Fatal(err)
	}
	if err := b.CreateTopic(embed.TopicSpec{Name: "output", PartitionCount: 4}); err != nil {
		t.Fatal(err)
	}

	top, err := streams.New(b.Transport(), streams.WithMaxTasks(4))
	if err != nil {
		t.Fatal(err)
	}
	top.Stream("input").GroupByKey().Count("by-key").To("output")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := top.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer top.Stop()

	// Wait for the four tasks to settle on stable assignments before
	// producing — otherwise rebalance churn confuses partition routing.
	time.Sleep(500 * time.Millisecond)

	// Act — produce records across 4 distinct keys; the sticky default
	// partitioner should distribute them across partitions.
	prod, err := sdk.NewProducer(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	for _, key := range []string{"alpha", "bravo", "charlie", "delta"} {
		if _, err := prod.Send(ctx, "input", proto.Record{Key: []byte(key)}); err != nil {
			t.Fatal(err)
		}
	}

	// Wait for all 4 records to flow through the pipeline.
	if got := collect(t, b, "output", 4, 3*time.Second); len(got) < 4 {
		t.Fatalf("output records: got %d, want >= 4", len(got))
	}

	// Assert — at least 2 distinct partitions hold state. With shared
	// stores (the pre-batch-21 design) this would always be 1.
	store := top.Store("by-key")
	parts := store.Partitions()
	if len(parts) < 2 {
		t.Fatalf("Partitions(): got %d distinct partitions, want >= 2 — state appears shared, not partition-scoped (parts=%v)", len(parts), parts)
	}

	// Aggregated lookup still works — each key counted at least once.
	for _, key := range []string{"alpha", "bravo", "charlie", "delta"} {
		v, ok := store.Get([]byte(key))
		if !ok {
			t.Errorf("key %q absent from aggregated view", key)
			continue
		}
		if streams.DecodeCount(v) < 1 {
			t.Errorf("key %q count: got %d, want >= 1", key, streams.DecodeCount(v))
		}
	}
}

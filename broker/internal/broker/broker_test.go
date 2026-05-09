package broker

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jedi-knights/holocron/broker/internal/storage"
	"github.com/jedi-knights/holocron/broker/internal/topic"
	"github.com/jedi-knights/holocron/proto"
)

func newTestBroker(t *testing.T, name string, partitions int32) (*Broker, proto.PartitionRef) {
	t.Helper()
	store := storage.NewMemoryStore()
	registry := topic.NewRegistry()
	if err := registry.Create(topic.Spec{Name: name, PartitionCount: partitions}); err != nil {
		t.Fatal(err)
	}
	b := New(store, registry)
	return b, proto.PartitionRef{Topic: name, Index: 0}
}

func TestBroker_PublishAssignsOffsets(t *testing.T) {
	b, p := newTestBroker(t, "t", 1)
	ctx := context.Background()
	for i := range 3 {
		off, err := b.Publish(ctx, p, proto.Record{Value: []byte("x")})
		if err != nil {
			t.Fatal(err)
		}
		if off != int64(i) {
			t.Fatalf("offset %d: got %d", i, off)
		}
	}
}

// TestBroker_DedupEvictsStaleProducerEntries proves the per-broker
// dedup table prunes entries for producers that have been silent
// longer than the configured TTL. Without eviction the map would
// grow unbounded as producers come and go.
//
// The test sets a very short TTL, publishes one record under a
// producer ID, sleeps past the TTL, then publishes a fresh
// sequence-zero record under the same producer ID. Without
// eviction the broker would still see the original entry and
// dedup the new write (since seq 0 <= the previously-stored
// seq 0); with eviction the entry is pruned and the new record
// lands at a fresh offset.
func TestBroker_DedupEvictsStaleProducerEntries(t *testing.T) {
	// Arrange — broker with a short dedup TTL.
	store := storage.NewMemoryStore()
	registry := topic.NewRegistry()
	if err := registry.Create(topic.Spec{Name: "t", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	b := New(store, registry, WithDedupTTL(50*time.Millisecond))
	ctx := context.Background()
	p := proto.PartitionRef{Topic: "t", Index: 0}

	withProducer := func(id string, seq uint64, value string) proto.Record {
		var seqBytes [8]byte
		for i := 7; i >= 0; i-- {
			seqBytes[i] = byte(seq)
			seq >>= 8
		}
		return proto.Record{
			Value: []byte(value),
			Headers: []proto.Header{
				{Key: proto.HeaderProducerID, Value: []byte(id)},
				{Key: proto.HeaderProducerSeq, Value: seqBytes[:]},
			},
		}
	}

	// Act — first publish.
	if _, err := b.Publish(ctx, p, withProducer("producer-A", 0, "first")); err != nil {
		t.Fatal(err)
	}

	// Sleep past the TTL so the entry is stale.
	time.Sleep(80 * time.Millisecond)

	// A fresh seq=0 from the same producer should land at a NEW
	// offset (the prior entry was pruned, so the broker treats
	// this as a brand-new producer).
	off, err := b.Publish(ctx, p, withProducer("producer-A", 0, "second"))
	if err != nil {
		t.Fatal(err)
	}
	if off != 1 {
		t.Errorf("post-eviction publish: got offset %d, want 1 (eviction failed — record was deduped)", off)
	}
}

// TestBroker_PublishDeduplicatesRetriedSequence proves that a
// producer retrying a Publish with the same (producer-id, sequence)
// after a successful append doesn't double-store the record. The
// broker recognizes the duplicate via the producer-id and sequence
// headers, returns the original offset, and leaves the partition
// unchanged.
//
// Without this, an at-least-once retry path would surface as
// duplicates downstream — the central correctness gap producer
// idempotency exists to close.
func TestBroker_PublishDeduplicatesRetriedSequence(t *testing.T) {
	// Arrange
	b, p := newTestBroker(t, "t", 1)
	ctx := context.Background()
	withProducer := func(id string, seq uint64, value string) proto.Record {
		var seqBytes [8]byte
		for i := 7; i >= 0; i-- {
			seqBytes[i] = byte(seq)
			seq >>= 8
		}
		return proto.Record{
			Value: []byte(value),
			Headers: []proto.Header{
				{Key: proto.HeaderProducerID, Value: []byte(id)},
				{Key: proto.HeaderProducerSeq, Value: seqBytes[:]},
			},
		}
	}

	// Act — first publish at seq 0 succeeds and gets offset 0.
	off0, err := b.Publish(ctx, p, withProducer("producer-A", 0, "first"))
	if err != nil {
		t.Fatal(err)
	}
	if off0 != 0 {
		t.Fatalf("first publish offset: got %d, want 0", off0)
	}

	// Retry the same (producer-id, seq) with the SAME payload — the
	// broker should detect the duplicate and return the original offset.
	off0Retry, err := b.Publish(ctx, p, withProducer("producer-A", 0, "first"))
	if err != nil {
		t.Fatalf("retry publish: %v", err)
	}
	if off0Retry != 0 {
		t.Errorf("retry publish offset: got %d, want 0 (dedup should return original)", off0Retry)
	}

	// A new sequence (1) lands at offset 1.
	off1, err := b.Publish(ctx, p, withProducer("producer-A", 1, "second"))
	if err != nil {
		t.Fatal(err)
	}
	if off1 != 1 {
		t.Fatalf("seq 1 publish offset: got %d, want 1", off1)
	}

	// Assert — only two records actually persisted: offsets 0 and 1.
	got, err := b.Read(ctx, p, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("partition contents: got %d records, want 2 (dedup failed)", len(got))
	}
	if string(got[0].Value) != "first" || string(got[1].Value) != "second" {
		t.Errorf("partition contents in unexpected order: %q, %q", got[0].Value, got[1].Value)
	}
}

func TestBroker_RejectsUnknownTopic(t *testing.T) {
	store := storage.NewMemoryStore()
	registry := topic.NewRegistry()
	b := New(store, registry)
	_, err := b.Publish(context.Background(), proto.PartitionRef{Topic: "nope", Index: 0}, proto.Record{})
	if err == nil {
		t.Fatal("expected error for unknown topic")
	}
}

func TestBroker_RejectsOutOfRangePartition(t *testing.T) {
	b, _ := newTestBroker(t, "t", 2)
	_, err := b.Publish(context.Background(), proto.PartitionRef{Topic: "t", Index: 5}, proto.Record{})
	if err == nil {
		t.Fatal("expected error for out-of-range partition")
	}
}

func TestBroker_SubscribeReceivesLiveRecords(t *testing.T) {
	b, p := newTestBroker(t, "t", 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	ch, err := b.Subscribe(ctx, p, 0)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := b.Publish(ctx, p, proto.Record{Value: []byte("hello")}); err != nil {
		t.Fatal(err)
	}

	select {
	case r, ok := <-ch:
		if !ok {
			t.Fatal("channel closed before record arrived")
		}
		if string(r.Value) != "hello" {
			t.Fatalf("got %q, want hello", r.Value)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for record")
	}
}

func TestBroker_SubscribeReplaysHistory(t *testing.T) {
	b, p := newTestBroker(t, "t", 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	for i := range 3 {
		if _, err := b.Publish(ctx, p, proto.Record{Value: []byte{byte('a' + i)}}); err != nil {
			t.Fatal(err)
		}
	}

	ch, err := b.Subscribe(ctx, p, 0)
	if err != nil {
		t.Fatal(err)
	}

	for i := range 3 {
		select {
		case r := <-ch:
			if r.Offset != int64(i) {
				t.Fatalf("record %d: got offset %d", i, r.Offset)
			}
		case <-ctx.Done():
			t.Fatalf("timed out at record %d", i)
		}
	}
}

func TestBroker_FanOutsToMultipleSubscribers(t *testing.T) {
	b, p := newTestBroker(t, "t", 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	const subs = 3
	chans := make([]<-chan proto.Record, 0, subs)
	for range subs {
		ch, err := b.Subscribe(ctx, p, 0)
		if err != nil {
			t.Fatal(err)
		}
		chans = append(chans, ch)
	}

	if _, err := b.Publish(ctx, p, proto.Record{Value: []byte("broadcast")}); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(subs)
	for _, ch := range chans {
		go func(ch <-chan proto.Record) {
			defer wg.Done()
			select {
			case r := <-ch:
				if string(r.Value) != "broadcast" {
					t.Errorf("got %q", r.Value)
				}
			case <-ctx.Done():
				t.Error("subscriber timed out")
			}
		}(ch)
	}
	wg.Wait()
}

func TestBroker_SubscribeFromOffsetSkipsEarlier(t *testing.T) {
	b, p := newTestBroker(t, "t", 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	for range 5 {
		if _, err := b.Publish(ctx, p, proto.Record{Value: []byte("x")}); err != nil {
			t.Fatal(err)
		}
	}

	ch, err := b.Subscribe(ctx, p, 3)
	if err != nil {
		t.Fatal(err)
	}

	for i := 3; i < 5; i++ {
		select {
		case r := <-ch:
			if r.Offset != int64(i) {
				t.Fatalf("got offset %d, want %d", r.Offset, i)
			}
		case <-ctx.Done():
			t.Fatalf("timed out at %d", i)
		}
	}
}

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

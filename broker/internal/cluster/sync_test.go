package cluster_test

import (
	"context"
	"testing"
	"time"

	"github.com/jedi-knights/holocron/broker/embed"
	"github.com/jedi-knights/holocron/broker/internal/cluster"
	"github.com/jedi-knights/holocron/broker/internal/storage"
	"github.com/jedi-knights/holocron/proto"
	"github.com/jedi-knights/holocron/sdk"
)

// TestSyncPartitionFromPeer_FillsEmptyLocalStore proves the Stage 9
// milestone-3 sync helper streams records from a peer's partition
// into a fresh local store via the wire-protocol Subscribe path —
// no segment-file copying, no Raft involvement. The local store
// ends up holding the same record set as the peer at the moment of
// the call.
//
// This is the read-only building block; milestone 4 wires it into
// the broker's Raft post-join hook so a fresh follower's local
// store catches up before the Apply pump starts.
func TestSyncPartitionFromPeer_FillsEmptyLocalStore(t *testing.T) {
	// Arrange — donor broker with 5 records on partition 0.
	donor := embed.NewMemory()
	defer donor.Close()
	if err := donor.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	prod, err := sdk.NewProducer(donor.Transport())
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := 0; i < 5; i++ {
		if _, err := prod.Send(ctx, "events", proto.Record{Value: []byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}

	// Act — sync into a fresh recipient store.
	recipient := storage.NewMemoryStore()
	pref := proto.PartitionRef{Topic: "events", Index: 0}
	peer := donor.Transport().(cluster.SyncPeer)
	appended, err := cluster.SyncPartitionFromPeer(ctx, peer, recipient, pref)
	if err != nil {
		t.Fatalf("SyncPartitionFromPeer: %v", err)
	}

	// Assert — recipient holds all 5 records in order.
	if appended != 5 {
		t.Errorf("appended count: got %d, want 5", appended)
	}
	hw, err := recipient.HighWater(ctx, pref)
	if err != nil {
		t.Fatal(err)
	}
	if hw != 5 {
		t.Errorf("recipient high-water: got %d, want 5", hw)
	}
	got, err := recipient.Read(ctx, pref, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 5 {
		t.Fatalf("recipient records: got %d, want 5", len(got))
	}
	for i, r := range got {
		if r.Offset != int64(i) {
			t.Errorf("record %d offset: got %d, want %d", i, r.Offset, i)
		}
		if len(r.Value) != 1 || r.Value[0] != byte(i) {
			t.Errorf("record %d value: got %v, want %v", i, r.Value, []byte{byte(i)})
		}
	}
}

// TestSyncPartitionFromPeer_NoOpWhenAlreadyCaughtUp proves the
// helper short-circuits when the local store's high-water already
// matches (or exceeds) the peer's. Useful for the M4 join hook
// where the local store may have been populated by an earlier
// sync attempt or a prior incarnation of the broker.
func TestSyncPartitionFromPeer_NoOpWhenAlreadyCaughtUp(t *testing.T) {
	donor := embed.NewMemory()
	defer donor.Close()
	if err := donor.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	prod, err := sdk.NewProducer(donor.Transport())
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for i := 0; i < 3; i++ {
		if _, err := prod.Send(ctx, "events", proto.Record{Value: []byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}

	// Pre-populate recipient to match donor's high-water.
	recipient := storage.NewMemoryStore()
	pref := proto.PartitionRef{Topic: "events", Index: 0}
	for i := 0; i < 3; i++ {
		if _, err := recipient.Append(ctx, pref, proto.Record{Value: []byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}

	peer := donor.Transport().(cluster.SyncPeer)
	appended, err := cluster.SyncPartitionFromPeer(ctx, peer, recipient, pref)
	if err != nil {
		t.Fatalf("SyncPartitionFromPeer: %v", err)
	}
	if appended != 0 {
		t.Errorf("appended count: got %d, want 0 (already caught up)", appended)
	}
}

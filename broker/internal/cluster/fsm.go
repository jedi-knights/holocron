package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/hashicorp/raft"

	"github.com/jedi-knights/holocron/broker/internal/storage"
	"github.com/jedi-knights/holocron/broker/internal/topic"
	"github.com/jedi-knights/holocron/proto"
)

// FSM is the Raft state machine for a holocron broker. Apply translates
// committed Raft log entries into operations on the local Store and
// Registry. Two-thirds of the broker's state ends up in the FSM:
// partition data (records), and topic metadata.
//
// Consumer-group offsets are deliberately *not* replicated through Raft
// in Stage 5 — they keep their JSON-file persistence. The Kafka trick of
// using an internal compacted topic for offsets is the natural follow-on
// once the FSM is in place; tracked in TODO.md.
type FSM struct {
	store    storage.Store
	registry *topic.Registry

	mu     sync.RWMutex
	closed bool
}

// NewFSM returns an FSM bound to the given store and registry.
func NewFSM(store storage.Store, registry *topic.Registry) *FSM {
	return &FSM{store: store, registry: registry}
}

// Apply implements raft.FSM. It is invoked on every node once a Raft log
// entry is committed by the cluster (majority quorum).
func (f *FSM) Apply(rl *raft.Log) any {
	kind, body, err := Decode(rl.Data)
	if err != nil {
		return err
	}
	switch kind {
	case CmdAppend:
		return f.applyAppend(body)
	case CmdCreateTopic:
		return f.applyCreateTopic(body)
	case CmdDeleteTopic:
		return f.applyDeleteTopic(body)
	case CmdUpdateTopicConfig:
		return f.applyUpdateTopicConfig(body)
	default:
		return fmt.Errorf("cluster: unknown command kind 0x%02x", byte(kind))
	}
}

func (f *FSM) applyUpdateTopicConfig(body []byte) any {
	cmd, err := DecodeUpdateTopicConfig(body)
	if err != nil {
		return err
	}
	return f.registry.UpdateConfig(cmd.Name, cmd.RetentionMs, cmd.SegmentBytes)
}

func (f *FSM) applyDeleteTopic(body []byte) any {
	cmd, err := DecodeDeleteTopic(body)
	if err != nil {
		return err
	}
	// Idempotent: if the topic is already gone (e.g. snapshot
	// restored a state that already lacks it), the registry returns
	// ErrTopicNotFound and we treat it as success.
	_ = f.registry.Delete(cmd.Name)
	return f.store.DeleteTopic(context.Background(), cmd.Name)
}

func (f *FSM) applyAppend(body []byte) any {
	cmd, err := DecodeAppend(body)
	if err != nil {
		return err
	}
	pref := proto.PartitionRef{Topic: cmd.Topic, Index: cmd.Partition}
	// Stage 9 dedup guard: a stamped Offset at or below the local
	// store's high-water means this record is already on disk —
	// either from a prior segment-sync or from a Raft replay
	// after restart. Skip the append but report success so the
	// Raft log advances. OffsetUnstamped (the milestone-1 default)
	// disables the guard so legacy commands behave as before.
	if cmd.Offset != OffsetUnstamped {
		hw, hwErr := f.store.HighWater(context.Background(), pref)
		if hwErr == nil && cmd.Offset < hw {
			return cmd.Offset
		}
	}
	off, err := f.store.Append(context.Background(), pref, cmd.Record)
	if err != nil {
		return err
	}
	return off
}

func (f *FSM) applyCreateTopic(body []byte) any {
	cmd, err := DecodeCreateTopic(body)
	if err != nil {
		return err
	}
	err = f.registry.Create(topic.Spec{
		Name:           cmd.Name,
		PartitionCount: cmd.PartitionCount,
		RetentionMs:    cmd.RetentionMs,
		SegmentBytes:   cmd.SegmentBytes,
	})
	// Idempotent for replays: a duplicate-topic error is fine after a
	// snapshot restore or replay because the topic already exists.
	if err != nil {
		if _, lookupErr := f.registry.Get(cmd.Name); lookupErr == nil {
			return nil
		}
		return err
	}
	return nil
}

// Snapshot implements raft.FSM. Captures the topic registry's current
// state so a follower restoring from this snapshot can serve metadata
// reads immediately. Records are *not* in the snapshot: the underlying
// file-backed Store is independently durable, and bootstrapping a
// brand-new follower from a leader's records is a separate
// data-dir-copy step (tracked in TODO.md as a follow-on).
//
// The lock is released before Persist runs — the captured slice is a
// detached copy, so concurrent Apply on the FSM can't disturb it.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return &fsmSnapshot{topics: f.registry.List()}, nil
}

// Restore implements raft.FSM. Replaces the registry's topic list with
// what the snapshot stream contains. The local Store is untouched —
// the records it already holds are the authoritative source for any
// (topic, partition) the snapshot mentions.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	body, err := io.ReadAll(rc)
	if err != nil {
		return fmt.Errorf("cluster: snapshot read: %w", err)
	}
	if len(body) == 0 {
		// Legacy (batch <17) snapshots carry no payload — leave the
		// registry as-is; it was either hydrated from disk metadata
		// or will be filled by future log entries.
		return nil
	}
	var topics []proto.TopicConfig
	if err := json.Unmarshal(body, &topics); err != nil {
		return fmt.Errorf("cluster: snapshot decode: %w", err)
	}
	f.registry.Hydrate(topics)
	return nil
}

// fsmSnapshot persists the topic-registry slice as a single JSON
// document on the snapshot sink. Idempotent — restoring the same
// snapshot twice is a no-op.
type fsmSnapshot struct {
	topics []proto.TopicConfig
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	body, err := json.Marshal(s.topics)
	if err != nil {
		_ = sink.Cancel()
		return err
	}
	if _, err := sink.Write(body); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}

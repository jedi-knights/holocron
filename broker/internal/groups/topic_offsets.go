package groups

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/jedi-knights/holocron/proto"
)

// DefaultOffsetsTopic is the internal topic name where commits live when
// using TopicOffsetStore. Single-partition for total ordering — same
// reason the schema registry's metadata topic is single-partition.
const DefaultOffsetsTopic = "__holocron_offsets"

// TopicAppender is the narrow interface TopicOffsetStore needs from a
// storage backend: append a record, read records back. The broker's
// storage.Store satisfies it; tests can pass a fake.
type TopicAppender interface {
	Append(ctx context.Context, p proto.PartitionRef, r proto.Record) (int64, error)
	Read(ctx context.Context, p proto.PartitionRef, fromOffset int64, maxRecords int) ([]proto.Record, error)
}

// TopicOffsetStore persists consumer-group commits as records on a
// holocron topic. State lives durably on the broker's own log; on
// startup the store replays the topic to rebuild the in-memory map.
//
// This is the same trick Kafka uses for `__consumer_offsets` and the
// trick the schema registry uses for `__holocron_schemas`. Combined
// with log compaction (Stage 9 follow-on landed alongside this) the
// topic stays bounded — only the latest commit per (group, topic,
// partition) survives.
type TopicOffsetStore struct {
	appender TopicAppender
	pref     proto.PartitionRef

	mu      sync.RWMutex
	entries map[string]int64
}

// OpenTopicOffsetStore returns a store backed by the given appender.
// topic is the internal topic name (typically DefaultOffsetsTopic). The
// store replays from offset 0 before returning so subsequent Lookup
// calls reflect the durable state.
//
// Caller is responsible for creating the topic ahead of time (single
// partition, compaction-enabled is recommended).
func OpenTopicOffsetStore(ctx context.Context, appender TopicAppender, topic string) (*TopicOffsetStore, error) {
	if appender == nil {
		return nil, errors.New("groups: TopicOffsetStore requires an appender")
	}
	if topic == "" {
		topic = DefaultOffsetsTopic
	}
	s := &TopicOffsetStore{
		appender: appender,
		pref:     proto.PartitionRef{Topic: topic, Index: 0},
		entries:  make(map[string]int64),
	}
	if err := s.replay(ctx); err != nil {
		return nil, fmt.Errorf("groups: replay offsets topic: %w", err)
	}
	return s, nil
}

func (s *TopicOffsetStore) replay(ctx context.Context) error {
	cursor := int64(0)
	for {
		records, err := s.appender.Read(ctx, s.pref, cursor, 256)
		if err != nil {
			return err
		}
		if len(records) == 0 {
			return nil
		}
		for _, r := range records {
			// Nil Value is the tombstone marker emitted by
			// DeleteGroup; treat it as "drop the entry" so a
			// deleted group's offsets don't resurface on a
			// restart-driven replay.
			if r.Value == nil {
				delete(s.entries, string(r.Key))
				continue
			}
			s.entries[string(r.Key)] = decodeOffsetValue(r.Value)
		}
		cursor = records[len(records)-1].Offset + 1
	}
}

// Commit writes the new offset as a record (key=group/topic/partition,
// value=8-byte BE offset) and updates the in-memory map.
func (s *TopicOffsetStore) Commit(group, topic string, partition int32, offset int64) error {
	key := []byte(joinKey(group, topic, partition))
	rec := proto.Record{Key: key, Value: encodeOffsetValue(offset)}
	if _, err := s.appender.Append(context.Background(), s.pref, rec); err != nil {
		return err
	}
	s.mu.Lock()
	s.entries[string(key)] = offset
	s.mu.Unlock()
	return nil
}

// Lookup returns the committed offset for the (group, topic, partition)
// triple, or NoOffset when uncommitted.
func (s *TopicOffsetStore) Lookup(group, topic string, partition int32) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if v, ok := s.entries[joinKey(group, topic, partition)]; ok {
		return v, nil
	}
	return NoOffset, nil
}

// List returns every (topic, partition, offset) entry committed under
// group. Order is unspecified.
func (s *TopicOffsetStore) List(group string) []OffsetEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return listEntries(s.entries, group)
}

// DeleteGroup writes a tombstone record (key = joinKey, Value = nil)
// for every (group, topic, partition) entry the group owns and drops
// the entries from the in-memory map. Replay treats nil-value records
// as deletions so the cleanup survives a broker restart.
func (s *TopicOffsetStore) DeleteGroup(group string) error {
	s.mu.Lock()
	keys := make([][]byte, 0)
	for k := range s.entries {
		if g, _, _ := splitKey(k); g == group {
			keys = append(keys, []byte(k))
		}
	}
	s.mu.Unlock()
	for _, k := range keys {
		rec := proto.Record{Key: k, Value: nil}
		if _, err := s.appender.Append(context.Background(), s.pref, rec); err != nil {
			return err
		}
	}
	s.mu.Lock()
	for _, k := range keys {
		delete(s.entries, string(k))
	}
	s.mu.Unlock()
	return nil
}

// Close is a no-op — the appender outlives the store.
func (s *TopicOffsetStore) Close() error { return nil }

func encodeOffsetValue(offset int64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(offset))
	return buf[:]
}

func decodeOffsetValue(b []byte) int64 {
	if len(b) != 8 {
		return NoOffset
	}
	return int64(binary.BigEndian.Uint64(b))
}

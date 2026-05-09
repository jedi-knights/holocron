package streams

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jedi-knights/holocron/proto"
	"github.com/jedi-knights/holocron/sdk"
)

// changelogReplayDeadline bounds how long the replay drain waits for an
// empty Poll before declaring the topic caught up. Identical reasoning
// to the schema registry's replay: the SDK's Poll blocks for the first
// record, so a short deadline is the floor on startup latency.
const changelogReplayDeadline = 200 * time.Millisecond

// ChangelogStore is a StateStore backed by a holocron topic. Every
// Put or Delete is written to a single partition of
// <name>-changelog as a record (Put = key+value, Delete = key+nil
// tombstone). On Open, the store replays just its own partition
// from offset 0 to rebuild in-memory state.
//
// Each ChangelogStore is scoped to one partition. Multi-partition
// topologies open one store per assigned partition so each
// partition's state survives restart on the same partition of the
// changelog topic, with no cross-partition leakage.
//
// Combined with broker-side log compaction (which keeps only the
// latest record per key), the changelog topic stays bounded — same
// trick used for consumer-group offsets and the schema registry.
//
// Topic creation is the caller's responsibility; the changelog topic
// must exist with at least (partition + 1) partitions before
// OpenChangelogStorePartition is called. Compaction is recommended
// to prevent unbounded growth.
type ChangelogStore struct {
	transport sdk.Transport
	topic     string
	partition int32

	mu      sync.RWMutex
	entries map[string][]byte
}

// OpenChangelogStorePartition replays the addressed partition of
// <name>-changelog into an in-memory map and returns a ready-to-use
// store. Records on other partitions of the changelog topic do not
// leak into this store's state — a topology with N source
// partitions opens N stores so each partition's state lives in its
// own topic partition.
func OpenChangelogStorePartition(ctx context.Context, transport sdk.Transport, name string, partition int) (*ChangelogStore, error) {
	if transport == nil {
		return nil, errors.New("streams: ChangelogStore requires a Transport")
	}
	if name == "" {
		return nil, errors.New("streams: ChangelogStore requires a name")
	}
	if partition < 0 {
		return nil, errors.New("streams: ChangelogStore requires partition >= 0")
	}
	s := &ChangelogStore{
		transport: transport,
		topic:     name + "-changelog",
		partition: int32(partition),
		entries:   make(map[string][]byte),
	}
	if err := s.replay(ctx); err != nil {
		return nil, fmt.Errorf("streams: replay changelog %q partition %d: %w", s.topic, partition, err)
	}
	return s, nil
}

func (s *ChangelogStore) replay(ctx context.Context) error {
	consumer, err := sdk.NewConsumer(s.transport)
	if err != nil {
		return err
	}
	defer consumer.Close()
	// Self-managed assignment so we read only our own partition,
	// not every partition the (default) Subscribe path would attach
	// to. Cross-partition records must not leak into this store.
	if err := consumer.Assign(ctx, proto.PartitionRef{Topic: s.topic, Index: s.partition}, 0); err != nil {
		return err
	}

	drainCtx, cancel := context.WithTimeout(ctx, changelogReplayDeadline)
	defer cancel()

	for {
		records, err := consumer.Poll(drainCtx, 256)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			return err
		}
		if len(records) == 0 {
			return nil
		}
		for _, r := range records {
			if r.Value == nil {
				delete(s.entries, string(r.Key))
			} else {
				cp := make([]byte, len(r.Value))
				copy(cp, r.Value)
				s.entries[string(r.Key)] = cp
			}
		}
	}
}

// Get returns the value for key, or (nil, false) if absent.
func (s *ChangelogStore) Get(key []byte) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.entries[string(key)]
	if !ok {
		return nil, false
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out, true
}

// Put writes a changelog record to this store's specific partition
// of the changelog topic AND updates the in-memory map. Transport
// errors are silently swallowed (state diverges from broker)
// — same at-least-once trade-off the in-memory store has against
// crashes.
func (s *ChangelogStore) Put(key, value []byte) {
	stored := make([]byte, len(value))
	copy(stored, value)

	s.mu.Lock()
	s.entries[string(key)] = stored
	s.mu.Unlock()

	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	valCopy := make([]byte, len(value))
	copy(valCopy, value)
	// Publish directly to the assigned partition so the record can't
	// land on a different partition's changelog and leak across
	// partitions on replay.
	_, _ = s.transport.Publish(context.Background(),
		proto.PartitionRef{Topic: s.topic, Index: s.partition},
		proto.Record{Key: keyCopy, Value: valCopy})
}

// Delete writes a tombstone (nil value) to this store's partition
// and removes the key from the in-memory map.
func (s *ChangelogStore) Delete(key []byte) {
	s.mu.Lock()
	delete(s.entries, string(key))
	s.mu.Unlock()

	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	_, _ = s.transport.Publish(context.Background(),
		proto.PartitionRef{Topic: s.topic, Index: s.partition},
		proto.Record{Key: keyCopy, Value: nil})
}

// Range iterates a snapshot of (key, value) pairs.
func (s *ChangelogStore) Range(fn func(key, value []byte) bool) {
	s.mu.RLock()
	snapshot := make([][2][]byte, 0, len(s.entries))
	for k, v := range s.entries {
		snapshot = append(snapshot, [2][]byte{[]byte(k), append([]byte(nil), v...)})
	}
	s.mu.RUnlock()
	for _, kv := range snapshot {
		if !fn(kv[0], kv[1]) {
			return
		}
	}
}

// Close is a no-op — the store doesn't own the Transport. Returns
// nil so callers can defer it uniformly.
func (s *ChangelogStore) Close() error { return nil }

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

// ChangelogStore is a StateStore backed by a holocron topic. Every Put
// or Delete is written to <name>-changelog as a record (Put = key+value,
// Delete = key+nil-value tombstone). On Open, the store replays the
// topic from offset 0 to rebuild in-memory state.
//
// Combined with broker-side log compaction (which keeps only the latest
// record per key), the changelog topic stays bounded — same trick used
// for consumer-group offsets and the schema registry. State persists
// across topology restarts.
//
// Topic creation is the caller's responsibility; the changelog topic
// must exist before OpenChangelogStore is called. For best results,
// create it with a single partition (so total ordering is preserved
// across keys) and broker-side compaction enabled.
type ChangelogStore struct {
	transport sdk.Transport
	topic     string
	producer  *sdk.Producer

	mu      sync.RWMutex
	entries map[string][]byte
}

// OpenChangelogStore replays <name>-changelog into an in-memory map and
// returns a ready-to-use store.
func OpenChangelogStore(ctx context.Context, transport sdk.Transport, name string) (*ChangelogStore, error) {
	if transport == nil {
		return nil, errors.New("streams: ChangelogStore requires a Transport")
	}
	if name == "" {
		return nil, errors.New("streams: ChangelogStore requires a name")
	}
	s := &ChangelogStore{
		transport: transport,
		topic:     name + "-changelog",
		entries:   make(map[string][]byte),
	}
	if err := s.replay(ctx); err != nil {
		return nil, fmt.Errorf("streams: replay changelog %q: %w", s.topic, err)
	}
	p, err := sdk.NewProducer(transport)
	if err != nil {
		return nil, fmt.Errorf("streams: changelog producer: %w", err)
	}
	s.producer = p
	return s, nil
}

func (s *ChangelogStore) replay(ctx context.Context) error {
	consumer, err := sdk.NewConsumer(s.transport)
	if err != nil {
		return err
	}
	defer consumer.Close()
	if err := consumer.Subscribe(ctx, s.topic, 0); err != nil {
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

// Put writes a changelog record AND updates the in-memory map. Producer
// errors are silently swallowed (state diverges from broker). Failures
// here are unlikely in a healthy system but represent an at-least-once
// gap if they happen — same trade-off as the in-memory store relative
// to crashes.
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
	_, _ = s.producer.Send(context.Background(), s.topic, proto.Record{
		Key:   keyCopy,
		Value: valCopy,
	})
}

// Delete writes a tombstone (nil value) and removes the key from the
// in-memory map.
func (s *ChangelogStore) Delete(key []byte) {
	s.mu.Lock()
	delete(s.entries, string(key))
	s.mu.Unlock()

	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	_, _ = s.producer.Send(context.Background(), s.topic, proto.Record{
		Key:   keyCopy,
		Value: nil,
	})
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

// Close releases the embedded Producer.
func (s *ChangelogStore) Close() error {
	if s.producer != nil {
		return s.producer.Close()
	}
	return nil
}

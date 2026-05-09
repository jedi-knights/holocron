package connect

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jedi-knights/holocron/proto"
	"github.com/jedi-knights/holocron/sdk"
)

// DefaultConnectOffsetsTopic is the broker topic where source-task
// offsets live when using TopicOffsetStore. Single-partition for total
// ordering — same shape as `__holocron_offsets` and `__holocron_schemas`.
const DefaultConnectOffsetsTopic = "__connect_offsets"

// OffsetStore persists a source connector's per-task offsets across
// worker restarts. The Worker calls Load when initializing each
// SourceTask (passing the result into Init's storedOffsets), and Save
// after the task's Commit signals records are durably published.
//
// V1 ships an in-memory implementation and a topic-backed one. The
// topic-backed one mirrors what schema registry and consumer-group
// offsets do: state lives on the broker's own log, the broker is its
// own metadata store.
type OffsetStore interface {
	// Save records offsets for (connector, taskIndex). Replaces any
	// previously saved offsets for that pair.
	Save(ctx context.Context, connector string, taskIndex int, offsets []map[string]any) error
	// Load returns the most recently saved offsets for the pair, or
	// nil + nil error if none exist.
	Load(ctx context.Context, connector string, taskIndex int) ([]map[string]any, error)
	Close() error
}

// MemoryOffsetStore is a non-durable OffsetStore. Useful for tests and
// for connector configurations that don't need restart resumption.
type MemoryOffsetStore struct {
	mu      sync.RWMutex
	entries map[string][]map[string]any
}

// NewMemoryOffsetStore returns an empty in-memory store.
func NewMemoryOffsetStore() *MemoryOffsetStore {
	return &MemoryOffsetStore{entries: make(map[string][]map[string]any)}
}

func memoryKey(connector string, taskIndex int) string {
	return fmt.Sprintf("%s/%d", connector, taskIndex)
}

// Save records offsets for (connector, taskIndex).
func (s *MemoryOffsetStore) Save(_ context.Context, connector string, taskIndex int, offsets []map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries[memoryKey(connector, taskIndex)] = offsets
	return nil
}

// Load returns offsets, or nil if none recorded.
func (s *MemoryOffsetStore) Load(_ context.Context, connector string, taskIndex int) ([]map[string]any, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.entries[memoryKey(connector, taskIndex)], nil
}

// Close is a no-op.
func (s *MemoryOffsetStore) Close() error { return nil }

// TopicOffsetStore persists offsets as records on a holocron topic. The
// Worker can keep using one for the lifetime of the program; on Save
// the store appends a record to the topic and updates its in-memory
// map. On creation it replays the topic from offset 0 to rebuild state.
//
// Topic shape: key = `<connector>/<taskIndex>`, value =
// JSON([]map[string]any). Single partition; broker-side compaction
// recommended so the topic stays bounded.
type TopicOffsetStore struct {
	transport sdk.Transport
	topic     string
	producer  *sdk.Producer

	mu      sync.RWMutex
	entries map[string][]map[string]any
}

const topicReplayDeadline = 200 * time.Millisecond

// OpenTopicOffsetStore returns a store bound to the given Transport,
// replaying topic state into memory before returning.
func OpenTopicOffsetStore(ctx context.Context, transport sdk.Transport, topic string) (*TopicOffsetStore, error) {
	if transport == nil {
		return nil, errors.New("connect: TopicOffsetStore requires a Transport")
	}
	if topic == "" {
		topic = DefaultConnectOffsetsTopic
	}
	s := &TopicOffsetStore{
		transport: transport,
		topic:     topic,
		entries:   make(map[string][]map[string]any),
	}
	if err := s.replay(ctx); err != nil {
		return nil, fmt.Errorf("connect: replay %s: %w", topic, err)
	}
	p, err := sdk.NewProducer(transport)
	if err != nil {
		return nil, fmt.Errorf("connect: producer: %w", err)
	}
	s.producer = p
	return s, nil
}

func (s *TopicOffsetStore) replay(ctx context.Context) error {
	consumer, err := sdk.NewConsumer(s.transport)
	if err != nil {
		return err
	}
	defer consumer.Close()
	if err := consumer.Subscribe(ctx, s.topic, 0); err != nil {
		return err
	}
	drainCtx, cancel := context.WithTimeout(ctx, topicReplayDeadline)
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
				continue
			}
			var offsets []map[string]any
			if err := json.Unmarshal(r.Value, &offsets); err != nil {
				return fmt.Errorf("connect: decode offsets %q: %w", r.Key, err)
			}
			s.entries[string(r.Key)] = offsets
		}
	}
}

// Save writes a record to the offsets topic and updates the in-memory map.
func (s *TopicOffsetStore) Save(ctx context.Context, connector string, taskIndex int, offsets []map[string]any) error {
	key := []byte(memoryKey(connector, taskIndex))
	value, err := json.Marshal(offsets)
	if err != nil {
		return fmt.Errorf("connect: encode offsets: %w", err)
	}
	if _, err := s.producer.Send(ctx, s.topic, proto.Record{Key: key, Value: value}); err != nil {
		return err
	}
	s.mu.Lock()
	s.entries[string(key)] = offsets
	s.mu.Unlock()
	return nil
}

// Load returns offsets from the in-memory map.
func (s *TopicOffsetStore) Load(_ context.Context, connector string, taskIndex int) ([]map[string]any, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.entries[memoryKey(connector, taskIndex)], nil
}

// Close releases the embedded producer.
func (s *TopicOffsetStore) Close() error {
	if s.producer != nil {
		return s.producer.Close()
	}
	return nil
}

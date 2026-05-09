// Package topic owns the in-memory registry of topics and their partition
// counts. It does not hold record data — that lives behind storage.Store —
// but it is the source of truth for which topics exist.
package topic

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sync"

	"github.com/jedi-knights/holocron/proto"
)

// Spec is the configuration a caller supplies when creating a topic.
type Spec struct {
	Name           string
	PartitionCount int32
	RetentionMs    int64
	SegmentBytes   int64
}

// Sentinel errors for callers that need to distinguish failure modes.
var (
	ErrTopicExists   = errors.New("topic: already exists")
	ErrTopicNotFound = errors.New("topic: not found")
	ErrInvalidName   = errors.New("topic: invalid name")
	ErrInvalidSpec   = errors.New("topic: invalid spec")
)

// Topic name rules match Kafka's so topic names remain safe to use as
// filesystem path components in Stage 2.
var nameRe = regexp.MustCompile(`^[a-zA-Z0-9._-]{1,249}$`)

// Registry tracks the broker's known topics. The optional persist hook is
// invoked under the registry's write lock whenever the topic set changes;
// callers wire it up to a JSON file in the data directory so topic
// metadata survives restarts.
type Registry struct {
	mu      sync.RWMutex
	topics  map[string]proto.TopicConfig
	persist func(snapshot []proto.TopicConfig) error
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{topics: make(map[string]proto.TopicConfig)}
}

// SetPersistHook installs a callback that fires whenever the topic set
// changes. The callback receives a stable snapshot and is invoked under
// the registry's write lock — implementations should not call back into
// the registry.
func (r *Registry) SetPersistHook(fn func(snapshot []proto.TopicConfig) error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.persist = fn
}

// Hydrate replaces the registry's contents with the given configs. Used
// by callers that load metadata from disk on startup.
func (r *Registry) Hydrate(configs []proto.TopicConfig) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.topics = make(map[string]proto.TopicConfig, len(configs))
	for _, cfg := range configs {
		r.topics[cfg.Name] = cfg
	}
}

// Create registers a new topic. Returns ErrTopicExists if the name is
// already registered, ErrInvalidName / ErrInvalidSpec for malformed input.
func (r *Registry) Create(spec Spec) error {
	if !nameRe.MatchString(spec.Name) {
		return fmt.Errorf("%w: %q", ErrInvalidName, spec.Name)
	}
	if spec.PartitionCount <= 0 {
		return fmt.Errorf("%w: partition count must be > 0", ErrInvalidSpec)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.topics[spec.Name]; ok {
		return fmt.Errorf("%w: %q", ErrTopicExists, spec.Name)
	}
	r.topics[spec.Name] = proto.TopicConfig{
		Name:           spec.Name,
		PartitionCount: spec.PartitionCount,
		RetentionMs:    spec.RetentionMs,
		SegmentBytes:   spec.SegmentBytes,
	}
	if r.persist != nil {
		return r.persist(snapshotLocked(r.topics))
	}
	return nil
}

func snapshotLocked(m map[string]proto.TopicConfig) []proto.TopicConfig {
	out := make([]proto.TopicConfig, 0, len(m))
	for _, cfg := range m {
		out = append(out, cfg)
	}
	return out
}

// LoadFile reads a topics file (JSON array of TopicConfig) from path and
// hydrates the registry. A missing file is not an error — it just means
// the registry stays empty.
func LoadFile(r *Registry, path string) error {
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("topic: read %s: %w", path, err)
	}
	var configs []proto.TopicConfig
	if err := json.Unmarshal(b, &configs); err != nil {
		return fmt.Errorf("topic: parse %s: %w", path, err)
	}
	r.Hydrate(configs)
	return nil
}

// SaveFile writes the snapshot atomically (temp + rename) to path.
func SaveFile(path string, snapshot []proto.TopicConfig) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// Get returns the configuration for a topic.
func (r *Registry) Get(name string) (proto.TopicConfig, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	cfg, ok := r.topics[name]
	if !ok {
		return proto.TopicConfig{}, fmt.Errorf("%w: %q", ErrTopicNotFound, name)
	}
	return cfg, nil
}

// PartitionsFor returns the partition count of a topic.
func (r *Registry) PartitionsFor(name string) (int32, error) {
	cfg, err := r.Get(name)
	if err != nil {
		return 0, err
	}
	return cfg.PartitionCount, nil
}

// List returns a snapshot of all registered topics.
func (r *Registry) List() []proto.TopicConfig {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]proto.TopicConfig, 0, len(r.topics))
	for _, cfg := range r.topics {
		out = append(out, cfg)
	}
	return out
}

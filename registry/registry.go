package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/jedi-knights/holocron/proto"
	"github.com/jedi-knights/holocron/sdk"
)

// DefaultTopic is the broker topic where schema registrations live. The
// topic should have exactly one partition so total order across all
// subjects is preserved (and so schema-ID assignment is monotonic).
const DefaultTopic = "__holocron_schemas"

// Compatibility mode — V1 ships only NONE. The API accommodates the
// other modes so callers can record their intent; enforcement of
// BACKWARD/FORWARD/FULL requires parsing the schema language and lands
// once the registry picks one.
type Compatibility string

const (
	CompatibilityNone     Compatibility = "NONE"
	CompatibilityBackward Compatibility = "BACKWARD"
	CompatibilityForward  Compatibility = "FORWARD"
	CompatibilityFull     Compatibility = "FULL"
)

// Schema is one registered (subject, version) tuple plus its globally
// unique ID and the schema text itself. The text is opaque to the
// registry — it might be Avro JSON, JSON Schema, Protobuf .proto, etc.
type Schema struct {
	ID      int    `json:"id"`
	Subject string `json:"subject"`
	Version int    `json:"version"`
	Schema  string `json:"schema"`
}

// Sentinel errors callers can test with errors.Is.
var (
	ErrSubjectNotFound = errors.New("registry: subject not found")
	ErrVersionNotFound = errors.New("registry: version not found")
	ErrSchemaNotFound  = errors.New("registry: schema not found")
)

// Service is the embeddable schema registry. Safe for concurrent use.
type Service struct {
	transport sdk.Transport
	topic     string

	mu        sync.RWMutex
	byID      map[int]Schema
	bySubject map[string][]Schema // ordered by version, ascending
	nextID    int

	producer *sdk.Producer
	started  bool
}

// Option configures a Service.
type Option func(*Service)

// WithTopic overrides the metadata topic name. Useful only if you have
// more than one registry sharing a broker (rare).
func WithTopic(topic string) Option {
	return func(s *Service) { s.topic = topic }
}

// New constructs a Service bound to the given Transport. Call Start
// before any other method to populate the in-memory tables.
func New(transport sdk.Transport, opts ...Option) (*Service, error) {
	if transport == nil {
		return nil, errors.New("registry: New requires a Transport")
	}
	s := &Service{
		transport: transport,
		topic:     DefaultTopic,
		byID:      make(map[int]Schema),
		bySubject: make(map[string][]Schema),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s, nil
}

// Start replays the metadata topic to rebuild in-memory tables and
// readies the Service for Register calls. The topic must already exist;
// holocron-registry's main creates it before calling Start.
func (s *Service) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return errors.New("registry: already started")
	}
	s.started = true
	s.mu.Unlock()

	producer, err := sdk.NewProducer(s.transport)
	if err != nil {
		return fmt.Errorf("registry: producer: %w", err)
	}
	s.producer = producer

	return s.replay(ctx)
}

// highWaterer is a duck-typed interface — Transports that can report
// the next-to-be-appended offset implement HighWater. inproc satisfies
// it via the broker's storage; the network transport falls back to the
// timeout-drain path until the wire protocol grows a high-water op.
type highWaterer interface {
	HighWater(ctx context.Context, p proto.PartitionRef) (int64, error)
}

// replay reads __holocron_schemas from offset 0 to high-water-at-
// subscribe-time and applies every record into the in-memory tables.
//
// When the Transport supports HighWater, the catch-up bound is exact:
// a slow disk cannot starve replay because we read until the cursor
// reaches the captured high-water. Otherwise we fall back to a 200ms
// drain timeout; that's a real correctness gap (very slow disks could
// silently miss late records) tracked under registry follow-ups.
func (s *Service) replay(ctx context.Context) error {
	consumer, err := sdk.NewConsumer(s.transport)
	if err != nil {
		return fmt.Errorf("registry: replay consumer: %w", err)
	}
	defer consumer.Close()

	pref := proto.PartitionRef{Topic: s.topic, Index: 0}
	var target int64
	var bounded bool
	if hw, ok := s.transport.(highWaterer); ok {
		t, err := hw.HighWater(ctx, pref)
		if err == nil {
			target = t
			bounded = true
		}
	}

	if err := consumer.Subscribe(ctx, s.topic, 0); err != nil {
		return fmt.Errorf("registry: subscribe %s: %w", s.topic, err)
	}

	if bounded {
		return s.replayBounded(ctx, consumer, target)
	}
	return s.replayTimeout(ctx, consumer)
}

// replayBounded reads exactly `target` records — the high-water captured
// before Subscribe.
func (s *Service) replayBounded(ctx context.Context, consumer *sdk.Consumer, target int64) error {
	if target <= 0 {
		return nil
	}
	var seen int64
	for seen < target {
		records, err := consumer.Poll(ctx, 256)
		if err != nil {
			return fmt.Errorf("registry: replay poll: %w", err)
		}
		for _, r := range records {
			if err := s.applyRecord(r); err != nil {
				return err
			}
		}
		seen += int64(len(records))
	}
	return nil
}

// replayTimeout drains records for 200ms after subscribe — the legacy
// path used when the Transport cannot report high-water.
func (s *Service) replayTimeout(ctx context.Context, consumer *sdk.Consumer) error {
	drainCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()

	for {
		records, err := consumer.Poll(drainCtx, 256)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			return fmt.Errorf("registry: replay poll: %w", err)
		}
		if len(records) == 0 {
			return nil
		}
		for _, r := range records {
			if err := s.applyRecord(r); err != nil {
				return err
			}
		}
	}
}

func (s *Service) applyLocked(sc Schema) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.byID[sc.ID] = sc
	s.bySubject[sc.Subject] = appendByVersion(s.bySubject[sc.Subject], sc)
	if sc.ID >= s.nextID {
		s.nextID = sc.ID + 1
	}
}

// applyTombstoneLocked drops every schema for the subject. Mirrors how
// log compaction treats a null-value record: the key (subject) is
// removed from the live state. Schema IDs are also removed from byID
// so a stale GetByID call doesn't resurrect the subject; the IDs
// themselves are not reused (nextID never goes backwards).
func (s *Service) applyTombstoneLocked(subject string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sc := range s.bySubject[subject] {
		delete(s.byID, sc.ID)
	}
	delete(s.bySubject, subject)
}

// applyRecord dispatches a replayed topic record to the right state
// transition: tombstone (nil/empty value) deletes the subject; any
// other value is decoded as a Schema and applied.
//
// Two pieces of state come from the broker, not from the record's
// JSON, so multi-instance registries don't collide:
//
//   - sc.ID is the record's broker offset (globally unique).
//   - sc.Version is one more than the count of non-tombstone schemas
//     already in this subject (count-derived from broker order).
//
// The Subject in the JSON is replaced with the record's key — the key
// is the wire-stable identity, the JSON copy is informational only.
func (s *Service) applyRecord(r proto.Record) error {
	if len(r.Value) == 0 {
		s.applyTombstoneLocked(string(r.Key))
		return nil
	}
	var sc Schema
	if err := json.Unmarshal(r.Value, &sc); err != nil {
		return fmt.Errorf("registry: replay decode: %w", err)
	}
	sc.Subject = string(r.Key)
	sc.ID = int(r.Offset)

	s.mu.Lock()
	defer s.mu.Unlock()

	// Multi-instance dedup: if the latest version of this subject
	// already carries the same schema text, the new record is just a
	// concurrent re-write from a peer that hadn't yet seen ours.
	// Bind this offset to the existing version so GetByID still
	// resolves, but don't create a duplicate version.
	versions := s.bySubject[sc.Subject]
	if n := len(versions); n > 0 && versions[n-1].Schema == sc.Schema {
		s.byID[sc.ID] = versions[n-1]
		if sc.ID >= s.nextID {
			s.nextID = sc.ID + 1
		}
		return nil
	}

	sc.Version = len(versions) + 1
	s.byID[sc.ID] = sc
	s.bySubject[sc.Subject] = append(versions, sc)
	if sc.ID >= s.nextID {
		s.nextID = sc.ID + 1
	}
	return nil
}

// Register records a schema under the given subject. The returned
// schema ID is the broker-assigned offset of the registration record
// on the schemas topic — globally unique across multi-instance
// deployments by virtue of the broker's per-partition ordering.
//
// If the schema text matches the latest version of the subject
// exactly, the existing ID is returned without writing a new record
// (idempotent within this instance; concurrent register-same-schema
// across instances may still produce duplicate records, which the
// replay path will deduplicate by offset).
func (s *Service) Register(ctx context.Context, subject, schemaText string) (int, error) {
	s.mu.RLock()
	versions := s.bySubject[subject]
	if n := len(versions); n > 0 && versions[n-1].Schema == schemaText {
		id := versions[n-1].ID
		s.mu.RUnlock()
		return id, nil
	}
	nextVersion := len(versions) + 1
	s.mu.RUnlock()

	sc := Schema{
		Subject: subject,
		Version: nextVersion,
		Schema:  schemaText,
	}
	body, err := json.Marshal(sc)
	if err != nil {
		return 0, err
	}
	offset, err := s.producer.Send(ctx, s.topic, proto.Record{
		Key:   []byte(subject),
		Value: body,
	})
	if err != nil {
		return 0, fmt.Errorf("registry: produce: %w", err)
	}
	sc.ID = int(offset)
	s.applyLocked(sc)
	return sc.ID, nil
}

// DeleteSubject removes every version of the named subject from the
// registry by writing a tombstone (nil-value) record to the schemas
// topic. Subsequent GetLatest / GetVersion / ListVersions calls return
// ErrSubjectNotFound, and the subject does not appear in
// ListSubjects. Re-Register on the same subject starts a fresh version
// sequence at 1.
//
// Tombstones are durable: a fresh Service replaying the topic observes
// the deletion. With broker-side log compaction enabled on the schemas
// topic, prior schema records for the deleted subject also get
// reclaimed; without compaction the history remains in the log but the
// registry's in-memory state still reflects deletion after replay.
//
// Returns ErrSubjectNotFound if no schema is currently registered under
// the subject — deletion is not idempotent in V1.
func (s *Service) DeleteSubject(ctx context.Context, subject string) error {
	s.mu.RLock()
	_, exists := s.bySubject[subject]
	s.mu.RUnlock()
	if !exists {
		return fmt.Errorf("%w: %q", ErrSubjectNotFound, subject)
	}
	if _, err := s.producer.Send(ctx, s.topic, proto.Record{
		Key:   []byte(subject),
		Value: nil,
	}); err != nil {
		return fmt.Errorf("registry: produce tombstone: %w", err)
	}
	s.applyTombstoneLocked(subject)
	return nil
}

// GetByID returns the schema with the given globally-unique ID.
func (s *Service) GetByID(id int) (Schema, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sc, ok := s.byID[id]
	if !ok {
		return Schema{}, fmt.Errorf("%w: id=%d", ErrSchemaNotFound, id)
	}
	return sc, nil
}

// GetVersion returns the schema for (subject, version). Pass version=-1
// or use GetLatest for the latest registered version.
func (s *Service) GetVersion(subject string, version int) (Schema, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	versions, ok := s.bySubject[subject]
	if !ok || len(versions) == 0 {
		return Schema{}, fmt.Errorf("%w: %q", ErrSubjectNotFound, subject)
	}
	if version < 1 || version > len(versions) {
		return Schema{}, fmt.Errorf("%w: subject=%q version=%d", ErrVersionNotFound, subject, version)
	}
	return versions[version-1], nil
}

// GetLatest returns the most recently registered schema for the subject.
func (s *Service) GetLatest(subject string) (Schema, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	versions, ok := s.bySubject[subject]
	if !ok || len(versions) == 0 {
		return Schema{}, fmt.Errorf("%w: %q", ErrSubjectNotFound, subject)
	}
	return versions[len(versions)-1], nil
}

// ListSubjects returns every subject with at least one registered schema.
func (s *Service) ListSubjects() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.bySubject))
	for subject := range s.bySubject {
		out = append(out, subject)
	}
	sort.Strings(out)
	return out
}

// ListVersions returns the version numbers registered for the subject.
func (s *Service) ListVersions(subject string) ([]int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	versions, ok := s.bySubject[subject]
	if !ok || len(versions) == 0 {
		return nil, fmt.Errorf("%w: %q", ErrSubjectNotFound, subject)
	}
	out := make([]int, len(versions))
	for i, v := range versions {
		out[i] = v.Version
	}
	return out, nil
}

// CheckCompatibility reports whether schemaText would be compatible with
// the current state of subject under the given mode. Stage 7 V1 only
// understands NONE, which always returns true. Other modes return an
// error so callers can detect the gap explicitly.
func (s *Service) CheckCompatibility(subject, schemaText string, mode Compatibility) (bool, error) {
	_ = subject
	_ = schemaText
	switch mode {
	case CompatibilityNone, "":
		return true, nil
	case CompatibilityBackward, CompatibilityForward, CompatibilityFull:
		return false, fmt.Errorf("registry: compatibility mode %q not implemented in V1", mode)
	default:
		return false, fmt.Errorf("registry: unknown compatibility mode %q", mode)
	}
}

// Close releases the embedded Producer. The Transport is owned by the
// caller and is not closed.
func (s *Service) Close() error {
	if s.producer != nil {
		return s.producer.Close()
	}
	return nil
}

// appendByVersion inserts sc into the version-ordered slice. Replays may
// see entries out of order if a future stage parallelizes; the function
// is defensive about this.
func appendByVersion(versions []Schema, sc Schema) []Schema {
	for i, v := range versions {
		if v.Version == sc.Version {
			versions[i] = sc
			return versions
		}
		if v.Version > sc.Version {
			versions = append(versions[:i+1], versions[i:]...)
			versions[i] = sc
			return versions
		}
	}
	return append(versions, sc)
}

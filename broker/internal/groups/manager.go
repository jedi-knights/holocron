package groups

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/jedi-knights/holocron/proto"
)

// Defaults are tuned for educational visibility, not production tuning.
const (
	DefaultSessionTimeout    = 15 * time.Second
	DefaultHeartbeatInterval = 5 * time.Second
)

// Sentinel errors callers can match with errors.Is.
var (
	ErrUnknownMember     = errors.New("groups: unknown member")
	ErrGenerationStale   = errors.New("groups: generation is stale")
	ErrNoTopicsRequested = errors.New("groups: join with no topics")
)

// Assignment describes one partition assigned to a member, paired with
// the committed offset (NoOffset if uncommitted).
type Assignment struct {
	Partition       proto.PartitionRef
	CommittedOffset int64
}

// JoinResult is what JoinGroup returns to a caller.
type JoinResult struct {
	MemberID    string
	Generation  int32
	Assignments []Assignment
}

// HeartbeatResult tells the caller whether they should rejoin.
type HeartbeatResult struct {
	RebalanceNeeded bool
}

// Strategy selects the partition-assignment algorithm a Manager applies
// during rebalance. Range (the default) hands each member a contiguous
// slice; round-robin strides one partition at a time across members,
// which spreads load more evenly when partition counts are uneven.
type Strategy int

const (
	// RangeStrategy hands each member a contiguous partition slice. The
	// default, matching Kafka's historical default.
	RangeStrategy Strategy = iota
	// RoundRobinStrategy strides partitions one-at-a-time across members
	// in lexical order.
	RoundRobinStrategy
	// StickyStrategy preserves the prior generation's assignment where
	// possible, displacing only what's needed to keep per-member counts
	// balanced. Useful when state-store warmth or in-flight processing
	// should survive rebalances cheaply.
	StickyStrategy
)

// Manager owns all groups in a broker. It is safe for concurrent use.
type Manager struct {
	offsets        OffsetStore
	partitionsFor  PartitionsForFunc
	sessionTimeout time.Duration
	now            func() time.Time
	strategy       Strategy

	mu     sync.Mutex
	groups map[string]*group
}

type group struct {
	name        string
	topics      []string
	members     map[string]*member // memberID → member
	assignments map[string][]proto.PartitionRef
	generation  int32
	dirty       bool // assignments out of date — recompute on next access
}

type member struct {
	id            string
	lastHeartbeat time.Time
}

// Option configures a Manager.
type Option func(*Manager)

// WithSessionTimeout sets how long a member can go without a heartbeat
// before it is evicted.
func WithSessionTimeout(d time.Duration) Option {
	return func(m *Manager) { m.sessionTimeout = d }
}

// WithClock injects a clock — used by tests to control session timeouts.
func WithClock(now func() time.Time) Option {
	return func(m *Manager) { m.now = now }
}

// WithStrategy selects the partition-assignment algorithm. Default is
// RangeStrategy. RoundRobinStrategy spreads load more evenly across
// members when partition counts are uneven.
func WithStrategy(s Strategy) Option {
	return func(m *Manager) { m.strategy = s }
}

// NewManager returns a Manager backed by the given offset store. The
// partitionsFor callback resolves topic → partition count; the broker
// passes its registry's lookup.
func NewManager(offsets OffsetStore, partitionsFor PartitionsForFunc, opts ...Option) *Manager {
	m := &Manager{
		offsets:        offsets,
		partitionsFor:  partitionsFor,
		sessionTimeout: DefaultSessionTimeout,
		now:            time.Now,
		groups:         make(map[string]*group),
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// Join inserts the member into the named group (assigning a fresh
// memberID if memberID is empty), records the topics it cares about, and
// returns the latest assignment + committed offsets for the partitions
// this member owns.
//
// A returning member with the same ID, the same topic set, and no other
// pending changes (no stale peers to evict, no dirty flag) skips the
// rebalance — generation does not bump, partitions do not shift. This
// makes WithMemberID-style sticky restarts effectively free for the
// rest of the group.
func (m *Manager) Join(groupName, memberID string, topics []string) (JoinResult, error) {
	if len(topics) == 0 {
		return JoinResult{}, ErrNoTopicsRequested
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	g, ok := m.groups[groupName]
	if !ok {
		g = &group{
			name:        groupName,
			members:     make(map[string]*member),
			assignments: make(map[string][]proto.PartitionRef),
		}
		m.groups[groupName] = g
	}

	if memberID == "" {
		memberID = newMemberID()
	}
	_, returning := g.members[memberID]
	if !returning {
		g.members[memberID] = &member{id: memberID}
	}
	g.members[memberID].lastHeartbeat = m.now()

	priorTopics := append([]string(nil), g.topics...)
	g.topics = mergeTopics(g.topics, topics)
	topicsExpanded := len(g.topics) != len(priorTopics)

	evicted := m.evictStaleLocked(g)
	skipRebalance := returning && !topicsExpanded && evicted == 0 && !g.dirty
	if !skipRebalance {
		if err := m.rebalanceLocked(g); err != nil {
			return JoinResult{}, err
		}
	}

	parts := append([]proto.PartitionRef(nil), g.assignments[memberID]...)
	out := make([]Assignment, 0, len(parts))
	for _, p := range parts {
		off, err := m.offsets.Lookup(groupName, p.Topic, p.Index)
		if err != nil {
			return JoinResult{}, fmt.Errorf("groups: lookup offset: %w", err)
		}
		out = append(out, Assignment{Partition: p, CommittedOffset: off})
	}
	return JoinResult{
		MemberID:    memberID,
		Generation:  g.generation,
		Assignments: out,
	}, nil
}

// Heartbeat records that memberID is still alive in the named group.
// It returns RebalanceNeeded=true if the member's generation is stale,
// which signals the caller to rejoin.
func (m *Manager) Heartbeat(groupName, memberID string, generation int32) (HeartbeatResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	g, ok := m.groups[groupName]
	if !ok {
		return HeartbeatResult{RebalanceNeeded: true}, ErrUnknownMember
	}
	mem, ok := g.members[memberID]
	if !ok {
		return HeartbeatResult{RebalanceNeeded: true}, ErrUnknownMember
	}
	mem.lastHeartbeat = m.now()

	// Evict any peers that haven't checked in. If we evict anyone, this
	// member needs to rejoin to pick up new partitions.
	evicted := m.evictStaleLocked(g)
	if evicted > 0 {
		g.dirty = true
	}
	if generation != g.generation || g.dirty {
		return HeartbeatResult{RebalanceNeeded: true}, nil
	}
	return HeartbeatResult{}, nil
}

// Leave removes memberID from the group and triggers a rebalance.
func (m *Manager) Leave(groupName, memberID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	g, ok := m.groups[groupName]
	if !ok {
		return nil
	}
	if _, ok := g.members[memberID]; !ok {
		return nil
	}
	delete(g.members, memberID)
	delete(g.assignments, memberID)
	g.dirty = true
	if len(g.members) == 0 {
		delete(m.groups, groupName)
		return nil
	}
	return nil
}

// Commit records a committed offset for (group, partition).
func (m *Manager) Commit(groupName, topic string, partition int32, offset int64) error {
	return m.offsets.Commit(groupName, topic, partition, offset)
}

// Committed returns the committed offset for (group, topic, partition),
// or NoOffset if uncommitted.
func (m *Manager) Committed(groupName, topic string, partition int32) (int64, error) {
	return m.offsets.Lookup(groupName, topic, partition)
}

// evictStaleLocked drops members whose last heartbeat is older than the
// session timeout. Caller must hold m.mu. Returns the number of evictions.
func (m *Manager) evictStaleLocked(g *group) int {
	cutoff := m.now().Add(-m.sessionTimeout)
	evicted := 0
	for id, mem := range g.members {
		if mem.lastHeartbeat.Before(cutoff) {
			delete(g.members, id)
			delete(g.assignments, id)
			evicted++
		}
	}
	if evicted > 0 {
		g.dirty = true
	}
	return evicted
}

// rebalanceLocked recomputes assignments and bumps the generation.
// Caller must hold m.mu.
func (m *Manager) rebalanceLocked(g *group) error {
	memberIDs := make([]string, 0, len(g.members))
	for id := range g.members {
		memberIDs = append(memberIDs, id)
	}
	sort.Strings(memberIDs)

	var assignments map[string][]proto.PartitionRef
	var err error
	switch m.strategy {
	case RoundRobinStrategy:
		assignments, err = assignRoundRobin(memberIDs, g.topics, m.partitionsFor)
	case StickyStrategy:
		assignments, err = assignSticky(memberIDs, g.topics, m.partitionsFor, g.assignments)
	default:
		assignments, err = assign(memberIDs, g.topics, m.partitionsFor)
	}
	if err != nil {
		return err
	}
	g.assignments = assignments
	g.generation++
	g.dirty = false
	return nil
}

// mergeTopics returns the deduplicated union of two topic lists, sorted
// for deterministic assignment input.
func mergeTopics(a, b []string) []string {
	seen := make(map[string]struct{}, len(a)+len(b))
	for _, t := range a {
		seen[t] = struct{}{}
	}
	for _, t := range b {
		seen[t] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for t := range seen {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

func newMemberID() string {
	var buf [8]byte
	_, _ = rand.Read(buf[:])
	return hex.EncodeToString(buf[:])
}

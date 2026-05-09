package groups

import (
	"context"
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

	// watchers receive a synchronous wake-up when this group's
	// generation changes. Each entry corresponds to one outstanding
	// long-poll heartbeat (HeartbeatWait). The slice is rebuilt on
	// every notify; entries whose context cancelled or who timed out
	// remove themselves on exit.
	watchers []chan struct{}
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
	return m.heartbeatLocked(groupName, memberID, generation)
}

func (m *Manager) heartbeatLocked(groupName, memberID string, generation int32) (HeartbeatResult, error) {
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

// HeartbeatWait is a long-poll variant of Heartbeat. It records the
// member as alive and then blocks for up to maxWait, returning
// immediately when:
//
//   - the member needs to rebalance (generation stale or peers
//     evicted) — RebalanceNeeded=true;
//   - a peer joins or leaves the group while the wait is active,
//     forcing a rebalance — RebalanceNeeded=true;
//   - maxWait elapses without a signal — RebalanceNeeded=false;
//   - ctx is cancelled — returns ctx.Err().
//
// maxWait ≤ 0 short-circuits to ordinary Heartbeat behavior. The
// "server-pushed" rebalance signal is delivered through this method:
// rather than the SDK polling Heartbeat every N ms, the SDK opens a
// single long-poll call that the broker resolves the moment a
// rebalance is needed, cutting the duplicate-production window
// during rebalance to roughly the round-trip time.
func (m *Manager) HeartbeatWait(ctx context.Context, groupName, memberID string, generation int32, maxWait time.Duration) (HeartbeatResult, error) {
	m.mu.Lock()
	res, err := m.heartbeatLocked(groupName, memberID, generation)
	if err != nil || res.RebalanceNeeded || maxWait <= 0 {
		m.mu.Unlock()
		return res, err
	}

	// Register a watcher under the lock so any rebalance scheduled
	// after this point will signal us. The watcher is buffered (cap 1)
	// so a notifier never blocks waiting for us to receive.
	g := m.groups[groupName]
	wake := make(chan struct{}, 1)
	g.watchers = append(g.watchers, wake)
	m.mu.Unlock()

	timer := time.NewTimer(maxWait)
	defer timer.Stop()
	select {
	case <-wake:
		return HeartbeatResult{RebalanceNeeded: true}, nil
	case <-timer.C:
		m.removeWatcher(groupName, wake)
		return HeartbeatResult{}, nil
	case <-ctx.Done():
		m.removeWatcher(groupName, wake)
		return HeartbeatResult{}, ctx.Err()
	}
}

// removeWatcher drops wake from the named group's watcher list.
// Called by HeartbeatWait paths that exit without being signalled
// (timer fired, ctx cancelled) so abandoned wake channels don't
// accumulate. Safe to call after the group has been deleted.
func (m *Manager) removeWatcher(groupName string, wake chan struct{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	g, ok := m.groups[groupName]
	if !ok {
		return
	}
	for i, w := range g.watchers {
		if w == wake {
			g.watchers = append(g.watchers[:i], g.watchers[i+1:]...)
			return
		}
	}
}

// notifyWatchersLocked delivers a wake to every registered watcher
// for g and clears the slice. Caller holds m.mu. The non-blocking
// send is safe because every watcher channel is cap 1: the receiver
// either picks up the signal or its select gets cancelled, and the
// channel garbage-collects when both ends drop.
func (m *Manager) notifyWatchersLocked(g *group) {
	for _, w := range g.watchers {
		select {
		case w <- struct{}{}:
		default:
		}
	}
	g.watchers = nil
}

// Leave removes memberID from the group and triggers a rebalance.
// Outstanding HeartbeatWait watchers fire immediately so existing
// peers learn to rejoin without waiting for their next heartbeat.
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
		m.notifyWatchersLocked(g)
		delete(m.groups, groupName)
		return nil
	}
	m.notifyWatchersLocked(g)
	return nil
}

// GroupSummary is the operator-facing view of a single group:
// its name, current generation, and member count. Cheap enough to
// compute for every known group in one call.
type GroupSummary struct {
	Name        string
	Generation  int32
	MemberCount int32
	Topics      []string
}

// MemberAssignment lists one member's currently-owned partitions
// inside a group. Returned by Describe.
type MemberAssignment struct {
	MemberID   string
	Partitions []proto.PartitionRef
}

// GroupDetails is the operator-facing detail view: every member
// and what each currently owns.
type GroupDetails struct {
	Name       string
	Generation int32
	Topics     []string
	Members    []MemberAssignment
}

// ErrUnknownGroup is returned by Describe when no group by the
// given name is registered with the manager.
var ErrUnknownGroup = errors.New("groups: unknown group")

// List returns a summary of every group the manager knows about.
// Order is unspecified.
func (m *Manager) List() []GroupSummary {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]GroupSummary, 0, len(m.groups))
	for _, g := range m.groups {
		topics := append([]string(nil), g.topics...)
		out = append(out, GroupSummary{
			Name:        g.name,
			Generation:  g.generation,
			MemberCount: int32(len(g.members)),
			Topics:      topics,
		})
	}
	return out
}

// Describe returns the per-member assignment for groupName. Members
// appear in lexical order so output is deterministic.
func (m *Manager) Describe(groupName string) (GroupDetails, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	g, ok := m.groups[groupName]
	if !ok {
		return GroupDetails{}, ErrUnknownGroup
	}
	memberIDs := make([]string, 0, len(g.members))
	for id := range g.members {
		memberIDs = append(memberIDs, id)
	}
	sort.Strings(memberIDs)
	members := make([]MemberAssignment, 0, len(memberIDs))
	for _, id := range memberIDs {
		members = append(members, MemberAssignment{
			MemberID:   id,
			Partitions: append([]proto.PartitionRef(nil), g.assignments[id]...),
		})
	}
	return GroupDetails{
		Name:       g.name,
		Generation: g.generation,
		Topics:     append([]string(nil), g.topics...),
		Members:    members,
	}, nil
}

// Delete drops the named group from the registry and clears every
// committed offset under it. Live members are evicted (their next
// heartbeat returns RebalanceNeeded so they fail fast); a fresh
// rejoin from the same name starts at generation 0 with no
// historical offsets.
//
// Use to clean up abandoned groups whose members are gone but whose
// committed offsets still pin retention. ErrUnknownGroup is
// returned only if the group has no in-memory registration AND no
// committed offsets — i.e., the name is genuinely unknown.
func (m *Manager) Delete(groupName string) error {
	m.mu.Lock()
	_, hadGroup := m.groups[groupName]
	if hadGroup {
		delete(m.groups, groupName)
	}
	m.mu.Unlock()
	hadOffsets := len(m.offsets.List(groupName)) > 0
	if !hadGroup && !hadOffsets {
		return ErrUnknownGroup
	}
	return m.offsets.DeleteGroup(groupName)
}

// Commit records a committed offset for (group, partition).
func (m *Manager) Commit(groupName, topic string, partition int32, offset int64) error {
	return m.offsets.Commit(groupName, topic, partition, offset)
}

// ListOffsets enumerates every (topic, partition, offset) tuple
// committed under groupName. The HighWater field of each entry is
// left at NoOffset; callers that want lag fill it in by looking up
// the broker's high-water for each partition. Order is sorted by
// (topic, partition) so output is deterministic.
func (m *Manager) ListOffsets(groupName string) []OffsetEntry {
	entries := m.offsets.List(groupName)
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Topic != entries[j].Topic {
			return entries[i].Topic < entries[j].Topic
		}
		return entries[i].Partition < entries[j].Partition
	})
	return entries
}

// Committed returns the committed offset for (group, topic, partition),
// or NoOffset if uncommitted.
func (m *Manager) Committed(groupName, topic string, partition int32) (int64, error) {
	return m.offsets.Lookup(groupName, topic, partition)
}

// evictStaleLocked drops members whose last heartbeat is older than
// the session timeout. Caller must hold m.mu. Returns the number of
// evictions. Any eviction marks the group dirty AND wakes
// outstanding watchers so a long-poll heartbeat held by a survivor
// sees the rebalance signal without waiting for its own deadline.
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
		m.notifyWatchersLocked(g)
	}
	return evicted
}

// rebalanceLocked recomputes assignments and bumps the generation.
// Caller must hold m.mu. After updating the generation, every
// outstanding HeartbeatWait watcher for g is signalled so existing
// members learn of the rebalance immediately rather than waiting for
// their next heartbeat tick.
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
	m.notifyWatchersLocked(g)
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

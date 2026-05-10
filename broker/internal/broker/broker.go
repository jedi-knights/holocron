// Package broker is the in-process publish/subscribe coordinator. It owns
// a storage.Store and a topic.Registry and arbitrates appends and fan-out
// subscriptions on top of them.
package broker

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jedi-knights/holocron/broker/internal/cluster"
	"github.com/jedi-knights/holocron/broker/internal/groups"
	"github.com/jedi-knights/holocron/broker/internal/log"
	"github.com/jedi-knights/holocron/broker/internal/metrics"
	"github.com/jedi-knights/holocron/broker/internal/storage"
	"github.com/jedi-knights/holocron/broker/internal/topic"
	"github.com/jedi-knights/holocron/proto"
)

func nowNanos() int64 { return time.Now().UnixNano() }

// ErrNotLeader is returned by Publish/CreateTopic when the broker is part
// of a cluster and is not the current Raft leader. It carries a hint at
// where the leader is so callers can redirect.
type ErrNotLeader struct {
	LeaderID   string
	LeaderAddr string
}

func (e *ErrNotLeader) Error() string {
	if e.LeaderAddr == "" {
		return "broker: not leader (no leader known)"
	}
	return fmt.Sprintf("broker: not leader (leader=%s addr=%s)", e.LeaderID, e.LeaderAddr)
}

const (
	defaultSubscriberBuffer = 256
	catchUpBatchSize        = 256
)

// Broker is the in-process pub/sub coordinator.
type Broker struct {
	store      storage.Store
	registry   *topic.Registry
	groups     *groups.Manager
	cluster    *cluster.Cluster
	metrics    *metrics.Registry
	dedupTTL   time.Duration // 0 disables eviction
	dedupTopic string        // empty disables persistence

	mu         sync.Mutex
	partitions map[proto.PartitionRef]*partitionState
}

// DefaultDedupTTL bounds how long the broker remembers a producer's
// last sequence after the producer goes silent. A producer that
// sends nothing for this long gets its checkpoint pruned, freeing
// the dedup-table slot. Retried writes from a pruned producer can
// surface as duplicates; tune higher for environments with very
// long quiet periods.
const DefaultDedupTTL = time.Hour

// partitionState gives each partition its own lock so cross-partition work
// can proceed in parallel — the throughput-first commitment. The dedup
// map records the last-seen sequence per producer so retries of an
// already-applied write return the original offset instead of
// duplicating the record.
type partitionState struct {
	mu          sync.Mutex
	subscribers []chan proto.Record
	dedup       map[string]producerCheckpoint // producer-id → last sequence + offset
}

// producerCheckpoint is the broker's memory of the last record a
// given producer instance landed on a partition. Used by the
// idempotent-retry path to recognize duplicates. lastSeen tracks
// the wall-clock time of the most recent matching Publish so
// stale entries (producers that haven't sent in DedupTTL) can be
// pruned to bound memory.
type producerCheckpoint struct {
	lastSeq    uint64
	lastOffset int64
	lastSeen   time.Time
}

// Option configures a Broker.
type Option func(*Broker)

// WithGroupManager attaches a consumer-group coordinator. Brokers without
// one return errors for group-related calls but otherwise function as
// Stage 1–3 brokers.
func WithGroupManager(g *groups.Manager) Option {
	return func(b *Broker) { b.groups = g }
}

// WithCluster attaches a Raft cluster. When set, Publish and topic
// creation route through Raft Apply on the leader; followers return
// ErrNotLeader. Reads continue to come from the local store.
func WithCluster(c *cluster.Cluster) Option {
	return func(b *Broker) { b.cluster = c }
}

// WithMetrics attaches a metrics registry the broker increments on
// every Publish/Read. Brokers without a registry are silent.
func WithMetrics(m *metrics.Registry) Option {
	return func(b *Broker) { b.metrics = m }
}

// WithDedupTTL bounds how long the broker holds a per-producer
// checkpoint in its idempotent-retry table. Producers that send
// nothing for this long get pruned on the next Publish to the
// affected partition. Zero disables eviction (entries live until
// the broker restarts) — useful when memory isn't a concern and
// retries can span arbitrary windows.
func WithDedupTTL(d time.Duration) Option {
	return func(b *Broker) { b.dedupTTL = d }
}

// WithDedupTopic enables persistent producer-idempotency: every
// dedup checkpoint update is written to single-partition topic
// `topic` (typically "__holocron_dedup") on the broker's own log.
// Pair with HydrateDedup at startup to restore in-memory state
// from the topic so retries that span a broker restart still
// dedup. Empty (the default) leaves dedup state in-memory only.
//
// Caller is responsible for creating the topic (single partition,
// compaction recommended) before any Publish runs. The broker
// writes through synchronously after every successful Append, so a
// crash between user-record append and dedup-record append leaves
// the dedup state slightly behind reality — a follow-on retry of
// the just-acked record will re-publish, surfacing as a duplicate.
// Acceptable trade-off for V1; tighter atomicity needs broker
// transactions.
func WithDedupTopic(topic string) Option {
	return func(b *Broker) { b.dedupTopic = topic }
}

// Metrics returns the broker's metrics registry, or nil.
func (b *Broker) Metrics() *metrics.Registry { return b.metrics }

// New returns a Broker wired to the given Store and Registry.
func New(store storage.Store, registry *topic.Registry, opts ...Option) *Broker {
	b := &Broker{
		store:      store,
		registry:   registry,
		dedupTTL:   DefaultDedupTTL,
		partitions: make(map[proto.PartitionRef]*partitionState),
	}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

// Registry exposes the topic registry. Callers — including the inproc
// transport — use it to create topics and look up partition counts.
func (b *Broker) Registry() *topic.Registry { return b.registry }

// Groups returns the consumer-group manager, or nil if the broker was
// constructed without one.
func (b *Broker) Groups() *groups.Manager { return b.groups }

// Store exposes the broker's underlying storage.Store, primarily so
// SyncFromLeader's external orchestrator can write records into
// the local store directly via cluster.SyncPartitionFromPeer
// without going through Publish (which would cycle through Raft on
// a clustered broker).
//
// Callers that aren't part of the Stage 9 catch-up path should use
// Publish/Read; this accessor is a deliberate seam, not the broker's
// general-purpose storage interface.
func (b *Broker) Store() storage.Store { return b.store }

// Publish appends a record to the addressed partition and fans it out to
// live subscribers. Both happen under the partition's lock so subscribers
// observe records in append order with consistent offsets.
//
// In cluster mode (WithCluster set) the append is replicated through
// Raft. Followers return ErrNotLeader; the network server translates this
// into a wire-level redirect.
func (b *Broker) Publish(ctx context.Context, p proto.PartitionRef, r proto.Record) (int64, error) {
	if err := b.validate(p); err != nil {
		return 0, err
	}
	if b.metrics != nil {
		b.metrics.IncProduceRequest()
	}
	if b.cluster != nil {
		return b.publishClustered(ctx, p, r)
	}

	ps := b.partitionState(p)
	ps.mu.Lock()
	defer ps.mu.Unlock()

	// Idempotent-retry dedup: when the record carries a producer ID
	// + sequence number, look up the last sequence we saw for that
	// producer on this partition. A non-increasing sequence means
	// this is a retry of an already-applied write — return the
	// original offset without appending again.
	producerID, seq, hasIdemp := producerIdempKey(r)
	now := time.Now()
	if hasIdemp {
		if b.dedupTTL > 0 {
			b.evictStaleDedupLocked(ps, now)
		}
		if cp, ok := ps.dedup[producerID]; ok && seq <= cp.lastSeq {
			return cp.lastOffset, nil
		}
	}

	// Pre-stamp the timestamp so the local copy of r matches the value
	// the store keeps. Otherwise the live-fanout subscribers below see
	// Timestamp=0 while catch-up readers (which read from the store)
	// see the stamped value — windowing and event-time logic that
	// depends on r.Timestamp would split on those paths.
	if r.Timestamp == 0 {
		r.Timestamp = nowNanos()
	}

	offset, err := b.store.Append(ctx, p, r)
	if err != nil {
		return 0, fmt.Errorf("broker: append %v: %w", p, err)
	}
	if hasIdemp {
		if ps.dedup == nil {
			ps.dedup = make(map[string]producerCheckpoint)
		}
		cp := producerCheckpoint{
			lastSeq:    seq,
			lastOffset: offset,
			lastSeen:   now,
		}
		ps.dedup[producerID] = cp
		// Persist the checkpoint to the configured dedup topic so a
		// retry that spans a broker restart still dedups. Skipped
		// for the dedup topic itself to avoid recursion (and because
		// dedup records carry no producer headers).
		if p.Topic != b.dedupTopic {
			b.persistDedupCheckpoint(producerID, p, cp)
		}
	}
	r.Offset = offset
	if b.metrics != nil {
		b.metrics.IncProduce(int64(len(r.Value)))
	}

	for _, ch := range ps.subscribers {
		select {
		case ch <- r:
		case <-ctx.Done():
			return offset, ctx.Err()
		}
	}
	return offset, nil
}

func (b *Broker) publishClustered(ctx context.Context, p proto.PartitionRef, r proto.Record) (int64, error) {
	if !b.cluster.IsLeader() {
		return 0, &ErrNotLeader{LeaderID: b.cluster.LeaderID(), LeaderAddr: b.cluster.LeaderWireAddr()}
	}
	resp, err := b.cluster.Apply(cluster.EncodeAppend(cluster.AppendCommand{
		Topic:     p.Topic,
		Partition: p.Index,
		// Offset stays unstamped until Stage 9 milestone 3 teaches
		// the leader to predict the next-offset under partition lock.
		// Followers' FSM treats OffsetUnstamped as "let store.Append
		// assign the offset" — pre-Stage-9 behavior.
		Offset: cluster.OffsetUnstamped,
		Record: r,
	}))
	if err != nil {
		return 0, fmt.Errorf("broker: cluster apply: %w", err)
	}
	off, ok := resp.(int64)
	if !ok {
		return 0, errors.New("broker: cluster apply returned unexpected type")
	}
	return off, nil
}

// CreateTopic registers a topic. In cluster mode the operation is routed
// through Raft so every node's registry sees it.
func (b *Broker) CreateTopic(spec topic.Spec) error {
	if b.cluster == nil {
		return b.registry.Create(spec)
	}
	if !b.cluster.IsLeader() {
		return &ErrNotLeader{LeaderID: b.cluster.LeaderID(), LeaderAddr: b.cluster.LeaderWireAddr()}
	}
	_, err := b.cluster.Apply(cluster.EncodeCreateTopic(cluster.CreateTopicCommand{
		Name:           spec.Name,
		PartitionCount: spec.PartitionCount,
		RetentionMs:    spec.RetentionMs,
		SegmentBytes:   spec.SegmentBytes,
	}))
	return err
}

// UpdateTopicConfig changes the retention and segment-size
// settings of an existing topic. Replicated through Raft so every
// cluster node converges. retentionMs and segmentBytes <= 0 are
// "no change" so callers can update one knob at a time.
//
// Partition count is intentionally NOT updatable here — changing
// it post-creation would break per-partition ordering for
// already-produced records.
func (b *Broker) UpdateTopicConfig(name string, retentionMs, segmentBytes int64) error {
	if b.cluster == nil {
		return b.registry.UpdateConfig(name, retentionMs, segmentBytes)
	}
	if !b.cluster.IsLeader() {
		return &ErrNotLeader{LeaderID: b.cluster.LeaderID(), LeaderAddr: b.cluster.LeaderWireAddr()}
	}
	_, err := b.cluster.Apply(cluster.EncodeUpdateTopicConfig(cluster.UpdateTopicConfigCommand{
		Name:         name,
		RetentionMs:  retentionMs,
		SegmentBytes: segmentBytes,
	}))
	return err
}

// DeleteTopic removes the topic from the registry and drops every
// partition's records. Replicated through Raft so every cluster
// node converges on the same topic set; followers reject the call
// with ErrNotLeader so the SDK can redirect.
//
// Drop is destructive: all records and on-disk segment files are
// gone after the call returns. A subsequent CreateTopic of the
// same name starts at offset 0 with no historical data.
func (b *Broker) DeleteTopic(name string) error {
	if b.cluster == nil {
		if err := b.registry.Delete(name); err != nil {
			return err
		}
		return b.store.DeleteTopic(context.Background(), name)
	}
	if !b.cluster.IsLeader() {
		return &ErrNotLeader{LeaderID: b.cluster.LeaderID(), LeaderAddr: b.cluster.LeaderWireAddr()}
	}
	_, err := b.cluster.Apply(cluster.EncodeDeleteTopic(cluster.DeleteTopicCommand{Name: name}))
	return err
}

// Read returns up to maxRecords records starting at fromOffset, from the
// underlying store. Used by the network server for long-poll Fetch — the
// in-process Subscribe channel-fan-out is not the right shape over the
// wire, where consumers control their own polling cadence.
func (b *Broker) Read(ctx context.Context, p proto.PartitionRef, fromOffset int64, maxRecords int) ([]proto.Record, error) {
	if err := b.validate(p); err != nil {
		return nil, err
	}
	if b.metrics != nil {
		b.metrics.IncFetchRequest()
	}
	records, err := b.store.Read(ctx, p, fromOffset, maxRecords)
	if err != nil {
		return nil, err
	}
	if b.metrics != nil && len(records) > 0 {
		var bytes int64
		for _, r := range records {
			bytes += int64(len(r.Value))
		}
		b.metrics.IncFetch(int64(len(records)), bytes)
	}
	return records, nil
}

// Sync requests durable persistence of any buffered writes for the
// partition. Used by acks=durable producers to wait for fsync.
func (b *Broker) Sync(ctx context.Context, p proto.PartitionRef) error {
	if err := b.validate(p); err != nil {
		return err
	}
	return b.store.Sync(ctx, p)
}

// HighWater returns the next-to-be-appended offset for the partition.
// Replay-style consumers use this to bound catch-up reads at the moment
// of subscription rather than relying on a drain timeout.
func (b *Broker) HighWater(ctx context.Context, p proto.PartitionRef) (int64, error) {
	if err := b.validate(p); err != nil {
		return 0, err
	}
	return b.store.HighWater(ctx, p)
}

// ListSegments returns the partition's segment manifest at snapshot
// time — base offsets and current (.log, .idx) sizes captured under
// the partition's mutex. Pairs with FetchSegmentChunk to ship a
// brand-new follower the donor's full state in bounded chunks.
//
// Returns ErrSnapshotsUnsupported when the underlying store has no
// on-disk representation (e.g. the in-memory store used by tests).
func (b *Broker) ListSegments(ctx context.Context, p proto.PartitionRef) ([]storage.SegmentInfo, error) {
	if err := b.validate(p); err != nil {
		return nil, err
	}
	snapper, ok := b.store.(segmentSnapshotter)
	if !ok {
		return nil, ErrSnapshotsUnsupported
	}
	return snapper.ListSegments(ctx, p)
}

// FetchSegmentChunk returns a byte range from one segment file.
// Pairs with ListSegments — the caller bounds reads by the size
// reported there to observe a self-consistent prefix even of a
// still-active segment.
func (b *Broker) FetchSegmentChunk(ctx context.Context, p proto.PartitionRef, base int64, kind log.SegmentKind, offset int64, maxBytes int) ([]byte, error) {
	if err := b.validate(p); err != nil {
		return nil, err
	}
	snapper, ok := b.store.(segmentSnapshotter)
	if !ok {
		return nil, ErrSnapshotsUnsupported
	}
	return snapper.FetchSegmentChunk(ctx, p, base, kind, offset, maxBytes)
}

// segmentSnapshotter is the optional capability stores opt into for
// the data-dir bootstrap path. FileStore implements it; MemoryStore
// does not.
type segmentSnapshotter interface {
	ListSegments(ctx context.Context, p proto.PartitionRef) ([]storage.SegmentInfo, error)
	FetchSegmentChunk(ctx context.Context, p proto.PartitionRef, base int64, kind log.SegmentKind, offset int64, maxBytes int) ([]byte, error)
}

// ErrSnapshotsUnsupported indicates the broker's storage backend does
// not expose on-disk snapshots — typically because it has no
// on-disk representation (e.g. the in-memory store).
var ErrSnapshotsUnsupported = errors.New("broker: store does not support partition snapshots")

// Cluster returns the attached Raft cluster, or nil for a non-cluster
// broker. Used by the network server to route admin RPCs (cluster
// membership ops) to the underlying Raft API.
func (b *Broker) Cluster() *cluster.Cluster { return b.cluster }

// Subscribe returns a channel of records appended to the partition at or
// after fromOffset. Records appended before subscription are read from the
// store; live records are forwarded as they arrive.
func (b *Broker) Subscribe(ctx context.Context, p proto.PartitionRef, fromOffset int64) (<-chan proto.Record, error) {
	if err := b.validate(p); err != nil {
		return nil, err
	}
	ps := b.partitionState(p)
	sub := make(chan proto.Record, defaultSubscriberBuffer)
	out := make(chan proto.Record, defaultSubscriberBuffer)

	ps.mu.Lock()
	hw, err := b.store.HighWater(ctx, p)
	if err != nil {
		ps.mu.Unlock()
		return nil, fmt.Errorf("broker: high water %v: %w", p, err)
	}
	ps.subscribers = append(ps.subscribers, sub)
	ps.mu.Unlock()

	go b.pump(ctx, p, fromOffset, hw, sub, out)
	return out, nil
}

// pump drives one subscription: catch-up reads from the store, then live
// fan-out from sub. Closes out and unregisters on exit.
func (b *Broker) pump(ctx context.Context, p proto.PartitionRef, fromOffset, hwAtRegister int64, sub chan proto.Record, out chan<- proto.Record) {
	defer close(out)
	defer b.removeSubscriber(p, sub)

	cursor := fromOffset
	for cursor < hwAtRegister {
		records, err := b.store.Read(ctx, p, cursor, catchUpBatchSize)
		if err != nil || len(records) == 0 {
			return
		}
		for _, r := range records {
			select {
			case out <- r:
			case <-ctx.Done():
				return
			}
		}
		cursor += int64(len(records))
	}

	for {
		select {
		case r, ok := <-sub:
			if !ok {
				return
			}
			if r.Offset < cursor {
				continue
			}
			select {
			case out <- r:
				cursor = r.Offset + 1
			case <-ctx.Done():
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func (b *Broker) removeSubscriber(p proto.PartitionRef, target chan proto.Record) {
	ps := b.partitionState(p)
	ps.mu.Lock()
	defer ps.mu.Unlock()
	for i, ch := range ps.subscribers {
		if ch == target {
			ps.subscribers = append(ps.subscribers[:i], ps.subscribers[i+1:]...)
			return
		}
	}
}

func (b *Broker) validate(p proto.PartitionRef) error {
	n, err := b.registry.PartitionsFor(p.Topic)
	if err != nil {
		return fmt.Errorf("broker: %w", err)
	}
	if p.Index < 0 || p.Index >= n {
		return fmt.Errorf("broker: partition %d out of range for topic %q (have %d)", p.Index, p.Topic, n)
	}
	return nil
}

// evictStaleDedupLocked drops every dedup checkpoint older than
// b.dedupTTL. Caller holds the partition's mu. Bounds the dedup
// table at roughly the count of producers active inside the TTL
// window, regardless of how many distinct producer IDs the broker
// has ever seen.
func (b *Broker) evictStaleDedupLocked(ps *partitionState, now time.Time) {
	if ps.dedup == nil || b.dedupTTL <= 0 {
		return
	}
	cutoff := now.Add(-b.dedupTTL)
	for id, cp := range ps.dedup {
		if cp.lastSeen.Before(cutoff) {
			delete(ps.dedup, id)
		}
	}
}

// HydrateDedup pre-populates the broker's in-memory dedup table
// from an externally-loaded checkpoint. Used at startup by
// embed.NewDisk to restore dedup state from the persistent
// `__holocron_dedup` topic so retries that span a broker restart
// still dedup. Safe to call before any Publish; ignored after the
// partition's been touched (a Publish will see the existing entry
// and behave as expected).
func (b *Broker) HydrateDedup(producerID string, p proto.PartitionRef, lastSeq uint64, lastOffset int64, lastSeen time.Time) {
	ps := b.partitionState(p)
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if ps.dedup == nil {
		ps.dedup = make(map[string]producerCheckpoint)
	}
	ps.dedup[producerID] = producerCheckpoint{
		lastSeq:    lastSeq,
		lastOffset: lastOffset,
		lastSeen:   lastSeen,
	}
}

// persistDedupCheckpoint writes one dedup checkpoint to the
// configured persistent topic. Called from Publish after a
// successful Append. Bypasses broker.Publish to avoid recursion
// (a Publish to the dedup topic would itself trigger a dedup
// check). Errors are silently swallowed — a failed persist
// degrades to in-memory dedup for that record, matching the V1
// trade-off documented on WithDedupTopic.
func (b *Broker) persistDedupCheckpoint(producerID string, p proto.PartitionRef, cp producerCheckpoint) {
	if b.dedupTopic == "" {
		return
	}
	key := encodeDedupKey(producerID, p)
	value := encodeDedupValue(cp)
	_, _ = b.store.Append(context.Background(),
		proto.PartitionRef{Topic: b.dedupTopic, Index: 0},
		proto.Record{Key: key, Value: value})
}

// encodeDedupKey serializes (producerID, topic, partition) into a
// length-prefixed wire key for the persistent dedup topic. Layout:
//
//	[u32: len(producerID)] [bytes] [u32: len(topic)] [bytes] [u32: partition]
//
// Length prefixes avoid ambiguity when producer IDs or topic names
// contain delimiter characters.
func encodeDedupKey(producerID string, p proto.PartitionRef) []byte {
	pid := []byte(producerID)
	topic := []byte(p.Topic)
	out := make([]byte, 0, 4+len(pid)+4+len(topic)+4)
	var u32 [4]byte
	binary.BigEndian.PutUint32(u32[:], uint32(len(pid)))
	out = append(out, u32[:]...)
	out = append(out, pid...)
	binary.BigEndian.PutUint32(u32[:], uint32(len(topic)))
	out = append(out, u32[:]...)
	out = append(out, topic...)
	binary.BigEndian.PutUint32(u32[:], uint32(p.Index))
	out = append(out, u32[:]...)
	return out
}

// DecodeDedupKey is the inverse of encodeDedupKey. Exported so
// embed.NewDisk's hydration loop can decode records from the
// persistent dedup topic without duplicating the encoding logic.
func DecodeDedupKey(b []byte) (producerID string, p proto.PartitionRef, ok bool) {
	if len(b) < 4 {
		return "", proto.PartitionRef{}, false
	}
	pidLen := int(binary.BigEndian.Uint32(b[:4]))
	b = b[4:]
	if len(b) < pidLen+4 {
		return "", proto.PartitionRef{}, false
	}
	producerID = string(b[:pidLen])
	b = b[pidLen:]
	topicLen := int(binary.BigEndian.Uint32(b[:4]))
	b = b[4:]
	if len(b) < topicLen+4 {
		return "", proto.PartitionRef{}, false
	}
	p.Topic = string(b[:topicLen])
	b = b[topicLen:]
	p.Index = int32(binary.BigEndian.Uint32(b[:4]))
	return producerID, p, true
}

// encodeDedupValue serializes the checkpoint as
// [lastSeq u64] [lastOffset i64] [lastSeen unix-nanos i64].
func encodeDedupValue(cp producerCheckpoint) []byte {
	var buf [24]byte
	binary.BigEndian.PutUint64(buf[0:8], cp.lastSeq)
	binary.BigEndian.PutUint64(buf[8:16], uint64(cp.lastOffset))
	binary.BigEndian.PutUint64(buf[16:24], uint64(cp.lastSeen.UnixNano()))
	return buf[:]
}

// DecodeDedupValue parses the on-topic encoding back into the
// hydration parameters.
func DecodeDedupValue(b []byte) (lastSeq uint64, lastOffset int64, lastSeen time.Time, ok bool) {
	if len(b) != 24 {
		return 0, 0, time.Time{}, false
	}
	lastSeq = binary.BigEndian.Uint64(b[0:8])
	lastOffset = int64(binary.BigEndian.Uint64(b[8:16]))
	lastSeen = time.Unix(0, int64(binary.BigEndian.Uint64(b[16:24])))
	return lastSeq, lastOffset, lastSeen, true
}

func (b *Broker) partitionState(p proto.PartitionRef) *partitionState {
	b.mu.Lock()
	defer b.mu.Unlock()
	if ps, ok := b.partitions[p]; ok {
		return ps
	}
	ps := &partitionState{}
	b.partitions[p] = ps
	return ps
}

// producerIdempKey extracts (producer-id, sequence) from r's
// reserved headers. ok is true only when both headers are present
// and the sequence is exactly 8 bytes (big-endian uint64).
//
// Records without these headers opt out of idempotent dedup: the
// broker treats every Publish as a fresh write, preserving
// pre-batch-23 at-least-once semantics for SDKs that haven't been
// updated.
func producerIdempKey(r proto.Record) (id string, seq uint64, ok bool) {
	var (
		seenID  bool
		seqBuf  []byte
		seenSeq bool
	)
	for _, h := range r.Headers {
		switch h.Key {
		case proto.HeaderProducerID:
			id = string(h.Value)
			seenID = true
		case proto.HeaderProducerSeq:
			seqBuf = h.Value
			seenSeq = true
		}
	}
	if !seenID || !seenSeq || len(seqBuf) != 8 || id == "" {
		return "", 0, false
	}
	for _, b := range seqBuf {
		seq = (seq << 8) | uint64(b)
	}
	return id, seq, true
}

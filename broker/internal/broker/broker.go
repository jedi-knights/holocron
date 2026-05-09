// Package broker is the in-process publish/subscribe coordinator. It owns
// a storage.Store and a topic.Registry and arbitrates appends and fan-out
// subscriptions on top of them.
package broker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jedi-knights/holocron/broker/internal/cluster"
	"github.com/jedi-knights/holocron/broker/internal/groups"
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
	store    storage.Store
	registry *topic.Registry
	groups   *groups.Manager
	cluster  *cluster.Cluster
	metrics  *metrics.Registry

	mu         sync.Mutex
	partitions map[proto.PartitionRef]*partitionState
}

// partitionState gives each partition its own lock so cross-partition work
// can proceed in parallel — the throughput-first commitment.
type partitionState struct {
	mu          sync.Mutex
	subscribers []chan proto.Record
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

// Metrics returns the broker's metrics registry, or nil.
func (b *Broker) Metrics() *metrics.Registry { return b.metrics }

// New returns a Broker wired to the given Store and Registry.
func New(store storage.Store, registry *topic.Registry, opts ...Option) *Broker {
	b := &Broker{
		store:      store,
		registry:   registry,
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
		Record:    r,
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

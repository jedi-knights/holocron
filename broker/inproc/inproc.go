// Package inproc adapts a broker.Broker to the sdk.Transport interface so
// the SDK can call into a broker that lives in the same process. It is the
// Stage 1 wiring; Stage 3 will introduce a network-backed alternative
// without changing any SDK code.
package inproc

import (
	"context"
	"errors"
	"time"

	"github.com/jedi-knights/holocron/broker/internal/broker"
	"github.com/jedi-knights/holocron/broker/internal/topic"
	"github.com/jedi-knights/holocron/proto"
	"github.com/jedi-knights/holocron/sdk"
)

// Transport is an in-process implementation of sdk.Transport.
type Transport struct {
	b *broker.Broker
}

// New returns a Transport wired to the given Broker.
func New(b *broker.Broker) *Transport {
	return &Transport{b: b}
}

// Compile-time assertion that Transport satisfies sdk.Transport.
var _ sdk.Transport = (*Transport)(nil)

// Publish appends a record by delegating to the broker.
func (t *Transport) Publish(ctx context.Context, p proto.PartitionRef, r proto.Record) (int64, error) {
	return t.b.Publish(ctx, p, r)
}

// PublishBatch appends all records via successive in-process Publish
// calls. The broker holds the partition lock for each call; in cluster
// mode each Publish goes through Raft Apply individually. A future
// optimization could submit one Raft Apply per batch to amortize the
// consensus cost.
func (t *Transport) PublishBatch(ctx context.Context, p proto.PartitionRef, records []proto.Record) (int64, error) {
	if len(records) == 0 {
		return 0, nil
	}
	baseOffset, err := t.b.Publish(ctx, p, records[0])
	if err != nil {
		return 0, err
	}
	for i := 1; i < len(records); i++ {
		if _, err := t.b.Publish(ctx, p, records[i]); err != nil {
			return baseOffset, err
		}
	}
	return baseOffset, nil
}

// Subscribe opens a record stream by delegating to the broker.
// Inproc has no async failure mode after subscribe (the broker either
// hands off the channel cleanly or returns an error synchronously),
// so the error channel is always nil — selects on a nil channel block
// forever, which is the correct "never errors" semantic.
func (t *Transport) Subscribe(ctx context.Context, p proto.PartitionRef, fromOffset int64) (<-chan proto.Record, <-chan error, error) {
	ch, err := t.b.Subscribe(ctx, p, fromOffset)
	if err != nil {
		return nil, nil, err
	}
	return ch, nil, nil
}

// Commit records the offset durably via the broker's group manager.
// Brokers without a group manager treat Commit as a no-op so non-group
// SDK use keeps working unchanged.
func (t *Transport) Commit(ctx context.Context, group string, p proto.PartitionRef, offset int64) error {
	_ = ctx
	mgr := t.b.Groups()
	if mgr == nil {
		return nil
	}
	return mgr.Commit(group, p.Topic, p.Index, offset)
}

// JoinGroup signs the caller into a consumer group via the broker's group
// manager and translates the result into SDK types.
func (t *Transport) JoinGroup(ctx context.Context, group, memberID string, topics []string) (sdk.JoinResult, error) {
	_ = ctx
	mgr := t.b.Groups()
	if mgr == nil {
		return sdk.JoinResult{}, errors.New("inproc: broker has no group manager")
	}
	res, err := mgr.Join(group, memberID, topics)
	if err != nil {
		return sdk.JoinResult{}, err
	}
	out := sdk.JoinResult{
		MemberID:    res.MemberID,
		Generation:  res.Generation,
		Assignments: make([]sdk.Assignment, 0, len(res.Assignments)),
	}
	for _, a := range res.Assignments {
		out.Assignments = append(out.Assignments, sdk.Assignment{
			Partition:       a.Partition,
			CommittedOffset: a.CommittedOffset,
		})
	}
	return out, nil
}

// Heartbeat reports liveness via the broker's group manager. When
// maxWait > 0 the call delegates to HeartbeatWait, which holds the
// reply until the group rebalances or the deadline elapses — the
// in-process equivalent of the network long-poll.
func (t *Transport) Heartbeat(ctx context.Context, group, memberID string, generation int32, maxWait time.Duration) (sdk.HeartbeatResult, error) {
	mgr := t.b.Groups()
	if mgr == nil {
		return sdk.HeartbeatResult{}, errors.New("inproc: broker has no group manager")
	}
	if maxWait > 0 {
		res, err := mgr.HeartbeatWait(ctx, group, memberID, generation, maxWait)
		return sdk.HeartbeatResult{RebalanceNeeded: res.RebalanceNeeded}, err
	}
	res, err := mgr.Heartbeat(group, memberID, generation)
	return sdk.HeartbeatResult{RebalanceNeeded: res.RebalanceNeeded}, err
}

// LeaveGroup deregisters a member from a group.
func (t *Transport) LeaveGroup(ctx context.Context, group, memberID string) error {
	_ = ctx
	mgr := t.b.Groups()
	if mgr == nil {
		return nil
	}
	return mgr.Leave(group, memberID)
}

// Sync requests durable persistence of buffered writes for the partition.
func (t *Transport) Sync(ctx context.Context, p proto.PartitionRef) error {
	return t.b.Sync(ctx, p)
}

// PartitionsFor reads the topic's partition count from the broker registry.
func (t *Transport) PartitionsFor(ctx context.Context, topic string) (int32, error) {
	_ = ctx
	return t.b.Registry().PartitionsFor(topic)
}

// HighWater returns the next-to-be-appended offset for the partition.
// Used by replay-style consumers (e.g., schema registry) to bound
// catch-up reads at the moment of subscription rather than relying on a
// drain timeout. Reads up to the value returned by this call cover
// every record present at subscribe time.
func (t *Transport) HighWater(ctx context.Context, p proto.PartitionRef) (int64, error) {
	// b.Read with a zero-record request would block; instead, use the
	// store directly via a single read attempt at MaxInt64. The broker
	// validates p first, so an unknown topic surfaces as an error here
	// the same way it does on Publish/Subscribe.
	hw, err := t.b.HighWater(ctx, p)
	if err != nil {
		return 0, err
	}
	return hw, nil
}

// EnsureTopic registers the topic with the given partition count if it
// does not already exist. Returns nil for an existing topic regardless
// of its current partition count — callers that need exact-match
// behavior should validate via PartitionsFor first. Used by the connect
// Worker to lazily provision coordination topics.
func (t *Transport) EnsureTopic(ctx context.Context, name string, partitions int32) error {
	_ = ctx
	if partitions <= 0 {
		partitions = 1
	}
	err := t.b.CreateTopic(topic.Spec{Name: name, PartitionCount: partitions})
	if err != nil && errors.Is(err, topic.ErrTopicExists) {
		return nil
	}
	return err
}

// DeleteTopic removes the named topic and every record on it from
// the embedded broker. Mirrors the network transport's DeleteTopic
// so in-process and over-the-wire callers see identical semantics.
func (t *Transport) DeleteTopic(ctx context.Context, name string) error {
	_ = ctx
	return t.b.DeleteTopic(name)
}

// Close releases transport resources. The broker itself is not owned by
// the transport; callers shut it down separately.
func (t *Transport) Close() error { return nil }

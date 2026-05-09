package sdk

import (
	"context"

	"github.com/jedi-knights/holocron/proto"
)

// Transport is the contract between the SDK and the broker. It is a Strategy:
// Stage 1 ships an in-process implementation (broker/inproc); Stage 3 ships
// a network implementation (sdk/net). Stage 4 added the consumer-group
// coordination surface (JoinGroup / Heartbeat / LeaveGroup) without
// changing the Producer/Consumer surface above.
type Transport interface {
	// Publish appends r to the addressed partition and returns the offset
	// the broker assigned.
	Publish(ctx context.Context, p proto.PartitionRef, r proto.Record) (int64, error)

	// PublishBatch appends all records to the same partition in one
	// round trip. Returns the offset assigned to the first record;
	// subsequent records have contiguous offsets. Empty batches are
	// no-ops returning zero.
	PublishBatch(ctx context.Context, p proto.PartitionRef, records []proto.Record) (baseOffset int64, err error)

	// Subscribe returns a channel of records appended at or after
	// fromOffset, plus an error channel that surfaces protocol-level
	// failures from the underlying pump (e.g., a fetch returning
	// StatusRateLimited or StatusNotLeader). The records channel
	// closes when ctx is cancelled or the broker shuts down; the
	// error channel may be nil for transports that can't fail
	// asynchronously (inproc).
	Subscribe(ctx context.Context, p proto.PartitionRef, fromOffset int64) (<-chan proto.Record, <-chan error, error)

	// Commit records the consumed offset for the named consumer group.
	// Stage 4 makes this broker-durable.
	Commit(ctx context.Context, group string, p proto.PartitionRef, offset int64) error

	// PartitionsFor reports the current partition count for a topic so the
	// SDK can route produces and fan out subscribes.
	PartitionsFor(ctx context.Context, topic string) (int32, error)

	// JoinGroup signs the caller into the named consumer group and returns
	// the broker's assignment + committed offsets. An empty memberID asks
	// the broker to assign one.
	JoinGroup(ctx context.Context, group, memberID string, topics []string) (JoinResult, error)

	// Heartbeat reports liveness for memberID. RebalanceNeeded=true means
	// the caller should JoinGroup again.
	Heartbeat(ctx context.Context, group, memberID string, generation int32) (HeartbeatResult, error)

	// LeaveGroup deregisters memberID. Brokers tolerate Leave for unknown
	// members so close paths can call it unconditionally.
	LeaveGroup(ctx context.Context, group, memberID string) error

	// Sync requests the broker to durably persist any buffered writes
	// for the partition. Used by acks=durable producers; in-memory or
	// already-durable backends treat it as a no-op.
	Sync(ctx context.Context, p proto.PartitionRef) error

	Close() error
}

// Assignment is one partition assigned to a consumer-group member, paired
// with its committed offset. An offset of -1 means uncommitted.
type Assignment struct {
	Partition       proto.PartitionRef
	CommittedOffset int64
}

// NoOffset is the sentinel CommittedOffset value for uncommitted partitions.
const NoOffset = int64(-1)

// JoinResult is what JoinGroup returns.
type JoinResult struct {
	MemberID    string
	Generation  int32
	Assignments []Assignment
}

// HeartbeatResult is what Heartbeat returns.
type HeartbeatResult struct {
	RebalanceNeeded bool
}

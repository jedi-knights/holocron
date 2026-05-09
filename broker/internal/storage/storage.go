// Package storage defines the broker's persistence boundary. Implementations
// are pluggable via the Strategy pattern: stage 1 ships an in-memory store,
// stage 2 adds a file-backed segmented log. The broker core depends only on
// the Store interface — never on a concrete implementation.
package storage

import (
	"context"

	"github.com/jedi-knights/holocron/proto"
)

// Store is the persistence contract. It is intentionally narrow: append a
// record, read a contiguous slice from an offset, report the high-water mark.
// Anything richer (compaction, retention) is the broker's responsibility,
// implemented on top of these primitives.
type Store interface {
	Append(ctx context.Context, p proto.PartitionRef, r proto.Record) (offset int64, err error)
	Read(ctx context.Context, p proto.PartitionRef, fromOffset int64, maxRecords int) ([]proto.Record, error)
	HighWater(ctx context.Context, p proto.PartitionRef) (int64, error)
	// Sync flushes any buffered writes for the partition to durable
	// storage. Used by acks=durable producers; in-memory stores treat
	// it as a no-op.
	Sync(ctx context.Context, p proto.PartitionRef) error
	// DeleteTopic removes every partition's records and segment files
	// for the named topic. Used when the broker drops a topic so the
	// underlying storage doesn't accumulate orphaned bytes. In-memory
	// stores discard the in-RAM map; file-backed stores rm the
	// per-topic directory tree.
	DeleteTopic(ctx context.Context, topic string) error
	Close() error
}

package connect

import (
	"context"

	"github.com/jedi-knights/holocron/proto"
)

// SourceRecord is one event produced by a SourceTask. Topic and payload
// are required; partition routing falls back to the producer's
// Partitioner when Partition is nil.
//
// SourceOffset is opaque to the framework: it captures the position in
// the source system (file byte offset, database LSN, API cursor) so a
// task can resume cleanly after a restart. The Worker persists it once
// the record is durably published.
type SourceRecord struct {
	Topic        string
	Partition    *int32
	Key          []byte
	Value        []byte
	Headers      []proto.Header
	SourceOffset map[string]any
}

// SourceConnector is a configured ETL source. It is a factory that
// produces one or more SourceTasks; each task runs in its own goroutine.
// Use multiple tasks to parallelize over independent streams (e.g., one
// per file, one per shard).
type SourceConnector interface {
	// Name identifies the connector. Used in logs and as the offset
	// namespace key.
	Name() string

	// Tasks returns up to maxTasks SourceTasks that together cover the
	// source's data. Implementations are free to return fewer if the
	// source can't be partitioned that finely.
	Tasks(maxTasks int) ([]SourceTask, error)
}

// SourceTask reads from the external system and emits records.
//
// Lifecycle:
//
//	Init    — exactly once; resume from the supplied stored offsets.
//	Poll    — called repeatedly until ctx is cancelled. Returns whatever
//	          records are currently available, or an empty slice and nil
//	          error to signal "no records right now."
//	Commit  — called by the Worker after the records returned by a Poll
//	          have been durably published. The task persists its source
//	          position via SourceOffset on each record.
//	Close   — release resources. Always called, even on error.
type SourceTask interface {
	Init(ctx context.Context, storedOffsets []map[string]any) error
	Poll(ctx context.Context) ([]SourceRecord, error)
	Commit(ctx context.Context, records []SourceRecord) error
	Close() error
}

// SinkConnector is a configured ETL sink. It declares the topics it
// consumes and emits SinkTasks that share a consumer group.
type SinkConnector interface {
	// Name identifies the connector. Becomes the consumer group ID
	// unless overridden by the SinkTaskConfig.
	Name() string

	// Topics is the list of holocron topics this sink consumes from.
	Topics() []string

	// Tasks returns up to maxTasks SinkTasks. They share a consumer
	// group, so the broker spreads partitions across them.
	Tasks(maxTasks int) ([]SinkTask, error)
}

// SinkTask receives batches of records and writes them to the external
// system.
//
// Lifecycle:
//
//	Init  — exactly once.
//	Put   — called with a batch of records read from the broker. The
//	        task buffers or writes them as it sees fit; it must NOT
//	        block forever.
//	Flush — called by the Worker before committing the consumer offset.
//	        The task ensures every record passed to Put since the last
//	        Flush is durably written. Returning nil means "I commit to
//	        not losing those records on a crash"; the Worker then
//	        commits the offset on the broker.
//	Close — release resources.
type SinkTask interface {
	Init(ctx context.Context) error
	Put(ctx context.Context, records []proto.Record) error
	Flush(ctx context.Context) error
	Close() error
}

package streams

import (
	"context"
	"errors"
	"fmt"

	"github.com/jedi-knights/holocron/proto"
	"github.com/jedi-knights/holocron/sdk"
)

// KTable is a materialized view of a topic, last-value-wins by key.
// It's the streams analog of Kafka's KTable: a continuously-updated
// snapshot of the latest record per key, backed by a state store so
// stream pipelines can join against it efficiently.
//
// Tombstones (records with nil Value) delete the key from the view.
// Records without a Key are ignored — the view's identity is its key
// space, and a record with no key has nowhere to live.
//
// A KTable runs one consumer goroutine per source partition (driven by
// the topology's maxTasks); the store is shared across them. As of
// V1, the same Get/Put atomicity caveat as Count applies under
// maxTasks > 1, mitigated by the streamsHeartbeatInterval tightening
// from batch 9.
type KTable struct {
	topology *Topology
	source   string
	store    StateStore
	group    string
}

// Table returns a materialized view of source backed by the named
// store. The first call to Table for a given (source, store) pair
// registers the table with the topology — subsequent calls share the
// same KTable.
func (t *Topology) Table(source, storeName string) *KTable {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, kt := range t.tables {
		if kt.source == source && kt.store == t.stores[storeName] {
			return kt
		}
	}
	store := t.stores[storeName]
	if store == nil {
		store = t.openStoreLocked(storeName)
		t.stores[storeName] = store
	}
	kt := &KTable{
		topology: t,
		source:   source,
		store:    store,
		group:    fmt.Sprintf("holocron-streams-ktable-%s-%s", source, storeName),
	}
	t.tables = append(t.tables, kt)
	return kt
}

// Get returns the latest value for key, or (nil, false) if the key
// has never been seen or its tombstone has fired. Safe for concurrent
// use by other pipeline operators.
func (k *KTable) Get(key []byte) ([]byte, bool) {
	return k.store.Get(key)
}

// runTable drives the per-table consumer loop, applying every record
// to the underlying store. Errors abort the loop; ctx-cancellation is
// the clean shutdown path.
func (t *Topology) runTable(ctx context.Context, kt *KTable) {
	defer t.wg.Done()

	consumer, err := sdk.NewConsumer(t.transport,
		sdk.WithGroup(kt.group),
		sdk.WithHeartbeatInterval(streamsHeartbeatInterval),
	)
	if err != nil {
		t.recordErr(fmt.Errorf("streams: ktable %q consumer: %w", kt.source, err))
		return
	}
	defer consumer.Close()
	if err := consumer.Subscribe(ctx, kt.source, 0); err != nil {
		t.recordErr(fmt.Errorf("streams: ktable %q subscribe: %w", kt.source, err))
		return
	}

	for {
		if err := ctx.Err(); err != nil {
			return
		}
		records, err := consumer.Poll(ctx, 256)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			t.recordErr(fmt.Errorf("streams: ktable %q poll: %w", kt.source, err))
			return
		}
		for _, r := range records {
			if len(r.Key) == 0 {
				continue
			}
			if r.Value == nil {
				kt.store.Delete(r.Key)
				continue
			}
			kt.store.Put(r.Key, r.Value)
		}
		if len(records) > 0 {
			for part, off := range consumer.LatestOffsets() {
				if err := consumer.Commit(ctx, part, off+1); err != nil {
					t.recordErr(fmt.Errorf("streams: ktable %q commit %v: %w", kt.source, part, err))
					return
				}
			}
		}
	}
}

// TableJoinFunc combines a stream record with the matching KTable
// entry's value (or nil when the table doesn't have the key) into one
// or more output records. Returning an empty slice drops the record —
// the join's filter-in-place mechanism for stream-side records that
// shouldn't propagate.
type TableJoinFunc func(streamRec proto.Record, tableValue []byte, tableHit bool) []proto.Record

// JoinTable joins this stream against a KTable: for each stream
// record, look up its key in the table and call joinFn. Inner-join
// semantics by default — joinFn decides whether to emit on a miss.
//
// Unlike Stream.Join (stream-stream join with windowing), JoinTable
// has no time component: the table's current value at the moment the
// stream record is processed is what joinFn sees.
func (s *Stream) JoinTable(table *KTable, joinFn TableJoinFunc) *Stream {
	return s.appendOp(func(r proto.Record, _ *Topology) []proto.Record {
		val, ok := table.Get(r.Key)
		return joinFn(r, val, ok)
	})
}

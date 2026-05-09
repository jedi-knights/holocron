package streams

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jedi-knights/holocron/proto"
	"github.com/jedi-knights/holocron/sdk"
)

// Topology is the DAG of operators a streams program declares. Build it
// at construction time; Run drives every registered pipeline against the
// transport.
// TimestampExtractor returns event-time (in Unix nanoseconds) for a
// record. Default extractors use Record.Timestamp directly.
type TimestampExtractor func(proto.Record) int64

// DefaultTimestampExtractor uses Record.Timestamp as the event time.
func DefaultTimestampExtractor(r proto.Record) int64 { return r.Timestamp }

type Topology struct {
	transport      sdk.Transport
	maxTasks       int
	useChangelog   bool
	tsExtractor    TimestampExtractor
	punctuationInt time.Duration
	idleWatermarkD time.Duration
	onError        ErrorHandler
	dlqTopic       string

	mu           sync.Mutex
	pipes        []pipeline
	joins        []joinPipeline
	tables       []*KTable // materialized views; one consumer goroutine per table
	stores       map[string]*PartitionedStore
	foreachSeq   int
	watermark    int64 // max event-time observed across all pipelines
	lastRecordAt int64 // wall-clock unix ns of most recent record receipt
	running      bool
	stopFunc     context.CancelFunc
	wg           sync.WaitGroup
	errs         []error
	errsMu       sync.Mutex
}

// Option configures a Topology.
type Option func(*Topology)

// WithMaxTasks sets the maximum number of parallel consumer goroutines
// per pipeline. The actual count is capped by the source topic's
// partition count — running more tasks than partitions doesn't help
// because the broker will idle the surplus. Default 1.
func WithMaxTasks(n int) Option {
	return func(t *Topology) { t.maxTasks = n }
}

// WithChangelogStores switches the topology's default state-store
// factory from in-memory to ChangelogStore — every Put writes a record
// to <store-name>-changelog and on startup the store replays the topic
// to rebuild local state.
//
// Caller must create each changelog topic ahead of time (single
// partition, broker-side compaction recommended). Stores accessed via
// topology.Store(name) before Start ensures the topic exists.
func WithChangelogStores() Option {
	return func(t *Topology) { t.useChangelog = true }
}

// WithTimestampExtractor overrides how event-time is derived from a
// record. The default uses Record.Timestamp, which is set by the broker
// on append unless the producer pre-populated it.
//
// Event-time drives windowing decisions and watermark advancement; for
// late-arriving records (event-time < watermark) windowed operators
// skip the record by default.
func WithTimestampExtractor(fn TimestampExtractor) Option {
	return func(t *Topology) { t.tsExtractor = fn }
}

// WithPunctuationInterval enables a per-pipeline punctuator goroutine
// that fires on the given tick to drive window-close emission even
// when no input records are flowing. Default zero disables the
// punctuator (windows close lazily on the next record).
func WithPunctuationInterval(d time.Duration) Option {
	return func(t *Topology) { t.punctuationInt = d }
}

// ErrorHandler observes errors that occur inside pipeline goroutines —
// op panics, sink-produce failures, consumer poll errors. Without
// a handler errors only surface via Stop()'s aggregated return;
// with one, monitoring code sees them live.
//
// The handler runs on the pipeline goroutine that observed the
// error, so it must be cheap and non-blocking. Long-running work
// belongs in a downstream queue.
type ErrorHandler func(err error)

// WithErrorHandler registers a callback fired live whenever a
// pipeline error is recorded. Pairs with the existing recordErr
// surface (which collects errors for Stop) — the handler observes
// the same errors as they happen.
//
// Op panics inside pipeline closures are recovered and routed to
// the handler instead of killing the goroutine, so a buggy op
// doesn't take down the whole pipeline. Records that triggered the
// panic are dropped; subsequent records continue to flow.
func WithErrorHandler(fn ErrorHandler) Option {
	return func(t *Topology) { t.onError = fn }
}

// WithDLQ routes records that panicked a pipeline op to the named
// dead-letter-queue topic. The DLQ record preserves the original
// record's key, value, and headers; an extra `holocron.dlq.error`
// header carries the panic message for forensic context.
//
// Pairs with WithErrorHandler — handler fires for live
// observability, DLQ captures the record for replay or
// investigation. The DLQ produce is best-effort: a failure to
// write the DLQ doesn't affect the pipeline, just gets recorded
// via recordErr like any other internal failure. The topic must
// exist beforehand.
const HeaderDLQError = "holocron.dlq.error"

// WithDLQ option.
func WithDLQ(topic string) Option {
	return func(t *Topology) { t.dlqTopic = topic }
}

// WithIdleWatermark enables a topology-level idle-detection goroutine.
// When no record has arrived for d, the watermark is advanced to the
// current wall-clock time so downstream time-driven operators (window
// close, join prune) keep making progress on quiet streams.
//
// Enabling this option mixes event-time and wall-clock — only use it
// when event-time roughly tracks wall-clock (real-time streams). For
// historical-replay pipelines where event-time may run hours behind
// wall-clock, leave this disabled or the watermark will jump to "now"
// and force-close every pending window prematurely.
func WithIdleWatermark(d time.Duration) Option {
	return func(t *Topology) { t.idleWatermarkD = d }
}

// eventTime returns the event-time for r, falling back to wall-clock
// nanoseconds if no extractor is configured.
func (t *Topology) eventTime(r proto.Record) int64 {
	if t.tsExtractor != nil {
		return t.tsExtractor(r)
	}
	return DefaultTimestampExtractor(r)
}

// advanceWatermark advances the topology's watermark to max(current, ts)
// using compare-and-swap so concurrent producers don't lose updates.
func (t *Topology) advanceWatermark(ts int64) int64 {
	for {
		cur := atomic.LoadInt64(&t.watermark)
		if ts <= cur {
			return cur
		}
		if atomic.CompareAndSwapInt64(&t.watermark, cur, ts) {
			return ts
		}
	}
}

// Watermark returns the current watermark — the highest event-time
// the topology has observed across all pipelines.
func (t *Topology) Watermark() int64 {
	return atomic.LoadInt64(&t.watermark)
}

// PunctuatorFunc is called by the punctuator goroutine on each tick.
// Returned records flow through the registering pipeline's sink. Used
// by stateful operators to emit closed windows even when no input
// records are arriving.
type PunctuatorFunc func(nowNanos int64) []proto.Record

// pipeline is a fully assembled stream from a source topic through a
// chain of ops, optionally terminated by a sink topic.
type pipeline struct {
	source      string
	ops         []op
	sink        string // empty if the pipeline does not write back to a topic
	group       string // consumer group this pipeline reads under
	punctuators []PunctuatorFunc
}

// op is the kernel of every operator: take a record, the partition the
// record originated from, and optionally consult the topology — return
// zero or more output records. Stateful operators use partition to
// scope their state-store accesses to a partition-specific substore,
// which keeps tasks operating on disjoint partitions race-free.
type op func(rec proto.Record, t *Topology, partition int) []proto.Record

// New constructs a Topology bound to a Transport.
func New(transport sdk.Transport, opts ...Option) (*Topology, error) {
	if transport == nil {
		return nil, errors.New("streams: New requires a Transport")
	}
	t := &Topology{
		transport: transport,
		maxTasks:  1,
		stores:    make(map[string]*PartitionedStore),
	}
	for _, opt := range opts {
		opt(t)
	}
	return t, nil
}

// Store returns the named state store, creating one on first access.
// Returned stores are partition-scoped: operators read and write to a
// per-partition substore via Store(name).For(partition); external
// inspectors call Store(name).Get / Range to view aggregated state
// across every partition.
//
// The default factory is in-memory — each partition gets its own
// MemoryStore. WithChangelogStores switches to a single shared
// ChangelogStore: every partition lookup returns the same instance,
// preserving pre-batch-21 changelog semantics until per-partition
// changelog topics arrive in a future stage.
//
// A factory failure (e.g. changelog topic missing) falls back to
// per-partition in-memory and records the error for Stop to surface.
func (t *Topology) Store(name string) *PartitionedStore {
	t.mu.Lock()
	defer t.mu.Unlock()
	if s, ok := t.stores[name]; ok {
		return s
	}
	s := t.openStoreLocked(name)
	t.stores[name] = s
	return s
}

// openStoreLocked builds the PartitionedStore wrapper for name. Caller
// holds t.mu.
//
// In changelog mode the factory opens a fresh per-partition
// ChangelogStore on demand: each partition's state lives in its own
// partition of the <name>-changelog topic, so a rebalance that
// hands partition N to a different member rebuilds N's state from
// N's changelog without leaking other partitions' history.
//
// Every partition of the changelog topic is opened eagerly so a
// subsequent aggregated Get/Range on the PartitionedStore — common
// in tests and external inspection — reflects the replayed state
// without first having to drive a record through each partition.
func (t *Topology) openStoreLocked(name string) *PartitionedStore {
	if !t.useChangelog {
		return NewPartitionedStore(NewMemoryStoreFactory())
	}
	transport := t.transport
	ps := NewPartitionedStore(func(partition int) StateStore {
		cs, err := OpenChangelogStorePartition(context.Background(), transport, name, partition)
		if err != nil {
			t.recordErr(fmt.Errorf("streams: changelog store %q partition %d (falling back to memory): %w", name, partition, err))
			return NewMemoryStore()
		}
		return cs
	})
	if n, err := transport.PartitionsFor(context.Background(), name+"-changelog"); err == nil {
		for i := range int(n) {
			ps.For(i)
		}
	}
	return ps
}

// Stream begins a new pipeline reading from sourceTopic.
func (t *Topology) Stream(sourceTopic string) *Stream {
	return &Stream{topology: t, source: sourceTopic}
}

// Stream is a lazily-built pipeline. Each operator method returns a new
// Stream so chains compose naturally; Stream is safe to discard mid-chain
// (no goroutines spawned until Run).
type Stream struct {
	topology    *Topology
	source      string
	ops         []op
	punctuators []PunctuatorFunc
}

func (s *Stream) appendOp(o op) *Stream {
	next := *s
	next.ops = make([]op, len(s.ops)+1)
	copy(next.ops, s.ops)
	next.ops[len(s.ops)] = o
	return &next
}

// withPunctuator returns a new Stream carrying fn in addition to all
// existing punctuators. Used by stateful operators (window ops) to
// register tick-driven emission alongside the appended op.
func (s *Stream) withPunctuator(fn PunctuatorFunc) *Stream {
	next := *s
	next.punctuators = append(append([]PunctuatorFunc(nil), s.punctuators...), fn)
	return &next
}

// Throttle caps emission to perSec records per second using a
// token bucket with capacity = perSec. A burst arriving in less
// time than 1/perSec drains the bucket; subsequent records during
// the burst are dropped until tokens refill.
//
// Drop-based, not blocking — pipelines can't queue records past an
// op so a blocking throttle would stall every upstream task. Use
// for cost control and downstream backpressure where dropping is
// preferable to queuing.
//
// perSec <= 0 drops everything; if you want unthrottled, just
// don't insert this op. Concurrent tasks share one bucket via a
// mutex so the rate is global.
func (s *Stream) Throttle(perSec float64) *Stream {
	if perSec <= 0 {
		return s.Filter(func(proto.Record) bool { return false })
	}
	var (
		mu     sync.Mutex
		tokens = perSec // bucket starts full so the first burst gets `perSec` tokens
		lastNs = time.Now().UnixNano()
	)
	cap := perSec
	return s.appendOp(func(r proto.Record, _ *Topology, _ int) []proto.Record {
		mu.Lock()
		defer mu.Unlock()
		now := time.Now().UnixNano()
		elapsed := float64(now-lastNs) / 1e9
		lastNs = now
		tokens += elapsed * perSec
		if tokens > cap {
			tokens = cap
		}
		if tokens < 1 {
			return nil
		}
		tokens -= 1
		return []proto.Record{r}
	})
}

// Sample emits the 1st record and every nth record thereafter,
// dropping the rest. n=1 is a no-op (every record passes); n<=0
// drops everything. Shared atomic counter across multi-task
// pipelines so the sampling rate is global, not per-task.
//
// Useful for downsampling high-volume streams when a
// representative subset is wanted rather than a windowed
// aggregate. The 1st-record bias means downstream observers see
// data immediately rather than waiting for the first n records to
// pass.
func (s *Stream) Sample(n int) *Stream {
	if n == 1 {
		return s
	}
	if n <= 0 {
		return s.Filter(func(proto.Record) bool { return false })
	}
	step := int64(n)
	var seen atomic.Int64
	return s.appendOp(func(r proto.Record, _ *Topology, _ int) []proto.Record {
		// 1, n+1, 2n+1, ... pass. seen-1 gives the zero-based
		// index of this record so the modulo lands the boundary
		// on indices 0, n, 2n, ... — i.e. records 1, n+1, 2n+1.
		idx := seen.Add(1) - 1
		if idx%step != 0 {
			return nil
		}
		return []proto.Record{r}
	})
}

// Distinct drops records whose derived key has been seen before in
// this pipeline run. keyFn extracts the dedup key from the record;
// returning a zero-length slice keys on "no key" (one such record
// will pass through, the rest are dropped).
//
// Dedup state is in-memory only — a topology restart resets it.
// The operator is responsible for bounding cardinality: a stream
// of all-distinct keys will eventually grow the internal map. For
// time-bounded dedup, pair with a windowed aggregation instead.
//
// With multi-task pipelines the seen-set is shared via a
// concurrent map, so cross-task duplicates are filtered too.
func (s *Stream) Distinct(keyFn func(proto.Record) []byte) *Stream {
	var seen sync.Map // string(key) → struct{}{}
	return s.appendOp(func(r proto.Record, _ *Topology, _ int) []proto.Record {
		k := string(keyFn(r))
		if _, loaded := seen.LoadOrStore(k, struct{}{}); loaded {
			return nil
		}
		return []proto.Record{r}
	})
}

// Skip drops the first n records that flow through the pipeline
// and passes through everything after. Inverse of Take. Shared
// atomic counter across multi-task pipelines so the dropped
// prefix is global, not per-task.
//
// Useful for "skip the historical bootstrap" scenarios when the
// pipeline restarts from offset 0 but the operator only cares
// about records past a certain point.
func (s *Stream) Skip(n int) *Stream {
	if n <= 0 {
		return s
	}
	skipUntil := int64(n)
	var seen atomic.Int64
	return s.appendOp(func(r proto.Record, _ *Topology, _ int) []proto.Record {
		if seen.Add(1) <= skipUntil {
			return nil
		}
		return []proto.Record{r}
	})
}

// Take caps emission at n records — past the cap, the operator
// drops every record. The pipeline keeps consuming from the source
// so committed offsets advance, but produces no output past the
// cap. With multi-task pipelines the counter is shared via a
// captured atomic so the cap is global, not per-task.
//
// Useful for bounded test pipelines, replay-up-to-N scenarios, and
// circuit-breakers that should let only a prefix of a stream
// through. Returns a fresh Stream; callers chain a sink (.To, etc.)
// to finalize.
func (s *Stream) Take(n int) *Stream {
	if n <= 0 {
		return s.Filter(func(proto.Record) bool { return false })
	}
	limit := int64(n)
	var seen atomic.Int64
	return s.appendOp(func(r proto.Record, _ *Topology, _ int) []proto.Record {
		// Increment-then-check so concurrent tasks can't both
		// observe seen<limit and both emit a record that pushes
		// the count past the cap.
		if seen.Add(1) > limit {
			return nil
		}
		return []proto.Record{r}
	})
}

// Filter drops records where pred returns false.
func (s *Stream) Filter(pred func(proto.Record) bool) *Stream {
	return s.appendOp(func(r proto.Record, _ *Topology, _ int) []proto.Record {
		if pred(r) {
			return []proto.Record{r}
		}
		return nil
	})
}

// FilterNot drops records where pred returns true. Sugar for
// `Filter(func(r) bool { return !pred(r) })` — reads more clearly
// when the natural predicate is the negation ("drop tombstones",
// "drop heartbeat records", etc.).
func (s *Stream) FilterNot(pred func(proto.Record) bool) *Stream {
	return s.Filter(func(r proto.Record) bool { return !pred(r) })
}

// Map transforms each record into a new record. Drop a record by
// returning a zero-value Record from a Filter step rather than from Map.
func (s *Stream) Map(fn func(proto.Record) proto.Record) *Stream {
	return s.appendOp(func(r proto.Record, _ *Topology, _ int) []proto.Record {
		return []proto.Record{fn(r)}
	})
}

// FlatMap transforms each record into zero or more records.
func (s *Stream) FlatMap(fn func(proto.Record) []proto.Record) *Stream {
	return s.appendOp(func(r proto.Record, _ *Topology, _ int) []proto.Record {
		return fn(r)
	})
}

// SelectKey replaces each record's key with fn(record). Value and
// headers are preserved. Useful before a join or
// GroupByKey-driven aggregation when the join/group key is
// derived from the value rather than the original key.
//
// Caveat: re-keying does NOT repartition. With a multi-partition
// source topic, two records with the new same key may live on
// different partitions; downstream `GroupByKey` aggregations stay
// per-partition (the per-partition state stores from batch 21).
// Pair with `Through` to a single-partition intermediate topic if
// you need cross-partition grouping after re-key.
func (s *Stream) SelectKey(fn func(proto.Record) []byte) *Stream {
	return s.appendOp(func(r proto.Record, _ *Topology, _ int) []proto.Record {
		next := r
		next.Key = fn(r)
		return []proto.Record{next}
	})
}

// MapValues transforms each record's value via fn while preserving
// key and headers. Lighter-weight than Map for the common case of
// "decode/transform/re-encode the payload" — saves the user from
// manually copying key/headers through.
func (s *Stream) MapValues(fn func([]byte) []byte) *Stream {
	return s.appendOp(func(r proto.Record, _ *Topology, _ int) []proto.Record {
		next := r
		next.Value = fn(r.Value)
		return []proto.Record{next}
	})
}

// MapKeyValue transforms key and value together, returning a new
// (key, value) pair while preserving headers. Reads more clearly
// than Map when the transformation only touches those two fields
// and avoids having to construct a fresh Record literal.
func (s *Stream) MapKeyValue(fn func(key, value []byte) (newKey, newValue []byte)) *Stream {
	return s.appendOp(func(r proto.Record, _ *Topology, _ int) []proto.Record {
		k, v := fn(r.Key, r.Value)
		next := r
		next.Key = k
		next.Value = v
		return []proto.Record{next}
	})
}

// Peek calls fn on each record without modifying the stream. Used
// for side effects: logging, tracing, metrics. Returns the
// original record unchanged so .Peek can be inserted anywhere in
// a chain without disturbing downstream operators.
//
// fn must not retain references to r — the runtime reuses
// Record instances. Copy fields if you need to outlive the call.
func (s *Stream) Peek(fn func(proto.Record)) *Stream {
	return s.appendOp(func(r proto.Record, _ *Topology, _ int) []proto.Record {
		fn(r)
		return []proto.Record{r}
	})
}

// Tap is an alias for Peek matching the Kafka-Streams convention
// of "tap into the stream" for side effects. Reads more naturally
// than Peek when the operation is documenting an observation
// rather than peeking past a wall.
func (s *Stream) Tap(fn func(proto.Record)) *Stream {
	return s.Peek(fn)
}

// Branch splits a stream into N parallel streams, each carrying
// only the records that match its predicate. Predicates are
// evaluated in order; a record routes to the FIRST branch whose
// predicate returns true. Records that match none are dropped.
//
// Each returned Stream is an independent pipeline — finalize each
// with .To(topic) to write its slice of the source. Because each
// branch becomes its own consumer-group pipeline reading the same
// source topic, total read bandwidth scales linearly with the
// branch count; a single-consumer fan-out is a future optimization.
func (s *Stream) Branch(preds ...func(proto.Record) bool) []*Stream {
	out := make([]*Stream, len(preds))
	for i := range preds {
		// Capture earlier predicates so this branch only sees
		// records they didn't claim. Captures are read-only after
		// Branch returns — predicates can't be added later.
		priors := append([]func(proto.Record) bool(nil), preds[:i]...)
		mine := preds[i]
		out[i] = s.Filter(func(r proto.Record) bool {
			for _, p := range priors {
				if p(r) {
					return false
				}
			}
			return mine(r)
		})
	}
	return out
}

// Through routes records through an intermediate broker topic and
// resumes the chain on the other side. Equivalent to
// `s.To(topic); topology.Stream(topic)` — the upstream chain
// terminates by writing to topic; the returned Stream is a fresh
// pipeline reading from topic with no upstream ops attached.
//
// Lets a topology persist intermediate state for durability or
// repartitioning without manually splitting into two topologies.
// The intermediate topic must already exist; Through does not
// create it (mirrors Stream and To).
func (s *Stream) Through(topic string) *Stream {
	s.To(topic)
	return s.topology.Stream(topic)
}

// To registers the pipeline with the topology, writing each output
// record to topic. After To, the pipeline is finalized; further chaining
// has no effect. The pipeline reads under a deterministic consumer group
// (`holocron-streams-<source>-<sink>`) so committed offsets resume
// across topology restarts.
func (s *Stream) To(topic string) {
	s.topology.mu.Lock()
	defer s.topology.mu.Unlock()
	s.topology.pipes = append(s.topology.pipes, pipeline{
		source:      s.source,
		ops:         s.ops,
		sink:        topic,
		group:       fmt.Sprintf("holocron-streams-%s-%s", s.source, topic),
		punctuators: append([]PunctuatorFunc(nil), s.punctuators...),
	})
}

// ToTable materializes the upstream pipeline into a KTable backed
// by the named state store. Each output record updates the table
// last-value-wins by key; a record with nil Value tombstones the
// key. Records without a Key are ignored — the table's identity is
// its key space. State is partition-scoped via the partitioned
// store, the same as Count/Aggregate/Reduce.
//
// Returns the resulting KTable so a subsequent JoinTable can look up
// against it. The pipeline is finalized as a terminal — no sink
// topic — and further chaining on the source Stream has no effect.
//
// Without ToTable the only way to derive a table from a transformed
// stream is `s.Through(topic); top.Table(topic, store)`, which
// requires creating an intermediate topic and runs an extra consumer
// goroutine to replay it. ToTable does the materialization in-place.
func (s *Stream) ToTable(storeName string) *KTable {
	store := s.topology.Store(storeName)
	updated := s.appendOp(func(r proto.Record, _ *Topology, partition int) []proto.Record {
		if r.Key == nil {
			return nil
		}
		sub := store.For(partition)
		if r.Value == nil {
			sub.Delete(r.Key)
		} else {
			sub.Put(append([]byte(nil), r.Key...), append([]byte(nil), r.Value...))
		}
		return nil
	})
	updated.ForEach()
	// The returned KTable is a free-standing read view — it shares
	// the store with the just-registered ForEach pipeline but is
	// NOT added to t.tables, so the topology won't spawn a runTable
	// consumer for it. The upstream pipeline is the only writer.
	return &KTable{
		topology: s.topology,
		source:   "",
		store:    store,
		group:    "",
	}
}

// Print registers a terminal that writes one debug line per record
// to os.Stdout, prefixed with prefix. Sugar for the common
// `ForEachFunc(func(r) { fmt.Println(...) })` pattern when
// debugging a pipeline. Output format:
//
//	prefix offset=N key=K value=V
//
// PrintTo lets callers redirect to any io.Writer (a test buffer,
// log writer, or io.Discard for benchmarks).
func (s *Stream) Print(prefix string) {
	s.PrintTo(os.Stdout, prefix)
}

// PrintTo registers a debug terminal that writes one line per
// record to w, prefixed with prefix. See Print for format.
func (s *Stream) PrintTo(w io.Writer, prefix string) {
	s.ForEachFunc(func(r proto.Record) {
		fmt.Fprintf(w, "%s offset=%d key=%q value=%q\n", prefix, r.Offset, r.Key, r.Value)
	})
}

// ForEachFunc registers a terminal that calls fn for every record
// flowing through the pipeline. Sister to ForEach() (passive,
// state-store-only): use ForEachFunc when records should reach a
// non-topic sink — logger, metrics emitter, external system —
// without an intermediate Peek upstream of an empty ForEach.
//
// fn must not retain references to r; the runtime reuses Record
// instances. Copy fields if you need to outlive the call.
//
// Pipeline finalized just like ForEach — further chaining on s has
// no effect.
func (s *Stream) ForEachFunc(fn func(proto.Record)) {
	s.Peek(fn).ForEach()
}

// ForEach registers the pipeline as a terminal — records are processed
// through the chain and discarded (or consumed via state-store
// inspection). Useful when a topology only updates a state store.
//
// Group ID is derived from the source plus a per-topology sequence, so
// multiple ForEach pipelines on the same source get distinct groups.
func (s *Stream) ForEach() {
	s.topology.mu.Lock()
	defer s.topology.mu.Unlock()
	s.topology.foreachSeq++
	s.topology.pipes = append(s.topology.pipes, pipeline{
		source: s.source,
		ops:    s.ops,
		group:  fmt.Sprintf("holocron-streams-%s-foreach-%d", s.source, s.topology.foreachSeq),
	})
}

// GroupByKey marks the stream for keyed aggregation. Holocron currently
// trusts the producer's partitioner to co-locate same-keyed records
// (which the default sdk Partitioner does), so GroupByKey is a no-op
// marker that gates Count / Aggregate.
func (s *Stream) GroupByKey() *GroupedStream {
	return &GroupedStream{stream: s}
}

// GroupBy derives the grouping key from each record via fn and
// returns a GroupedStream ready for Count / Aggregate / Reduce /
// windowed counts. Sugar over SelectKey followed by GroupByKey —
// reads more clearly at call sites where the grouping isn't the
// record's original key.
//
// Same partitioning caveat as SelectKey: re-keying does NOT
// repartition the source topic, so two records with the new same
// key may land on different partitions. Aggregations stay
// per-partition (per the batch-21 partitioned-state-store model).
// Pair with `Through` to a single-partition intermediate topic if
// cross-partition grouping is required.
func (s *Stream) GroupBy(fn func(proto.Record) []byte) *GroupedStream {
	return &GroupedStream{stream: s.SelectKey(fn)}
}

// GroupedStream is a Stream marked for keyed aggregation.
type GroupedStream struct {
	stream *Stream
}

// Count emits an updated count for each input record, keyed the same.
// State lives in the named store, scoped to the source partition; the
// output record's value is the big-endian uint64 encoding of the new
// count.
func (g *GroupedStream) Count(storeName string) *Stream {
	store := g.stream.topology.Store(storeName)
	return g.stream.appendOp(func(r proto.Record, _ *Topology, partition int) []proto.Record {
		sub := store.For(partition)
		current, _ := sub.Get(r.Key)
		next := DecodeCount(current) + 1
		encoded := EncodeCount(next)
		sub.Put(r.Key, encoded)
		return []proto.Record{{
			Key:     append([]byte(nil), r.Key...),
			Value:   encoded,
			Headers: r.Headers,
		}}
	})
}

// Reduce combines the values for each key via a user-supplied
// associative function. The first record establishes the
// accumulator; each subsequent record's value is folded into the
// accumulator via fn(accum, recordValue). State is scoped to the
// source partition.
//
// Sugar over Aggregate with the conventional reduce shape (input
// and output share a value space) — callers that need a separate
// accumulator type should use Aggregate directly.
func (g *GroupedStream) Reduce(storeName string, fn func(accum, value []byte) []byte) *Stream {
	store := g.stream.topology.Store(storeName)
	return g.stream.appendOp(func(r proto.Record, _ *Topology, partition int) []proto.Record {
		sub := store.For(partition)
		prev, ok := sub.Get(r.Key)
		var next []byte
		if ok {
			next = fn(prev, r.Value)
		} else {
			next = append([]byte(nil), r.Value...)
		}
		sub.Put(r.Key, next)
		return []proto.Record{{
			Key:     append([]byte(nil), r.Key...),
			Value:   append([]byte(nil), next...),
			Headers: r.Headers,
		}}
	})
}

// Sum is a pre-canned numeric aggregation: extracts an int64 from
// each record via valueFn and accumulates per-key totals. State is
// encoded as 8-byte big-endian int64 — readable via DecodeCount.
//
// Sugar over Aggregate with a fixed accumulator. Useful for
// running totals (revenue per customer, bytes per source, etc.)
// where Reduce's "operate on bytes" surface is awkward.
func (g *GroupedStream) Sum(storeName string, valueFn func(proto.Record) int64) *Stream {
	store := g.stream.topology.Store(storeName)
	return g.stream.appendOp(func(r proto.Record, _ *Topology, partition int) []proto.Record {
		sub := store.For(partition)
		prev, _ := sub.Get(r.Key)
		next := int64(DecodeCount(prev)) + valueFn(r)
		encoded := EncodeCount(uint64(next))
		sub.Put(r.Key, encoded)
		return []proto.Record{{
			Key:     append([]byte(nil), r.Key...),
			Value:   encoded,
			Headers: r.Headers,
		}}
	})
}

// Aggregate emits an updated value per key by combining the previous
// aggregate with the new record. The aggregator receives the existing
// value (nil if first time), the incoming record, and returns the new
// aggregate. State is scoped to the source partition.
func (g *GroupedStream) Aggregate(storeName string, agg func(prev []byte, r proto.Record) []byte) *Stream {
	store := g.stream.topology.Store(storeName)
	return g.stream.appendOp(func(r proto.Record, _ *Topology, partition int) []proto.Record {
		sub := store.For(partition)
		prev, _ := sub.Get(r.Key)
		next := agg(prev, r)
		sub.Put(r.Key, next)
		return []proto.Record{{
			Key:     append([]byte(nil), r.Key...),
			Value:   append([]byte(nil), next...),
			Headers: r.Headers,
		}}
	})
}

// EncodeCount serializes a uint64 count as 8 big-endian bytes — the
// wire form Count and Aggregate use for their output values. Exported
// so callers can decode/produce the same shape from outside the
// pipeline (custom aggregators, tests, downstream consumers).
func EncodeCount(n uint64) []byte {
	var buf [8]byte
	for i := 7; i >= 0; i-- {
		buf[i] = byte(n)
		n >>= 8
	}
	return buf[:]
}

// DecodeCount reverses EncodeCount. nil or any non-8-byte input yields 0.
func DecodeCount(b []byte) uint64 {
	if len(b) != 8 {
		return 0
	}
	var n uint64
	for _, v := range b {
		n = (n << 8) | uint64(v)
	}
	return n
}

// streamsHeartbeatInterval is how often a streams pipeline consumer
// pings the broker. Tighter than the SDK default (5s) because streams
// runs many parallel tasks under the same group, and the rebalance
// cascade as tasks join must resolve before records start flowing —
// otherwise stale pumps from earlier-generation assignments race
// new pumps for the same partition, and any non-atomic operator
// (e.g. Count's Get/Put) loses increments to the resulting churn.
const streamsHeartbeatInterval = 200 * time.Millisecond

// recordErr stashes a per-pipeline error for Stop to surface and
// fires the user's WithErrorHandler callback if registered. Both
// happen under the errsMu lock, so the handler sees errors in the
// order they were recorded.
func (t *Topology) recordErr(err error) {
	t.errsMu.Lock()
	t.errs = append(t.errs, err)
	handler := t.onError
	t.errsMu.Unlock()
	if handler != nil {
		handler(err)
	}
}

// runOp invokes one pipeline op with panic recovery. If the op
// panics, the error is recorded (firing any WithErrorHandler), the
// failing record is best-effort routed to the DLQ topic when
// WithDLQ is configured, and the function returns nil so the
// pipeline drops the failing record but keeps processing
// subsequent ones. Without recovery a buggy op would kill the
// goroutine and silently drop every downstream record until Stop
// surfaced the failure.
func (t *Topology) runOp(o op, r proto.Record, partition int, producer *sdk.Producer) (out []proto.Record) {
	defer func() {
		if rv := recover(); rv != nil {
			err := fmt.Errorf("streams: op panic: %v", rv)
			t.recordErr(err)
			if t.dlqTopic != "" && producer != nil {
				t.routeToDLQ(producer, r, err)
			}
			out = nil
		}
	}()
	return o(r, t, partition)
}

// routeToDLQ produces the failing record to the DLQ topic with an
// added error-context header. Best-effort: a DLQ failure surfaces
// via recordErr but doesn't propagate to the caller (which is the
// recover path of a panic — propagating would defeat recovery).
func (t *Topology) routeToDLQ(producer *sdk.Producer, r proto.Record, panicErr error) {
	dlq := proto.Record{
		Key:   r.Key,
		Value: r.Value,
		Headers: append(append([]proto.Header(nil), r.Headers...),
			proto.Header{Key: HeaderDLQError, Value: []byte(panicErr.Error())}),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := producer.Send(ctx, t.dlqTopic, dlq); err != nil {
		t.recordErr(fmt.Errorf("streams: dlq produce: %w", err))
	}
}

// run drives one pipeline: subscribe to its source topic, apply ops to
// each record, optionally produce results to the sink, then commit the
// consumer's offset so a restart resumes where this run stopped.
//
// Each pipeline reads under its own consumer group (p.group) so offsets
// persist on the broker. State-store updates and sink writes happen
// before the commit; if the process dies mid-batch the broker's
// committed offset still points at records the pipeline never finished
// — at-least-once semantics, which the operators must be idempotent
// against (Count and Aggregate are not, by design; users that need
// exactly-once need broker-side transactions, which we do not have).
func (t *Topology) run(ctx context.Context, p pipeline, producer *sdk.Producer) {
	defer t.wg.Done()

	consumer, err := sdk.NewConsumer(t.transport,
		sdk.WithGroup(p.group),
		sdk.WithHeartbeatInterval(streamsHeartbeatInterval),
	)
	if err != nil {
		t.recordErr(fmt.Errorf("streams: source %q consumer: %w", p.source, err))
		return
	}
	defer consumer.Close()
	if err := consumer.Subscribe(ctx, p.source, 0); err != nil {
		t.recordErr(fmt.Errorf("streams: subscribe %q: %w", p.source, err))
		return
	}

	for {
		if err := ctx.Err(); err != nil {
			return
		}
		records, err := consumer.PollMeta(ctx, 256)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			t.recordErr(fmt.Errorf("streams: poll %q: %w", p.source, err))
			return
		}
		if len(records) > 0 {
			atomic.StoreInt64(&t.lastRecordAt, time.Now().UnixNano())
		}
		for _, pr := range records {
			t.advanceWatermark(t.eventTime(pr.Record))
			partition := int(pr.Partition.Index)
			out := []proto.Record{pr.Record}
			for _, o := range p.ops {
				next := out[:0]
				for _, in := range out {
					next = append(next, t.runOp(o, in, partition, producer)...)
				}
				out = next
				if len(out) == 0 {
					break
				}
			}
			if p.sink != "" {
				for _, r := range out {
					if _, err := producer.Send(ctx, p.sink, r); err != nil {
						t.recordErr(fmt.Errorf("streams: produce %q: %w", p.sink, err))
						return
					}
				}
			}
		}
		// Commit committed offsets after every successful batch. The
		// broker uses "next to read" semantics so we add 1 to the
		// highest delivered offset.
		if len(records) > 0 {
			for part, off := range consumer.LatestOffsets() {
				if err := consumer.Commit(ctx, part, off+1); err != nil {
					t.recordErr(fmt.Errorf("streams: commit %v: %w", part, err))
					return
				}
			}
		}
	}
}

// Start launches every pipeline in its own goroutine and returns once
// they're set up. The pipelines run until Stop is called.
func (t *Topology) Start(ctx context.Context) error {
	t.mu.Lock()
	if t.running {
		t.mu.Unlock()
		return errors.New("streams: topology already running")
	}
	t.running = true
	runCtx, cancel := context.WithCancel(ctx)
	t.stopFunc = cancel
	pipes := append([]pipeline(nil), t.pipes...)
	t.mu.Unlock()

	producer, err := sdk.NewProducer(t.transport)
	if err != nil {
		cancel()
		return fmt.Errorf("streams: producer: %w", err)
	}

	for _, p := range pipes {
		// Each pipeline runs maxTasks goroutines in the same consumer
		// group; the broker assigns partitions across them. Running
		// more tasks than the source topic's partitions just leaves
		// the surplus idle, which is the operator's choice.
		for range t.maxTasks {
			t.wg.Add(1)
			go t.run(runCtx, p, producer)
		}
		// Per-pipeline punctuator goroutine. Fires on the configured
		// tick when a punctuator interval is set AND the pipeline has
		// at least one stateful operator that registered a punctuator.
		if t.punctuationInt > 0 && len(p.punctuators) > 0 && p.sink != "" {
			t.wg.Add(1)
			go t.runPunctuator(runCtx, p, producer)
		}
	}

	// Join pipelines run with two goroutines (one per source side)
	// sharing the joinPipeline.state.
	t.mu.Lock()
	joins := append([]joinPipeline(nil), t.joins...)
	tables := append([]*KTable(nil), t.tables...)
	t.mu.Unlock()
	for _, jp := range joins {
		t.runJoin(runCtx, jp, producer)
	}
	// KTables: one consumer goroutine per registered table. Tables
	// must finish their initial catch-up before any pipeline that
	// joins against them sees its first record — so spawn them BEFORE
	// returning from Start.
	for _, kt := range tables {
		t.wg.Add(1)
		go t.runTable(runCtx, kt)
	}

	// Topology-level idle-watermark goroutine. Advances the watermark
	// to wall-clock time when no record has arrived for the configured
	// interval, so downstream time-driven operators keep making
	// progress on quiet streams.
	if t.idleWatermarkD > 0 {
		t.wg.Add(1)
		go t.runIdleWatermark(runCtx)
	}
	return nil
}

// runIdleWatermark advances the watermark to wall-clock time when the
// topology has been idle for longer than idleWatermarkD. It ticks at
// half the idle interval so the worst-case detection latency is one
// tick after the threshold is crossed.
func (t *Topology) runIdleWatermark(ctx context.Context) {
	defer t.wg.Done()
	tickEvery := t.idleWatermarkD / 2
	if tickEvery <= 0 {
		tickEvery = t.idleWatermarkD
	}
	ticker := time.NewTicker(tickEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			nowNs := now.UnixNano()
			last := atomic.LoadInt64(&t.lastRecordAt)
			if last == 0 || nowNs-last >= t.idleWatermarkD.Nanoseconds() {
				t.advanceWatermark(nowNs)
			}
		}
	}
}

// runPunctuator drives the registered punctuators for a pipeline,
// producing emitted records to the pipeline's sink topic. Exits on
// ctx cancellation.
func (t *Topology) runPunctuator(ctx context.Context, p pipeline, producer *sdk.Producer) {
	defer t.wg.Done()
	ticker := time.NewTicker(t.punctuationInt)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			ts := now.UnixNano()
			for _, fn := range p.punctuators {
				for _, rec := range fn(ts) {
					if _, err := producer.Send(ctx, p.sink, rec); err != nil {
						t.recordErr(fmt.Errorf("streams: punctuator emit %q: %w", p.sink, err))
						return
					}
				}
			}
		}
	}
}

// Stop cancels every running pipeline and waits for them to exit.
// Returns the first error any pipeline reported.
func (t *Topology) Stop() error {
	t.mu.Lock()
	if !t.running {
		t.mu.Unlock()
		return nil
	}
	t.running = false
	stop := t.stopFunc
	t.stopFunc = nil
	t.mu.Unlock()
	if stop != nil {
		stop()
	}
	t.wg.Wait()

	t.errsMu.Lock()
	defer t.errsMu.Unlock()
	if len(t.errs) == 0 {
		return nil
	}
	return errors.Join(t.errs...)
}

// Run is a convenience that calls Start, blocks until ctx is cancelled,
// then calls Stop.
func (t *Topology) Run(ctx context.Context) error {
	if err := t.Start(ctx); err != nil {
		return err
	}
	<-ctx.Done()
	return t.Stop()
}

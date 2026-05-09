package streams

import (
	"context"
	"errors"
	"fmt"
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
	transport       sdk.Transport
	maxTasks        int
	useChangelog    bool
	tsExtractor     TimestampExtractor
	punctuationInt  time.Duration
	idleWatermarkD  time.Duration

	mu           sync.Mutex
	pipes        []pipeline
	joins        []joinPipeline
	tables       []*KTable // materialized views; one consumer goroutine per table
	stores       map[string]StateStore
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

// op is the kernel of every operator: take a record, possibly use a
// state store, return zero or more output records. Stateful operators
// reference their store by name through topology.stores.
type op func(rec proto.Record, t *Topology) []proto.Record

// New constructs a Topology bound to a Transport.
func New(transport sdk.Transport, opts ...Option) (*Topology, error) {
	if transport == nil {
		return nil, errors.New("streams: New requires a Transport")
	}
	t := &Topology{
		transport: transport,
		maxTasks:  1,
		stores:    make(map[string]StateStore),
	}
	for _, opt := range opts {
		opt(t)
	}
	return t, nil
}

// Store returns the named state store, creating one on first access.
// The default backing is in-memory; WithChangelogStores switches the
// factory to ChangelogStore (replays from <name>-changelog on creation,
// writes through to the topic on Put).
//
// A factory failure (e.g. changelog topic missing) falls back to
// in-memory and records the error for Stop to surface.
func (t *Topology) Store(name string) StateStore {
	t.mu.Lock()
	defer t.mu.Unlock()
	if s, ok := t.stores[name]; ok {
		return s
	}
	s := t.openStoreLocked(name)
	t.stores[name] = s
	return s
}

func (t *Topology) openStoreLocked(name string) StateStore {
	if !t.useChangelog {
		return NewMemoryStore()
	}
	cs, err := OpenChangelogStore(context.Background(), t.transport, name)
	if err != nil {
		t.recordErr(fmt.Errorf("streams: changelog store %q (falling back to memory): %w", name, err))
		return NewMemoryStore()
	}
	return cs
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

// Filter drops records where pred returns false.
func (s *Stream) Filter(pred func(proto.Record) bool) *Stream {
	return s.appendOp(func(r proto.Record, _ *Topology) []proto.Record {
		if pred(r) {
			return []proto.Record{r}
		}
		return nil
	})
}

// Map transforms each record into a new record. Drop a record by
// returning a zero-value Record from a Filter step rather than from Map.
func (s *Stream) Map(fn func(proto.Record) proto.Record) *Stream {
	return s.appendOp(func(r proto.Record, _ *Topology) []proto.Record {
		return []proto.Record{fn(r)}
	})
}

// FlatMap transforms each record into zero or more records.
func (s *Stream) FlatMap(fn func(proto.Record) []proto.Record) *Stream {
	return s.appendOp(func(r proto.Record, _ *Topology) []proto.Record {
		return fn(r)
	})
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

// GroupedStream is a Stream marked for keyed aggregation.
type GroupedStream struct {
	stream *Stream
}

// Count emits an updated count for each input record, keyed the same.
// State lives in the named store; the output record's value is the
// big-endian uint64 encoding of the new count.
func (g *GroupedStream) Count(storeName string) *Stream {
	store := g.stream.topology.Store(storeName)
	return g.stream.appendOp(func(r proto.Record, _ *Topology) []proto.Record {
		current, _ := store.Get(r.Key)
		next := DecodeCount(current) + 1
		encoded := EncodeCount(next)
		store.Put(r.Key, encoded)
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
// aggregate.
func (g *GroupedStream) Aggregate(storeName string, agg func(prev []byte, r proto.Record) []byte) *Stream {
	store := g.stream.topology.Store(storeName)
	return g.stream.appendOp(func(r proto.Record, _ *Topology) []proto.Record {
		prev, _ := store.Get(r.Key)
		next := agg(prev, r)
		store.Put(r.Key, next)
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

// recordErr stashes a per-pipeline error for Stop to surface.
func (t *Topology) recordErr(err error) {
	t.errsMu.Lock()
	defer t.errsMu.Unlock()
	t.errs = append(t.errs, err)
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
		records, err := consumer.Poll(ctx, 256)
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
		for _, r := range records {
			t.advanceWatermark(t.eventTime(r))
			out := []proto.Record{r}
			for _, o := range p.ops {
				next := out[:0]
				for _, in := range out {
					next = append(next, o(in, t)...)
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

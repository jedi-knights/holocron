package connect

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jedi-knights/holocron/proto"
	"github.com/jedi-knights/holocron/sdk"
)

const (
	defaultPollBackoff   = 100 * time.Millisecond
	defaultBatchSize     = 256
	defaultFlushInterval = 5 * time.Second
	// sinkPollDeadline bounds each consumer.Poll call so the flush
	// ticker can preempt long stretches of inactivity. Without it,
	// Poll would block forever when records stop flowing and the
	// flush+commit cycle would never run.
	sinkPollDeadline = 500 * time.Millisecond
)

// RetryPolicy configures how the Worker retries a failing sink Put.
// Exponential backoff: delay doubles each attempt, capped at MaxDelay.
// MaxAttempts of 1 disables retry. Zero values mean "no retry."
type RetryPolicy struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
}

// nextDelay returns how long to sleep before attempt N (0-indexed
// attempt; first attempt has zero delay).
func (p RetryPolicy) nextDelay(attempt int) time.Duration {
	if attempt <= 0 || p.BaseDelay <= 0 {
		return 0
	}
	d := p.BaseDelay
	for range attempt - 1 {
		d *= 2
		if p.MaxDelay > 0 && d > p.MaxDelay {
			return p.MaxDelay
		}
	}
	return d
}

// putWithRetry calls task.Put up to policy.MaxAttempts times, sleeping
// for the policy's exponentially-backed-off delay between attempts.
// Returns the last error encountered.
func putWithRetry(ctx context.Context, task SinkTask, records []proto.Record, policy RetryPolicy) error {
	attempts := max(policy.MaxAttempts, 1)
	var lastErr error
	for i := range attempts {
		if delay := policy.nextDelay(i); delay > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}
		if err := task.Put(ctx, records); err != nil {
			lastErr = err
			continue
		}
		return nil
	}
	return lastErr
}

// writeDLQ produces records to the dead-letter topic. The original Key,
// Value, and Headers are preserved; downstream tooling can inspect
// what couldn't be sunk.
func writeDLQ(ctx context.Context, producer *sdk.Producer, topic string, records []proto.Record) error {
	for _, r := range records {
		if _, err := producer.Send(ctx, topic, proto.Record{
			Key:     r.Key,
			Value:   r.Value,
			Headers: r.Headers,
		}); err != nil {
			return err
		}
	}
	return nil
}

// Worker hosts one or more SourceConnectors and SinkConnectors against a
// single sdk.Transport. It is the lifecycle owner — Start/Stop bracket
// every task's goroutine, and the Worker recovers from per-task panics
// without bringing the rest down.
//
// Workers are not themselves clustered in Stage 6: a sink connector's
// tasks share a consumer group, so multiple workers running the same
// connector configuration will divide partitions automatically — but
// configuration distribution is the operator's responsibility, not the
// Worker's.
type Worker struct {
	transport   sdk.Transport
	offsetStore OffsetStore

	mu             sync.Mutex
	sources        []sourceMount
	sinks          []sinkMount
	running        bool
	stopFunc       context.CancelFunc
	runCtx         context.Context // valid while running; used by AddSourceLive
	producer       *sdk.Producer   // valid while running
	mountCancels   map[string]context.CancelFunc // connector name → per-mount cancel
	wg             sync.WaitGroup
	errs           []error
	errsMu         sync.Mutex

	statsMu sync.Mutex
	stats   map[string]*taskStatCounters // key = connector|index
}

// TaskStats is a cumulative snapshot of one task's throughput.
// Records counts records successfully produced (for source tasks)
// or consumed (for sink tasks); Bytes sums the record values.
// Snapshots are point-in-time and may differ from the live state
// by the time the caller reads them.
type TaskStats struct {
	Connector string
	TaskIndex int
	Records   int64
	Bytes     int64
}

// taskStatCounters is the live counter store. Atomic so the
// per-task hot paths can update without coordinating through the
// Worker mutex.
type taskStatCounters struct {
	records atomic.Int64
	bytes   atomic.Int64
}

// taskStatsKey encodes a (connector, taskIndex) pair as a stable
// map key.
func taskStatsKey(connector string, index int) string {
	return fmt.Sprintf("%s|%d", connector, index)
}

// counters returns the existing counters for the (connector,
// taskIndex) pair or creates them on first access.
func (w *Worker) counters(connector string, index int) *taskStatCounters {
	key := taskStatsKey(connector, index)
	w.statsMu.Lock()
	defer w.statsMu.Unlock()
	if w.stats == nil {
		w.stats = make(map[string]*taskStatCounters)
	}
	c, ok := w.stats[key]
	if !ok {
		c = &taskStatCounters{}
		w.stats[key] = c
	}
	return c
}

// Stats returns a snapshot of every task's cumulative counters.
// Order is unspecified. Operators poll this for dashboards or
// liveness checks.
func (w *Worker) Stats() []TaskStats {
	w.statsMu.Lock()
	defer w.statsMu.Unlock()
	out := make([]TaskStats, 0, len(w.stats))
	for key, c := range w.stats {
		// Decode "connector|index" — index is the last |-separated
		// segment so connector names can contain | safely.
		idx := strings.LastIndex(key, "|")
		if idx < 0 {
			continue
		}
		conn := key[:idx]
		taskIdx, err := strconv.Atoi(key[idx+1:])
		if err != nil {
			continue
		}
		out = append(out, TaskStats{
			Connector: conn,
			TaskIndex: taskIdx,
			Records:   c.records.Load(),
			Bytes:     c.bytes.Load(),
		})
	}
	return out
}

// WorkerOption configures a Worker.
type WorkerOption func(*Worker)

// WithOffsetStore plumbs source-task offset persistence through the
// Worker. On task Init the Worker calls store.Load to seed the task's
// resume position; after each successful task.Commit it calls
// store.Save with the offsets carried on the just-published records.
//
// Without an offset store, source connectors restart from their
// connector-defined initial position (e.g., file source restarts at
// byte 0).
func WithOffsetStore(store OffsetStore) WorkerOption {
	return func(w *Worker) { w.offsetStore = store }
}

type sourceMount struct {
	connector  SourceConnector
	maxTasks   int
	coordTopic string
}

// SourceOption configures a single AddSource registration.
type SourceOption func(*sourceMount)

// WithSourceCoordTopic enables distributed coordination for a source
// mount. Workers in the pool form a consumer group on the named topic;
// the broker's partition assignment dictates which task indices each
// worker runs. Two workers running the same source connector with the
// same coord topic will not double-produce — one worker's tasks own
// the records.
//
// The coord topic must exist before the workers start, and its
// partition count must equal maxTasks. The topic carries no real
// records; it is used purely as the broker's coordination primitive.
// Single-partition coord topic == leader election (one owner runs all
// tasks); maxTasks-partition coord topic == round-robin task
// distribution across the worker pool.
//
// V1 limitations:
//   - The coord topic is not auto-created; pre-create it via
//     `holocronctl create-topic` or `embed.Broker.CreateTopic`.
//   - On worker death the broker session-times out before reassigning;
//     a few seconds of source pause is expected.
func WithSourceCoordTopic(topic string) SourceOption {
	return func(m *sourceMount) { m.coordTopic = topic }
}

type sinkMount struct {
	connector SinkConnector
	maxTasks  int
	retry     RetryPolicy
	dlqTopic  string
}

// SinkOption configures a single AddSink registration.
type SinkOption func(*sinkMount)

// WithSinkRetry installs a retry policy on the sink. The Worker retries
// task.Put up to MaxAttempts times, sleeping for the policy's
// exponentially-backed-off delay between attempts.
func WithSinkRetry(policy RetryPolicy) SinkOption {
	return func(m *sinkMount) { m.retry = policy }
}

// WithSinkDLQ routes batches that fail every retry attempt to the named
// dead-letter topic instead of failing the task. Records go in
// untransformed; the original Key, Value, and Headers are preserved so
// downstream tooling can inspect what couldn't be sunk.
func WithSinkDLQ(topic string) SinkOption {
	return func(m *sinkMount) { m.dlqTopic = topic }
}

// NewWorker returns a Worker bound to the given Transport.
func NewWorker(t sdk.Transport, opts ...WorkerOption) (*Worker, error) {
	if t == nil {
		return nil, errors.New("connect: NewWorker requires a Transport")
	}
	w := &Worker{
		transport:    t,
		mountCancels: make(map[string]context.CancelFunc),
	}
	for _, opt := range opts {
		opt(w)
	}
	return w, nil
}

// sourceMountKey + sinkMountKey namespace the per-mount cancel map so a
// source and sink that happen to share a name don't collide.
func sourceMountKey(name string) string { return "source:" + name }
func sinkMountKey(name string) string   { return "sink:" + name }

// AddSource registers a source connector. maxTasks bounds how many
// SourceTasks the connector may produce. Must be called before Start.
//
// Distributed coordination across a worker pool is opt-in via
// WithSourceCoordTopic.
func (w *Worker) AddSource(c SourceConnector, maxTasks int, opts ...SourceOption) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.running {
		return errors.New("connect: cannot AddSource after Start; use AddSourceLive")
	}
	if maxTasks <= 0 {
		maxTasks = 1
	}
	mount := sourceMount{connector: c, maxTasks: maxTasks}
	for _, opt := range opts {
		opt(&mount)
	}
	w.sources = append(w.sources, mount)
	return nil
}

// RemoveSource cancels the named source's per-mount context, stopping
// its coordinator/task goroutines without disturbing the rest of the
// Worker. Returns an error if no source by that name is registered.
//
// The call is asynchronous: the cancel signal is sent immediately and
// the goroutines exit shortly after. Use Stop to wait for everything
// to finish, or call Stop+Start for a clean Worker rebuild.
func (w *Worker) RemoveSource(name string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	idx := -1
	for i, m := range w.sources {
		if m.connector.Name() == name {
			idx = i
			break
		}
	}
	if idx < 0 {
		return fmt.Errorf("connect: no source named %q", name)
	}
	w.sources = append(w.sources[:idx], w.sources[idx+1:]...)
	if cancel, ok := w.mountCancels[sourceMountKey(name)]; ok {
		cancel()
		delete(w.mountCancels, sourceMountKey(name))
	}
	return nil
}

// AddSourceLive registers a source connector on a Worker that's
// already running. The Worker spawns the new source's goroutines
// (coordinated or eager) immediately under a per-mount context so
// RemoveSource can later bring just that mount down.
//
// Useful for operators that need to add sources without restarting
// the Worker — e.g., a control-plane handler that watches a config
// store.
func (w *Worker) AddSourceLive(c SourceConnector, maxTasks int, opts ...SourceOption) error {
	if maxTasks <= 0 {
		maxTasks = 1
	}
	mount := sourceMount{connector: c, maxTasks: maxTasks}
	for _, opt := range opts {
		opt(&mount)
	}

	w.mu.Lock()
	if !w.running {
		w.mu.Unlock()
		return errors.New("connect: AddSourceLive requires a running Worker; use AddSource before Start")
	}
	runCtx := w.runCtx
	producer := w.producer
	w.sources = append(w.sources, mount)
	w.mu.Unlock()

	return w.spawnSourceMount(runCtx, mount, producer)
}

// spawnSourceMount launches the goroutines for one source mount under
// a fresh per-mount context derived from parent. RemoveSource cancels
// that scope to bring the mount down without disturbing the rest of
// the Worker. Coordinated mounts get a single coordinator goroutine;
// eager mounts spawn one goroutine per task index up front.
func (w *Worker) spawnSourceMount(parent context.Context, mount sourceMount, producer *sdk.Producer) error {
	mountCtx, cancel := context.WithCancel(parent)
	w.mu.Lock()
	w.mountCancels[sourceMountKey(mount.connector.Name())] = cancel
	w.mu.Unlock()

	if mount.coordTopic != "" {
		w.wg.Add(1)
		go w.runCoordinatedSource(mountCtx, mount, producer)
		return nil
	}
	tasks, err := mount.connector.Tasks(mount.maxTasks)
	if err != nil {
		cancel()
		w.clearMountCancel(sourceMountKey(mount.connector.Name()))
		return fmt.Errorf("connect: source %q tasks: %w", mount.connector.Name(), err)
	}
	for i, task := range tasks {
		var stored []map[string]any
		if w.offsetStore != nil {
			loaded, err := w.offsetStore.Load(mountCtx, mount.connector.Name(), i)
			if err != nil {
				cancel()
				w.clearMountCancel(sourceMountKey(mount.connector.Name()))
				return fmt.Errorf("connect: source %q task %d offset load: %w", mount.connector.Name(), i, err)
			}
			stored = loaded
		}
		if err := task.Init(mountCtx, stored); err != nil {
			cancel()
			w.clearMountCancel(sourceMountKey(mount.connector.Name()))
			return fmt.Errorf("connect: source %q task %d init: %w", mount.connector.Name(), i, err)
		}
		w.wg.Add(1)
		go w.runSourceTask(mountCtx, mount.connector.Name(), i, task, producer)
	}
	return nil
}

// clearMountCancel drops the per-mount cancel entry without invoking
// it. Used after a spawn that rolled back its own goroutines.
func (w *Worker) clearMountCancel(key string) {
	w.mu.Lock()
	delete(w.mountCancels, key)
	w.mu.Unlock()
}

// AddSink registers a sink connector. Must be called before Start.
//
// Sink-level retries and dead-letter routing are configured via
// SinkOption (WithSinkRetry, WithSinkDLQ).
func (w *Worker) AddSink(c SinkConnector, maxTasks int, opts ...SinkOption) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.running {
		return errors.New("connect: cannot AddSink after Start; use AddSinkLive")
	}
	if maxTasks <= 0 {
		maxTasks = 1
	}
	mount := sinkMount{connector: c, maxTasks: maxTasks}
	for _, opt := range opts {
		opt(&mount)
	}
	w.sinks = append(w.sinks, mount)
	return nil
}

// AddSinkLive registers a sink connector on a Worker that's already
// running. The Worker spawns the sink's goroutines immediately under
// a per-mount context that RemoveSink can later cancel.
func (w *Worker) AddSinkLive(c SinkConnector, maxTasks int, opts ...SinkOption) error {
	if maxTasks <= 0 {
		maxTasks = 1
	}
	mount := sinkMount{connector: c, maxTasks: maxTasks}
	for _, opt := range opts {
		opt(&mount)
	}

	w.mu.Lock()
	if !w.running {
		w.mu.Unlock()
		return errors.New("connect: AddSinkLive requires a running Worker; use AddSink before Start")
	}
	runCtx := w.runCtx
	producer := w.producer
	w.sinks = append(w.sinks, mount)
	w.mu.Unlock()

	return w.spawnSinkMount(runCtx, mount, producer)
}

// RemoveSink cancels the named sink's per-mount context, stopping its
// task goroutines without disturbing the rest of the Worker. Returns
// an error if no sink by that name is registered. Asynchronous: the
// cancel signal is sent immediately and the goroutines exit shortly
// after.
func (w *Worker) RemoveSink(name string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	idx := -1
	for i, m := range w.sinks {
		if m.connector.Name() == name {
			idx = i
			break
		}
	}
	if idx < 0 {
		return fmt.Errorf("connect: no sink named %q", name)
	}
	w.sinks = append(w.sinks[:idx], w.sinks[idx+1:]...)
	if cancel, ok := w.mountCancels[sinkMountKey(name)]; ok {
		cancel()
		delete(w.mountCancels, sinkMountKey(name))
	}
	return nil
}

// Start launches every registered task. Returns once all tasks have
// completed Init; the tasks then run until Stop is called or ctx is
// cancelled.
func (w *Worker) Start(ctx context.Context) error {
	w.mu.Lock()
	if w.running {
		w.mu.Unlock()
		return errors.New("connect: worker already running")
	}
	w.running = true
	runCtx, cancel := context.WithCancel(ctx)
	w.stopFunc = cancel
	sources := append([]sourceMount(nil), w.sources...)
	sinks := append([]sinkMount(nil), w.sinks...)
	w.mu.Unlock()

	producer, err := sdk.NewProducer(w.transport)
	if err != nil {
		cancel()
		return fmt.Errorf("connect: producer: %w", err)
	}

	w.mu.Lock()
	w.runCtx = runCtx
	w.producer = producer
	w.mu.Unlock()

	for _, mount := range sources {
		if err := w.spawnSourceMount(runCtx, mount, producer); err != nil {
			cancel()
			return err
		}
	}

	for _, mount := range sinks {
		if err := w.spawnSinkMount(runCtx, mount, producer); err != nil {
			cancel()
			return err
		}
	}

	return nil
}

// spawnSinkMount launches the goroutines for one sink mount under a
// fresh per-mount context derived from parent. RemoveSink cancels
// that scope to bring just the mount down.
func (w *Worker) spawnSinkMount(parent context.Context, mount sinkMount, producer *sdk.Producer) error {
	mountCtx, cancel := context.WithCancel(parent)
	w.mu.Lock()
	w.mountCancels[sinkMountKey(mount.connector.Name())] = cancel
	w.mu.Unlock()

	tasks, err := mount.connector.Tasks(mount.maxTasks)
	if err != nil {
		cancel()
		w.clearMountCancel(sinkMountKey(mount.connector.Name()))
		return fmt.Errorf("connect: sink %q tasks: %w", mount.connector.Name(), err)
	}
	for i, task := range tasks {
		if err := task.Init(mountCtx); err != nil {
			cancel()
			w.clearMountCancel(sinkMountKey(mount.connector.Name()))
			return fmt.Errorf("connect: sink %q task %d init: %w", mount.connector.Name(), i, err)
		}
		w.wg.Add(1)
		go w.runSinkTask(mountCtx, mount, i, task, producer)
	}
	return nil
}

// Stop cancels every running task and waits for them to exit. Returns
// the first error any task reported. Safe to call multiple times.
func (w *Worker) Stop() error {
	w.mu.Lock()
	if !w.running {
		w.mu.Unlock()
		return nil
	}
	w.running = false
	stop := w.stopFunc
	w.stopFunc = nil
	w.mu.Unlock()

	if stop != nil {
		stop()
	}
	w.wg.Wait()

	w.errsMu.Lock()
	defer w.errsMu.Unlock()
	if len(w.errs) == 0 {
		return nil
	}
	return errors.Join(w.errs...)
}

func (w *Worker) recordErr(err error) {
	w.errsMu.Lock()
	defer w.errsMu.Unlock()
	w.errs = append(w.errs, err)
}

func (w *Worker) runSourceTask(ctx context.Context, name string, taskIndex int, task SourceTask, producer *sdk.Producer) {
	defer w.wg.Done()
	defer task.Close()

	for {
		if err := ctx.Err(); err != nil {
			return
		}
		records, err := task.Poll(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			w.recordErr(fmt.Errorf("source %q poll: %w", name, err))
			return
		}
		if len(records) == 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(defaultPollBackoff):
				continue
			}
		}
		// Track how many records the broker actually accepted so a
		// mid-batch cancellation can still persist offsets for
		// records that landed. Without this, a Stop fired between
		// Send #N and Send #N+1 would leave records 1..N on the
		// broker unacknowledged in the offset store — a restart
		// would re-emit them as duplicates.
		sentCount := 0
		var sendErr error
		stats := w.counters(name, taskIndex)
		for i, sr := range records {
			rec := proto.Record{
				Key:     sr.Key,
				Value:   sr.Value,
				Headers: sr.Headers,
			}
			if _, err := producer.Send(ctx, sr.Topic, rec); err != nil {
				sendErr = err
				sentCount = i
				break
			}
			stats.records.Add(1)
			stats.bytes.Add(int64(len(sr.Value)))
		}
		if sendErr == nil {
			sentCount = len(records)
		}

		// Persist offsets for records that DID land on the broker,
		// even if the surrounding context is cancelled. Save with a
		// fresh ctx in that case so the in-memory offset store
		// (which is the test's failure mode) sees the write.
		if sentCount > 0 {
			w.saveSourceOffsetsBestEffort(ctx, name, taskIndex, records[:sentCount])
		}

		if sendErr != nil {
			if errors.Is(sendErr, context.Canceled) {
				return
			}
			w.recordErr(fmt.Errorf("source %q produce: %w", name, sendErr))
			return
		}
		if err := task.Commit(ctx, records); err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			w.recordErr(fmt.Errorf("source %q commit: %w", name, err))
			return
		}
	}
}

// saveSourceOffsetsBestEffort persists offsets for records that
// reached the broker, even when the surrounding ctx is cancelled.
// Falls back to a fresh background context with a short timeout
// so a Stop mid-batch doesn't lose the durability boundary that
// keeps a restart from re-emitting already-delivered records.
func (w *Worker) saveSourceOffsetsBestEffort(ctx context.Context, name string, taskIndex int, records []SourceRecord) {
	if w.offsetStore == nil {
		return
	}
	offsets := make([]map[string]any, 0, len(records))
	for _, sr := range records {
		if sr.SourceOffset != nil {
			offsets = append(offsets, sr.SourceOffset)
		}
	}
	if len(offsets) == 0 {
		return
	}
	saveCtx := ctx
	if saveCtx.Err() != nil {
		var cancel context.CancelFunc
		saveCtx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
	}
	if err := w.offsetStore.Save(saveCtx, name, taskIndex, offsets); err != nil {
		w.recordErr(fmt.Errorf("source %q offset save: %w", name, err))
	}
}

func (w *Worker) runSinkTask(ctx context.Context, mount sinkMount, taskIndex int, task SinkTask, producer *sdk.Producer) {
	defer w.wg.Done()
	defer task.Close()
	conn := mount.connector
	stats := w.counters(conn.Name(), taskIndex)

	// Pre-rebalance flush+commit. Fired before the consumer loses its
	// current partitions; flushes the sink and commits offsets so the
	// next assignee resumes cleanly. Errors here block the rejoin
	// (sdk wraps and returns them), which is the right behavior — we
	// would rather session-timeout than ack records we've lost track of.
	revoke := func(rctx context.Context, _ []proto.PartitionRef) error {
		if err := task.Flush(rctx); err != nil {
			return err
		}
		// commitAssigned will be set after the consumer is constructed;
		// we capture it via closure below.
		return nil
	}

	consumer, err := sdk.NewConsumer(
		w.transport,
		sdk.WithGroup(conn.Name()),
		sdk.WithRevokeListener(func(rctx context.Context, parts []proto.PartitionRef) error {
			return revoke(rctx, parts)
		}),
	)
	if err != nil {
		w.recordErr(fmt.Errorf("sink %q consumer: %w", conn.Name(), err))
		return
	}
	defer consumer.Close()

	// Now that the consumer exists, we can rebind `revoke` to also
	// commit per-partition offsets after Flush succeeds.
	revoke = func(rctx context.Context, parts []proto.PartitionRef) error {
		if err := task.Flush(rctx); err != nil {
			return err
		}
		latest := consumer.LatestOffsets()
		for _, p := range parts {
			off, ok := latest[p]
			if !ok {
				continue
			}
			if err := consumer.Commit(rctx, p, off+1); err != nil {
				return err
			}
		}
		return nil
	}

	for _, topic := range conn.Topics() {
		if err := consumer.Subscribe(ctx, topic, 0); err != nil {
			w.recordErr(fmt.Errorf("sink %q subscribe %s: %w", conn.Name(), topic, err))
			return
		}
	}

	flushTicker := time.NewTicker(defaultFlushInterval)
	defer flushTicker.Stop()

	for {
		if err := ctx.Err(); err != nil {
			return
		}
		// Bounded Poll so the flush ticker can preempt long stretches
		// where no records arrive.
		pollCtx, pollCancel := context.WithTimeout(ctx, sinkPollDeadline)
		records, err := consumer.Poll(pollCtx, defaultBatchSize)
		pollCancel()
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			if errors.Is(err, context.Canceled) {
				return
			}
			w.recordErr(fmt.Errorf("sink %q poll: %w", conn.Name(), err))
			return
		}
		if len(records) > 0 {
			if err := putWithRetry(ctx, task, records, mount.retry); err != nil {
				if mount.dlqTopic == "" {
					w.recordErr(fmt.Errorf("sink %q put: %w", conn.Name(), err))
					return
				}
				if dlqErr := writeDLQ(ctx, producer, mount.dlqTopic, records); dlqErr != nil {
					w.recordErr(fmt.Errorf("sink %q dlq %q: %w", conn.Name(), mount.dlqTopic, dlqErr))
					return
				}
			}
			// Successful put (or DLQ-redirected): count records as
			// processed. Each record's value-byte count contributes
			// to the per-task bytes counter.
			stats.records.Add(int64(len(records)))
			for _, r := range records {
				stats.bytes.Add(int64(len(r.Value)))
			}
		}

		select {
		case <-flushTicker.C:
			if err := task.Flush(ctx); err != nil {
				w.recordErr(fmt.Errorf("sink %q flush: %w", conn.Name(), err))
				return
			}
			// Auto-commit on a successful flush. Committed offset is
			// "next to read" (Stage 4 convention), so add 1 to the
			// highest offset the consumer has actually delivered.
			for p, off := range consumer.LatestOffsets() {
				if err := consumer.Commit(ctx, p, off+1); err != nil {
					w.recordErr(fmt.Errorf("sink %q commit %v: %w", conn.Name(), p, err))
					return
				}
			}
		default:
		}
	}
}

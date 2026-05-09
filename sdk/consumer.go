package sdk

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/jedi-knights/holocron/proto"
)

// defaultHeartbeatInterval is how often a grouped consumer pings the
// broker to signal liveness. Short enough that an evicted member learns
// within session-timeout/3; long enough not to spam the broker.
const defaultHeartbeatInterval = 5 * time.Second

// inflight pairs a record with its source partition so Poll can drop
// records whose partition was revoked before they were delivered to
// the caller. Pre-rebalance pumps may have buffered records into fanIn
// that the new owner of the partition will also fetch from the
// committed offset; without this filter, both the old owner and the
// new owner would process the same records.
//
// A non-nil err signals that the pump for `partition` failed with a
// protocol-level error. Poll forwards the error to its caller and
// drops any pending records in the same batch.
type inflight struct {
	rec       proto.Record
	partition proto.PartitionRef
	err       error
}

// Consumer reads records from one or more partitions. Records arrive on a
// single internal fan-in channel so callers see one ordered stream per
// partition but a unified Poll API across partitions.
//
// Consumers come in two flavors:
//
//   - **Group-coordinated** (constructed with WithGroup). The broker
//     assigns a subset of partitions; commits are durable on the broker.
//     Subscribe → JoinGroup → fan in assigned partitions from committed
//     offsets → background heartbeat → on rebalance, redo.
//   - **Self-managed**. The consumer subscribes to all partitions of a
//     topic and tracks offsets itself. fromOffset on Subscribe applies
//     literally.
type Consumer struct {
	transport         Transport
	group             string
	heartbeatInterval time.Duration
	onRevoke          RevokeFunc
	onAssign          AssignFunc
	stickyMemberID    bool // memberID was caller-supplied; skip LeaveGroup on Close

	mu         sync.Mutex
	fanIn      chan inflight
	pumpCancel map[proto.PartitionRef]context.CancelFunc
	latest     map[proto.PartitionRef]int64 // highest offset seen per assigned partition
	assignment []proto.PartitionRef          // current assignment (for revoke callback)
	closed     bool

	// group-mode state, protected by mu
	memberID    string
	generation  int32
	topics      []string // topics this consumer subscribed to (for rejoin)
	hbCancel    context.CancelFunc
	hbDone      chan struct{}
	rebalanceCh chan struct{} // signalled by heartbeat when broker says rebalance
	rejoinFrom  int64         // fromOffset to use when no committed offset on rejoin
}

// ConsumerOption configures a Consumer.
type ConsumerOption func(*Consumer)

// WithGroup names the consumer's group. When set, Subscribe coordinates
// with the broker to receive a subset of partitions and to resume from
// committed offsets; commits are persisted on the broker.
func WithGroup(name string) ConsumerOption {
	return func(c *Consumer) { c.group = name }
}

// WithHeartbeatInterval overrides how often the group consumer pings the
// broker. Useful for tests; production should leave the default.
func WithHeartbeatInterval(d time.Duration) ConsumerOption {
	return func(c *Consumer) { c.heartbeatInterval = d }
}

// RevokeFunc is invoked just before the Consumer's current group
// assignment is dropped — typically because the broker triggered a
// rebalance. Listeners use it to flush downstream side effects and
// commit offsets on the partitions they're about to lose. The callback
// runs synchronously in the heartbeat goroutine; long-running cleanup
// will delay rejoin and risk session timeout.
type RevokeFunc func(ctx context.Context, revoked []proto.PartitionRef) error

// AssignFunc is invoked after the Consumer receives a new partition
// assignment — both on the initial Subscribe and on every successful
// rejoin after a rebalance. Listeners use it to spawn per-partition
// work whose lifetime tracks the assignment.
//
// The callback runs synchronously on the path that produced the new
// assignment (Subscribe goroutine for the initial assign, heartbeat
// goroutine for rejoins). Long-running setup will delay record fetches
// and risk session timeout — kick off heavy work asynchronously.
type AssignFunc func(ctx context.Context, assigned []proto.PartitionRef) error

// WithRevokeListener registers a callback that fires before a rebalance
// reassigns this consumer's partitions. Errors from the callback are
// reported via Stop's aggregated error but do not block the rejoin.
func WithRevokeListener(fn RevokeFunc) ConsumerOption {
	return func(c *Consumer) { c.onRevoke = fn }
}

// WithAssignListener registers a callback that fires after each
// successful JoinGroup — once for the initial Subscribe and once per
// rebalance. Pair it with WithRevokeListener to manage external state
// whose lifetime tracks the consumer's partition assignment.
func WithAssignListener(fn AssignFunc) ConsumerOption {
	return func(c *Consumer) { c.onAssign = fn }
}

// WithMemberID seeds the consumer's group member ID. Pass a value the
// caller persists across restarts so the broker treats the restarted
// consumer as the same member — it keeps the prior assignment instead
// of triggering a fresh rebalance. Empty (the default) lets the broker
// generate a random ID; the consumer caches it for the rest of its
// lifetime but loses it on Close.
//
// Sticky IDs are most useful when restart cycles are short relative to
// session timeout (15s default): the broker hasn't evicted the prior
// instance yet, the new one rejoins with the same ID, and a quorum of
// peers need not rebalance.
func WithMemberID(id string) ConsumerOption {
	return func(c *Consumer) {
		c.memberID = id
		c.stickyMemberID = id != ""
	}
}

// NewConsumer constructs a Consumer bound to the given Transport.
func NewConsumer(t Transport, opts ...ConsumerOption) (*Consumer, error) {
	if t == nil {
		return nil, errors.New("sdk: NewConsumer requires a Transport")
	}
	c := &Consumer{
		transport:         t,
		fanIn:             make(chan inflight, 256),
		latest:            make(map[proto.PartitionRef]int64),
		heartbeatInterval: defaultHeartbeatInterval,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c, nil
}

// Assign pins the consumer to a specific partition starting at fromOffset.
// Only valid for self-managed (non-group) consumers.
func (c *Consumer) Assign(ctx context.Context, p proto.PartitionRef, fromOffset int64) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return errors.New("sdk: consumer is closed")
	}
	if c.group != "" {
		c.mu.Unlock()
		return errors.New("sdk: Assign is not valid on a group consumer; use Subscribe")
	}
	c.mu.Unlock()
	return c.startPump(ctx, p, fromOffset)
}

// Subscribe attaches the consumer to a topic.
//
// Without a group, the consumer subscribes to every partition of the topic
// starting at fromOffset.
//
// With a group, the consumer joins the group, receives an assignment from
// the broker, and starts fetches at each partition's committed offset
// (or fromOffset when uncommitted). A background goroutine heartbeats; on
// rebalance signals the consumer transparently rejoins.
func (c *Consumer) Subscribe(ctx context.Context, topic string, fromOffset int64) error {
	if c.group == "" {
		return c.subscribeAll(ctx, topic, fromOffset)
	}
	return c.subscribeAsGroup(ctx, topic, fromOffset)
}

func (c *Consumer) subscribeAll(ctx context.Context, topic string, fromOffset int64) error {
	n, err := c.transport.PartitionsFor(ctx, topic)
	if err != nil {
		return fmt.Errorf("sdk: partition count for %q: %w", topic, err)
	}
	for i := range n {
		if err := c.startPump(ctx, proto.PartitionRef{Topic: topic, Index: i}, fromOffset); err != nil {
			return err
		}
	}
	return nil
}

func (c *Consumer) subscribeAsGroup(ctx context.Context, topic string, fromOffset int64) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return errors.New("sdk: consumer is closed")
	}
	memberID := c.memberID
	c.topics = appendUnique(c.topics, topic)
	c.rejoinFrom = fromOffset
	topics := append([]string(nil), c.topics...)
	c.mu.Unlock()

	res, err := c.transport.JoinGroup(ctx, c.group, memberID, topics)
	if err != nil {
		return fmt.Errorf("sdk: JoinGroup %q: %w", c.group, err)
	}

	c.mu.Lock()
	c.memberID = res.MemberID
	c.generation = res.Generation
	c.cancelPumpsLocked()
	c.assignment = c.assignment[:0]
	for _, a := range res.Assignments {
		c.assignment = append(c.assignment, a.Partition)
	}
	assignFn := c.onAssign
	assigned := append([]proto.PartitionRef(nil), c.assignment...)
	c.startHeartbeatLocked(ctx)
	c.mu.Unlock()

	for _, a := range res.Assignments {
		start := startOffset(a.CommittedOffset, fromOffset)
		if err := c.startPump(ctx, a.Partition, start); err != nil {
			return err
		}
	}
	if assignFn != nil {
		if err := assignFn(ctx, assigned); err != nil {
			return fmt.Errorf("sdk: assign listener: %w", err)
		}
	}
	return nil
}

// startOffset chooses where to fetch from. The "committed = next to read"
// convention matches Kafka: a consumer that read up to offset N calls
// Commit(N+1) so the next read begins at N+1.
func startOffset(committed, fallback int64) int64 {
	if committed == NoOffset {
		return fallback
	}
	return committed
}

func appendUnique(seen []string, v string) []string {
	if slices.Contains(seen, v) {
		return seen
	}
	return append(seen, v)
}

// Poll returns up to maxRecords currently-available records across all
// assigned partitions. It blocks for the first record up to ctx's deadline,
// then drains additional ready records non-blockingly.
//
// Records pre-fetched from a partition that has since been revoked
// (rebalance reassigned it to a different consumer) are dropped here,
// not returned. Without this filter the run loop would process records
// the new owner has also picked up — a guaranteed double-processing.
//
// Protocol-level pump failures (e.g., StatusRateLimited from a fetch
// quota, StatusNotLeader after a leadership change) are surfaced as
// the returned error after any already-buffered records are drained
// from this call.
func (c *Consumer) Poll(ctx context.Context, maxRecords int) ([]proto.Record, error) {
	polled, err := c.PollMeta(ctx, maxRecords)
	if polled == nil {
		return nil, err
	}
	out := make([]proto.Record, len(polled))
	for i, p := range polled {
		out[i] = p.Record
	}
	return out, err
}

// PolledRecord pairs a record with the partition it was fetched from.
// PollMeta returns these so callers that route per-partition state
// (e.g. the streams runtime's per-partition state stores) can scope
// state operations to the originating partition.
type PolledRecord struct {
	Record    proto.Record
	Partition proto.PartitionRef
}

// PollMeta is like Poll but returns each record with its source
// partition. All other semantics — blocking on the first record,
// dropping records from revoked partitions, surfacing protocol-level
// errors after draining already-buffered records — match Poll.
func (c *Consumer) PollMeta(ctx context.Context, maxRecords int) ([]PolledRecord, error) {
	if maxRecords <= 0 {
		return nil, nil
	}
	out := make([]PolledRecord, 0, maxRecords)
	for len(out) == 0 {
		select {
		case in, ok := <-c.fanIn:
			if !ok {
				return out, nil
			}
			if in.err != nil {
				return out, in.err
			}
			if !c.isCurrentlyAssigned(in.partition) {
				continue
			}
			out = append(out, PolledRecord{Record: in.rec, Partition: in.partition})
		case <-ctx.Done():
			return out, ctx.Err()
		}
	}
	for len(out) < maxRecords {
		select {
		case in, ok := <-c.fanIn:
			if !ok {
				return out, nil
			}
			if in.err != nil {
				return out, in.err
			}
			if !c.isCurrentlyAssigned(in.partition) {
				continue
			}
			out = append(out, PolledRecord{Record: in.rec, Partition: in.partition})
		default:
			return out, nil
		}
	}
	return out, nil
}

// isCurrentlyAssigned reports whether p is in this consumer's current
// group assignment. Self-managed (non-group) consumers consider every
// partition assigned, since their assignment is set explicitly via
// Assign or Subscribe and never revoked by a rebalance.
func (c *Consumer) isCurrentlyAssigned(p proto.PartitionRef) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.group == "" {
		return true
	}
	return slices.Contains(c.assignment, p)
}

// Handler processes a batch of records pulled by Run. Returning a
// non-nil error stops Run; the caller decides whether to retry.
type Handler func(ctx context.Context, records []proto.Record) error

// Run is a callback-driven Poll loop. It calls handler with each batch
// of records (up to maxBatch per call) and commits offsets after every
// successful handler invocation when the consumer is in group mode.
// Returns when ctx is cancelled (nil error) or the handler returns an
// error.
//
// Sugar over Poll for the common SNS-style "process each record"
// shape; saves callers from rewriting the same loop in every consumer.
func (c *Consumer) Run(ctx context.Context, maxBatch int, handler Handler) error {
	if maxBatch <= 0 {
		maxBatch = 256
	}
	for {
		if err := ctx.Err(); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			return err
		}
		records, err := c.Poll(ctx, maxBatch)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			return fmt.Errorf("sdk: Run poll: %w", err)
		}
		if len(records) == 0 {
			continue
		}
		if err := handler(ctx, records); err != nil {
			return err
		}
		// Auto-commit on successful handler return for group consumers.
		if c.group != "" {
			for p, off := range c.LatestOffsets() {
				if err := c.Commit(ctx, p, off+1); err != nil {
					return fmt.Errorf("sdk: Run commit: %w", err)
				}
			}
		}
	}
}

// Commit records the offset for the consumer's group on the given partition.
// Group consumers commit broker-side; self-managed consumers send a no-op
// commit that the broker ignores.
func (c *Consumer) Commit(ctx context.Context, p proto.PartitionRef, offset int64) error {
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return errors.New("sdk: consumer is closed")
	}
	return c.transport.Commit(ctx, c.group, p, offset)
}

// Close releases broker resources, stops every assigned partition pump,
// and (for group consumers) sends LeaveGroup so the broker rebalances
// promptly. Consumers constructed with WithMemberID skip LeaveGroup so
// the broker holds the member's slot until session timeout, letting a
// restart with the same memberID rejoin without churn.
func (c *Consumer) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	group := c.group
	memberID := c.memberID
	sticky := c.stickyMemberID
	c.cancelPumpsLocked()
	hbCancel := c.hbCancel
	hbDone := c.hbDone
	c.hbCancel = nil
	c.hbDone = nil
	c.mu.Unlock()

	if hbCancel != nil {
		hbCancel()
		<-hbDone
	}
	if group != "" && memberID != "" && !sticky {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = c.transport.LeaveGroup(ctx, group, memberID)
	}
	return nil
}

func (c *Consumer) startPump(ctx context.Context, p proto.PartitionRef, fromOffset int64) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return errors.New("sdk: consumer is closed")
	}
	pumpCtx, cancel := context.WithCancel(ctx)
	if c.pumpCancel == nil {
		c.pumpCancel = make(map[proto.PartitionRef]context.CancelFunc)
	}
	// Cancel any prior pump for the same partition (pause/resume,
	// rebalance restart on the same partition) so we don't leak a
	// goroutine still pumping into fanIn.
	if prior, ok := c.pumpCancel[p]; ok {
		prior()
	}
	c.pumpCancel[p] = cancel
	c.mu.Unlock()

	ch, errCh, err := c.transport.Subscribe(pumpCtx, p, fromOffset)
	if err != nil {
		cancel()
		c.mu.Lock()
		delete(c.pumpCancel, p)
		c.mu.Unlock()
		return fmt.Errorf("sdk: subscribe %v: %w", p, err)
	}
	go c.pump(pumpCtx, p, ch, errCh)
	return nil
}

func (c *Consumer) pump(ctx context.Context, p proto.PartitionRef, ch <-chan proto.Record, errCh <-chan error) {
	for {
		select {
		case r, ok := <-ch:
			if !ok {
				return
			}
			c.recordLatest(p, r.Offset)
			select {
			case c.fanIn <- inflight{rec: r, partition: p}:
			case <-ctx.Done():
				return
			}
		case err, ok := <-errCh:
			if !ok {
				// errCh closed without a value — pump exited cleanly
				// (ctx-cancellation or natural EOF). Keep listening on
				// the records channel until it too closes.
				errCh = nil
				continue
			}
			if err == nil {
				continue
			}
			// Forward the failure to fanIn so Poll surfaces it.
			select {
			case c.fanIn <- inflight{partition: p, err: err}:
			case <-ctx.Done():
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func (c *Consumer) recordLatest(p proto.PartitionRef, offset int64) {
	c.mu.Lock()
	if cur, ok := c.latest[p]; !ok || offset > cur {
		c.latest[p] = offset
	}
	c.mu.Unlock()
}

// Pause stops fetching from p without leaving the group. The
// partition stays in this consumer's assignment so a rebalance
// won't move it; it just halts the per-partition pump goroutine
// so no records arrive. Resume restarts the pump from the offset
// just past the last record this consumer saw.
//
// Useful for backpressure: when a downstream sink is overwhelmed,
// Pause the source partition until the sink catches up. Records
// produced to the broker while paused accumulate there and stream
// down on Resume.
//
// Returns nil for an unknown partition (idempotent — a Pause
// after Resume is a no-op).
func (c *Consumer) Pause(p proto.PartitionRef) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return errors.New("sdk: consumer is closed")
	}
	if cancel, ok := c.pumpCancel[p]; ok {
		cancel()
		delete(c.pumpCancel, p)
	}
	return nil
}

// Resume restarts a paused partition's pump from the offset just
// past the last record this consumer observed. If no records have
// been seen, resumes from the supplied fallback offset (typically
// 0). Returns an error if the partition was never assigned.
func (c *Consumer) Resume(ctx context.Context, p proto.PartitionRef, fallback int64) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return errors.New("sdk: consumer is closed")
	}
	from := fallback
	if last, ok := c.latest[p]; ok {
		from = last + 1
	}
	if _, alreadyRunning := c.pumpCancel[p]; alreadyRunning {
		c.mu.Unlock()
		return nil // idempotent
	}
	c.mu.Unlock()
	return c.startPump(ctx, p, from)
}

// LatestOffsets returns a snapshot of the highest offset received per
// assigned partition since this Consumer was created. Used by callers
// (e.g. the connect Worker) that auto-commit on a flush boundary.
func (c *Consumer) LatestOffsets() map[proto.PartitionRef]int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[proto.PartitionRef]int64, len(c.latest))
	maps.Copy(out, c.latest)
	return out
}

// cancelPumpsLocked stops every partition pump and drops the cancel list.
// Caller holds c.mu.
func (c *Consumer) cancelPumpsLocked() {
	for _, cancel := range c.pumpCancel {
		cancel()
	}
	c.pumpCancel = nil
}

// startHeartbeatLocked launches the heartbeat goroutine for a group
// consumer. Caller holds c.mu.
func (c *Consumer) startHeartbeatLocked(parent context.Context) {
	if c.hbCancel != nil {
		return
	}
	hbCtx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	c.hbCancel = cancel
	c.hbDone = done
	c.rebalanceCh = make(chan struct{}, 1)
	go c.heartbeatLoop(hbCtx, done)
}

func (c *Consumer) heartbeatLoop(ctx context.Context, done chan<- struct{}) {
	defer close(done)
	for {
		if ctx.Err() != nil {
			return
		}
		c.mu.Lock()
		group := c.group
		memberID := c.memberID
		generation := c.generation
		closed := c.closed
		wait := c.heartbeatInterval
		c.mu.Unlock()
		if closed || group == "" || memberID == "" {
			// No active membership — sleep a tick before re-checking.
			select {
			case <-ctx.Done():
				return
			case <-time.After(wait):
				continue
			}
		}

		// Each heartbeat is a long-poll: the broker holds the call
		// open for up to wait, returning immediately when a rebalance
		// is needed. This delivers the server-pushed rebalance signal
		// the moment a peer joins or leaves, rather than after a
		// ticker tick. heartbeatInterval is reused as the long-poll
		// deadline so the existing tuning still applies.
		res, err := c.transport.Heartbeat(ctx, group, memberID, generation, wait)
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}
		if err != nil || res.RebalanceNeeded {
			_ = c.rejoin(ctx)
		}
	}
}

// rejoin re-runs JoinGroup → restart pumps after a rebalance signal. The
// previously assigned topics and the fallback fromOffset are remembered
// from the last Subscribe call.
//
// Before re-joining, rejoin invokes the registered RevokeFunc with the
// current assignment so callers can flush + commit on the partitions
// they're about to lose. The callback runs synchronously here.
func (c *Consumer) rejoin(ctx context.Context) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	memberID := c.memberID
	topics := append([]string(nil), c.topics...)
	rejoinFrom := c.rejoinFrom
	revokeFn := c.onRevoke
	revoked := append([]proto.PartitionRef(nil), c.assignment...)
	c.mu.Unlock()

	if len(topics) == 0 {
		return nil
	}

	if revokeFn != nil && len(revoked) > 0 {
		if err := revokeFn(ctx, revoked); err != nil {
			return fmt.Errorf("sdk: revoke listener: %w", err)
		}
	}

	res, err := c.transport.JoinGroup(ctx, c.group, memberID, topics)
	if err != nil {
		return fmt.Errorf("sdk: rejoin %q: %w", c.group, err)
	}

	c.mu.Lock()
	c.memberID = res.MemberID
	c.generation = res.Generation
	c.cancelPumpsLocked()
	c.assignment = c.assignment[:0]
	for _, a := range res.Assignments {
		c.assignment = append(c.assignment, a.Partition)
	}
	assignFn := c.onAssign
	assigned := append([]proto.PartitionRef(nil), c.assignment...)
	c.mu.Unlock()

	for _, a := range res.Assignments {
		start := startOffset(a.CommittedOffset, rejoinFrom)
		if err := c.startPump(ctx, a.Partition, start); err != nil {
			return err
		}
	}
	if assignFn != nil {
		if err := assignFn(ctx, assigned); err != nil {
			return fmt.Errorf("sdk: assign listener: %w", err)
		}
	}
	return nil
}

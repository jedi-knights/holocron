package sdk

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jedi-knights/holocron/proto"
)

// newProducerID returns a hex-encoded 16-byte random identifier.
// Each Producer instance gets its own ID so the broker can dedup
// retries from the same instance without confusing them with
// concurrent producers.
func newProducerID() (string, error) {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf[:]), nil
}

// RewindIdempotencySequence rolls the producer's per-partition
// sequence counter back by one so the next Send to that partition
// reuses the most recently stamped sequence number. Useful for
// tests that simulate a retry of an already-applied write; in
// production, the broker's dedup logic catches genuine retries
// because the SDK's transport-level retry path keeps the same
// stamped record across attempts.
//
// No-op when the Producer was constructed without WithIdempotency.
func (p *Producer) RewindIdempotencySequence(pref proto.PartitionRef) {
	if !p.idempotent {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if seq, ok := p.idempSeqs[pref]; ok && seq > 0 {
		p.idempSeqs[pref] = seq - 1
	}
}

// stampIdempotencyHeaders returns r with HeaderProducerID and
// HeaderProducerSeq appended to its Headers. Caller is the Send
// path, holding the lock that protects idempSeqs.
func (p *Producer) stampIdempotencyHeaders(pref proto.PartitionRef, r proto.Record) proto.Record {
	seq := p.idempSeqs[pref]
	p.idempSeqs[pref] = seq + 1
	var seqBytes [8]byte
	binary.BigEndian.PutUint64(seqBytes[:], seq)
	out := proto.Record{
		Offset:    r.Offset,
		Timestamp: r.Timestamp,
		Key:       r.Key,
		Value:     r.Value,
		Headers:   make([]proto.Header, 0, len(r.Headers)+2),
	}
	out.Headers = append(out.Headers, r.Headers...)
	out.Headers = append(out.Headers,
		proto.Header{Key: proto.HeaderProducerID, Value: []byte(p.producerID)},
		proto.Header{Key: proto.HeaderProducerSeq, Value: seqBytes[:]},
	)
	return out
}

// Acks names the durability level a producer waits for before Send returns.
// AcksLocal (the default) returns once the broker has accepted the record
// into its store (page cache for the file backend). AcksDurable additionally
// waits for the broker to fsync. AcksNone is fire-and-forget — Send returns
// as soon as the record is on the wire.
type Acks uint8

const (
	// AcksLocal waits for the broker's normal append path. Default.
	AcksLocal Acks = 0
	// AcksDurable waits for fsync via Transport.Sync after Append.
	AcksDurable Acks = 1
	// AcksNone returns as soon as the request is sent.
	AcksNone Acks = 2
)

const (
	// defaultLinger of zero disables batching. Set via WithLinger to
	// enable accumulation of records before they're sent.
	defaultLinger = time.Duration(0)
	// defaultBatchSize bounds how many records accumulate before a
	// flush is forced even if linger hasn't expired.
	defaultBatchSize = 256
)

// Producer appends records to topics. It is safe for concurrent use.
//
// Without WithLinger, every Send goes through the wire as a single
// Publish RPC. With a linger window, records are accumulated per
// partition; when the window expires or the buffer hits batchSize, the
// producer flushes the batch as one ProduceBatch RPC. Send returns
// after the batch has been sent (and ack'd if AcksDurable is set).
type Producer struct {
	transport     Transport
	partitioner   Partitioner
	acks          Acks
	linger        time.Duration
	batchSize     int
	codec         proto.Codec
	codecLevel    uint8 // LZ4 level: 0 = fast, 1..9 = HC at that level
	// inFlight is a semaphore bounding concurrent SendNoWait
	// publishes when WithMaxInFlight is set. nil means unbounded
	// (the default).
	inFlight chan struct{}
	rateLimitTries int                    // 0 disables retry; 1+ retries per Send
	rateLimitWait  time.Duration          // base wait between retry attempts
	retryStatuses  map[proto.Status]bool  // additional statuses that trigger retry
	idempotent     bool             // producer stamps records for broker dedup
	producerID     string           // unique to this Producer instance
	onSent         []SentHook       // fired after each successful Send / SendBatch
	onAsyncError   []AsyncErrorHook // fired per failed SendNoWait record

	mu     sync.Mutex
	closed bool
	// Per-partition batchers. Allocated on first Send to that partition.
	batchers map[proto.PartitionRef]*batcher
	// idempSeqs tracks the next sequence number to stamp per
	// partition. Only used when idempotent is true.
	idempSeqs map[proto.PartitionRef]uint64
	// asyncErrors counts flush failures for SendNoWait records —
	// errors that didn't surface to a synchronous caller. Operators
	// poll AsyncErrors() to detect a misbehaving producer.
	asyncErrors atomic.Int64
	// sendCount counts records successfully delivered to the
	// transport across Send / SendBatch / SendNoWait. Pairs with
	// AsyncErrors() and PendingCount() for a full health snapshot.
	sendCount atomic.Int64
}

// ProducerOption configures a Producer.
type ProducerOption func(*Producer)

// WithPartitioner sets the routing strategy for produced records.
func WithPartitioner(p Partitioner) ProducerOption {
	return func(pr *Producer) { pr.partitioner = p }
}

// WithAcks sets the durability level. See the Acks docs for the trade-offs.
func WithAcks(a Acks) ProducerOption {
	return func(pr *Producer) { pr.acks = a }
}

// WithLinger enables producer-side batching: records destined for the
// same partition are accumulated for up to the given duration, then
// flushed as one ProduceBatch RPC. Trades latency (each Send waits up
// to linger before going on the wire) for throughput (fewer round
// trips, larger payloads). Zero (default) disables batching — every
// Send sends immediately.
func WithLinger(d time.Duration) ProducerOption {
	return func(pr *Producer) { pr.linger = d }
}

// WithBatchSize bounds how many records accumulate per partition
// before the producer flushes early, even if the linger window hasn't
// expired. Default 256.
func WithBatchSize(n int) ProducerOption {
	return func(pr *Producer) {
		if n > 0 {
			pr.batchSize = n
		}
	}
}

// Codec re-exports the wire-protocol compression codec for callers
// that don't want to import proto directly.
type Codec = proto.Codec

const (
	// CodecNone leaves the records uncompressed on the wire (default).
	CodecNone = proto.CodecNone
	// CodecLZ4 compresses each ProduceBatch's records portion with LZ4.
	CodecLZ4 = proto.CodecLZ4
)

// WithCompression sets the wire-level compression codec applied to
// every ProduceBatch this Producer sends. Most useful in combination
// with WithLinger — single-record produces don't compress, since they
// don't go through PublishBatch.
func WithCompression(c Codec) ProducerOption {
	return func(pr *Producer) { pr.codec = c }
}

// WithCompressionLevel tunes the LZ4 codec's compression level.
// 0 (default) picks the fast LZ4 block compressor — same behavior
// as before. Levels 1..9 switch to LZ4-HC at that level for
// progressively higher compression ratio at higher CPU cost.
//
// Only effective when paired with WithCompression(CodecLZ4); a
// no-op for other codecs. The wire format is identical regardless
// of level so consumers (and brokers running an older SDK) decode
// without change.
func WithCompressionLevel(level uint8) ProducerOption {
	return func(pr *Producer) { pr.codecLevel = level }
}

// WithRateLimitRetry causes Send / SendBatch to retry on a broker
// StatusRateLimited response, sleeping `wait` between attempts (with
// exponential backoff capped at 8x). After `tries` attempts the
// rate-limit error surfaces to the caller. Zero `tries` (the default)
// disables retry — the rate-limit error fails fast.
func WithRateLimitRetry(tries int, wait time.Duration) ProducerOption {
	return func(pr *Producer) {
		pr.rateLimitTries = tries
		pr.rateLimitWait = wait
	}
}

// WithRetryOn extends Send/SendBatch retry behavior to the named
// broker statuses in addition to the built-in StatusRateLimited.
// Useful for transient errors like StatusNotLeader during a
// leadership change — without retrying that status, every Send
// during the failover window fails. Tries and backoff are
// controlled by WithRateLimitRetry; WithRetryOn only widens the
// set of statuses that trigger retry.
func WithRetryOn(statuses ...proto.Status) ProducerOption {
	return func(pr *Producer) {
		if pr.retryStatuses == nil {
			pr.retryStatuses = make(map[proto.Status]bool, len(statuses))
		}
		for _, s := range statuses {
			pr.retryStatuses[s] = true
		}
	}
}

// SentHook is invoked after every successful Send / SendBatch
// with the partition the record landed on and its broker-assigned
// offset. Used for instrumentation, audit logs, and downstream
// offset tracking without wrapping the Producer entirely.
//
// SendNoWait deliberately skips the hook — the caller opted out
// of per-record observability when they chose fire-and-forget.
type SentHook func(p proto.PartitionRef, offset int64)

// WithOnSent registers a callback invoked after every successful
// Send / SendBatch. Multiple WithOnSent calls chain — each hook
// fires in registration order. Hooks must be cheap (run on the
// hot publish path) and must not block.
func WithOnSent(fn SentHook) ProducerOption {
	return func(pr *Producer) {
		pr.onSent = append(pr.onSent, fn)
	}
}

// AsyncErrorHook is invoked once per fire-and-forget (SendNoWait)
// record that fails to reach the broker. Pairs with the
// AsyncErrors() counter — the counter answers "is anything wrong
// at all?", the hook answers "which partition, with what error?".
//
// The hook fires from the publish goroutine that observed the
// failure. It must be cheap and non-blocking; long-running work
// belongs in a downstream queue.
type AsyncErrorHook func(p proto.PartitionRef, err error)

// WithOnAsyncError registers a callback invoked per failed
// SendNoWait record. Multiple registrations chain in order. Useful
// for surfacing fire-and-forget failures to alerting systems
// without polling AsyncErrors().
func WithOnAsyncError(fn AsyncErrorHook) ProducerOption {
	return func(pr *Producer) {
		pr.onAsyncError = append(pr.onAsyncError, fn)
	}
}

// WithMaxInFlight caps the number of concurrent in-flight
// SendNoWait publishes at n. Once n goroutines are publishing,
// subsequent SendNoWait calls block until one completes — the
// caller's send rate is implicitly throttled by the wire.
//
// Bounds memory under fire-and-forget bursts where unconstrained
// SendNoWait would otherwise spawn an unbounded number of
// goroutines (each holding a record). Synchronous Send is
// unaffected — its goroutine count is already bounded by the
// caller's concurrency.
//
// n <= 0 means unbounded (the default). Only effective for
// SendNoWait without WithLinger; the linger path already coalesces
// records through per-partition batchers.
func WithMaxInFlight(n int) ProducerOption {
	return func(pr *Producer) {
		if n > 0 {
			pr.inFlight = make(chan struct{}, n)
		}
	}
}

// WithIdempotency enables producer-side idempotent retry. The
// Producer assigns a unique per-instance ID and a monotonic
// per-partition sequence number to every record it sends; the
// broker uses the (producer-id, sequence) pair to recognize
// retries of an already-applied write and return the original
// offset without duplicating the record.
//
// Pair with WithRateLimitRetry (or any other retry strategy on top
// of Send) to get exactly-once semantics on the produce path. The
// broker's dedup state is in-memory only — a broker restart resets
// the window, so retries that survive a restart can still
// duplicate. Persistent dedup is a follow-on.
func WithIdempotency() ProducerOption {
	return func(pr *Producer) { pr.idempotent = true }
}

// NewProducer constructs a Producer bound to the given Transport.
func NewProducer(t Transport, opts ...ProducerOption) (*Producer, error) {
	if t == nil {
		return nil, errors.New("sdk: NewProducer requires a Transport")
	}
	p := &Producer{
		transport:   t,
		partitioner: &DefaultPartitioner{},
		linger:      defaultLinger,
		batchSize:   defaultBatchSize,
		batchers:    make(map[proto.PartitionRef]*batcher),
		idempSeqs:   make(map[proto.PartitionRef]uint64),
	}
	for _, opt := range opts {
		opt(p)
	}
	if p.idempotent {
		id, err := newProducerID()
		if err != nil {
			return nil, fmt.Errorf("sdk: producer-id: %w", err)
		}
		p.producerID = id
	}
	// Auto-enable sticky partitioning during the linger window so
	// keyless records consistently fill the same batch instead of
	// round-robining (which would defeat the throughput point of
	// linger).
	if p.linger > 0 {
		if _, alreadySticky := p.partitioner.(*StickyPartitioner); !alreadySticky {
			p.partitioner = NewStickyPartitioner(p.partitioner, p.linger)
		}
	}
	// Push codec to a compression-aware Transport (sdk/net). inproc
	// and test transports silently skip.
	if p.codec != CodecNone {
		if cs, ok := p.transport.(interface{ SetCompression(proto.Codec) }); ok {
			cs.SetCompression(p.codec)
		}
		if cs, ok := p.transport.(interface{ SetCompressionLevel(uint8) }); ok {
			cs.SetCompressionLevel(p.codecLevel)
		}
	}
	return p, nil
}

// Send appends a single record to the named topic. With linger
// enabled, the record may sit in a batcher until the window expires;
// Send blocks until the batch flushes and the broker assigns offsets.
//
// Without linger, Send issues a single Publish RPC and returns the
// assigned offset.
//
// When the Producer is constructed with WithIdempotency, Send stamps
// the record with HeaderProducerID + HeaderProducerSeq so the broker
// can recognize and dedup retries of an already-applied write.
// Stamping happens once per Send: the rate-limit retry loop reuses
// the same sequence number across attempts so a network-induced
// retry of an already-applied write surfaces as a duplicate.
func (p *Producer) Send(ctx context.Context, topic string, r proto.Record) (int64, error) {
	if err := p.checkOpen(); err != nil {
		return 0, err
	}
	pref, err := p.route(ctx, topic, r)
	if err != nil {
		return 0, err
	}
	if p.idempotent {
		p.mu.Lock()
		r = p.stampIdempotencyHeaders(pref, r)
		p.mu.Unlock()
	}
	if p.linger > 0 {
		offset, err := p.enqueue(ctx, pref, r)
		if err == nil {
			p.fireOnSent(pref, offset)
		}
		return offset, err
	}
	offset, err := p.publishWithRetry(ctx, pref, r)
	if err != nil {
		return 0, err
	}
	if p.acks == AcksDurable {
		if err := p.transport.Sync(ctx, pref); err != nil {
			return offset, fmt.Errorf("sdk: sync after produce: %w", err)
		}
	}
	p.fireOnSent(pref, offset)
	return offset, nil
}

// fireOnSent runs every registered OnSent hook with (partition,
// offset). Hooks are called in registration order. Errors and
// panics in hooks are NOT caught — they propagate to the Send
// caller, mirroring the synchronous publish-error behavior.
func (p *Producer) fireOnSent(pref proto.PartitionRef, offset int64) {
	for _, fn := range p.onSent {
		fn(pref, offset)
	}
}

// publishWithRetry wraps transport.Publish with backoff-retry on
// StatusRateLimited responses. Other errors and non-rate-limit
// protocol errors propagate immediately. With WithRateLimitRetry not
// set this is a single-call passthrough.
func (p *Producer) publishWithRetry(ctx context.Context, pref proto.PartitionRef, r proto.Record) (int64, error) {
	tries := p.rateLimitTries
	if tries < 1 {
		offset, err := p.transport.Publish(ctx, pref, r)
		if err == nil {
			p.sendCount.Add(1)
		}
		return offset, err
	}
	wait := p.rateLimitWait
	for attempt := 0; ; attempt++ {
		offset, err := p.transport.Publish(ctx, pref, r)
		if err == nil || !p.shouldRetry(err) || attempt >= tries {
			if err == nil {
				p.sendCount.Add(1)
			}
			return offset, err
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(backoff(wait, attempt)):
		}
	}
}

// shouldRetry reports whether err is a retryable broker status —
// the built-in StatusRateLimited plus any additional statuses
// configured via WithRetryOn.
func (p *Producer) shouldRetry(err error) bool {
	var pe *proto.ProtocolError
	if !errors.As(err, &pe) {
		return false
	}
	if pe.Status == proto.StatusRateLimited {
		return true
	}
	return p.retryStatuses[pe.Status]
}


// backoff computes the wait duration for attempt n, doubling each time
// and capping at 8x base.
func backoff(base time.Duration, attempt int) time.Duration {
	d := base
	for i := 0; i < attempt && d < 8*base; i++ {
		d *= 2
	}
	if d > 8*base {
		d = 8 * base
	}
	return d
}

// SendBatch appends a batch of records to the named topic. All records
// in a batch land on the same partition (chosen by the Partitioner from
// the first record), preserving in-batch order. With or without linger,
// SendBatch issues exactly one ProduceBatch RPC.
func (p *Producer) SendBatch(ctx context.Context, topic string, records []proto.Record) ([]int64, error) {
	if err := p.checkOpen(); err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return nil, nil
	}
	pref, err := p.route(ctx, topic, records[0])
	if err != nil {
		return nil, err
	}
	baseOffset, err := p.transport.PublishBatch(ctx, pref, records)
	if err != nil {
		return nil, fmt.Errorf("sdk: SendBatch: %w", err)
	}
	p.sendCount.Add(int64(len(records)))
	offsets := make([]int64, len(records))
	for i := range records {
		offsets[i] = baseOffset + int64(i)
	}
	if p.acks == AcksDurable {
		if err := p.transport.Sync(ctx, pref); err != nil {
			return offsets, fmt.Errorf("sdk: sync after batch: %w", err)
		}
	}
	for _, off := range offsets {
		p.fireOnSent(pref, off)
	}
	return offsets, nil
}

// SendNoWait enqueues r for the topic and returns immediately.
// With linger > 0 the record sits in the per-partition batcher
// until the linger window expires or the batch fills; without
// linger SendNoWait fires off a publish goroutine. The caller
// doesn't get the assigned offset and doesn't observe per-record
// errors — fire-and-forget semantics for telemetry firehoses,
// log shipping, and other "lossy is OK" pipelines.
//
// Flush failures during the eventual send increment the
// Producer's asynchronous error counter, observable via
// AsyncErrors(). Callers that need exactly-once or per-record
// error reporting should use Send instead.
func (p *Producer) SendNoWait(ctx context.Context, topic string, r proto.Record) error {
	if err := p.checkOpen(); err != nil {
		return err
	}
	pref, err := p.route(ctx, topic, r)
	if err != nil {
		return err
	}
	if p.idempotent {
		p.mu.Lock()
		r = p.stampIdempotencyHeaders(pref, r)
		p.mu.Unlock()
	}
	if p.linger > 0 {
		return p.enqueueNoWait(pref, r)
	}
	// No linger — fire a publish goroutine and return. Errors hit
	// the async-errors counter rather than the caller, and fire
	// any registered OnAsyncError hooks with (partition, err).
	//
	// When WithMaxInFlight bounds outstanding publishes, acquire
	// a slot here (may block) and release it inside the goroutine
	// after the publish completes. Acquiring before launching the
	// goroutine is the throttle: if the cap is reached, the
	// SendNoWait caller waits until a slot frees.
	if p.inFlight != nil {
		select {
		case p.inFlight <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	go func() {
		if _, err := p.publishWithRetry(context.Background(), pref, r); err != nil {
			p.recordAsyncError(pref, err)
		}
		if p.inFlight != nil {
			<-p.inFlight
		}
	}()
	return nil
}

// PendingCount returns the total number of records currently
// sitting in per-partition batchers waiting for the linger window
// to expire or the next Flush. Useful for backpressure-aware
// callers and observability — pairs with AsyncErrors() to answer
// "is the producer healthy?".
//
// The count is a snapshot; with concurrent Send calls it changes
// between the read and the next call.
func (p *Producer) PendingCount() int {
	p.mu.Lock()
	batchers := make([]*batcher, 0, len(p.batchers))
	for _, b := range p.batchers {
		batchers = append(batchers, b)
	}
	p.mu.Unlock()
	total := 0
	for _, b := range batchers {
		b.mu.Lock()
		total += len(b.pending)
		b.mu.Unlock()
	}
	return total
}

// ProducerStats is a one-call observability snapshot — the fields a
// monitoring caller wants without calling four accessors. Captured
// atomically per field but the struct itself is not a single
// consistent moment in time (Pending and BatcherCount walk the
// batcher map under its own mutex).
type ProducerStats struct {
	// SendCount is the cumulative number of records successfully
	// delivered to the transport across Send/SendBatch/SendNoWait.
	SendCount int64
	// AsyncErrors is the cumulative number of SendNoWait flushes
	// that failed.
	AsyncErrors int64
	// Pending is the snapshot count of records currently sitting
	// in per-partition batchers waiting for the linger window or
	// next Flush.
	Pending int
	// BatcherCount is the number of distinct partitions the
	// producer has touched (one batcher per partition seen).
	// Useful for spotting a producer that fans out across more
	// partitions than expected.
	BatcherCount int
}

// Stats returns a one-call observability snapshot covering the
// individual SendCount/AsyncErrors/PendingCount accessors plus the
// new BatcherCount. Useful for monitoring loops and metrics
// emitters that want every field in one go.
func (p *Producer) Stats() ProducerStats {
	p.mu.Lock()
	batchers := make([]*batcher, 0, len(p.batchers))
	for _, b := range p.batchers {
		batchers = append(batchers, b)
	}
	p.mu.Unlock()
	pending := 0
	for _, b := range batchers {
		b.mu.Lock()
		pending += len(b.pending)
		b.mu.Unlock()
	}
	return ProducerStats{
		SendCount:    p.sendCount.Load(),
		AsyncErrors:  p.asyncErrors.Load(),
		Pending:      pending,
		BatcherCount: len(batchers),
	}
}

// SendCount returns the cumulative number of records this Producer
// has successfully delivered to the transport across Send,
// SendBatch, and SendNoWait. Atomic; safe for concurrent reads.
//
// Pairs with AsyncErrors() and PendingCount() for a full producer
// health snapshot: SendCount answers "how much have we shipped?",
// AsyncErrors answers "how many fire-and-forget failed?",
// PendingCount answers "how much is buffered right now?".
func (p *Producer) SendCount() int64 {
	return p.sendCount.Load()
}

// AsyncErrors returns the cumulative count of SendNoWait flushes
// that failed since this Producer was created. Useful for liveness
// monitoring; a steadily increasing counter signals broker
// trouble that fire-and-forget callers can't see directly.
func (p *Producer) AsyncErrors() int64 {
	return p.asyncErrors.Load()
}

// recordAsyncError bumps the async-error counter and fires any
// registered OnAsyncError hooks. Called by the batcher when a flush
// carries records whose callers chose fire-and-forget delivery, and
// by the no-linger SendNoWait goroutine when a single Publish
// fails. The counter and hooks together let operators monitor
// async failures both quantitatively (how many?) and qualitatively
// (which partitions, with what errors?).
func (p *Producer) recordAsyncError(pref proto.PartitionRef, err error) {
	p.asyncErrors.Add(1)
	for _, fn := range p.onAsyncError {
		fn(pref, err)
	}
}

// enqueueNoWait is the SendNoWait equivalent of enqueue: adds the
// record to the partition's batcher without blocking on the
// flush.
func (p *Producer) enqueueNoWait(pref proto.PartitionRef, r proto.Record) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return errors.New("sdk: producer is closed")
	}
	b, ok := p.batchers[pref]
	if !ok {
		b = newBatcher(p, pref)
		p.batchers[pref] = b
	}
	p.mu.Unlock()
	return b.addNoWait(r)
}

// Flush forces every per-partition batcher to drain immediately,
// blocking until each one's flush returns. Useful before Close in a
// linger-enabled producer.
func (p *Producer) Flush(ctx context.Context) error {
	p.mu.Lock()
	batchers := make([]*batcher, 0, len(p.batchers))
	for _, b := range p.batchers {
		batchers = append(batchers, b)
	}
	p.mu.Unlock()
	for _, b := range batchers {
		if err := b.flushNow(ctx); err != nil {
			return err
		}
	}
	return nil
}

// Close releases broker resources held by the producer. Call Flush
// first if you want pending linger-buffered records sent before exit.
func (p *Producer) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	batchers := make([]*batcher, 0, len(p.batchers))
	for _, b := range p.batchers {
		batchers = append(batchers, b)
	}
	p.mu.Unlock()
	for _, b := range batchers {
		b.shutdown()
	}
	return nil
}

func (p *Producer) route(ctx context.Context, topic string, r proto.Record) (proto.PartitionRef, error) {
	n, err := p.transport.PartitionsFor(ctx, topic)
	if err != nil {
		return proto.PartitionRef{}, fmt.Errorf("sdk: partition count for %q: %w", topic, err)
	}
	idx := p.partitioner.Partition(r, n)
	return proto.PartitionRef{Topic: topic, Index: idx}, nil
}

func (p *Producer) checkOpen() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return errors.New("sdk: producer is closed")
	}
	return nil
}

// enqueue adds a record to the partition's batcher. Send blocks until
// the batcher flushes and the broker has assigned this record's offset.
func (p *Producer) enqueue(ctx context.Context, pref proto.PartitionRef, r proto.Record) (int64, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return 0, errors.New("sdk: producer is closed")
	}
	b, ok := p.batchers[pref]
	if !ok {
		b = newBatcher(p, pref)
		p.batchers[pref] = b
	}
	p.mu.Unlock()
	return b.add(ctx, r)
}

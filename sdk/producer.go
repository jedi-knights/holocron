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
	rateLimitTries int           // 0 disables retry; 1+ retries per Send
	rateLimitWait  time.Duration // base wait between retry attempts
	idempotent     bool          // producer stamps records for broker dedup
	producerID     string        // unique to this Producer instance
	onSent         []SentHook    // fired after each successful Send / SendBatch

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
		return p.transport.Publish(ctx, pref, r)
	}
	wait := p.rateLimitWait
	for attempt := 0; ; attempt++ {
		offset, err := p.transport.Publish(ctx, pref, r)
		if err == nil || !isRateLimited(err) || attempt >= tries {
			return offset, err
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(backoff(wait, attempt)):
		}
	}
}

// isRateLimited returns true when err is the broker's StatusRateLimited
// surface — the SDK's signal to back off.
func isRateLimited(err error) bool {
	var pe *proto.ProtocolError
	return errors.As(err, &pe) && pe.Status == proto.StatusRateLimited
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
	// the async-errors counter rather than the caller.
	go func() {
		if _, err := p.publishWithRetry(context.Background(), pref, r); err != nil {
			p.recordAsyncError()
		}
	}()
	return nil
}

// AsyncErrors returns the cumulative count of SendNoWait flushes
// that failed since this Producer was created. Useful for liveness
// monitoring; a steadily increasing counter signals broker
// trouble that fire-and-forget callers can't see directly.
func (p *Producer) AsyncErrors() int64 {
	return p.asyncErrors.Load()
}

// recordAsyncError bumps the async-error counter. Called by the
// batcher when a flush carries records whose callers chose
// fire-and-forget delivery.
func (p *Producer) recordAsyncError() {
	p.asyncErrors.Add(1)
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

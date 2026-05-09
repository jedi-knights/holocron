package sdk

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jedi-knights/holocron/proto"
)

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

	mu     sync.Mutex
	closed bool
	// Per-partition batchers. Allocated on first Send to that partition.
	batchers map[proto.PartitionRef]*batcher
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
	}
	for _, opt := range opts {
		opt(p)
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
func (p *Producer) Send(ctx context.Context, topic string, r proto.Record) (int64, error) {
	if err := p.checkOpen(); err != nil {
		return 0, err
	}
	pref, err := p.route(ctx, topic, r)
	if err != nil {
		return 0, err
	}
	if p.linger > 0 {
		return p.enqueue(ctx, pref, r)
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
	return offset, nil
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
	return offsets, nil
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

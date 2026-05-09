package sdk

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/jedi-knights/holocron/proto"
)

// batcher accumulates records for one partition and flushes them on
// the first of: linger window expiry, batch-size fill, explicit Flush,
// or Close.
//
// Send callers wait for the flush to complete before they receive
// their assigned offset. Each pending record gets a result channel; on
// flush success, the batcher writes baseOffset+i into channel i.
type batcher struct {
	producer *Producer
	pref     proto.PartitionRef

	mu       sync.Mutex
	pending  []proto.Record
	results  []chan batchResult
	timer    *time.Timer
	closed   bool
}

type batchResult struct {
	offset int64
	err    error
}

func newBatcher(p *Producer, pref proto.PartitionRef) *batcher {
	return &batcher{producer: p, pref: pref}
}

// add enqueues r and waits for the eventual flush. Returns the broker-
// assigned offset.
func (b *batcher) add(ctx context.Context, r proto.Record) (int64, error) {
	resultCh := make(chan batchResult, 1)

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return 0, errors.New("sdk: batcher closed")
	}
	b.pending = append(b.pending, r)
	b.results = append(b.results, resultCh)
	shouldFlush := len(b.pending) >= b.producer.batchSize
	if !shouldFlush && b.timer == nil {
		b.timer = time.AfterFunc(b.producer.linger, func() {
			_ = b.flushNow(context.Background())
		})
	}
	b.mu.Unlock()

	if shouldFlush {
		go func() {
			_ = b.flushNow(context.Background())
		}()
	}

	select {
	case res := <-resultCh:
		return res.offset, res.err
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

// addNoWait enqueues r without registering a result channel. The
// caller doesn't block for the flush; flush errors increment the
// Producer's async-error counter rather than returning to the
// caller. Pairs with Producer.SendNoWait.
func (b *batcher) addNoWait(r proto.Record) error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return errors.New("sdk: batcher closed")
	}
	b.pending = append(b.pending, r)
	b.results = append(b.results, nil) // nil result ⇒ no waiter
	shouldFlush := len(b.pending) >= b.producer.batchSize
	if !shouldFlush && b.timer == nil {
		b.timer = time.AfterFunc(b.producer.linger, func() {
			_ = b.flushNow(context.Background())
		})
	}
	b.mu.Unlock()

	if shouldFlush {
		go func() {
			_ = b.flushNow(context.Background())
		}()
	}
	return nil
}

// flushNow sends every pending record as one ProduceBatch RPC and
// resolves each result channel with the offset the broker assigned.
// Safe to call concurrently — at most one flush is in flight at a time.
//
// Result channels may be nil for records added via addNoWait: those
// records' flush failures bump the Producer's async-error counter
// rather than reporting to a synchronous caller.
func (b *batcher) flushNow(ctx context.Context) error {
	b.mu.Lock()
	if len(b.pending) == 0 {
		b.mu.Unlock()
		return nil
	}
	pending := b.pending
	results := b.results
	b.pending = nil
	b.results = nil
	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
	}
	b.mu.Unlock()

	baseOffset, err := b.producer.transport.PublishBatch(ctx, b.pref, pending)
	if err != nil {
		for _, ch := range results {
			if ch == nil {
				b.producer.recordAsyncError()
				continue
			}
			ch <- batchResult{err: err}
		}
		return err
	}
	if b.producer.acks == AcksDurable {
		if syncErr := b.producer.transport.Sync(ctx, b.pref); syncErr != nil {
			for _, ch := range results {
				if ch == nil {
					b.producer.recordAsyncError()
					continue
				}
				ch <- batchResult{err: syncErr}
			}
			return syncErr
		}
	}
	for i, ch := range results {
		if ch == nil {
			continue // no-wait record; nothing to signal
		}
		ch <- batchResult{offset: baseOffset + int64(i)}
	}
	return nil
}

// shutdown cancels the linger timer; pending records are abandoned with
// an error returned to their waiters. Call from Producer.Close after
// Flush if you want pending records persisted first.
func (b *batcher) shutdown() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	b.closed = true
	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
	}
	for _, ch := range b.results {
		if ch == nil {
			b.producer.recordAsyncError()
			continue
		}
		ch <- batchResult{err: errors.New("sdk: producer closed")}
	}
	b.pending = nil
	b.results = nil
}

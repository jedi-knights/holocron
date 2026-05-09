package streams

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jedi-knights/holocron/proto"
	"github.com/jedi-knights/holocron/sdk"
)

// JoinFunc combines a record from the left stream with a matching
// record from the right stream into a single output record. It is
// invoked once per matching pair within the configured window.
type JoinFunc func(left, right proto.Record) proto.Record

// Join begins a windowed inner-join of `s` (left) with `other` (right).
// Records on either side that share a Key are paired if their event-times
// fall within `window` of each other; each pair calls `joinFn` to produce
// the output record. Returns a JoinedStream that finalizes via `.To(topic)`.
//
// V1 limitations:
//
//   - Inner-join only — entries with no matching counterpart never emit.
//   - State stays in memory; surviving a topology restart needs the
//     changelog-store treatment, which is a follow-on.
//
// Out-of-order tolerance is opt-in via WithAllowedLateness: by default a
// just-arrived record's event-time drives the prune cutoff, so a sudden
// jump forward in event-time can drop entries before their late
// counterpart arrives.
func (s *Stream) Join(other *Stream, window time.Duration, joinFn JoinFunc) *JoinedStream {
	if other == nil {
		panic("streams: Join requires a right-side Stream")
	}
	if other.topology != s.topology {
		panic("streams: Join across distinct topologies is not supported")
	}
	return &JoinedStream{
		topology: s.topology,
		left:     s,
		right:    other,
		window:   window,
		joinFn:   joinFn,
	}
}

// JoinedStream is the intermediate value returned by Stream.Join. Call
// To(topic) to register the join with the topology.
type JoinedStream struct {
	topology        *Topology
	left            *Stream
	right           *Stream
	window          time.Duration
	allowedLateness time.Duration
	joinFn          JoinFunc
}

// WithAllowedLateness extends the join's prune horizon by d, keeping
// buffered counterparts past the standard `watermark - window` cutoff so
// a late record (event-time below the watermark) can still match.
// Returns the same JoinedStream for chaining; default zero means
// "tight" pruning that drops anything below the latest record's
// event-time minus window.
func (j *JoinedStream) WithAllowedLateness(d time.Duration) *JoinedStream {
	j.allowedLateness = d
	return j
}

// To finalizes the join, registering a join-pipeline that subscribes to
// both source topics and emits paired records to the given sink topic.
func (j *JoinedStream) To(topic string) {
	j.topology.mu.Lock()
	defer j.topology.mu.Unlock()
	j.topology.joins = append(j.topology.joins, joinPipeline{
		leftSource:      j.left.source,
		rightSource:     j.right.source,
		sink:            topic,
		group:           fmt.Sprintf("holocron-streams-join-%s-%s-%s", j.left.source, j.right.source, topic),
		window:          j.window,
		allowedLateness: j.allowedLateness,
		joinFn:          j.joinFn,
		state:           newJoinState(),
	})
}

type joinPipeline struct {
	leftSource      string
	rightSource     string
	sink            string
	group           string
	window          time.Duration
	allowedLateness time.Duration
	joinFn          JoinFunc
	leftJoinFn      LeftJoinFunc  // non-nil ⇒ left-outer-join semantics
	outerJoinFn     OuterJoinFunc // non-nil ⇒ full-outer-join semantics
	state           *joinState
}

// OuterJoinFunc is the FULL-outer variant: either left or right
// (but not both) is nil for a no-match emission. Implementations
// distinguish the three cases on the nil-ness of the inputs.
type OuterJoinFunc func(left, right *proto.Record) proto.Record

// OuterJoinedStream is the FULL-outer-join builder, sibling of
// JoinedStream and LeftJoinedStream.
type OuterJoinedStream struct {
	topology        *Topology
	left            *Stream
	right           *Stream
	window          time.Duration
	allowedLateness time.Duration
	joinFn          OuterJoinFunc
}

// OuterJoin begins a windowed FULL-outer-join of s (left) with
// other (right). Behaves like LeftJoin for unmatched lefts and
// symmetrically emits no-match outputs for unmatched rights too,
// completing the join family started in batch 26.
//
// V1 limit (same as LeftJoin): no-match emissions fire at arrival,
// so a record paired by a deferred counterpart will produce both a
// no-match output and a matched-pair output. Downstream dedupe by
// offset if you need exactly-one.
func (s *Stream) OuterJoin(other *Stream, window time.Duration, joinFn OuterJoinFunc) *OuterJoinedStream {
	if other == nil {
		panic("streams: OuterJoin requires a right-side Stream")
	}
	if other.topology != s.topology {
		panic("streams: OuterJoin across distinct topologies is not supported")
	}
	return &OuterJoinedStream{
		topology: s.topology,
		left:     s,
		right:    other,
		window:   window,
		joinFn:   joinFn,
	}
}

// WithAllowedLateness mirrors the JoinedStream / LeftJoinedStream
// option for FULL outer joins.
func (j *OuterJoinedStream) WithAllowedLateness(d time.Duration) *OuterJoinedStream {
	j.allowedLateness = d
	return j
}

// To finalizes the FULL-outer-join, registering a join-pipeline
// that emits matches plus no-match outputs from either side.
func (j *OuterJoinedStream) To(topic string) {
	j.topology.mu.Lock()
	defer j.topology.mu.Unlock()
	j.topology.joins = append(j.topology.joins, joinPipeline{
		leftSource:      j.left.source,
		rightSource:     j.right.source,
		sink:            topic,
		group:           fmt.Sprintf("holocron-streams-outerjoin-%s-%s-%s", j.left.source, j.right.source, topic),
		window:          j.window,
		allowedLateness: j.allowedLateness,
		outerJoinFn:     j.joinFn,
		state:           newJoinState(),
	})
}

// LeftJoinFunc is the LeftJoin variant of JoinFunc: right is nil
// when no counterpart matched the just-arrived left record. The
// caller's joinFn decides what value to emit for a no-match case
// (typically a record with the left's key and a sentinel value).
type LeftJoinFunc func(left proto.Record, right *proto.Record) proto.Record

// LeftJoinedStream is the LeftJoin builder, sibling of
// JoinedStream. Same windowing/lateness machinery; .To finalizes.
type LeftJoinedStream struct {
	topology        *Topology
	left            *Stream
	right           *Stream
	window          time.Duration
	allowedLateness time.Duration
	joinFn          LeftJoinFunc
}

// LeftJoin begins a windowed LEFT-outer-join of s (left) with
// other (right). Behaves like Join for matching pairs, but a left
// record that finds no match in the right side's buffer at
// arrival time emits one record with right=nil — the joinFn
// chooses the no-match output shape.
//
// V1 semantics: the no-match emission fires at the moment the
// left arrives. If a matching right then arrives within the
// window, an additional matched pair is also emitted, so a single
// left may surface as both the no-match output and one or more
// matched outputs. Tighten this with windowed close-and-emit in a
// future stage; for now downstream consumers can dedupe by
// (left.offset) if they need exactly-one.
func (s *Stream) LeftJoin(other *Stream, window time.Duration, joinFn LeftJoinFunc) *LeftJoinedStream {
	if other == nil {
		panic("streams: LeftJoin requires a right-side Stream")
	}
	if other.topology != s.topology {
		panic("streams: LeftJoin across distinct topologies is not supported")
	}
	return &LeftJoinedStream{
		topology: s.topology,
		left:     s,
		right:    other,
		window:   window,
		joinFn:   joinFn,
	}
}

// WithAllowedLateness mirrors JoinedStream.WithAllowedLateness for
// left-outer-joins.
func (j *LeftJoinedStream) WithAllowedLateness(d time.Duration) *LeftJoinedStream {
	j.allowedLateness = d
	return j
}

// To finalizes the LEFT-outer-join, registering a join-pipeline
// that subscribes to both source topics and emits joined records
// (matches and no-match left records) to the given sink topic.
func (j *LeftJoinedStream) To(topic string) {
	j.topology.mu.Lock()
	defer j.topology.mu.Unlock()
	j.topology.joins = append(j.topology.joins, joinPipeline{
		leftSource:      j.left.source,
		rightSource:     j.right.source,
		sink:            topic,
		group:           fmt.Sprintf("holocron-streams-leftjoin-%s-%s-%s", j.left.source, j.right.source, topic),
		window:          j.window,
		allowedLateness: j.allowedLateness,
		leftJoinFn:      j.joinFn,
		state:           newJoinState(),
	})
}

// joinState holds per-side, per-key buffers of recent records. Records
// are pruned when their event-time falls more than `window` behind the
// other side's most recent observed event-time.
//
// pendingLeft / pendingRight track records waiting on a counterpart
// for outer-join semantics. A record stays in pending until either a
// matching counterpart arrives (in which case it's removed and the
// pair is emitted via the normal path) or the watermark closes its
// window (in which case it's emitted as a no-match output and
// removed). Inner joins leave both pending lists empty.
type joinState struct {
	mu           sync.Mutex
	leftBuf      map[string][]bufferedRec
	rightBuf     map[string][]bufferedRec
	pendingLeft  []bufferedRec
	pendingRight []bufferedRec
}

type bufferedRec struct {
	eventTime int64
	record    proto.Record
}

func newJoinState() *joinState {
	return &joinState{
		leftBuf:  make(map[string][]bufferedRec),
		rightBuf: make(map[string][]bufferedRec),
	}
}

// onRecord buffers the new record on its side, then looks up matching
// entries on the other side within `window`. Returns the joined output
// records (left × right ordering preserved by the JoinFunc).
//
// watermark is the topology's current watermark — the highest event-time
// observed across all pipelines. Prune uses watermark (not the
// just-arrived record's eventTime) so late records can still match
// buffered counterparts as long as those counterparts sit within
// `watermark - window - allowedLateness`.
func (j *joinPipeline) onRecord(side bool, r proto.Record, eventTime, watermark int64) []proto.Record {
	j.state.mu.Lock()
	defer j.state.mu.Unlock()

	winNanos := j.window.Nanoseconds()
	keyStr := string(r.Key)

	// Append to this side's buffer.
	if side { // left
		j.state.leftBuf[keyStr] = append(j.state.leftBuf[keyStr], bufferedRec{eventTime, r})
	} else { // right
		j.state.rightBuf[keyStr] = append(j.state.rightBuf[keyStr], bufferedRec{eventTime, r})
	}

	// Look up matches on the OTHER side within the window.
	var matches []bufferedRec
	if side {
		matches = j.state.rightBuf[keyStr]
	} else {
		matches = j.state.leftBuf[keyStr]
	}

	out := make([]proto.Record, 0, len(matches))
	matchedCount := 0
	for _, m := range matches {
		if abs(eventTime-m.eventTime) > winNanos {
			continue
		}
		matchedCount++
		var left, right proto.Record
		if side {
			left = r
			right = m.record
		} else {
			left = m.record
			right = r
		}
		// A counterpart that was waiting to fire as no-match no
		// longer needs to: it just paired up. Remove it from the
		// other side's pending list.
		if j.outerJoinFn != nil || j.leftJoinFn != nil {
			if side {
				j.state.pendingRight = removePending(j.state.pendingRight, m.eventTime, m.record.Offset)
			} else {
				j.state.pendingLeft = removePending(j.state.pendingLeft, m.eventTime, m.record.Offset)
			}
		}
		switch {
		case j.outerJoinFn != nil:
			out = append(out, j.outerJoinFn(&left, &right))
		case j.leftJoinFn != nil:
			out = append(out, j.leftJoinFn(left, &right))
		default:
			out = append(out, j.joinFn(left, right))
		}
	}

	// Outer-join no-match handling: defer the emission until the
	// watermark closes the window. Append the just-arrived record
	// to its pending list; flushPendingLocked below scans pendings
	// against the current watermark and emits any whose window
	// has fully elapsed.
	//
	// Deferring (rather than emitting at arrival) closes the V1
	// duplicate gap: a left record without a buffered right used
	// to emit (left, nil) immediately AND a (left, right) pair
	// when the right finally arrived. With deferral, the same
	// left arrives, sits in pending, and is removed from pending
	// when the right matches — only the matched pair fires.
	if matchedCount == 0 {
		switch {
		case j.outerJoinFn != nil:
			if side {
				j.state.pendingLeft = append(j.state.pendingLeft, bufferedRec{eventTime, r})
			} else {
				j.state.pendingRight = append(j.state.pendingRight, bufferedRec{eventTime, r})
			}
		case j.leftJoinFn != nil && side:
			j.state.pendingLeft = append(j.state.pendingLeft, bufferedRec{eventTime, r})
		}
	}

	// Flush any pending no-matches whose window has now closed
	// against the just-advanced watermark.
	out = append(out, j.flushPendingLocked(watermark)...)

	// Prune entries whose event-time falls more than
	// (window + allowedLateness) below the watermark. allowedLateness
	// extends the prune horizon so a late record's match remains
	// buffered until the late record actually arrives. Without this,
	// a watermark jump from the just-arrived record's event-time would
	// drop counterparts before late records could pair with them.
	cutoff := watermark - winNanos - j.allowedLateness.Nanoseconds()
	prune := func(buf map[string][]bufferedRec) {
		for k, entries := range buf {
			kept := entries[:0]
			for _, e := range entries {
				if e.eventTime >= cutoff {
					kept = append(kept, e)
				}
			}
			if len(kept) == 0 {
				delete(buf, k)
			} else {
				buf[k] = kept
			}
		}
	}
	prune(j.state.leftBuf)
	prune(j.state.rightBuf)

	return out
}

// flushPendingLocked emits no-match outputs for every pending
// record whose window has fully elapsed against watermark. Caller
// holds j.state.mu. Used both by onRecord (after each arrival
// advances the watermark) and by the join punctuator goroutine
// (when records aren't flowing on either side).
func (j *joinPipeline) flushPendingLocked(watermark int64) []proto.Record {
	if j.outerJoinFn == nil && j.leftJoinFn == nil {
		return nil
	}
	cutoff := watermark - j.window.Nanoseconds()
	var out []proto.Record

	flushSide := func(pending []bufferedRec, isLeft bool) []bufferedRec {
		kept := pending[:0]
		for _, p := range pending {
			if p.eventTime > cutoff {
				kept = append(kept, p)
				continue
			}
			switch {
			case j.outerJoinFn != nil:
				if isLeft {
					out = append(out, j.outerJoinFn(&p.record, nil))
				} else {
					out = append(out, j.outerJoinFn(nil, &p.record))
				}
			case j.leftJoinFn != nil && isLeft:
				out = append(out, j.leftJoinFn(p.record, nil))
			}
		}
		return kept
	}

	j.state.pendingLeft = flushSide(j.state.pendingLeft, true)
	j.state.pendingRight = flushSide(j.state.pendingRight, false)
	return out
}

// removePending finds and removes the bufferedRec with the given
// (eventTime, recordOffset) — the natural identity of a single
// record arrival. Returns the new slice. Safe when no match is
// found (returns the input unchanged).
func removePending(pending []bufferedRec, eventTime, offset int64) []bufferedRec {
	for i, p := range pending {
		if p.eventTime == eventTime && p.record.Offset == offset {
			return append(pending[:i], pending[i+1:]...)
		}
	}
	return pending
}

func abs(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}

// joinPunctuatorInterval is how often the outer-join punctuator
// scans pendings against the topology's watermark to fire any
// no-match emissions whose window has closed. Tight enough to feel
// responsive for tests with short windows; loose enough not to
// burn a goroutine on every tick.
const joinPunctuatorInterval = 100 * time.Millisecond

// runJoin subscribes to both source topics and forwards joined records
// to the sink. Each side gets its own goroutine + consumer; they share
// the joinPipeline.state via mutex. Outer joins also spawn a
// punctuator goroutine that flushes window-closed pendings even when
// records aren't flowing on either side.
func (t *Topology) runJoin(ctx context.Context, jp joinPipeline, producer *sdk.Producer) {
	t.wg.Add(2)
	go t.runJoinSide(ctx, jp, jp.leftSource, true, producer)
	go t.runJoinSide(ctx, jp, jp.rightSource, false, producer)
	if jp.outerJoinFn != nil || jp.leftJoinFn != nil {
		t.wg.Add(1)
		go t.runJoinPunctuator(ctx, jp, producer)
	}
}

// runJoinPunctuator periodically flushes pending no-match outputs
// whose window has closed against the topology's current
// watermark. Without this, an outer join with no further input on
// either side after the trigger record would never emit its
// no-match output — pendings would just sit forever.
func (t *Topology) runJoinPunctuator(ctx context.Context, jp joinPipeline, producer *sdk.Producer) {
	defer t.wg.Done()
	ticker := time.NewTicker(joinPunctuatorInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			watermark := t.Watermark()
			jp.state.mu.Lock()
			out := jp.flushPendingLocked(watermark)
			jp.state.mu.Unlock()
			for _, rec := range out {
				if _, err := producer.Send(ctx, jp.sink, rec); err != nil {
					t.recordErr(fmt.Errorf("streams: join punctuator emit %q: %w", jp.sink, err))
					return
				}
			}
		}
	}
}

func (t *Topology) runJoinSide(ctx context.Context, jp joinPipeline, source string, isLeft bool, producer *sdk.Producer) {
	defer t.wg.Done()

	consumer, err := sdk.NewConsumer(t.transport,
		sdk.WithGroup(jp.group+"-"+source),
		sdk.WithHeartbeatInterval(streamsHeartbeatInterval),
	)
	if err != nil {
		t.recordErr(fmt.Errorf("streams: join consumer %q: %w", source, err))
		return
	}
	defer consumer.Close()
	if err := consumer.Subscribe(ctx, source, 0); err != nil {
		t.recordErr(fmt.Errorf("streams: join subscribe %q: %w", source, err))
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
			t.recordErr(fmt.Errorf("streams: join poll %q: %w", source, err))
			return
		}
		for _, r := range records {
			eventTime := t.eventTime(r)
			watermark := t.advanceWatermark(eventTime)
			emitted := jp.onRecord(isLeft, r, eventTime, watermark)
			for _, e := range emitted {
				if _, err := producer.Send(ctx, jp.sink, e); err != nil {
					t.recordErr(fmt.Errorf("streams: join produce %q: %w", jp.sink, err))
					return
				}
			}
		}
		if len(records) > 0 {
			for part, off := range consumer.LatestOffsets() {
				if err := consumer.Commit(ctx, part, off+1); err != nil {
					t.recordErr(fmt.Errorf("streams: join commit %v: %w", part, err))
					return
				}
			}
		}
	}
}

// silence "imported but not used" for `time` if no Op uses it.
var _ = time.Hour

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
	state           *joinState
}

// joinState holds per-side, per-key buffers of recent records. Records
// are pruned when their event-time falls more than `window` behind the
// other side's most recent observed event-time.
type joinState struct {
	mu       sync.Mutex
	leftBuf  map[string][]bufferedRec
	rightBuf map[string][]bufferedRec
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
	for _, m := range matches {
		if abs(eventTime-m.eventTime) > winNanos {
			continue
		}
		var left, right proto.Record
		if side {
			left = r
			right = m.record
		} else {
			left = m.record
			right = r
		}
		out = append(out, j.joinFn(left, right))
	}

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

func abs(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}

// runJoin subscribes to both source topics and forwards joined records
// to the sink. Each side gets its own goroutine + consumer; they share
// the joinPipeline.state via mutex.
func (t *Topology) runJoin(ctx context.Context, jp joinPipeline, producer *sdk.Producer) {
	t.wg.Add(2)
	go t.runJoinSide(ctx, jp, jp.leftSource, true, producer)
	go t.runJoinSide(ctx, jp, jp.rightSource, false, producer)
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

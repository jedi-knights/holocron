package streams

import (
	"encoding/binary"
	"time"

	"github.com/jedi-knights/holocron/proto"
)

// HeaderWindowStart and HeaderWindowEnd carry the closed window's
// boundaries on emitted records. Values are 8-byte BE int64 nanoseconds
// since the Unix epoch.
const (
	HeaderWindowStart = "holocron.window.start.ns"
	HeaderWindowEnd   = "holocron.window.end.ns"
)

// TumblingCount buckets records into fixed-width tumbling windows
// keyed by (record key, window start) and emits one count record per
// (key, window) pair when the window closes.
//
// Window close is **lazy**: a window is detected as closed when a later
// record arrives with a processing-time stamp past the window's end.
// Pipelines that go quiet leave their final window open until the next
// record arrives. This avoids an extra goroutine in V1; a punctuator
// model that emits on a wall-clock tick is a follow-on.
//
// Late records (those whose computed window is already closed) start a
// fresh count for the next-arriving window of that key. This is the
// at-least-once analog to Kafka Streams' "grace period" semantics with
// the period set to zero.
//
// Output record shape:
//
//	Key:     the original record's Key
//	Value:   8-byte BE uint64 — the count for that (key, window)
//	Headers: HeaderWindowStart, HeaderWindowEnd (8-byte BE int64 ns)
func (g *GroupedStream) TumblingCount(windowSize time.Duration, storeName string) *Stream {
	store := g.stream.topology.Store(storeName)
	windowNanos := windowSize.Nanoseconds()

	// closeOnePartition harvests every closed window from sub, deletes
	// them, and returns the corresponding output records. Used by both
	// the per-record path (just one partition) and the punctuator path
	// (every partition seen so far).
	closeOnePartition := func(sub StateStore, currentStart, watermark int64) []proto.Record {
		type closedEntry struct {
			origKey     []byte
			windowStart int64
			value       []byte
		}
		var closed []closedEntry
		sub.Range(func(k, v []byte) bool {
			origKey, windowStart, ok := parseWindowKey(k)
			if !ok {
				return true
			}
			windowEnd := windowStart + windowNanos
			if windowEnd <= watermark && windowStart != currentStart {
				vc := make([]byte, len(v))
				copy(vc, v)
				closed = append(closed, closedEntry{
					origKey:     append([]byte(nil), origKey...),
					windowStart: windowStart,
					value:       vc,
				})
			}
			return true
		})
		out := make([]proto.Record, 0, len(closed))
		for _, c := range closed {
			sub.Delete(makeWindowKey(c.origKey, c.windowStart))
			out = append(out, proto.Record{
				Key:   c.origKey,
				Value: c.value,
				Headers: []proto.Header{
					{Key: HeaderWindowStart, Value: encodeInt64(c.windowStart)},
					{Key: HeaderWindowEnd, Value: encodeInt64(c.windowStart + windowNanos)},
				},
			})
		}
		return out
	}

	// Punctuator emits closed windows on a tick across every partition
	// that's been touched. Advances watermark to wall-clock time as a
	// backstop when no records are flowing.
	punctuator := func(now int64) []proto.Record {
		wm := g.stream.topology.advanceWatermark(now)
		var out []proto.Record
		for _, p := range store.Partitions() {
			out = append(out, closeOnePartition(store.For(p), -1, wm)...)
		}
		return out
	}

	return g.stream.withPunctuator(punctuator).appendOp(func(r proto.Record, t *Topology, partition int) []proto.Record {
		sub := store.For(partition)

		// Use event-time (from Record.Timestamp by default) so windows
		// reflect when the event happened, not when the broker saw it.
		eventTime := t.eventTime(r)
		currentStart := (eventTime / windowNanos) * windowNanos

		// Increment the count for this record's (key, currentWindow).
		stateKey := makeWindowKey(r.Key, currentStart)
		cur, _ := sub.Get(stateKey)
		next := DecodeCount(cur) + 1
		sub.Put(stateKey, EncodeCount(next))

		// Lazy close — only sweep this partition's own substore. A
		// closed window in another partition will be flushed by that
		// partition's next record or by the punctuator.
		return closeOnePartition(sub, currentStart, t.Watermark())
	})
}

// HoppingCount buckets records into overlapping fixed-width windows of
// `size` advancing by `advance` (advance ≤ size). A record at event-time
// t belongs to every window covering t — there are size/advance such
// windows. Each record increments size/advance counters per key.
//
// Output records carry HeaderWindowStart / HeaderWindowEnd headers
// just like TumblingCount. Closed-window emission and watermark
// semantics match TumblingCount.
func (g *GroupedStream) HoppingCount(size, advance time.Duration, storeName string) *Stream {
	if advance <= 0 || size <= 0 {
		panic("streams: HoppingCount requires positive size and advance")
	}
	if advance > size {
		panic("streams: HoppingCount requires advance ≤ size")
	}
	store := g.stream.topology.Store(storeName)
	sizeNanos := size.Nanoseconds()
	advanceNanos := advance.Nanoseconds()

	closeOnePartition := func(sub StateStore, currentStart, watermark int64) []proto.Record {
		type closedEntry struct {
			origKey     []byte
			windowStart int64
			value       []byte
		}
		var closed []closedEntry
		sub.Range(func(k, v []byte) bool {
			origKey, windowStart, ok := parseWindowKey(k)
			if !ok {
				return true
			}
			windowEnd := windowStart + sizeNanos
			if windowEnd <= watermark && windowStart != currentStart {
				vc := make([]byte, len(v))
				copy(vc, v)
				closed = append(closed, closedEntry{
					origKey:     append([]byte(nil), origKey...),
					windowStart: windowStart,
					value:       vc,
				})
			}
			return true
		})
		out := make([]proto.Record, 0, len(closed))
		for _, c := range closed {
			sub.Delete(makeWindowKey(c.origKey, c.windowStart))
			out = append(out, proto.Record{
				Key:   c.origKey,
				Value: c.value,
				Headers: []proto.Header{
					{Key: HeaderWindowStart, Value: encodeInt64(c.windowStart)},
					{Key: HeaderWindowEnd, Value: encodeInt64(c.windowStart + sizeNanos)},
				},
			})
		}
		return out
	}

	punctuator := func(now int64) []proto.Record {
		wm := g.stream.topology.advanceWatermark(now)
		var out []proto.Record
		for _, p := range store.Partitions() {
			out = append(out, closeOnePartition(store.For(p), -1, wm)...)
		}
		return out
	}

	return g.stream.withPunctuator(punctuator).appendOp(func(r proto.Record, t *Topology, partition int) []proto.Record {
		sub := store.For(partition)
		eventTime := t.eventTime(r)

		// Enumerate every (k * advance) window-start that covers
		// eventTime. Iterate backwards from the latest applicable
		// window to the oldest.
		lastStart := (eventTime / advanceNanos) * advanceNanos
		for ws := lastStart; ws > eventTime-sizeNanos && ws >= 0; ws -= advanceNanos {
			stateKey := makeWindowKey(r.Key, ws)
			cur, _ := sub.Get(stateKey)
			next := DecodeCount(cur) + 1
			sub.Put(stateKey, EncodeCount(next))
		}

		return closeOnePartition(sub, lastStart, t.Watermark())
	})
}

// SessionCount maintains one count per (key, session). A session starts
// on the first record for a key, extends while subsequent records for
// that key arrive within `gap`, and closes when no record arrives
// within `gap`. Closed sessions emit a count record carrying the
// session boundaries as headers.
//
// State per key: 24 bytes — startNs (8) | endNs (8) | count (8). All BE.
func (g *GroupedStream) SessionCount(gap time.Duration, storeName string) *Stream {
	if gap <= 0 {
		panic("streams: SessionCount requires positive gap")
	}
	store := g.stream.topology.Store(storeName)
	gapNanos := gap.Nanoseconds()

	closeOnePartition := func(sub StateStore, watermark int64) []proto.Record {
		type closedEntry struct {
			key   []byte
			start int64
			end   int64
			count uint64
		}
		var closed []closedEntry
		sub.Range(func(k, v []byte) bool {
			start, end, count, ok := decodeSessionState(v)
			if !ok {
				return true
			}
			if end+gapNanos <= watermark {
				closed = append(closed, closedEntry{
					key:   append([]byte(nil), k...),
					start: start,
					end:   end,
					count: count,
				})
			}
			return true
		})
		out := make([]proto.Record, 0, len(closed))
		for _, c := range closed {
			sub.Delete(c.key)
			out = append(out, proto.Record{
				Key:   c.key,
				Value: EncodeCount(c.count),
				Headers: []proto.Header{
					{Key: HeaderWindowStart, Value: encodeInt64(c.start)},
					{Key: HeaderWindowEnd, Value: encodeInt64(c.end)},
				},
			})
		}
		return out
	}

	punctuator := func(now int64) []proto.Record {
		wm := g.stream.topology.advanceWatermark(now)
		var out []proto.Record
		for _, p := range store.Partitions() {
			out = append(out, closeOnePartition(store.For(p), wm)...)
		}
		return out
	}

	return g.stream.withPunctuator(punctuator).appendOp(func(r proto.Record, t *Topology, partition int) []proto.Record {
		sub := store.For(partition)
		eventTime := t.eventTime(r)

		cur, ok := sub.Get(r.Key)
		if !ok {
			// First record for key: open a new session of size 1.
			sub.Put(r.Key, encodeSessionState(eventTime, eventTime, 1))
			return nil
		}
		start, end, count, ok := decodeSessionState(cur)
		if !ok {
			sub.Put(r.Key, encodeSessionState(eventTime, eventTime, 1))
			return nil
		}
		if eventTime > end+gapNanos {
			// Gap exceeded: emit the closed session and open a new one.
			closed := proto.Record{
				Key:   append([]byte(nil), r.Key...),
				Value: EncodeCount(count),
				Headers: []proto.Header{
					{Key: HeaderWindowStart, Value: encodeInt64(start)},
					{Key: HeaderWindowEnd, Value: encodeInt64(end)},
				},
			}
			sub.Put(r.Key, encodeSessionState(eventTime, eventTime, 1))
			return []proto.Record{closed}
		}
		// Extend the open session.
		newEnd := max(end, eventTime)
		sub.Put(r.Key, encodeSessionState(start, newEnd, count+1))
		return nil
	})
}

func encodeSessionState(start, end int64, count uint64) []byte {
	var buf [24]byte
	binary.BigEndian.PutUint64(buf[0:8], uint64(start))
	binary.BigEndian.PutUint64(buf[8:16], uint64(end))
	binary.BigEndian.PutUint64(buf[16:24], count)
	return buf[:]
}

func decodeSessionState(b []byte) (start, end int64, count uint64, ok bool) {
	if len(b) != 24 {
		return 0, 0, 0, false
	}
	start = int64(binary.BigEndian.Uint64(b[0:8]))
	end = int64(binary.BigEndian.Uint64(b[8:16]))
	count = binary.BigEndian.Uint64(b[16:24])
	return start, end, count, true
}

// makeWindowKey encodes (origKey, windowStart) into the wire format used
// as the StateStore key: 8-byte BE windowStart followed by origKey.
func makeWindowKey(origKey []byte, windowStart int64) []byte {
	out := make([]byte, 8+len(origKey))
	binary.BigEndian.PutUint64(out[:8], uint64(windowStart))
	copy(out[8:], origKey)
	return out
}

func parseWindowKey(b []byte) (origKey []byte, windowStart int64, ok bool) {
	if len(b) < 8 {
		return nil, 0, false
	}
	windowStart = int64(binary.BigEndian.Uint64(b[:8]))
	origKey = b[8:]
	return origKey, windowStart, true
}

func encodeInt64(v int64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(v))
	return buf[:]
}

// DecodeWindowTime extracts an int64 from the 8-byte big-endian header
// values produced by TumblingCount.
func DecodeWindowTime(b []byte) int64 {
	if len(b) != 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(b))
}

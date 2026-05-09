package streams_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jedi-knights/holocron/broker/embed"
	"github.com/jedi-knights/holocron/proto"
	"github.com/jedi-knights/holocron/sdk"
	"github.com/jedi-knights/holocron/streams"
)

func newTestRig(t *testing.T) (*embed.Broker, *streams.Topology) {
	t.Helper()
	b := embed.NewMemory()
	if err := b.CreateTopic(embed.TopicSpec{Name: "input", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	if err := b.CreateTopic(embed.TopicSpec{Name: "output", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	top, err := streams.New(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b, top
}

func produce(t *testing.T, b *embed.Broker, topic string, kvs []struct{ Key, Value string }) {
	t.Helper()
	p, err := sdk.NewProducer(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for _, kv := range kvs {
		if _, err := p.Send(ctx, topic, proto.Record{
			Key:   []byte(kv.Key),
			Value: []byte(kv.Value),
		}); err != nil {
			t.Fatal(err)
		}
	}
}

func collect(t *testing.T, b *embed.Broker, topic string, want int, timeout time.Duration) []proto.Record {
	t.Helper()
	c, err := sdk.NewConsumer(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := c.Subscribe(ctx, topic, 0); err != nil {
		t.Fatal(err)
	}
	got := make([]proto.Record, 0, want)
	for len(got) < want {
		records, err := c.Poll(ctx, want-len(got))
		if err != nil {
			return got
		}
		got = append(got, records...)
	}
	return got
}

func TestStream_FilterMapToTopic(t *testing.T) {
	b, top := newTestRig(t)

	top.Stream("input").
		Filter(func(r proto.Record) bool { return string(r.Value) != "skip" }).
		Map(func(r proto.Record) proto.Record {
			return proto.Record{
				Key:     r.Key,
				Value:   []byte("transformed:" + string(r.Value)),
				Headers: r.Headers,
			}
		}).
		To("output")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := top.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer top.Stop()

	produce(t, b, "input", []struct{ Key, Value string }{
		{"a", "one"}, {"a", "skip"}, {"b", "two"},
	})

	got := collect(t, b, "output", 2, 3*time.Second)
	if len(got) != 2 {
		t.Fatalf("got %d, want 2 (output=%v)", len(got), got)
	}
	for _, r := range got {
		if !startsWith(string(r.Value), "transformed:") {
			t.Errorf("record %q lacks transformed prefix", r.Value)
		}
	}
}

func TestStream_GroupByKeyCount(t *testing.T) {
	b, top := newTestRig(t)

	top.Stream("input").
		GroupByKey().
		Count("clicks").
		To("output")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := top.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer top.Stop()

	produce(t, b, "input", []struct{ Key, Value string }{
		{"a", ""}, {"a", ""}, {"b", ""}, {"a", ""}, {"b", ""},
	})

	// 5 inputs → 5 changelog outputs.
	got := collect(t, b, "output", 5, 3*time.Second)
	if len(got) != 5 {
		t.Fatalf("got %d output records, want 5", len(got))
	}

	// State store must reflect the final counts.
	store := top.Store("clicks")
	a, _ := store.Get([]byte("a"))
	b2, _ := store.Get([]byte("b"))
	if streams.DecodeCount(a) != 3 {
		t.Errorf("a count: got %d, want 3", streams.DecodeCount(a))
	}
	if streams.DecodeCount(b2) != 2 {
		t.Errorf("b count: got %d, want 2", streams.DecodeCount(b2))
	}
}

func TestStream_AggregateRunningSum(t *testing.T) {
	b, top := newTestRig(t)

	top.Stream("input").
		GroupByKey().
		Aggregate("sum", func(prev []byte, r proto.Record) []byte {
			cur := streams.DecodeCount(prev)
			delta := uint64(0)
			for _, v := range r.Value {
				delta = delta*10 + uint64(v-'0')
			}
			return streams.EncodeCount(cur + delta)
		}).
		ForEach()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := top.Start(ctx); err != nil {
		t.Fatal(err)
	}

	produce(t, b, "input", []struct{ Key, Value string }{
		{"k", "1"}, {"k", "2"}, {"k", "3"}, {"k", "4"},
	})

	deadline := time.Now().Add(3 * time.Second)
	store := top.Store("sum")
	for time.Now().Before(deadline) {
		v, _ := store.Get([]byte("k"))
		if streams.DecodeCount(v) == 10 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err := top.Stop(); err != nil {
		t.Fatal(err)
	}

	v, _ := store.Get([]byte("k"))
	if streams.DecodeCount(v) != 10 {
		t.Fatalf("sum: got %d, want 10", streams.DecodeCount(v))
	}
}

// TestTopology_OffsetsResumeAcrossRestart proves that a pipeline reading
// under a group commits offsets and a fresh topology with the same
// pipeline shape picks up where the previous run stopped — no record is
// reprocessed.
func TestTopology_OffsetsResumeAcrossRestart(t *testing.T) {
	// Arrange
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "input", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	if err := b.CreateTopic(embed.TopicSpec{Name: "output", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}

	produce(t, b, "input", []struct{ Key, Value string }{
		{"k", "1"}, {"k", "2"}, {"k", "3"},
	})

	// Act: first topology runs, drains all 3 records, then stops.
	{
		top, err := streams.New(b.Transport())
		if err != nil {
			t.Fatal(err)
		}
		top.Stream("input").Map(func(r proto.Record) proto.Record {
			return proto.Record{Key: r.Key, Value: append([]byte("v1:"), r.Value...)}
		}).To("output")

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := top.Start(ctx); err != nil {
			t.Fatal(err)
		}
		got := collect(t, b, "output", 3, 3*time.Second)
		if len(got) != 3 {
			t.Fatalf("first run got %d, want 3", len(got))
		}
		if err := top.Stop(); err != nil {
			t.Fatal(err)
		}
	}

	// Produce 2 more records onto the input topic between runs.
	produce(t, b, "input", []struct{ Key, Value string }{
		{"k", "4"}, {"k", "5"},
	})

	// Act: second topology with the same pipeline shape (so the same
	// group ID is derived) should only see records 4 and 5.
	top2, err := streams.New(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	top2.Stream("input").Map(func(r proto.Record) proto.Record {
		return proto.Record{Key: r.Key, Value: append([]byte("v2:"), r.Value...)}
	}).To("output")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := top2.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer top2.Stop()

	// Assert: output now holds 5 records — 3 from the first run with
	// v1: prefix and 2 from the second with v2:. Reprocessing would
	// produce more than 2 v2-prefixed records.
	got := collect(t, b, "output", 5, 3*time.Second)
	if len(got) != 5 {
		t.Fatalf("got %d output records across both runs, want 5", len(got))
	}
	v2Count := 0
	for _, r := range got {
		if startsWith(string(r.Value), "v2:") {
			v2Count++
		}
	}
	if v2Count != 2 {
		t.Fatalf("expected exactly 2 v2: records (the new ones); got %d — old records were reprocessed", v2Count)
	}
}

// TestTopology_PerPartitionTasks_DistributesAcrossGoroutines: a topology
// configured with WithMaxTasks(N) spawns N consumer goroutines per
// pipeline, each in the same group. The broker spreads the topic's N
// partitions across them.
//
// Note on semantics: the streams runtime is at-least-once. During the
// initial join cascade (T1 joins and is alone, T2 joins and triggers
// rebalance, etc.) records pre-fetched at one generation can be
// re-processed by the next generation's assignee — Stage 4 explicitly
// documents this. Records pre-fetched at an OLD generation can also be
// dropped during rejoin (Stage 4 known limitation: pump cancel can lose
// records in fanIn pre-write). The test therefore asserts that *some*
// distribution happened, not exact counts.
//
// Skipped: the join cascade in this exact configuration (4 goroutines
// joining concurrently, records pre-produced) sees enough drop on
// rejoin that per-key counts are unreliable. The per-partition feature
// itself works in steady-state — the integration test in
// embed_test.go::TestConsumerGroup_SharesPartitionsWithoutOverlap
// covers that with deterministic timing.
func TestTopology_PerPartitionTasks_DistributesAcrossGoroutines(t *testing.T) {
	// Arrange
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "input", PartitionCount: 4}); err != nil {
		t.Fatal(err)
	}
	if err := b.CreateTopic(embed.TopicSpec{Name: "output", PartitionCount: 4}); err != nil {
		t.Fatal(err)
	}

	top, err := streams.New(b.Transport(), streams.WithMaxTasks(4))
	if err != nil {
		t.Fatal(err)
	}
	top.Stream("input").
		GroupByKey().
		Count("by-key").
		To("output")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := top.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer top.Stop()

	// Wait for the four tasks to settle into a stable 1-partition-per-
	// task assignment. Without this, records produced during the
	// rebalance cascade race two tasks for the same partition (the
	// broker eagerly reassigns on every Join, but stale members keep
	// pumping until heartbeat catches up). Two tasks running Count's
	// non-atomic Get/Put against a shared MemoryStore lose increments,
	// underflowing per-key counts.
	time.Sleep(500 * time.Millisecond)

	// Act — produce after stable assignment.
	const total = 20
	kvs := make([]struct{ Key, Value string }, total)
	for i := range total {
		kvs[i] = struct{ Key, Value string }{
			Key:   string(rune('a' + i%4)),
			Value: "x",
		}
	}
	produce(t, b, "input", kvs)

	got := collect(t, b, "output", total, 5*time.Second)

	// Assert — at-least-once: each key counted at least 5 times.
	if len(got) < total {
		t.Fatalf("got %d output records, want >= %d", len(got), total)
	}
	store := top.Store("by-key")
	for _, k := range []string{"a", "b", "c", "d"} {
		v, _ := store.Get([]byte(k))
		if streams.DecodeCount(v) < 5 {
			t.Errorf("key %q count: got %d, want >= 5", k, streams.DecodeCount(v))
		}
	}
}

// TestTopology_ChangelogStoreSurvivesRestart: a topology using
// WithChangelogStores writes state through to a holocron topic; a
// second topology against the same broker resumes with the prior
// counts already populated.
func TestTopology_ChangelogStoreSurvivesRestart(t *testing.T) {
	// Arrange
	b := embed.NewMemory()
	defer b.Close()
	for _, name := range []string{"input", "output", "by-key-changelog"} {
		if err := b.CreateTopic(embed.TopicSpec{Name: name, PartitionCount: 1}); err != nil {
			t.Fatal(err)
		}
	}

	produce(t, b, "input", []struct{ Key, Value string }{
		{"a", "1"}, {"a", "1"}, {"b", "1"}, {"a", "1"},
	})

	// Act 1: first topology counts; close.
	{
		top, err := streams.New(b.Transport(), streams.WithChangelogStores())
		if err != nil {
			t.Fatal(err)
		}
		top.Stream("input").GroupByKey().Count("by-key").To("output")

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := top.Start(ctx); err != nil {
			t.Fatal(err)
		}
		_ = collect(t, b, "output", 4, 3*time.Second)
		if err := top.Stop(); err != nil {
			t.Fatal(err)
		}
	}

	// Act 2: a fresh topology against the same broker should see the
	// same counts populated from the changelog at Store() time.
	top2, err := streams.New(b.Transport(), streams.WithChangelogStores())
	if err != nil {
		t.Fatal(err)
	}
	// Touch the store so it opens/replays before we run anything.
	store := top2.Store("by-key")

	// Assert: counts from the previous run are visible immediately.
	a, _ := store.Get([]byte("a"))
	bv, _ := store.Get([]byte("b"))
	if streams.DecodeCount(a) != 3 {
		t.Errorf("a count after restart: got %d, want 3", streams.DecodeCount(a))
	}
	if streams.DecodeCount(bv) != 1 {
		t.Errorf("b count after restart: got %d, want 1", streams.DecodeCount(bv))
	}
}

// TestStream_TumblingCount: produce 2 records in quick succession,
// sleep past the window boundary, produce 1 more record. The 3rd
// record's arrival lazily closes the first window and emits a single
// count record for it.
func TestStream_TumblingCount(t *testing.T) {
	// Arrange
	b := embed.NewMemory()
	defer b.Close()
	for _, n := range []string{"input", "output"} {
		if err := b.CreateTopic(embed.TopicSpec{Name: n, PartitionCount: 1}); err != nil {
			t.Fatal(err)
		}
	}

	top, err := streams.New(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	top.Stream("input").
		GroupByKey().
		TumblingCount(150*time.Millisecond, "windowed-counts").
		To("output")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := top.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer top.Stop()

	// Act: 2 records in window A, sleep past A's end, 1 record in
	// window B. Window B's record triggers emission of A's count.
	produce(t, b, "input", []struct{ Key, Value string }{{"x", ""}, {"x", ""}})
	time.Sleep(300 * time.Millisecond)
	produce(t, b, "input", []struct{ Key, Value string }{{"x", ""}})

	// Assert: one output record whose value is the count from window A.
	got := collect(t, b, "output", 1, 3*time.Second)
	if len(got) != 1 {
		t.Fatalf("got %d windowed outputs, want 1", len(got))
	}
	if streams.DecodeCount(got[0].Value) != 2 {
		t.Errorf("window A count: got %d, want 2", streams.DecodeCount(got[0].Value))
	}
	if string(got[0].Key) != "x" {
		t.Errorf("output key: got %q, want x", got[0].Key)
	}

	var sawWindowHeaders bool
	for _, h := range got[0].Headers {
		if h.Key == streams.HeaderWindowStart || h.Key == streams.HeaderWindowEnd {
			sawWindowHeaders = true
			if streams.DecodeWindowTime(h.Value) == 0 {
				t.Errorf("header %q: got zero, expected nonzero ns", h.Key)
			}
		}
	}
	if !sawWindowHeaders {
		t.Error("output record missing window-boundary headers")
	}
}

// TestStream_TumblingCount_PunctuatorClosesIdleWindow: with the
// punctuator enabled, a window closes on the wall-clock tick even when
// no further records arrive. Without it, lazy-close-on-next-record is
// the only emission path.
func TestStream_TumblingCount_PunctuatorClosesIdleWindow(t *testing.T) {
	// Arrange
	b := embed.NewMemory()
	defer b.Close()
	for _, n := range []string{"input", "output"} {
		if err := b.CreateTopic(embed.TopicSpec{Name: n, PartitionCount: 1}); err != nil {
			t.Fatal(err)
		}
	}

	top, err := streams.New(b.Transport(),
		streams.WithPunctuationInterval(50*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	top.Stream("input").
		GroupByKey().
		TumblingCount(100*time.Millisecond, "windowed-counts").
		To("output")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := top.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer top.Stop()

	// Act: 2 records in one window, no further records — the
	// punctuator's wall-clock tick must close it.
	produce(t, b, "input", []struct{ Key, Value string }{{"x", ""}, {"x", ""}})

	// Assert: 1 windowed output appears within ~3s (window: 100ms,
	// tick: 50ms, plenty of time).
	got := collect(t, b, "output", 1, 3*time.Second)
	if len(got) != 1 {
		t.Fatalf("punctuator did not close idle window: got %d outputs", len(got))
	}
	if streams.DecodeCount(got[0].Value) != 2 {
		t.Errorf("count: got %d, want 2", streams.DecodeCount(got[0].Value))
	}
}

// TestTopology_IdleWatermarkAdvances proves WithIdleWatermark advances
// the watermark to wall-clock time when the input has been idle longer
// than the configured interval. Without idle-detection, the watermark
// stays pinned at the most recent record's event-time, and downstream
// time-driven operators (window close, join prune) stall on quiet
// streams.
func TestTopology_IdleWatermarkAdvances(t *testing.T) {
	// Arrange
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "input", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}

	top, err := streams.New(b.Transport(),
		streams.WithIdleWatermark(50*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	// A no-op pipeline; we only care about the watermark.
	top.Stream("input").Filter(func(_ proto.Record) bool { return true }).ForEach()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := top.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer top.Stop()

	// Act — wait past the idle interval without producing.
	time.Sleep(300 * time.Millisecond)

	// Assert — the idle goroutine must have advanced the watermark to
	// wall-clock-now, which is far in excess of the zero baseline.
	wm := top.Watermark()
	if wm == 0 {
		t.Fatal("watermark stuck at 0 — idle advance never fired")
	}
	if wm < time.Now().Add(-1*time.Second).UnixNano() {
		t.Errorf("watermark %d is far below wall-clock — idle advance not tracking real time", wm)
	}
}

// TestStream_HoppingCount: a single record at event-time t should land
// in size/advance overlapping windows. With size=300ms, advance=100ms,
// each record contributes to 3 windows.
func TestStream_HoppingCount(t *testing.T) {
	// Arrange
	b := embed.NewMemory()
	defer b.Close()
	for _, n := range []string{"input", "output"} {
		if err := b.CreateTopic(embed.TopicSpec{Name: n, PartitionCount: 1}); err != nil {
			t.Fatal(err)
		}
	}

	top, err := streams.New(b.Transport(),
		streams.WithPunctuationInterval(50*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	top.Stream("input").
		GroupByKey().
		HoppingCount(300*time.Millisecond, 100*time.Millisecond, "hops").
		To("output")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := top.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer top.Stop()

	// Act: 1 record. Should land in 3 hopping windows.
	produce(t, b, "input", []struct{ Key, Value string }{{"x", ""}})

	// Assert: at least 3 outputs (one per closed window). Punctuator
	// drives the close after window ends elapse.
	got := collect(t, b, "output", 3, 4*time.Second)
	if len(got) < 3 {
		t.Fatalf("hopping closed-window emissions: got %d, want >= 3", len(got))
	}
	for _, r := range got {
		if streams.DecodeCount(r.Value) != 1 {
			t.Errorf("count: got %d, want 1 (one record per window)", streams.DecodeCount(r.Value))
		}
	}
}

// TestStream_SessionCount: 2 records within gap form one session; a
// record beyond gap closes the session and opens a new one.
func TestStream_SessionCount(t *testing.T) {
	// Arrange
	b := embed.NewMemory()
	defer b.Close()
	for _, n := range []string{"input", "output"} {
		if err := b.CreateTopic(embed.TopicSpec{Name: n, PartitionCount: 1}); err != nil {
			t.Fatal(err)
		}
	}

	top, err := streams.New(b.Transport(),
		streams.WithPunctuationInterval(50*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	top.Stream("input").
		GroupByKey().
		SessionCount(100*time.Millisecond, "sessions").
		To("output")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := top.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer top.Stop()

	// Act: 2 records inside the gap, then sleep past the gap, then 1 record.
	produce(t, b, "input", []struct{ Key, Value string }{{"x", ""}, {"x", ""}})
	time.Sleep(300 * time.Millisecond)
	produce(t, b, "input", []struct{ Key, Value string }{{"x", ""}})

	// Assert: the first session (count=2) emitted on the third record
	// (gap exceeded). At least one output expected.
	got := collect(t, b, "output", 1, 3*time.Second)
	if len(got) == 0 {
		t.Fatalf("session emission missing")
	}
	if streams.DecodeCount(got[0].Value) != 2 {
		t.Errorf("first session count: got %d, want 2", streams.DecodeCount(got[0].Value))
	}
}

// TestStream_StreamStreamJoin: two source streams emit records with
// matching keys within the join window. Each match should produce one
// joined output record.
func TestStream_StreamStreamJoin(t *testing.T) {
	// Arrange
	b := embed.NewMemory()
	defer b.Close()
	for _, n := range []string{"clicks", "impressions", "joined"} {
		if err := b.CreateTopic(embed.TopicSpec{Name: n, PartitionCount: 1}); err != nil {
			t.Fatal(err)
		}
	}

	top, err := streams.New(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	left := top.Stream("clicks")
	right := top.Stream("impressions")
	left.Join(right, time.Second, func(l, r proto.Record) proto.Record {
		return proto.Record{
			Key:   l.Key,
			Value: append(append([]byte{}, l.Value...), append([]byte("|"), r.Value...)...),
		}
	}).To("joined")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := top.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer top.Stop()

	// Act: 2 records on each side, two with matching keys.
	produce(t, b, "clicks", []struct{ Key, Value string }{
		{"a", "click-a"},
		{"b", "click-b"},
	})
	produce(t, b, "impressions", []struct{ Key, Value string }{
		{"a", "imp-a"},
		{"b", "imp-b"},
	})

	// Assert: 2 joined records (a×a, b×b).
	got := collect(t, b, "joined", 2, 3*time.Second)
	if len(got) != 2 {
		t.Fatalf("got %d joined records, want 2", len(got))
	}
	for _, r := range got {
		if !bytes.Contains(r.Value, []byte("|")) {
			t.Errorf("joined record %q lacks separator", r.Value)
		}
	}
}

// TestStream_JoinTable proves a stream record looks up its key in a
// KTable and emits the joined output. Tombstones in the table delete
// the entry — a follow-up stream record for the same key sees a miss.
func TestStream_JoinTable(t *testing.T) {
	// Arrange
	b := embed.NewMemory()
	defer b.Close()
	for _, n := range []string{"clicks", "users", "joined"} {
		if err := b.CreateTopic(embed.TopicSpec{Name: n, PartitionCount: 1}); err != nil {
			t.Fatal(err)
		}
	}

	top, err := streams.New(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	users := top.Table("users", "users-store")
	clicks := top.Stream("clicks")
	clicks.JoinTable(users, func(stream proto.Record, tableValue []byte, hit bool) []proto.Record {
		if !hit {
			return nil
		}
		return []proto.Record{{
			Key: stream.Key,
			Value: append(append([]byte{}, stream.Value...),
				append([]byte("|"), tableValue...)...),
		}}
	}).To("joined")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := top.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer top.Stop()

	// Seed the users table.
	produce(t, b, "users", []struct{ Key, Value string }{
		{"alice", "ALICE-PROFILE"},
		{"bob", "BOB-PROFILE"},
	})
	// Wait briefly for the KTable consumer to absorb both updates.
	time.Sleep(300 * time.Millisecond)

	// Act — emit clicks; alice/bob should join, charlie should miss.
	produce(t, b, "clicks", []struct{ Key, Value string }{
		{"alice", "click-1"},
		{"charlie", "click-2"},
		{"bob", "click-3"},
	})

	// Assert
	got := collect(t, b, "joined", 2, 3*time.Second)
	if len(got) != 2 {
		t.Fatalf("got %d joined records, want 2 (charlie should miss)", len(got))
	}
	for _, r := range got {
		if !bytes.Contains(r.Value, []byte("|")) {
			t.Errorf("joined record %q lacks separator", r.Value)
		}
	}
}

// TestStream_Sum_AggregatesNumericValues proves Sum sums valueFn(r)
// per key into the named state store. State is encoded as 8-byte
// big-endian int64 — readable via streams.DecodeCount which the
// existing Count operator already uses.
func TestStream_Sum_AggregatesNumericValues(t *testing.T) {
	// Arrange — values encoded as ASCII digits ("1", "2", "3").
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "input", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	if err := b.CreateTopic(embed.TopicSpec{Name: "output", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	top, err := streams.New(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	top.Stream("input").
		GroupByKey().
		Sum("totals", func(r proto.Record) int64 {
			n := int64(0)
			for _, c := range r.Value {
				n = n*10 + int64(c-'0')
			}
			return n
		}).
		To("output")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := top.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer top.Stop()

	// Act — produce a=1, a=2, b=3, a=4 → totals: a=7, b=3.
	produce(t, b, "input", []struct{ Key, Value string }{
		{"a", "1"}, {"a", "2"}, {"b", "3"}, {"a", "4"},
	})

	// Wait for output to drain.
	_ = collect(t, b, "output", 4, 3*time.Second)

	// Assert — store reflects the per-key sums.
	store := top.Store("totals")
	if v, ok := store.Get([]byte("a")); !ok {
		t.Errorf("a: not in store")
	} else if streams.DecodeCount(v) != 7 {
		t.Errorf("a sum: got %d, want 7", streams.DecodeCount(v))
	}
	if v, ok := store.Get([]byte("b")); !ok {
		t.Errorf("b: not in store")
	} else if streams.DecodeCount(v) != 3 {
		t.Errorf("b sum: got %d, want 3", streams.DecodeCount(v))
	}
}

// TestTopology_WithDLQ_CapturesPanickedRecord proves WithDLQ
// routes records that panicked an op to a DLQ topic so the
// failing payload survives for forensics. Pairs with
// WithErrorHandler — handler fires for live observability, DLQ
// captures the record itself for replay/investigation.
//
// The DLQ record carries the original record's value plus a
// header `holocron.dlq.error` describing the panic.
func TestTopology_WithDLQ_CapturesPanickedRecord(t *testing.T) {
	// Arrange
	b := embed.NewMemory()
	defer b.Close()
	for _, name := range []string{"input", "output", "dlq"} {
		if err := b.CreateTopic(embed.TopicSpec{Name: name, PartitionCount: 1}); err != nil {
			t.Fatal(err)
		}
	}
	top, err := streams.New(b.Transport(),
		streams.WithDLQ("dlq"),
	)
	if err != nil {
		t.Fatal(err)
	}
	top.Stream("input").Map(func(r proto.Record) proto.Record {
		if string(r.Value) == "BOOM" {
			panic("simulated op failure")
		}
		return r
	}).To("output")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := top.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer top.Stop()

	produce(t, b, "input", []struct{ Key, Value string }{
		{"", "v1"}, {"", "BOOM"}, {"", "v2"},
	})

	// Output gets v1, v2; DLQ gets BOOM.
	out := collect(t, b, "output", 2, 3*time.Second)
	if len(out) != 2 {
		t.Fatalf("output: got %d, want 2", len(out))
	}
	dlq := collect(t, b, "dlq", 1, 3*time.Second)
	if len(dlq) != 1 {
		t.Fatalf("dlq: got %d, want 1", len(dlq))
	}
	if string(dlq[0].Value) != "BOOM" {
		t.Errorf("dlq value: got %q, want BOOM", dlq[0].Value)
	}
	// Header carries the panic message.
	var sawErrorHeader bool
	for _, h := range dlq[0].Headers {
		if h.Key == "holocron.dlq.error" {
			sawErrorHeader = true
			if !bytes.Contains(h.Value, []byte("simulated op failure")) {
				t.Errorf("dlq error header: got %q, want substring 'simulated op failure'", h.Value)
			}
		}
	}
	if !sawErrorHeader {
		t.Errorf("dlq record missing holocron.dlq.error header (headers=%v)", dlq[0].Headers)
	}
}

// TestTopology_WithErrorHandler_RecoversFromOpPanic proves a
// topology configured with WithErrorHandler catches op panics and
// fires the handler with the recovered error, then keeps the
// pipeline running so subsequent records pass through. Without
// this, an op panic kills the goroutine and silently drops every
// downstream record until Stop surfaces the failure.
func TestTopology_WithErrorHandler_RecoversFromOpPanic(t *testing.T) {
	// Arrange — handler captures every error.
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "input", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	if err := b.CreateTopic(embed.TopicSpec{Name: "output", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	var (
		errMu sync.Mutex
		errs  []error
	)
	top, err := streams.New(b.Transport(),
		streams.WithErrorHandler(func(e error) {
			errMu.Lock()
			defer errMu.Unlock()
			errs = append(errs, e)
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	// Op panics when value == "BOOM"; otherwise passes through.
	top.Stream("input").Map(func(r proto.Record) proto.Record {
		if string(r.Value) == "BOOM" {
			panic("simulated op failure")
		}
		return r
	}).To("output")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := top.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer top.Stop()

	// Act — produce v1, BOOM, v2. v1 and v2 should reach the
	// output; BOOM panics inside the op, recover fires the handler,
	// loop continues.
	produce(t, b, "input", []struct{ Key, Value string }{
		{"", "v1"}, {"", "BOOM"}, {"", "v2"},
	})

	// Assert — output has 2 records; handler observed 1 error.
	got := collect(t, b, "output", 2, 3*time.Second)
	if len(got) != 2 {
		t.Fatalf("output: got %d records, want 2 (post-panic pipeline kept running?)", len(got))
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		errMu.Lock()
		n := len(errs)
		errMu.Unlock()
		if n >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	errMu.Lock()
	defer errMu.Unlock()
	if len(errs) == 0 {
		t.Fatal("WithErrorHandler: no error fired (panic not caught)")
	}
}

// TestStream_Tap_FiresWithoutModification proves Tap is an alias
// for Peek matching the Kafka-Streams convention. The callback
// observes each record; the original record continues downstream
// unchanged.
func TestStream_Tap_FiresWithoutModification(t *testing.T) {
	// Arrange
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "input", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	if err := b.CreateTopic(embed.TopicSpec{Name: "output", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	top, err := streams.New(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	var (
		mu  sync.Mutex
		got []string
	)
	top.Stream("input").Tap(func(r proto.Record) {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, string(r.Value))
	}).To("output")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := top.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer top.Stop()

	produce(t, b, "input", []struct{ Key, Value string }{
		{"a", "v1"}, {"b", "v2"},
	})

	// Assert — both records reach the output (Tap doesn't drop) AND
	// the callback collected both.
	out := collect(t, b, "output", 2, 3*time.Second)
	if len(out) != 2 {
		t.Fatalf("output: got %d records, want 2", len(out))
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 2 {
		t.Errorf("Tap callback fired %d times, want 2", len(got))
	}
}

// TestStream_Throttle_CapsBurst proves Throttle(rate) drops records
// that arrive faster than the configured rate per second, using a
// token-bucket algorithm with capacity = rate. A burst of records
// within microseconds picks up at most `rate` tokens (bucket
// capacity); the rest are dropped.
func TestStream_Throttle_CapsBurst(t *testing.T) {
	// Arrange — Throttle(2.0): bucket capacity = 2 tokens.
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "input", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	if err := b.CreateTopic(embed.TopicSpec{Name: "output", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	top, err := streams.New(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	top.Stream("input").Throttle(2.0).To("output")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := top.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer top.Stop()

	// Act — produce 10 records as fast as possible. With rate=2/s
	// and capacity=2, the bucket starts full and the burst drains
	// it almost instantly — at most ~2 records (plus a sliver of
	// refill during the burst) make it through within the test's
	// quiescence window.
	records := make([]struct{ Key, Value string }, 10)
	for i := range records {
		records[i] = struct{ Key, Value string }{"", fmt.Sprintf("v%d", i)}
	}
	produce(t, b, "input", records)

	// Wait briefly for emission to settle, then collect with a
	// short timeout so we capture only the throttled subset, not
	// the slow refill stream.
	got := collect(t, b, "output", 10, 200*time.Millisecond)
	// Assert — at most 3 records (capacity 2 + a sliver of refill
	// during the burst). With rate=2/s and a sub-200ms burst, the
	// refill contributes <1 token, so we'd expect 2; allow 3 for
	// jitter without making the test flaky.
	if len(got) == 0 || len(got) > 3 {
		t.Fatalf("output: got %d records, want 1..3 (Throttle(2/s) on 10-record burst)", len(got))
	}
}

// TestStream_Sample_PassesEveryNth proves Sample(every) emits the
// 1st record and every Nth thereafter, dropping the rest. Useful
// for downsampling high-volume streams when the operator wants a
// representative subset rather than a windowed aggregate.
func TestStream_Sample_PassesEveryNth(t *testing.T) {
	// Arrange
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "input", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	if err := b.CreateTopic(embed.TopicSpec{Name: "output", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	top, err := streams.New(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	top.Stream("input").Sample(3).To("output")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := top.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer top.Stop()

	// Act — produce 9 records; Sample(3) should emit 1st, 4th, 7th.
	produce(t, b, "input", []struct{ Key, Value string }{
		{"", "v1"}, {"", "v2"}, {"", "v3"},
		{"", "v4"}, {"", "v5"}, {"", "v6"},
		{"", "v7"}, {"", "v8"}, {"", "v9"},
	})

	// Assert — three records on the output (positions 1, 4, 7).
	got := collect(t, b, "output", 3, 3*time.Second)
	if len(got) != 3 {
		t.Fatalf("output: got %d records, want 3 (Sample(3) of 9 → 3)", len(got))
	}
	wantValues := []string{"v1", "v4", "v7"}
	for i, w := range wantValues {
		if string(got[i].Value) != w {
			t.Errorf("record %d: got %q, want %q", i, got[i].Value, w)
		}
	}
}

// TestStream_Distinct_DropsRepeatedKeys proves Distinct(keyFn)
// drops records whose derived key has been seen before in this
// pipeline run. In-memory dedup via sync.Map; the operator is
// responsible for bounding cardinality (high-cardinality keys
// will eventually grow the map).
func TestStream_Distinct_DropsRepeatedKeys(t *testing.T) {
	// Arrange
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "input", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	if err := b.CreateTopic(embed.TopicSpec{Name: "output", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	top, err := streams.New(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	// Dedupe on the record's key.
	top.Stream("input").Distinct(func(r proto.Record) []byte {
		return r.Key
	}).To("output")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := top.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer top.Stop()

	// Act — keys: a, b, a, c, b → distinct emits: a, b, c.
	produce(t, b, "input", []struct{ Key, Value string }{
		{"a", "v1"}, {"b", "v2"}, {"a", "v3"}, {"c", "v4"}, {"b", "v5"},
	})

	// Assert — only first occurrence of each key passes through.
	got := collect(t, b, "output", 3, 3*time.Second)
	if len(got) != 3 {
		t.Fatalf("output: got %d records, want 3 (distinct keys)", len(got))
	}
	keys := make([]string, 0, 3)
	for _, r := range got {
		keys = append(keys, string(r.Key))
	}
	want := []string{"a", "b", "c"}
	for i, k := range want {
		if keys[i] != k {
			t.Errorf("record %d key: got %q, want %q (full=%v)", i, keys[i], k, keys)
		}
	}
}

// TestStream_Skip_DropsPrefix proves Skip(n) drops the first n
// records and passes through everything after. Inverse of Take.
// Shared atomic counter across multi-task pipelines so the
// dropped-prefix is global, not per-task.
func TestStream_Skip_DropsPrefix(t *testing.T) {
	// Arrange
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "input", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	if err := b.CreateTopic(embed.TopicSpec{Name: "output", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	top, err := streams.New(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	top.Stream("input").Skip(2).To("output")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := top.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer top.Stop()

	// Act — produce 5 records.
	produce(t, b, "input", []struct{ Key, Value string }{
		{"a", "v1"}, {"b", "v2"}, {"c", "v3"}, {"d", "v4"}, {"e", "v5"},
	})

	// Assert — only the last 3 reach the output.
	got := collect(t, b, "output", 3, 3*time.Second)
	if len(got) != 3 {
		t.Fatalf("output: got %d records, want 3 (Skip dropped 2)", len(got))
	}
	if string(got[0].Value) != "v3" {
		t.Errorf("first surviving record: got %q, want v3", got[0].Value)
	}
}

// TestStream_Take_BoundsOutput proves Take(n) caps emission at n
// records — the n+1st onward are dropped. Useful for bounded test
// pipelines and replay scenarios that should stop after a prefix
// of the source.
//
// The pipeline keeps consuming from the source (so committed
// offsets advance), but produces no output past the cap. With
// multi-task pipelines the counter is shared so the cap is global,
// not per-task.
func TestStream_Take_BoundsOutput(t *testing.T) {
	// Arrange
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "input", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	if err := b.CreateTopic(embed.TopicSpec{Name: "output", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	top, err := streams.New(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	top.Stream("input").Take(2).To("output")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := top.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer top.Stop()

	// Act — produce 5 records.
	produce(t, b, "input", []struct{ Key, Value string }{
		{"a", "v1"}, {"b", "v2"}, {"c", "v3"}, {"d", "v4"}, {"e", "v5"},
	})

	// Assert — only the first 2 reach the output.
	got := collect(t, b, "output", 2, 3*time.Second)
	if len(got) != 2 {
		t.Fatalf("output: got %d records, want 2 (Take caps at 2)", len(got))
	}
	// Wait briefly to confirm no more arrive.
	extra := collect(t, b, "output", 3, 500*time.Millisecond)
	if len(extra) != 2 {
		t.Errorf("late output: got %d, want still 2 (Take must drop past cap)", len(extra))
	}
}

// TestStream_PrintTo_WritesPerRecord proves PrintTo registers a
// terminal that writes one line per record to the supplied
// io.Writer with the prefix tag. Sugar over ForEachFunc — debug a
// pipeline by inserting `.PrintTo(buf, "out")` instead of writing
// the format string each time.
func TestStream_PrintTo_WritesPerRecord(t *testing.T) {
	// Arrange
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "input", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	top, err := streams.New(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	var buf threadSafeBuffer
	top.Stream("input").PrintTo(&buf, "DBG")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := top.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer top.Stop()

	// Act
	produce(t, b, "input", []struct{ Key, Value string }{
		{"a", "v1"}, {"b", "v2"},
	})
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Count(buf.String(), "\n") >= 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Assert — both records show with the prefix.
	out := buf.String()
	if !strings.Contains(out, "DBG") {
		t.Errorf("output missing prefix: %q", out)
	}
	if strings.Count(out, "\n") < 2 {
		t.Errorf("output line count: got %d, want >= 2 (output=%q)", strings.Count(out, "\n"), out)
	}
	if !strings.Contains(out, "v1") || !strings.Contains(out, "v2") {
		t.Errorf("output missing values: %q", out)
	}
}

// threadSafeBuffer is a bytes.Buffer with a mutex so PrintTo can
// write from the pipeline goroutine while the test reads from the
// main goroutine without a race.
type threadSafeBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *threadSafeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *threadSafeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestStream_ForEachFunc_FiresPerRecord proves ForEachFunc registers
// a terminal that invokes the callback per record. Closes the gap
// where ForEach() (passive, state-store-only) was the only terminal
// for non-topic sinks: an external system that consumes the stream
// (logger, metrics emitter, custom sink) had to wrap a Peek upstream
// of an empty ForEach. ForEachFunc collapses that into one call.
func TestStream_ForEachFunc_FiresPerRecord(t *testing.T) {
	// Arrange
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "input", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	top, err := streams.New(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	var (
		mu  sync.Mutex
		got []string
	)
	top.Stream("input").ForEachFunc(func(r proto.Record) {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, string(r.Value))
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := top.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer top.Stop()

	// Act
	produce(t, b, "input", []struct{ Key, Value string }{
		{"a", "v1"}, {"b", "v2"}, {"c", "v3"},
	})

	// Wait for the callback to land all three records.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(got)
		mu.Unlock()
		if n >= 3 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Assert
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 3 {
		t.Fatalf("ForEachFunc fires: got %d, want 3 (got=%v)", len(got), got)
	}
	want := map[string]bool{"v1": true, "v2": true, "v3": true}
	for _, v := range got {
		if !want[v] {
			t.Errorf("unexpected value %q", v)
		}
	}
}

// TestStream_ToTable_MaterializesFilteredStream proves a Stream's
// ToTable terminal materializes the upstream pipeline into a KTable —
// last-value-wins per key, tombstones delete. The materialized view
// reflects only what the pipeline emits, not the raw source: a Filter
// upstream of ToTable is the natural way to derive a table-of-interesting-
// records without a separate intermediate topic.
//
// Without ToTable the only way to materialize a transformed stream
// would be `s.Through(topic); top.Table(topic, store)` — two topics
// where one suffices.
func TestStream_ToTable_MaterializesFilteredStream(t *testing.T) {
	// Arrange
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "input", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	top, err := streams.New(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	// Filter retains only "keep" records, then materializes.
	kept := top.Stream("input").
		Filter(func(r proto.Record) bool { return string(r.Value) != "skip" }).
		ToTable("kept-store")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := top.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer top.Stop()

	// Act — produce a mix of kept and dropped records, plus an
	// overwrite (a=v3 replaces a=v1) and a tombstone (b's nil-value
	// record deletes the entry from the materialized view).
	produce(t, b, "input", []struct{ Key, Value string }{
		{"a", "v1"},
		{"b", "v2"},
		{"a", "skip"}, // dropped by Filter
		{"a", "v3"},   // overwrites a=v1
		{"c", "v4"},
	})
	// Wait for the pipeline to absorb all five records.
	time.Sleep(300 * time.Millisecond)

	// Tombstone b — nil Value (not empty) is the delete-the-key signal.
	// produce() converts string→[]byte which yields a non-nil empty
	// slice, so use the SDK directly for the tombstone.
	prod, err := sdk.NewProducer(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	if _, err := prod.Send(ctx, "input", proto.Record{Key: []byte("b"), Value: nil}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)

	// Assert — a = "v3" (latest), b = absent (tombstoned), c = "v4".
	if v, ok := kept.Get([]byte("a")); !ok || string(v) != "v3" {
		t.Errorf("a: got (%q, %v), want (v3, true)", v, ok)
	}
	if _, ok := kept.Get([]byte("b")); ok {
		t.Errorf("b: got present, want tombstoned")
	}
	if v, ok := kept.Get([]byte("c")); !ok || string(v) != "v4" {
		t.Errorf("c: got (%q, %v), want (v4, true)", v, ok)
	}
}

// produceTimestamped produces records with the given event-times so a
// test can simulate out-of-order arrival deterministically.
func produceTimestamped(t *testing.T, b *embed.Broker, topic string, recs []proto.Record) {
	t.Helper()
	p, err := sdk.NewProducer(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for _, r := range recs {
		if _, err := p.Send(ctx, topic, r); err != nil {
			t.Fatal(err)
		}
	}
}

// TestStream_FilterNot_InvertsPredicate proves FilterNot is the
// negation of Filter — records where the predicate returns true
// are dropped, the rest pass through.
func TestStream_FilterNot_InvertsPredicate(t *testing.T) {
	// Arrange
	b := embed.NewMemory()
	defer b.Close()
	for _, n := range []string{"input", "output"} {
		if err := b.CreateTopic(embed.TopicSpec{Name: n, PartitionCount: 1}); err != nil {
			t.Fatal(err)
		}
	}

	top, err := streams.New(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	top.Stream("input").
		FilterNot(func(r proto.Record) bool {
			return string(r.Value) == "skip"
		}).
		To("output")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := top.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer top.Stop()

	// Act
	produce(t, b, "input", []struct{ Key, Value string }{
		{"k", "keep1"}, {"k", "skip"}, {"k", "keep2"},
	})

	// Assert — only the two non-"skip" records flow through.
	got := collect(t, b, "output", 2, 3*time.Second)
	if len(got) != 2 {
		t.Fatalf("got %d records, want 2", len(got))
	}
	for _, r := range got {
		if string(r.Value) == "skip" {
			t.Errorf("FilterNot let through a record it should have dropped: %q", r.Value)
		}
	}
}

// TestStream_GroupBy_AggregatesByDerivedKey proves GroupBy
// derives the aggregation key from a function applied to each
// record, then Count tallies under that key. Different records
// that hash to the same derived key share one count slot.
func TestStream_GroupBy_AggregatesByDerivedKey(t *testing.T) {
	// Arrange
	b := embed.NewMemory()
	defer b.Close()
	for _, n := range []string{"input", "output"} {
		if err := b.CreateTopic(embed.TopicSpec{Name: n, PartitionCount: 1}); err != nil {
			t.Fatal(err)
		}
	}

	top, err := streams.New(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	// Group by first byte of the value — so "apple"/"avocado"/"ant"
	// all roll up under "a".
	top.Stream("input").
		GroupBy(func(r proto.Record) []byte {
			if len(r.Value) > 0 {
				return r.Value[:1]
			}
			return r.Value
		}).
		Count("first-letter").
		To("output")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := top.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer top.Stop()

	// Act
	produce(t, b, "input", []struct{ Key, Value string }{
		{"k", "apple"}, {"k", "avocado"}, {"k", "ant"}, {"k", "banana"},
	})

	// Assert — final state: a=3, b=1.
	got := collect(t, b, "output", 4, 3*time.Second)
	if len(got) != 4 {
		t.Fatalf("got %d records, want 4", len(got))
	}
	store := top.Store("first-letter")
	a, _ := store.Get([]byte("a"))
	bv, _ := store.Get([]byte("b"))
	if streams.DecodeCount(a) != 3 {
		t.Errorf("count(a): got %d, want 3", streams.DecodeCount(a))
	}
	if streams.DecodeCount(bv) != 1 {
		t.Errorf("count(b): got %d, want 1", streams.DecodeCount(bv))
	}
}

// TestStream_SelectKey_ReKeysRecords proves SelectKey replaces
// each record's key with the function's return while preserving
// value and headers.
func TestStream_SelectKey_ReKeysRecords(t *testing.T) {
	// Arrange
	b := embed.NewMemory()
	defer b.Close()
	for _, n := range []string{"input", "output"} {
		if err := b.CreateTopic(embed.TopicSpec{Name: n, PartitionCount: 1}); err != nil {
			t.Fatal(err)
		}
	}

	top, err := streams.New(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	top.Stream("input").
		SelectKey(func(r proto.Record) []byte {
			return append([]byte("k:"), r.Value...)
		}).
		To("output")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := top.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer top.Stop()

	// Act — produce with one key, observe re-keyed output.
	produce(t, b, "input", []struct{ Key, Value string }{{"orig", "abc"}})

	// Assert — output's key is derived from the value.
	got := collect(t, b, "output", 1, 3*time.Second)
	if len(got) != 1 {
		t.Fatalf("got %d records, want 1", len(got))
	}
	if string(got[0].Key) != "k:abc" {
		t.Errorf("key: got %q, want \"k:abc\"", got[0].Key)
	}
	if string(got[0].Value) != "abc" {
		t.Errorf("value: got %q, want \"abc\" (preserved)", got[0].Value)
	}
}

// TestStream_MapKeyValue_TransformsBoth proves MapKeyValue
// updates key and value in one step while preserving headers.
func TestStream_MapKeyValue_TransformsBoth(t *testing.T) {
	// Arrange
	b := embed.NewMemory()
	defer b.Close()
	for _, n := range []string{"input", "output"} {
		if err := b.CreateTopic(embed.TopicSpec{Name: n, PartitionCount: 1}); err != nil {
			t.Fatal(err)
		}
	}

	top, err := streams.New(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	top.Stream("input").
		MapKeyValue(func(k, v []byte) ([]byte, []byte) {
			return append([]byte("k:"), k...), bytes.ToUpper(v)
		}).
		To("output")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := top.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer top.Stop()

	// Act
	produce(t, b, "input", []struct{ Key, Value string }{{"orig", "abc"}})

	// Assert
	got := collect(t, b, "output", 1, 3*time.Second)
	if len(got) != 1 {
		t.Fatalf("got %d records, want 1", len(got))
	}
	if string(got[0].Key) != "k:orig" {
		t.Errorf("key: got %q, want \"k:orig\"", got[0].Key)
	}
	if string(got[0].Value) != "ABC" {
		t.Errorf("value: got %q, want \"ABC\"", got[0].Value)
	}
}

// TestStream_MapValues_PreservesKey proves MapValues transforms
// only the value; key and headers pass through untouched.
func TestStream_MapValues_PreservesKey(t *testing.T) {
	// Arrange
	b := embed.NewMemory()
	defer b.Close()
	for _, n := range []string{"input", "output"} {
		if err := b.CreateTopic(embed.TopicSpec{Name: n, PartitionCount: 1}); err != nil {
			t.Fatal(err)
		}
	}

	top, err := streams.New(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	top.Stream("input").
		MapValues(func(v []byte) []byte {
			return bytes.ToUpper(v)
		}).
		To("output")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := top.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer top.Stop()

	// Act
	produce(t, b, "input", []struct{ Key, Value string }{{"k", "abc"}})

	// Assert
	got := collect(t, b, "output", 1, 3*time.Second)
	if len(got) != 1 {
		t.Fatalf("got %d records, want 1", len(got))
	}
	if string(got[0].Key) != "k" {
		t.Errorf("key: got %q, want \"k\" (preserved)", got[0].Key)
	}
	if string(got[0].Value) != "ABC" {
		t.Errorf("value: got %q, want \"ABC\"", got[0].Value)
	}
}

// TestStream_Peek_FiresWithoutModification proves Peek's callback
// runs for every record while the stream content passes through
// unchanged.
func TestStream_Peek_FiresWithoutModification(t *testing.T) {
	// Arrange
	b := embed.NewMemory()
	defer b.Close()
	for _, n := range []string{"input", "output"} {
		if err := b.CreateTopic(embed.TopicSpec{Name: n, PartitionCount: 1}); err != nil {
			t.Fatal(err)
		}
	}

	top, err := streams.New(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	var (
		mu     sync.Mutex
		seen   []string
		expect = []string{"v1", "v2", "v3"}
	)
	top.Stream("input").
		Peek(func(r proto.Record) {
			mu.Lock()
			seen = append(seen, string(r.Value))
			mu.Unlock()
		}).
		To("output")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := top.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer top.Stop()

	// Act
	produce(t, b, "input", []struct{ Key, Value string }{
		{"k", "v1"}, {"k", "v2"}, {"k", "v3"},
	})

	// Assert — output got every record unchanged.
	got := collect(t, b, "output", 3, 3*time.Second)
	if len(got) != 3 {
		t.Fatalf("got %d records, want 3", len(got))
	}
	for i, r := range got {
		if string(r.Value) != expect[i] {
			t.Errorf("output[%d]: got %q, want %q", i, r.Value, expect[i])
		}
	}
	// Peek saw the same records.
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 3 {
		t.Fatalf("Peek saw %d records, want 3", len(seen))
	}
}

// TestStream_Reduce_CombinesValuesPerKey proves Reduce folds
// per-key values through the supplied associative function. The
// first record establishes the accumulator; subsequent records'
// values are combined into it. Final state matches the
// concatenation of all values for each key.
func TestStream_Reduce_CombinesValuesPerKey(t *testing.T) {
	// Arrange
	b := embed.NewMemory()
	defer b.Close()
	for _, n := range []string{"input", "output"} {
		if err := b.CreateTopic(embed.TopicSpec{Name: n, PartitionCount: 1}); err != nil {
			t.Fatal(err)
		}
	}

	top, err := streams.New(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	top.Stream("input").
		GroupByKey().
		Reduce("concat", func(accum, v []byte) []byte {
			out := make([]byte, 0, len(accum)+1+len(v))
			out = append(out, accum...)
			out = append(out, '|')
			return append(out, v...)
		}).
		To("output")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := top.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer top.Stop()

	// Act — three records: a/b/a. The first "a" establishes the
	// accumulator (no separator); the second "a" appends.
	produce(t, b, "input", []struct{ Key, Value string }{
		{"a", "1"}, {"b", "x"}, {"a", "2"},
	})

	// Assert — final state for "a" is "1|2"; "b" is "x".
	got := collect(t, b, "output", 3, 3*time.Second)
	if len(got) != 3 {
		t.Fatalf("got %d outputs, want 3", len(got))
	}
	store := top.Store("concat")
	a, _ := store.Get([]byte("a"))
	bVal, _ := store.Get([]byte("b"))
	if string(a) != "1|2" {
		t.Errorf("key a: got %q, want \"1|2\"", a)
	}
	if string(bVal) != "x" {
		t.Errorf("key b: got %q, want \"x\"", bVal)
	}
}

// TestStream_Through_PersistsIntermediateTopic proves a record
// produced through an intermediate topic surfaces at the final
// sink with the upstream transform applied AND lands in the
// intermediate topic for inspection. Sugar-equivalent to
// `s.To(mid); topology.Stream(mid).chain.To(out)` but as one
// fluent pipeline.
func TestStream_Through_PersistsIntermediateTopic(t *testing.T) {
	// Arrange
	b := embed.NewMemory()
	defer b.Close()
	for _, n := range []string{"input", "intermediate", "output"} {
		if err := b.CreateTopic(embed.TopicSpec{Name: n, PartitionCount: 1}); err != nil {
			t.Fatal(err)
		}
	}

	top, err := streams.New(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	top.Stream("input").
		Map(func(r proto.Record) proto.Record {
			return proto.Record{Key: r.Key, Value: append([]byte("up:"), r.Value...)}
		}).
		Through("intermediate").
		Map(func(r proto.Record) proto.Record {
			return proto.Record{Key: r.Key, Value: append([]byte("dn:"), r.Value...)}
		}).
		To("output")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := top.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer top.Stop()

	// Act
	produce(t, b, "input", []struct{ Key, Value string }{{"k", "x"}})

	// Assert — intermediate has the upstream-only transform.
	mid := collect(t, b, "intermediate", 1, 3*time.Second)
	if len(mid) != 1 || string(mid[0].Value) != "up:x" {
		t.Fatalf("intermediate: got %q, want \"up:x\"", mid[0].Value)
	}
	// Output has both transforms applied.
	out := collect(t, b, "output", 1, 3*time.Second)
	if len(out) != 1 || string(out[0].Value) != "dn:up:x" {
		t.Fatalf("output: got %q, want \"dn:up:x\"", out[0].Value)
	}
}

// TestStream_Branch_RoutesByPredicate proves Stream.Branch fans
// out a source pipeline into multiple sinks based on per-branch
// predicates: each input record routes to the FIRST matching
// branch, and records that match none are dropped.
func TestStream_Branch_RoutesByPredicate(t *testing.T) {
	// Arrange
	b := embed.NewMemory()
	defer b.Close()
	for _, n := range []string{"input", "alphas", "bravos"} {
		if err := b.CreateTopic(embed.TopicSpec{Name: n, PartitionCount: 1}); err != nil {
			t.Fatal(err)
		}
	}

	top, err := streams.New(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	branches := top.Stream("input").Branch(
		func(r proto.Record) bool { return string(r.Value) == "a" },
		func(r proto.Record) bool { return string(r.Value) == "b" },
	)
	branches[0].To("alphas")
	branches[1].To("bravos")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := top.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer top.Stop()

	// Act — produce a mix of a, b, and c (c matches no branch).
	produce(t, b, "input", []struct{ Key, Value string }{
		{"k", "a"}, {"k", "b"}, {"k", "c"}, {"k", "a"}, {"k", "b"},
	})

	// Assert — alphas got 2 records, bravos got 2 records, and
	// "c" was silently dropped from both.
	gotAlpha := collect(t, b, "alphas", 2, 3*time.Second)
	gotBravo := collect(t, b, "bravos", 2, 3*time.Second)
	if len(gotAlpha) != 2 {
		t.Errorf("alphas: got %d records, want 2", len(gotAlpha))
	}
	for _, r := range gotAlpha {
		if string(r.Value) != "a" {
			t.Errorf("alphas saw non-a: %q", r.Value)
		}
	}
	if len(gotBravo) != 2 {
		t.Errorf("bravos: got %d records, want 2", len(gotBravo))
	}
	for _, r := range gotBravo {
		if string(r.Value) != "b" {
			t.Errorf("bravos saw non-b: %q", r.Value)
		}
	}
}

// TestStream_LeftJoin_DeferredEmitNoDuplicate proves the
// window-close-and-emit fix from batch 31: a left record arrives
// without a buffered right counterpart, then a matching right
// arrives within the window. Pre-batch-31 the left would surface
// twice — once as no-match at arrival, once as a matched pair
// when the right paired up. The fix defers the no-match emission
// until the window closes; the right's arrival removes the left
// from pending, so only the matched pair fires.
func TestStream_LeftJoin_DeferredEmitNoDuplicate(t *testing.T) {
	// Arrange
	b := embed.NewMemory()
	defer b.Close()
	for _, n := range []string{"clicks", "impressions", "joined"} {
		if err := b.CreateTopic(embed.TopicSpec{Name: n, PartitionCount: 1}); err != nil {
			t.Fatal(err)
		}
	}

	top, err := streams.New(b.Transport(),
		streams.WithIdleWatermark(50*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	left := top.Stream("clicks")
	right := top.Stream("impressions")
	left.LeftJoin(right, 500*time.Millisecond, func(l proto.Record, r *proto.Record) proto.Record {
		out := proto.Record{Key: l.Key}
		if r == nil {
			out.Value = []byte("nomatch:" + string(l.Value))
		} else {
			out.Value = []byte("match:" + string(l.Value) + "|" + string(r.Value))
		}
		return out
	}).To("joined")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := top.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer top.Stop()

	// Act — left first, right shortly after (within 500ms window).
	produce(t, b, "clicks", []struct{ Key, Value string }{{"k", "L1"}})
	time.Sleep(50 * time.Millisecond)
	produce(t, b, "impressions", []struct{ Key, Value string }{{"k", "R1"}})

	// Wait long enough that any deferred no-match would have
	// fired (window=500ms, punctuator=100ms).
	time.Sleep(800 * time.Millisecond)

	// Assert — exactly one record on the joined topic, the
	// matched pair. With the V1 (pre-fix) emit-on-arrival
	// semantic, a "nomatch:L1" would also have landed.
	c, err := sdk.NewConsumer(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	subCtx, subCancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer subCancel()
	if err := c.Subscribe(subCtx, "joined", 0); err != nil {
		t.Fatal(err)
	}
	got, err := c.Poll(subCtx, 10)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if len(got) != 1 {
		values := make([]string, len(got))
		for i, r := range got {
			values[i] = string(r.Value)
		}
		t.Fatalf("got %d outputs, want 1 (records: %v)", len(got), values)
	}
	if string(got[0].Value) != "match:L1|R1" {
		t.Errorf("output: got %q, want \"match:L1|R1\"", got[0].Value)
	}
}

// TestStream_OuterJoin_LeftNoMatchEmits proves a left record
// without a matching right surfaces as a no-match output. Same
// shape as the LeftJoin test, but exercises the OuterJoin path.
func TestStream_OuterJoin_LeftNoMatchEmits(t *testing.T) {
	// Arrange
	b := embed.NewMemory()
	defer b.Close()
	for _, n := range []string{"clicks", "impressions", "joined"} {
		if err := b.CreateTopic(embed.TopicSpec{Name: n, PartitionCount: 1}); err != nil {
			t.Fatal(err)
		}
	}

	top, err := streams.New(b.Transport(),
		streams.WithIdleWatermark(50*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	left := top.Stream("clicks")
	right := top.Stream("impressions")
	left.OuterJoin(right, 200*time.Millisecond, func(l, r *proto.Record) proto.Record {
		switch {
		case l != nil && r != nil:
			return proto.Record{Key: l.Key, Value: []byte("match:" + string(l.Value) + "|" + string(r.Value))}
		case l != nil:
			return proto.Record{Key: l.Key, Value: []byte("left-only:" + string(l.Value))}
		default:
			return proto.Record{Key: r.Key, Value: []byte("right-only:" + string(r.Value))}
		}
	}).To("joined")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := top.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer top.Stop()

	// Act — left only, no right ever. The idle-watermark goroutine
	// will advance the watermark past left's eventTime + window, at
	// which point the join punctuator flushes the no-match output.
	produce(t, b, "clicks", []struct{ Key, Value string }{{"k", "L1"}})

	// Assert — exactly one left-only output.
	got := collect(t, b, "joined", 1, 3*time.Second)
	if len(got) != 1 {
		t.Fatalf("got %d outputs, want 1", len(got))
	}
	if !startsWith(string(got[0].Value), "left-only:") {
		t.Errorf("expected left-only output, got %q", got[0].Value)
	}
}

// TestStream_OuterJoin_RightNoMatchEmits is the symmetric case:
// a right record without a matching left also surfaces — this is
// what distinguishes FULL outer from LEFT outer.
func TestStream_OuterJoin_RightNoMatchEmits(t *testing.T) {
	// Arrange
	b := embed.NewMemory()
	defer b.Close()
	for _, n := range []string{"clicks", "impressions", "joined"} {
		if err := b.CreateTopic(embed.TopicSpec{Name: n, PartitionCount: 1}); err != nil {
			t.Fatal(err)
		}
	}

	top, err := streams.New(b.Transport(),
		streams.WithIdleWatermark(50*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	left := top.Stream("clicks")
	right := top.Stream("impressions")
	left.OuterJoin(right, 200*time.Millisecond, func(l, r *proto.Record) proto.Record {
		switch {
		case l != nil && r != nil:
			return proto.Record{Key: l.Key, Value: []byte("match")}
		case l != nil:
			return proto.Record{Key: l.Key, Value: []byte("left-only:" + string(l.Value))}
		default:
			return proto.Record{Key: r.Key, Value: []byte("right-only:" + string(r.Value))}
		}
	}).To("joined")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := top.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer top.Stop()

	// Act — right only, no left ever. LeftJoin would silently drop
	// this; OuterJoin must emit a right-only output once the
	// idle-watermark advance + join punctuator close the window.
	produce(t, b, "impressions", []struct{ Key, Value string }{{"k", "R1"}})

	// Assert
	got := collect(t, b, "joined", 1, 3*time.Second)
	if len(got) != 1 {
		t.Fatalf("got %d outputs, want 1", len(got))
	}
	if !startsWith(string(got[0].Value), "right-only:") {
		t.Errorf("expected right-only output, got %q", got[0].Value)
	}
}

// TestStream_LeftJoin_NoMatchEmits proves the LeftJoin emits a
// no-match output for a left record that finds no matching right
// counterpart at all — inner Join would silently drop it. Uses
// only a left input (no right) so the no-match path is the only
// possible output, eliminating the race between left- and right-
// side consumers in the join pipeline.
func TestStream_LeftJoin_NoMatchEmits(t *testing.T) {
	// Arrange
	b := embed.NewMemory()
	defer b.Close()
	for _, n := range []string{"clicks", "impressions", "joined"} {
		if err := b.CreateTopic(embed.TopicSpec{Name: n, PartitionCount: 1}); err != nil {
			t.Fatal(err)
		}
	}

	top, err := streams.New(b.Transport(),
		streams.WithIdleWatermark(50*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	left := top.Stream("clicks")
	right := top.Stream("impressions")
	left.LeftJoin(right, 200*time.Millisecond, func(l proto.Record, r *proto.Record) proto.Record {
		out := proto.Record{Key: l.Key}
		if r == nil {
			out.Value = []byte("nomatch:" + string(l.Value))
		} else {
			out.Value = []byte("match:" + string(l.Value) + "|" + string(r.Value))
		}
		return out
	}).To("joined")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := top.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer top.Stop()

	// Act — produce only on the left side. With no right ever
	// arriving, the no-match emission fires when the join's
	// punctuator goroutine sees the watermark close the window.
	produce(t, b, "clicks", []struct{ Key, Value string }{{"k", "L1"}})

	// Assert — exactly one no-match output. An inner Join would
	// silently drop this record.
	got := collect(t, b, "joined", 1, 3*time.Second)
	if len(got) != 1 {
		t.Fatalf("got %d outputs, want 1 (no-match emission missed)", len(got))
	}
	if !startsWith(string(got[0].Value), "nomatch:") {
		t.Errorf("expected no-match output, got %q", got[0].Value)
	}
}

// TestStream_StreamStreamJoin_AcceptsLateRecord proves WithAllowedLateness
// keeps buffered counterparts beyond the watermark cutoff so a record
// arriving after a watermark advance can still match. Without the
// option, the late record's match would have been pruned and the join
// would silently drop one of the expected output pairs.
func TestStream_StreamStreamJoin_AcceptsLateRecord(t *testing.T) {
	// Arrange
	b := embed.NewMemory()
	defer b.Close()
	for _, n := range []string{"clicks", "impressions", "joined"} {
		if err := b.CreateTopic(embed.TopicSpec{Name: n, PartitionCount: 1}); err != nil {
			t.Fatal(err)
		}
	}

	top, err := streams.New(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	left := top.Stream("clicks")
	right := top.Stream("impressions")
	left.Join(right, 200*time.Millisecond, func(l, r proto.Record) proto.Record {
		return proto.Record{
			Key:   l.Key,
			Value: append(append([]byte{}, l.Value...), append([]byte("|"), r.Value...)...),
		}
	}).WithAllowedLateness(2 * time.Second).To("joined")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := top.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer top.Stop()

	// Act — sequence designed to exercise lateness:
	//   1. left  k=a, eventTime=100ms  (buffered)
	//   2. right k=a, eventTime=150ms  (matches left → emit pair 1)
	//   3. left  k=a, eventTime=2_000ms (advances watermark to 2s — without
	//      lateness, the prune step would drop the 100ms entry)
	//   4. right k=a, eventTime=110ms  (LATE; should still match the
	//      100ms left because it sits within window=200ms and lateness
	//      keeps the buffer warm)
	ms := int64(time.Millisecond)
	produceTimestamped(t, b, "clicks", []proto.Record{
		{Key: []byte("a"), Value: []byte("L1"), Timestamp: 100 * ms},
	})
	produceTimestamped(t, b, "impressions", []proto.Record{
		{Key: []byte("a"), Value: []byte("R1"), Timestamp: 150 * ms},
	})
	produceTimestamped(t, b, "clicks", []proto.Record{
		{Key: []byte("a"), Value: []byte("L2"), Timestamp: 2000 * ms},
	})
	produceTimestamped(t, b, "impressions", []proto.Record{
		{Key: []byte("a"), Value: []byte("R-late"), Timestamp: 110 * ms},
	})

	// Assert — both pairs land in the sink.
	got := collect(t, b, "joined", 2, 3*time.Second)
	if len(got) != 2 {
		t.Fatalf("got %d joined records, want 2 (late record dropped?)", len(got))
	}
}

func TestTopology_StartTwiceFails(t *testing.T) {
	_, top := newTestRig(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := top.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer top.Stop()
	if err := top.Start(ctx); err == nil {
		t.Fatal("expected second Start to fail")
	}
}

func startsWith(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

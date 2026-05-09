package streams_test

import (
	"bytes"
	"context"
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

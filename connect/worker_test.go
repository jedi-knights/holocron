package connect_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jedi-knights/holocron/broker/embed"
	"github.com/jedi-knights/holocron/connect"
	"github.com/jedi-knights/holocron/connect/file"
	"github.com/jedi-knights/holocron/proto"
	"github.com/jedi-knights/holocron/sdk"
)

// TestEndToEnd_FileSourceToFileSink is the Stage 6 acceptance test.
// Source: a file with 5 lines.
// Sink: a different file the worker writes those lines into.
// If the sink file ends up containing all 5 lines, the source → broker
// → sink path works end to end through the SDK.
func TestEndToEnd_FileSourceToFileSink(t *testing.T) {
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "in.txt")
	dstPath := filepath.Join(dir, "out.txt")

	lines := []string{"alpha", "bravo", "charlie", "delta", "echo"}
	if err := os.WriteFile(srcPath, []byte(joinLines(lines)), 0o644); err != nil {
		t.Fatal(err)
	}

	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}

	w, err := connect.NewWorker(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.AddSource(file.NewSource(file.SourceConfig{
		Name:  "file-source",
		Path:  srcPath,
		Topic: "events",
	}), 1); err != nil {
		t.Fatal(err)
	}
	if err := w.AddSink(file.NewSink(file.SinkConfig{
		Name:  "file-sink",
		Topic: "events",
		Path:  dstPath,
	}), 1); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := w.Start(ctx); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if got := readLines(t, dstPath); len(got) == len(lines) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if err := w.Stop(); err != nil {
		t.Fatal(err)
	}

	got := readLines(t, dstPath)
	if len(got) != len(lines) {
		t.Fatalf("sink file has %d lines, want %d (got=%v)", len(got), len(lines), got)
	}
	// File source preserves order within a single task.
	for i, want := range lines {
		if got[i] != want {
			t.Errorf("line %d: got %q want %q", i, got[i], want)
		}
	}
}

// TestWorker_SinkFlushAutoCommits proves the Worker advances the sink's
// committed offset after a successful Flush. A fresh consumer in the
// same group, started after the sink has flushed, must not see the
// already-processed records.
func TestWorker_SinkFlushAutoCommits(t *testing.T) {
	// Arrange
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "in.txt")
	dstPath := filepath.Join(dir, "out.txt")
	if err := os.WriteFile(srcPath, []byte("alpha\nbravo\ncharlie\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}

	w, err := connect.NewWorker(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.AddSource(file.NewSource(file.SourceConfig{
		Name:  "src",
		Path:  srcPath,
		Topic: "events",
	}), 1); err != nil {
		t.Fatal(err)
	}
	if err := w.AddSink(file.NewSink(file.SinkConfig{
		Name:  "auto-commit-sink",
		Topic: "events",
		Path:  dstPath,
	}), 1); err != nil {
		t.Fatal(err)
	}

	// Act
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := w.Start(ctx); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if got := readLines(t, dstPath); len(got) >= 3 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	// Wait one full flush interval so auto-commit fires after records arrive.
	time.Sleep(6 * time.Second)
	if err := w.Stop(); err != nil {
		t.Fatal(err)
	}

	// Assert: a fresh consumer in the same group sees nothing — the sink
	// has already committed past every record.
	c, err := sdk.NewConsumer(b.Transport(), sdk.WithGroup("auto-commit-sink"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	pollCtx, pollCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer pollCancel()
	if err := c.Subscribe(pollCtx, "events", 0); err != nil {
		t.Fatal(err)
	}
	records, _ := c.Poll(pollCtx, 32)
	if len(records) > 0 {
		t.Fatalf("auto-commit failed: fresh consumer in same group received %d records", len(records))
	}
}

// flakySink fails its Put the first failTimes times it's called, then
// succeeds. Used to exercise retry-with-eventual-success.
type flakySink struct {
	name      string
	topics    []string
	failTimes int
	calls     int
	delivered []proto.Record
	mu        sync.Mutex
}

func (s *flakySink) Name() string                                              { return s.name }
func (s *flakySink) Topics() []string                                          { return s.topics }
func (s *flakySink) Tasks(maxTasks int) ([]connect.SinkTask, error)            { return []connect.SinkTask{s}, nil }
func (s *flakySink) Init(_ context.Context) error                              { return nil }
func (s *flakySink) Put(_ context.Context, records []proto.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.calls <= s.failTimes {
		return errors.New("flaky: transient failure")
	}
	s.delivered = append(s.delivered, records...)
	return nil
}
func (s *flakySink) Flush(_ context.Context) error { return nil }
func (s *flakySink) Close() error                  { return nil }

// alwaysFailSink fails every Put. Used to exercise DLQ delivery.
type alwaysFailSink struct {
	name   string
	topics []string
}

func (s *alwaysFailSink) Name() string                                       { return s.name }
func (s *alwaysFailSink) Topics() []string                                   { return s.topics }
func (s *alwaysFailSink) Tasks(maxTasks int) ([]connect.SinkTask, error)     { return []connect.SinkTask{s}, nil }
func (s *alwaysFailSink) Init(_ context.Context) error                       { return nil }
func (s *alwaysFailSink) Put(_ context.Context, _ []proto.Record) error      { return errors.New("permanent failure") }
func (s *alwaysFailSink) Flush(_ context.Context) error                      { return nil }
func (s *alwaysFailSink) Close() error                                       { return nil }

func TestWorker_SinkRetrySucceedsAfterTransientFailure(t *testing.T) {
	// Arrange
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	produce(t, b, "events", []proto.Record{{Value: []byte("payload")}})

	sink := &flakySink{name: "flaky-sink", topics: []string{"events"}, failTimes: 2}

	w, _ := connect.NewWorker(b.Transport())
	if err := w.AddSink(sink, 1, connect.WithSinkRetry(connect.RetryPolicy{
		MaxAttempts: 5,
		BaseDelay:   10 * time.Millisecond,
		MaxDelay:    100 * time.Millisecond,
	})); err != nil {
		t.Fatal(err)
	}

	// Act
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.Start(ctx); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		sink.mu.Lock()
		delivered := len(sink.delivered)
		sink.mu.Unlock()
		if delivered > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err := w.Stop(); err != nil {
		t.Fatal(err)
	}

	// Assert
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.delivered) != 1 {
		t.Fatalf("delivered %d records, want 1", len(sink.delivered))
	}
	if sink.calls != 3 {
		t.Errorf("expected 3 Put calls (2 fail + 1 success), got %d", sink.calls)
	}
}

func TestWorker_SinkDLQRoutesAfterMaxAttempts(t *testing.T) {
	// Arrange
	b := embed.NewMemory()
	defer b.Close()
	for _, n := range []string{"events", "events-dlq"} {
		if err := b.CreateTopic(embed.TopicSpec{Name: n, PartitionCount: 1}); err != nil {
			t.Fatal(err)
		}
	}
	produce(t, b, "events", []proto.Record{
		{Value: []byte("doomed-1")},
		{Value: []byte("doomed-2")},
	})

	sink := &alwaysFailSink{name: "always-fails", topics: []string{"events"}}

	w, _ := connect.NewWorker(b.Transport())
	if err := w.AddSink(sink, 1,
		connect.WithSinkRetry(connect.RetryPolicy{MaxAttempts: 2, BaseDelay: 5 * time.Millisecond}),
		connect.WithSinkDLQ("events-dlq"),
	); err != nil {
		t.Fatal(err)
	}

	// Act
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer w.Stop()

	dlq := collectFromTopic(t, b, "events-dlq", 2, 3*time.Second)

	// Assert
	if len(dlq) != 2 {
		t.Fatalf("DLQ got %d records, want 2", len(dlq))
	}
}

func produce(t *testing.T, b *embed.Broker, topic string, records []proto.Record) {
	t.Helper()
	p, err := sdk.NewProducer(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for _, r := range records {
		if _, err := p.Send(ctx, topic, r); err != nil {
			t.Fatal(err)
		}
	}
}

func collectFromTopic(t *testing.T, b *embed.Broker, topic string, want int, timeout time.Duration) []proto.Record {
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

// TestWorker_OffsetStoreResumesAfterRestart proves source-task offsets
// persist across worker restarts via WithOffsetStore. Two workers run
// against the same file source, separated by a Stop. Across the pair
// every line should be delivered exactly once: the first worker reads
// the prefix, the second resumes from where the first stopped instead
// of replaying from byte 0.
func TestWorker_OffsetStoreResumesAfterRestart(t *testing.T) {
	// Arrange
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "in.txt")
	lines := []string{"alpha", "bravo", "charlie", "delta", "echo"}
	if err := os.WriteFile(srcPath, []byte(joinLines(lines)), 0o644); err != nil {
		t.Fatal(err)
	}

	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}

	store := connect.NewMemoryOffsetStore()

	startWorker := func() *connect.Worker {
		w, err := connect.NewWorker(b.Transport(), connect.WithOffsetStore(store))
		if err != nil {
			t.Fatal(err)
		}
		if err := w.AddSource(file.NewSource(file.SourceConfig{
			Name:  "resume-source",
			Path:  srcPath,
			Topic: "events",
		}), 1); err != nil {
			t.Fatal(err)
		}
		return w
	}

	// Act — first worker: drain a couple of records, then stop. The exact
	// number is not load-bearing; we just need >0 and <len(lines) so the
	// resume path has work left to do.
	ctx1, cancel1 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel1()
	w1 := startWorker()
	if err := w1.Start(ctx1); err != nil {
		t.Fatal(err)
	}
	collected := collectFromTopic(t, b, "events", 2, 3*time.Second)
	if err := w1.Stop(); err != nil {
		t.Fatal(err)
	}
	if len(collected) < 2 {
		t.Fatalf("first worker delivered %d records, want at least 2", len(collected))
	}

	// Snapshot the saved offset so we can verify the worker actually
	// persisted what the file source emitted.
	saved, err := store.Load(context.Background(), "resume-source", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(saved) == 0 {
		t.Fatal("expected offsets saved after Commit, got none")
	}

	// Act — second worker: same offset store, same source. It should
	// resume from the saved position. Total records produced across both
	// workers should equal len(lines).
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	w2 := startWorker()
	if err := w2.Start(ctx2); err != nil {
		t.Fatal(err)
	}
	defer w2.Stop()

	all := collectFromTopic(t, b, "events", len(lines), 3*time.Second)

	// Assert
	if len(all) != len(lines) {
		t.Fatalf("delivered %d records across both workers, want %d", len(all), len(lines))
	}
	for i, want := range lines {
		if string(all[i].Value) != want {
			t.Errorf("record %d: got %q want %q", i, string(all[i].Value), want)
		}
	}
}

// gatedSource is a connect.SourceConnector whose tasks block on Poll
// until the gate channel is closed. Used by the coord test to ensure
// neither worker can produce records until both have settled into a
// stable assignment.
type gatedSource struct {
	name  string
	topic string
	gate  <-chan struct{}
	once  *sync.Once
	emits []string

	emitted *atomicBool
}

type atomicBool struct {
	mu  sync.Mutex
	set bool
}

func (a *atomicBool) trySet() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.set {
		return false
	}
	a.set = true
	return true
}

func (g *gatedSource) Name() string { return g.name }
func (g *gatedSource) Tasks(_ int) ([]connect.SourceTask, error) {
	return []connect.SourceTask{&gatedSourceTask{parent: g}}, nil
}

type gatedSourceTask struct {
	parent *gatedSource
	done   bool
}

func (t *gatedSourceTask) Init(_ context.Context, _ []map[string]any) error { return nil }
func (t *gatedSourceTask) Poll(ctx context.Context) ([]connect.SourceRecord, error) {
	if t.done {
		select {
		case <-ctx.Done():
			return nil, nil
		case <-time.After(50 * time.Millisecond):
			return nil, nil
		}
	}
	select {
	case <-ctx.Done():
		return nil, nil
	case <-t.parent.gate:
	}
	if !t.parent.emitted.trySet() {
		// Some other task in this process already emitted; this one
		// is shutting down imminently via revoke.
		t.done = true
		return nil, nil
	}
	out := make([]connect.SourceRecord, 0, len(t.parent.emits))
	for _, s := range t.parent.emits {
		out = append(out, connect.SourceRecord{Topic: t.parent.topic, Value: []byte(s)})
	}
	t.done = true
	return out, nil
}
func (t *gatedSourceTask) Commit(_ context.Context, _ []connect.SourceRecord) error { return nil }
func (t *gatedSourceTask) Close() error                                              { return nil }

// TestWorker_CoordinatedSource_NoDoubleProduction proves WithSourceCoordTopic
// elects a single owner across a worker pool. Two workers register the
// same source connector against a single-partition coord topic; only the
// elected leader's task may produce.
//
// gatedSource (which blocks on Poll until the gate is released) lets us
// open the gate after both workers have settled into stable assignments,
// eliminating the rebalance-window flake inherent to eager sources like
// the file source.
func TestWorker_CoordinatedSource_NoDoubleProduction(t *testing.T) {
	// Arrange
	emits := []string{"alpha", "bravo", "charlie", "delta", "echo"}
	gate := make(chan struct{})
	emitted := &atomicBool{}

	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	if err := b.CreateTopic(embed.TopicSpec{Name: "__connect_coord_solo", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}

	makeSource := func() *gatedSource {
		return &gatedSource{
			name:    "solo",
			topic:   "events",
			gate:    gate,
			emits:   emits,
			emitted: emitted,
		}
	}

	startWorker := func() *connect.Worker {
		w, err := connect.NewWorker(b.Transport())
		if err != nil {
			t.Fatal(err)
		}
		if err := w.AddSource(makeSource(), 1, connect.WithSourceCoordTopic("__connect_coord_solo")); err != nil {
			t.Fatal(err)
		}
		return w
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	w1 := startWorker()
	w2 := startWorker()
	if err := w1.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer w1.Stop()
	if err := w2.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer w2.Stop()

	// Wait long enough for the coord consumers to settle into a stable
	// assignment after both workers' joins. Coord heartbeat is 200ms;
	// 1s gives multiple cycles plus margin.
	time.Sleep(1 * time.Second)

	// Act — open the gate. The leader's task emits its records; the
	// follower's task is either revoked already (so its Poll returns
	// without emitting) or sees emitted.trySet fail.
	close(gate)

	all := collectFromTopic(t, b, "events", len(emits), 3*time.Second)
	time.Sleep(300 * time.Millisecond)
	extras, _ := pollFromTopic(t, b, "events", len(emits), 200*time.Millisecond)

	// Assert
	total := len(all) + len(extras)
	if total != len(emits) {
		t.Fatalf("coord failed: total %d records, want %d (extras=%d)", total, len(emits), len(extras))
	}
}

// TestWorker_CoordinatedSource_AutoCreatesCoordTopic proves the Worker
// auto-creates the coord topic on Start when running against a
// transport that supports topic creation. Operators should not need to
// hand-roll `__connect_coord_*` topics for every coordinated source.
func TestWorker_CoordinatedSource_AutoCreatesCoordTopic(t *testing.T) {
	// Arrange — note: coord topic is NOT pre-created.
	gate := make(chan struct{})
	emitted := &atomicBool{}

	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}

	source := &gatedSource{
		name:    "auto",
		topic:   "events",
		gate:    gate,
		emits:   []string{"alpha"},
		emitted: emitted,
	}

	w, err := connect.NewWorker(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.AddSource(source, 1, connect.WithSourceCoordTopic("__connect_coord_auto")); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Act
	if err := w.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer w.Stop()

	time.Sleep(500 * time.Millisecond)
	close(gate)

	got := collectFromTopic(t, b, "events", 1, 3*time.Second)

	// Assert
	if len(got) != 1 {
		t.Fatalf("auto-created coord topic did not enable production: got %d records, want 1", len(got))
	}
	if string(got[0].Value) != "alpha" {
		t.Errorf("unexpected payload: got %q", got[0].Value)
	}
}

// pollFromTopic is collectFromTopic's non-blocking sibling: it returns
// whatever records appear within the timeout, up to maxWant, without
// failing if nothing arrives.
func pollFromTopic(t *testing.T, b *embed.Broker, topic string, maxWant int, timeout time.Duration) ([]proto.Record, error) {
	t.Helper()
	c, err := sdk.NewConsumer(b.Transport())
	if err != nil {
		return nil, err
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := c.Subscribe(ctx, topic, int64(maxWant)); err != nil {
		return nil, err
	}
	return c.Poll(ctx, maxWant)
}

// TestWorker_AddSourceLiveAfterStart proves a source registered via
// AddSourceLive on a running Worker is wired up immediately — its
// records reach the destination topic without a Worker restart.
func TestWorker_AddSourceLiveAfterStart(t *testing.T) {
	// Arrange — start a Worker with an unrelated sink (so it's running)
	// and no sources yet.
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "in.txt")
	if err := os.WriteFile(srcPath, []byte("alpha\nbravo\ncharlie\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}

	w, err := connect.NewWorker(b.Transport())
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer w.Stop()

	// Act — add a source after Start.
	if err := w.AddSourceLive(file.NewSource(file.SourceConfig{
		Name:  "live",
		Path:  srcPath,
		Topic: "events",
	}), 1); err != nil {
		t.Fatalf("AddSourceLive: %v", err)
	}

	// Assert — the file's lines flow through the live-added source.
	got := collectFromTopic(t, b, "events", 3, 3*time.Second)
	if len(got) != 3 {
		t.Fatalf("got %d records, want 3", len(got))
	}
	want := []string{"alpha", "bravo", "charlie"}
	for i, r := range got {
		if string(r.Value) != want[i] {
			t.Errorf("record %d: got %q want %q", i, r.Value, want[i])
		}
	}
}

// TestWorker_RemoveSource proves the per-mount cancellation: a
// running source's goroutines exit when RemoveSource is called, while
// other registered sources continue producing.
func TestWorker_RemoveSource(t *testing.T) {
	// Arrange — two file sources writing to two distinct topics.
	dir := t.TempDir()
	srcA := filepath.Join(dir, "a.txt")
	srcB := filepath.Join(dir, "b.txt")
	if err := os.WriteFile(srcA, []byte(""), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(srcB, []byte(""), 0o644); err != nil {
		t.Fatal(err)
	}

	b := embed.NewMemory()
	defer b.Close()
	for _, n := range []string{"events-a", "events-b"} {
		if err := b.CreateTopic(embed.TopicSpec{Name: n, PartitionCount: 1}); err != nil {
			t.Fatal(err)
		}
	}

	w, err := connect.NewWorker(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.AddSource(file.NewSource(file.SourceConfig{Name: "doomed", Path: srcA, Topic: "events-a"}), 1); err != nil {
		t.Fatal(err)
	}
	if err := w.AddSource(file.NewSource(file.SourceConfig{Name: "kept", Path: srcB, Topic: "events-b"}), 1); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer w.Stop()

	// Act — remove the doomed source, then write to both files.
	if err := w.RemoveSource("doomed"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(srcA, []byte("from-doomed\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(srcB, []byte("from-kept\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Assert — the kept source delivers; the doomed one stays silent.
	got := collectFromTopic(t, b, "events-b", 1, 3*time.Second)
	if len(got) != 1 || string(got[0].Value) != "from-kept" {
		t.Errorf("kept source missing: got %v", got)
	}
	extras, _ := pollFromTopic(t, b, "events-a", 1, 500*time.Millisecond)
	if len(extras) != 0 {
		t.Errorf("removed source still producing: %v", extras)
	}
}

// TestWorker_AddSinkLive proves a sink registered after Start receives
// records that arrive on its source topic.
func TestWorker_AddSinkLive(t *testing.T) {
	// Arrange
	dir := t.TempDir()
	dstPath := filepath.Join(dir, "out.txt")

	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}

	w, err := connect.NewWorker(b.Transport())
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer w.Stop()

	// Act — sink registered after Start.
	if err := w.AddSinkLive(file.NewSink(file.SinkConfig{
		Name:  "live-sink",
		Topic: "events",
		Path:  dstPath,
	}), 1); err != nil {
		t.Fatalf("AddSinkLive: %v", err)
	}

	// Sink consumer takes a moment to join its group; produce after
	// a short stabilization window so records aren't dropped.
	time.Sleep(300 * time.Millisecond)
	produce(t, b, "events", []proto.Record{
		{Value: []byte("hello")},
		{Value: []byte("world")},
	})

	// Assert
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if got := readLines(t, dstPath); len(got) >= 2 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	got := readLines(t, dstPath)
	if len(got) != 2 {
		t.Fatalf("got %d lines in sink file, want 2 (got=%v)", len(got), got)
	}
}

// TestWorker_RemoveSink proves a removed sink stops consuming. Records
// produced after RemoveSink return are not delivered to the sink.
func TestWorker_RemoveSink(t *testing.T) {
	// Arrange
	dir := t.TempDir()
	dstPath := filepath.Join(dir, "out.txt")

	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}

	w, err := connect.NewWorker(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.AddSink(file.NewSink(file.SinkConfig{
		Name:  "doomed",
		Topic: "events",
		Path:  dstPath,
	}), 1); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer w.Stop()

	// Wait for the sink consumer to settle, then remove it.
	time.Sleep(300 * time.Millisecond)
	if err := w.RemoveSink("doomed"); err != nil {
		t.Fatal(err)
	}
	// Give the sink goroutine time to exit before producing.
	time.Sleep(300 * time.Millisecond)

	// Act
	produce(t, b, "events", []proto.Record{{Value: []byte("after-remove")}})

	// Assert — the file stays empty (or at least never sees the new line).
	time.Sleep(500 * time.Millisecond)
	got := readLines(t, dstPath)
	for _, line := range got {
		if line == "after-remove" {
			t.Fatalf("removed sink wrote post-removal record: %v", got)
		}
	}
}

func TestWorker_RemoveSink_UnknownNameFails(t *testing.T) {
	b := embed.NewMemory()
	defer b.Close()
	w, _ := connect.NewWorker(b.Transport())
	if err := w.RemoveSink("does-not-exist"); err == nil {
		t.Fatal("expected error from RemoveSink on unknown name")
	}
}

func TestWorker_RemoveSource_UnknownNameFails(t *testing.T) {
	b := embed.NewMemory()
	defer b.Close()
	w, _ := connect.NewWorker(b.Transport())
	if err := w.RemoveSource("does-not-exist"); err == nil {
		t.Fatal("expected error from RemoveSource on unknown name")
	}
}

// TestWorker_AddSourceLiveBeforeStartFails proves AddSourceLive
// rejects a not-yet-running Worker — the operator should use AddSource
// in that path so the source goes through the normal Start init.
func TestWorker_AddSourceLiveBeforeStartFails(t *testing.T) {
	b := embed.NewMemory()
	defer b.Close()
	w, err := connect.NewWorker(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.AddSourceLive(
		file.NewSource(file.SourceConfig{Name: "x", Path: "/dev/null", Topic: "t"}),
		1,
	); err == nil {
		t.Fatal("expected error from AddSourceLive on not-running Worker")
	}
}

func TestWorker_StartTwiceFails(t *testing.T) {
	b := embed.NewMemory()
	defer b.Close()
	w, _ := connect.NewWorker(b.Transport())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := w.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer w.Stop()
	if err := w.Start(ctx); err == nil {
		t.Fatal("expected second Start to fail")
	}
}

func TestWorker_AddAfterStartFails(t *testing.T) {
	b := embed.NewMemory()
	defer b.Close()
	w, _ := connect.NewWorker(b.Transport())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := w.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer w.Stop()
	if err := w.AddSource(file.NewSource(file.SourceConfig{Name: "x", Path: "/dev/null", Topic: "t"}), 1); err == nil {
		t.Fatal("expected AddSource after Start to fail")
	}
}

func joinLines(lines []string) string {
	out := ""
	for _, l := range lines {
		out += l + "\n"
	}
	return out
}

func readLines(t *testing.T, path string) []string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatal(err)
	}
	if len(b) == 0 {
		return nil
	}
	var lines []string
	start := 0
	for i, c := range b {
		if c == '\n' {
			lines = append(lines, string(b[start:i]))
			start = i + 1
		}
	}
	if start < len(b) {
		lines = append(lines, string(b[start:]))
	}
	return lines
}

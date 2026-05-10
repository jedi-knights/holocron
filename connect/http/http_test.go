package http_test

import (
	"context"
	"io"
	stdhttp "net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/jedi-knights/holocron/broker/embed"
	"github.com/jedi-knights/holocron/connect"
	connecthttp "github.com/jedi-knights/holocron/connect/http"
	"github.com/jedi-knights/holocron/proto"
	"github.com/jedi-knights/holocron/sdk"
)

// TestHTTPSource_PollsAndEmits proves the HTTP source GETs its URL on
// the configured interval and emits one record per non-empty line of
// the response body.
func TestHTTPSource_PollsAndEmits(t *testing.T) {
	// Arrange — server returns a fixed three-line payload.
	var hits int32
	var mu sync.Mutex
	srv := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		mu.Lock()
		hits++
		mu.Unlock()
		_, _ = w.Write([]byte("alpha\nbravo\ncharlie\n"))
	}))
	defer srv.Close()

	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}

	w, err := connect.NewWorker(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.AddSource(connecthttp.NewSource(connecthttp.SourceConfig{
		Name:         "http-src",
		URL:          srv.URL,
		Topic:        "events",
		PollInterval: 50 * time.Millisecond,
	}), 1); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer w.Stop()

	// Act — collect the first three records.
	got := collectFromTopic(t, b, "events", 3, 3*time.Second)

	// Assert
	want := []string{"alpha", "bravo", "charlie"}
	if len(got) != 3 {
		t.Fatalf("got %d records, want 3", len(got))
	}
	for i, r := range got {
		if string(r.Value) != want[i] {
			t.Errorf("record %d: got %q want %q", i, r.Value, want[i])
		}
	}
}

// TestHTTPSource_CursorAdvancesAndPersists proves the HTTP source
// passes the current cursor on the URL, reads the next cursor from a
// configured response header, and persists it via OffsetStore. A
// second worker built on the same store resumes from the captured
// cursor instead of starting over.
func TestHTTPSource_CursorAdvancesAndPersists(t *testing.T) {
	// Arrange — server returns batch-{n} text and X-Next-Cursor=batch-{n+1}
	// keyed off the `?since` query param.
	pages := map[string]string{
		"":       "alpha\n",
		"page-1": "bravo\n",
		"page-2": "charlie\n",
		"page-3": "",
	}
	nextOf := map[string]string{
		"":       "page-1",
		"page-1": "page-2",
		"page-2": "page-3",
		"page-3": "page-3",
	}

	srv := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		cur := r.URL.Query().Get("since")
		w.Header().Set("X-Next-Cursor", nextOf[cur])
		_, _ = w.Write([]byte(pages[cur]))
	}))
	defer srv.Close()

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
		if err := w.AddSource(connecthttp.NewSource(connecthttp.SourceConfig{
			Name:         "http-cursor",
			URL:          srv.URL,
			Topic:        "events",
			PollInterval: 50 * time.Millisecond,
			CursorParam:  "since",
			CursorHeader: "X-Next-Cursor",
		}), 1); err != nil {
			t.Fatal(err)
		}
		return w
	}

	// Act — first worker reads alpha + bravo, then we restart.
	ctx1, cancel1 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel1()
	w1 := startWorker()
	if err := w1.Start(ctx1); err != nil {
		t.Fatal(err)
	}
	first := collectFromTopic(t, b, "events", 2, 3*time.Second)
	if err := w1.Stop(); err != nil {
		t.Fatal(err)
	}

	saved, err := store.Load(context.Background(), "http-cursor", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(saved) == 0 {
		t.Fatal("expected stored cursor after first worker, got none")
	}

	// Second worker should resume from saved cursor — picks up charlie
	// without re-emitting alpha or bravo.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	w2 := startWorker()
	if err := w2.Start(ctx2); err != nil {
		t.Fatal(err)
	}
	defer w2.Stop()

	all := collectFromTopic(t, b, "events", 3, 3*time.Second)

	// Assert
	if len(first) < 2 {
		t.Fatalf("first worker delivered %d records, want at least 2", len(first))
	}
	if len(all) != 3 {
		t.Fatalf("total records: got %d, want 3 (cursor restart re-emitted?)", len(all))
	}
	want := []string{"alpha", "bravo", "charlie"}
	for i, r := range all {
		if string(r.Value) != want[i] {
			t.Errorf("record %d: got %q, want %q", i, r.Value, want[i])
		}
	}
}

// TestHTTPSink_PostsEachRecord proves the HTTP sink POSTs each record's
// Value to the configured URL.
func TestHTTPSink_PostsEachRecord(t *testing.T) {
	// Arrange — server records request bodies.
	var mu sync.Mutex
	var bodies []string
	srv := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, string(body))
		mu.Unlock()
		w.WriteHeader(stdhttp.StatusNoContent)
	}))
	defer srv.Close()

	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}

	produce(t, b, "events", []proto.Record{
		{Value: []byte("one")},
		{Value: []byte("two")},
		{Value: []byte("three")},
	})

	w, _ := connect.NewWorker(b.Transport())
	if err := w.AddSink(connecthttp.NewSink(connecthttp.SinkConfig{
		Name:  "http-sink",
		Topic: "events",
		URL:   srv.URL,
	}), 1); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer w.Stop()

	// Act — wait until the server has seen all three.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(bodies)
		mu.Unlock()
		if n >= 3 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Assert
	mu.Lock()
	defer mu.Unlock()
	if len(bodies) != 3 {
		t.Fatalf("got %d bodies, want 3", len(bodies))
	}
	want := map[string]bool{"one": true, "two": true, "three": true}
	for _, b := range bodies {
		if !want[b] {
			t.Errorf("unexpected body %q", b)
		}
	}
}

// TestHTTPSink_NonOKReturnsError proves a non-2xx response surfaces as
// a Put error so the Worker's retry/DLQ machinery can intervene.
func TestHTTPSink_NonOKReturnsError(t *testing.T) {
	// Arrange — server always 500s.
	srv := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, _ *stdhttp.Request) {
		w.WriteHeader(stdhttp.StatusInternalServerError)
	}))
	defer srv.Close()

	sink := connecthttp.NewSink(connecthttp.SinkConfig{
		Name:  "boom",
		Topic: "events",
		URL:   srv.URL,
	})
	tasks, err := sink.Tasks(1)
	if err != nil {
		t.Fatal(err)
	}
	task := tasks[0]
	if err := task.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	// Act
	err = task.Put(context.Background(), []proto.Record{{Value: []byte("x")}})

	// Assert
	if err == nil {
		t.Fatal("expected error from 500 response, got nil")
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

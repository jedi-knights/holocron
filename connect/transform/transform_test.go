package transform_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jedi-knights/holocron/broker/embed"
	"github.com/jedi-knights/holocron/connect"
	"github.com/jedi-knights/holocron/connect/transform"
	"github.com/jedi-knights/holocron/proto"
	"github.com/jedi-knights/holocron/sdk"
)

// TestTransform_AppliesFunction proves the transform connector applies
// its Fn to every consumed record and publishes non-dropped results to
// the target topic.
func TestTransform_AppliesFunction(t *testing.T) {
	// Arrange — three records get uppercased and re-emitted.
	b := embed.NewMemory()
	defer b.Close()
	for _, name := range []string{"events", "events-upper"} {
		if err := b.CreateTopic(embed.TopicSpec{Name: name, PartitionCount: 1}); err != nil {
			t.Fatal(err)
		}
	}

	produce(t, b, "events", []proto.Record{
		{Value: []byte("alpha")},
		{Value: []byte("bravo")},
		{Value: []byte("charlie")},
	})

	upper := func(in proto.Record) (proto.Record, bool, error) {
		return proto.Record{
			Key:     in.Key,
			Value:   []byte(strings.ToUpper(string(in.Value))),
			Headers: in.Headers,
		}, true, nil
	}

	conn, err := transform.New(b.Transport(), transform.Config{
		Name:        "uppercase",
		SourceTopic: "events",
		TargetTopic: "events-upper",
		Fn:          upper,
	})
	if err != nil {
		t.Fatal(err)
	}

	w, err := connect.NewWorker(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.AddSink(conn, 1); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer w.Stop()

	// Act
	got := collectFromTopic(t, b, "events-upper", 3, 3*time.Second)

	// Assert
	want := []string{"ALPHA", "BRAVO", "CHARLIE"}
	if len(got) != 3 {
		t.Fatalf("got %d records, want 3", len(got))
	}
	for i, r := range got {
		if string(r.Value) != want[i] {
			t.Errorf("record %d: got %q want %q", i, r.Value, want[i])
		}
	}
}

// TestTransform_DropsWhenKeepFalse proves a Fn that returns keep=false
// removes the record from the output stream entirely.
func TestTransform_DropsWhenKeepFalse(t *testing.T) {
	// Arrange — drop records with empty Key, keep the rest.
	b := embed.NewMemory()
	defer b.Close()
	for _, name := range []string{"events", "events-keyed"} {
		if err := b.CreateTopic(embed.TopicSpec{Name: name, PartitionCount: 1}); err != nil {
			t.Fatal(err)
		}
	}

	produce(t, b, "events", []proto.Record{
		{Key: []byte("a"), Value: []byte("v1")},
		{Key: nil, Value: []byte("v2")},
		{Key: []byte("c"), Value: []byte("v3")},
	})

	keepKeyed := func(in proto.Record) (proto.Record, bool, error) {
		if len(in.Key) == 0 {
			return proto.Record{}, false, nil
		}
		return in, true, nil
	}

	conn, err := transform.New(b.Transport(), transform.Config{
		Name:        "keyed-only",
		SourceTopic: "events",
		TargetTopic: "events-keyed",
		Fn:          keepKeyed,
	})
	if err != nil {
		t.Fatal(err)
	}

	w, _ := connect.NewWorker(b.Transport())
	if err := w.AddSink(conn, 1); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer w.Stop()

	// Act — wait for the two keyed records.
	got := collectFromTopic(t, b, "events-keyed", 2, 3*time.Second)

	// Assert
	if len(got) != 2 {
		t.Fatalf("got %d records, want 2 (keyless dropped)", len(got))
	}
	for _, r := range got {
		if len(r.Key) == 0 {
			t.Errorf("kept a keyless record: %q", r.Value)
		}
	}
}

func TestTransform_NewRejectsBadConfig(t *testing.T) {
	// Arrange
	b := embed.NewMemory()
	defer b.Close()
	noop := func(in proto.Record) (proto.Record, bool, error) { return in, true, nil }

	cases := []struct {
		name string
		cfg  transform.Config
	}{
		{"missing name", transform.Config{SourceTopic: "a", TargetTopic: "b", Fn: noop}},
		{"missing source", transform.Config{Name: "x", TargetTopic: "b", Fn: noop}},
		{"missing target", transform.Config{Name: "x", SourceTopic: "a", Fn: noop}},
		{"same source and target", transform.Config{Name: "x", SourceTopic: "a", TargetTopic: "a", Fn: noop}},
		{"missing fn", transform.Config{Name: "x", SourceTopic: "a", TargetTopic: "b"}},
	}

	// Act / Assert
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := transform.New(b.Transport(), tc.cfg); err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}

// TestTransform_FnErrorAbortsBatch proves a Fn that returns an error
// surfaces it through the SinkTask, so the Worker's retry/DLQ paths
// can intervene.
func TestTransform_FnErrorAbortsBatch(t *testing.T) {
	// Arrange
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	if err := b.CreateTopic(embed.TopicSpec{Name: "events-out", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}

	bad := func(_ proto.Record) (proto.Record, bool, error) {
		return proto.Record{}, false, errors.New("boom")
	}

	conn, err := transform.New(b.Transport(), transform.Config{
		Name:        "bad",
		SourceTopic: "events",
		TargetTopic: "events-out",
		Fn:          bad,
	})
	if err != nil {
		t.Fatal(err)
	}

	tasks, err := conn.Tasks(1)
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
		t.Fatal("expected error from Fn, got nil")
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

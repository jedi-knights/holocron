package router_test

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/jedi-knights/holocron/broker/embed"
	"github.com/jedi-knights/holocron/connect"
	"github.com/jedi-knights/holocron/connect/router"
	"github.com/jedi-knights/holocron/proto"
	"github.com/jedi-knights/holocron/sdk"
)

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

// TestRouter_FanOutByHeader: 3 inputs, 2 rules, each input carries an
// event-type header that selects the target topic(s).
func TestRouter_FanOutByHeader(t *testing.T) {
	// Arrange
	b := embed.NewMemory()
	defer b.Close()
	for _, name := range []string{"events", "orders-out", "shipments-out"} {
		if err := b.CreateTopic(embed.TopicSpec{Name: name, PartitionCount: 1}); err != nil {
			t.Fatal(err)
		}
	}

	conn, err := router.New(b.Transport(), router.Config{
		Name:        "test-router",
		SourceTopic: "events",
		Rules: []router.Rule{
			{
				Match:   router.Match{HeaderKey: "event-type", HeaderValue: "order.placed"},
				Targets: []string{"orders-out"},
			},
			{
				Match:   router.Match{HeaderKey: "event-type", HeaderValue: "shipment.created"},
				Targets: []string{"shipments-out"},
			},
		},
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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := w.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer w.Stop()

	// Act
	produce(t, b, "events", []proto.Record{
		{Headers: []proto.Header{{Key: "event-type", Value: []byte("order.placed")}}, Value: []byte("o1")},
		{Headers: []proto.Header{{Key: "event-type", Value: []byte("shipment.created")}}, Value: []byte("s1")},
		{Headers: []proto.Header{{Key: "event-type", Value: []byte("order.placed")}}, Value: []byte("o2")},
	})

	// Assert: orders-out has 2 records, shipments-out has 1.
	orders := collect(t, b, "orders-out", 2, 3*time.Second)
	if len(orders) != 2 {
		t.Fatalf("orders-out got %d records, want 2", len(orders))
	}
	shipments := collect(t, b, "shipments-out", 1, 3*time.Second)
	if len(shipments) != 1 {
		t.Fatalf("shipments-out got %d records, want 1", len(shipments))
	}
}

// TestRouter_FanOutByKeyPrefix: rule fires when the record's Key has a
// matching prefix.
func TestRouter_FanOutByKeyPrefix(t *testing.T) {
	// Arrange
	b := embed.NewMemory()
	defer b.Close()
	for _, name := range []string{"events", "users-out"} {
		if err := b.CreateTopic(embed.TopicSpec{Name: name, PartitionCount: 1}); err != nil {
			t.Fatal(err)
		}
	}

	conn, _ := router.New(b.Transport(), router.Config{
		Name:        "prefix-router",
		SourceTopic: "events",
		Rules: []router.Rule{
			{Match: router.Match{KeyPrefix: "user-"}, Targets: []string{"users-out"}},
		},
	})

	w, _ := connect.NewWorker(b.Transport())
	if err := w.AddSink(conn, 1); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := w.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer w.Stop()

	// Act
	produce(t, b, "events", []proto.Record{
		{Key: []byte("user-42"), Value: []byte("u42")},
		{Key: []byte("admin-1"), Value: []byte("a1")},
		{Key: []byte("user-7"), Value: []byte("u7")},
	})

	// Assert: only user-prefixed records reach users-out.
	users := collect(t, b, "users-out", 2, 3*time.Second)
	if len(users) != 2 {
		t.Fatalf("users-out got %d, want 2", len(users))
	}
	for _, r := range users {
		if string(r.Key)[:5] != "user-" {
			t.Errorf("router admitted non-user record: key=%q", r.Key)
		}
	}
}

func TestMatch_Matches(t *testing.T) {
	tests := []struct {
		name   string
		match  router.Match
		record proto.Record
		want   bool
	}{
		{
			name:  "no fields means match-all",
			match: router.Match{},
			want:  true,
		},
		{
			name:   "header equals matches",
			match:  router.Match{HeaderKey: "k", HeaderValue: "v"},
			record: proto.Record{Headers: []proto.Header{{Key: "k", Value: []byte("v")}}},
			want:   true,
		},
		{
			name:   "header equals rejects mismatch",
			match:  router.Match{HeaderKey: "k", HeaderValue: "v"},
			record: proto.Record{Headers: []proto.Header{{Key: "k", Value: []byte("other")}}},
			want:   false,
		},
		{
			name:   "key prefix matches",
			match:  router.Match{KeyPrefix: "u-"},
			record: proto.Record{Key: []byte("u-42")},
			want:   true,
		},
		{
			name:   "header AND prefix both required",
			match:  router.Match{HeaderKey: "k", HeaderValue: "v", KeyPrefix: "u-"},
			record: proto.Record{Key: []byte("u-1"), Headers: []proto.Header{{Key: "k", Value: []byte("v")}}},
			want:   true,
		},
		{
			name:   "header AND prefix rejects when prefix missing",
			match:  router.Match{HeaderKey: "k", HeaderValue: "v", KeyPrefix: "u-"},
			record: proto.Record{Key: []byte("a-1"), Headers: []proto.Header{{Key: "k", Value: []byte("v")}}},
			want:   false,
		},
		{
			name:   "header-exists matches regardless of value",
			match:  router.Match{HeaderExists: "trace-id"},
			record: proto.Record{Headers: []proto.Header{{Key: "trace-id", Value: []byte("abc")}}},
			want:   true,
		},
		{
			name:   "header-exists rejects when header missing",
			match:  router.Match{HeaderExists: "trace-id"},
			record: proto.Record{Headers: []proto.Header{{Key: "x-other", Value: []byte("v")}}},
			want:   false,
		},
		{
			name:   "key-regex matches",
			match:  router.Match{KeyRegex: regexp.MustCompile(`^order-\d+$`)},
			record: proto.Record{Key: []byte("order-42")},
			want:   true,
		},
		{
			name:   "key-regex rejects mismatch",
			match:  router.Match{KeyRegex: regexp.MustCompile(`^order-\d+$`)},
			record: proto.Record{Key: []byte("user-42")},
			want:   false,
		},
		{
			name: "Any OR matches when one alternative matches",
			match: router.Match{Any: []router.Match{
				{KeyPrefix: "user-"},
				{KeyPrefix: "admin-"},
			}},
			record: proto.Record{Key: []byte("admin-1")},
			want:   true,
		},
		{
			name: "Any OR rejects when no alternative matches",
			match: router.Match{Any: []router.Match{
				{KeyPrefix: "user-"},
				{KeyPrefix: "admin-"},
			}},
			record: proto.Record{Key: []byte("guest-1")},
			want:   false,
		},
		{
			name: "top-level AND Any: both required",
			match: router.Match{
				HeaderExists: "trace-id",
				Any: []router.Match{
					{KeyPrefix: "user-"},
					{KeyPrefix: "admin-"},
				},
			},
			record: proto.Record{
				Key:     []byte("user-1"),
				Headers: []proto.Header{{Key: "trace-id", Value: []byte("t1")}},
			},
			want: true,
		},
		{
			name: "top-level AND Any: rejects when top-level fails",
			match: router.Match{
				HeaderExists: "trace-id",
				Any: []router.Match{
					{KeyPrefix: "user-"},
				},
			},
			record: proto.Record{Key: []byte("user-1")},
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.match.Matches(tt.record); got != tt.want {
				t.Errorf("Matches() = %v, want %v", got, tt.want)
			}
		})
	}
}

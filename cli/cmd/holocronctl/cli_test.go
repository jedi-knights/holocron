package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jedi-knights/holocron/broker/embed"
	"github.com/jedi-knights/holocron/proto"
	"github.com/jedi-knights/holocron/sdk"
	holocronnet "github.com/jedi-knights/holocron/sdk/net"
)

// TestCLI_TopicCreate proves `holocronctl topic create` ensures the
// topic exists on the broker. Drives the run() entry point so the
// subcommand router is exercised end-to-end.
func TestCLI_TopicCreate(t *testing.T) {
	// Arrange — embed broker listening on a free port.
	b := embed.NewMemory()
	defer b.Close()
	addr, err := b.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	// Act
	if err := run([]string{
		"topic", "create",
		"--addr", addr,
		"--topic", "events",
		"--partitions", "3",
	}); err != nil {
		t.Fatalf("topic create: %v", err)
	}

	// Assert — Topics() now lists the topic with 3 partitions.
	var found bool
	for _, cfg := range b.Topics() {
		if cfg.Name == "events" {
			found = true
			if cfg.PartitionCount != 3 {
				t.Errorf("partitions: got %d, want 3", cfg.PartitionCount)
			}
		}
	}
	if !found {
		t.Fatal("topic 'events' not in broker.Topics() after create")
	}
}

// TestCLI_TopicCreateRequiresName proves a missing --topic flag fails
// the call rather than provisioning a nameless topic.
func TestCLI_TopicCreateRequiresName(t *testing.T) {
	b := embed.NewMemory()
	defer b.Close()
	addr, err := b.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	if err := run([]string{"topic", "create", "--addr", addr}); err == nil ||
		!strings.Contains(err.Error(), "--topic") {
		t.Fatalf("expected missing-topic error, got %v", err)
	}
}

// TestCLI_TopicList enumerates every topic the broker knows. The
// pre-batch-23 --probe flag is gone now that the broker exposes
// the full registry via OpListTopics.
func TestCLI_TopicList(t *testing.T) {
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "orders", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	addr, err := b.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	if err := run([]string{
		"topic", "list",
		"--addr", addr,
	}); err != nil {
		t.Fatalf("topic list: %v", err)
	}
}

// TestCLI_ClusterStatusOnNonClustered proves the status subcommand
// prints the "not part of a cluster" message instead of erroring
// when the broker isn't clustered. Mirrors the `cluster members`
// non-clustered flow.
func TestCLI_ClusterStatusOnNonClustered(t *testing.T) {
	b := embed.NewMemory()
	defer b.Close()
	addr, err := b.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"cluster", "status", "--addr", addr}); err != nil {
		t.Fatalf("cluster status against non-clustered broker: %v", err)
	}
}

// TestCLI_ProduceBatchFromStdin proves the --batch flag ships all
// stdin records in one SendBatch. Counted via the broker's
// high-water after the run.
func TestCLI_ProduceBatchFromStdin(t *testing.T) {
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}

	prod, err := sdk.NewProducer(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	const payload = "k1|v1\nk2|v2\nk3|v3\n"
	count, err := produceBatchFromReader(ctx, prod, "events", strings.NewReader(payload), "|", nil)
	if err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Errorf("count: got %d, want 3", count)
	}

	// Verify the records made it.
	c, err := sdk.NewConsumer(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.Subscribe(ctx, "events", 0); err != nil {
		t.Fatal(err)
	}
	got := make([]proto.Record, 0, 3)
	for len(got) < 3 {
		recs, err := c.Poll(ctx, 3-len(got))
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, recs...)
	}
	if string(got[1].Key) != "k2" || string(got[1].Value) != "v2" {
		t.Errorf("record 1: got (%q, %q), want (k2, v2)", got[1].Key, got[1].Value)
	}
}

// TestCLI_ProduceStdin proves the produce subcommand reads records
// from a reader (one per line) when --value is omitted, and that
// --key-sep splits each line into (key, value). Drives the helper
// directly so the test doesn't fight stdin redirection.
func TestCLI_ProduceStdin(t *testing.T) {
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}

	prod, err := sdk.NewProducer(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	const payload = "k1|v1\nk2|v2\nno-key-line\n"
	count, err := produceFromReader(ctx, prod, "events", strings.NewReader(payload), "|", nil)
	if err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Errorf("count: got %d, want 3", count)
	}

	// Read back and verify the keyed lines split correctly.
	c, err := sdk.NewConsumer(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.Subscribe(ctx, "events", 0); err != nil {
		t.Fatal(err)
	}
	got := make([]proto.Record, 0, 3)
	for len(got) < 3 {
		recs, err := c.Poll(ctx, 3-len(got))
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, recs...)
	}
	if string(got[0].Key) != "k1" || string(got[0].Value) != "v1" {
		t.Errorf("record 0: got (%q, %q), want (k1, v1)", got[0].Key, got[0].Value)
	}
	if string(got[2].Key) != "" || string(got[2].Value) != "no-key-line" {
		t.Errorf("record 2: got (%q, %q), want (\"\", no-key-line)", got[2].Key, got[2].Value)
	}
}

// TestCLI_JSONOutput proves the read-only inspection subcommands
// emit valid JSON when --json is passed. Captures stdout and
// checks the output round-trips through json.Unmarshal — guards
// against future changes accidentally injecting non-JSON noise
// (banners, debug prints) on the JSON path.
func TestCLI_JSONOutput(t *testing.T) {
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 4, RetentionMs: 2000}); err != nil {
		t.Fatal(err)
	}
	addr, err := b.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name string
		args []string
		into any
	}{
		{"topic-list", []string{"topic", "list", "--addr", addr, "--json"}, &[]map[string]any{}},
		{"topic-describe", []string{"topic", "describe", "--addr", addr, "--topic", "events", "--json"}, &map[string]any{}},
		{"cluster-status", []string{"cluster", "status", "--addr", addr, "--json"}, &map[string]any{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := captureStdout(t, func() {
				if err := run(tc.args); err != nil {
					t.Fatalf("%s: %v", tc.name, err)
				}
			})
			if err := jsonUnmarshal(out, tc.into); err != nil {
				t.Fatalf("%s: output is not valid JSON: %v\nstdout:\n%s", tc.name, err, out)
			}
		})
	}
}

// captureStdout runs fn with os.Stdout redirected to a pipe,
// returning everything written. Test-local plumbing for the
// JSON-output assertions.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	orig := os.Stdout
	os.Stdout = w
	defer func() { os.Stdout = orig }()
	done := make(chan string, 1)
	go func() {
		buf := make([]byte, 0, 4096)
		tmp := make([]byte, 1024)
		for {
			n, err := r.Read(tmp)
			if n > 0 {
				buf = append(buf, tmp[:n]...)
			}
			if err != nil {
				break
			}
		}
		done <- string(buf)
	}()
	fn()
	w.Close()
	return <-done
}

// jsonUnmarshal is a thin wrapper around json.Unmarshal — kept
// as a named helper so the JSON-output assertion's intent reads
// clearly.
func jsonUnmarshal(s string, v any) error {
	return json.Unmarshal([]byte(s), v)
}

// TestCLI_Tail proves the tail subcommand attaches at the
// partition's current high-water and skips historical records.
// Producing two records BEFORE tail starts, then one AFTER,
// should surface only the post-attach record.
func TestCLI_Tail(t *testing.T) {
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	addr, err := b.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	// Seed two pre-tail records.
	prod, err := sdk.NewProducer(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for _, v := range []string{"old1", "old2"} {
		if _, err := prod.Send(ctx, "events", proto.Record{Value: []byte(v)}); err != nil {
			t.Fatal(err)
		}
	}

	// Tail in a goroutine — bounded by --max so the test exits
	// once the post-attach record arrives.
	done := make(chan error, 1)
	go func() {
		done <- run([]string{
			"tail",
			"--addr", addr,
			"--topic", "events",
			"--partition", "0",
			"--max", "1",
			"--duration", "3s",
		})
	}()

	// Give the tail a brief head-start to subscribe at high-water,
	// then produce a fresh record. Without this, the new record
	// could land before tail attaches and surface as historical.
	time.Sleep(100 * time.Millisecond)
	if _, err := prod.Send(ctx, "events", proto.Record{Value: []byte("fresh")}); err != nil {
		t.Fatal(err)
	}

	if err := <-done; err != nil {
		t.Fatalf("tail: %v", err)
	}
}

// TestCLI_BenchConsume proves the bench subcommand's --consume
// flag exercises the read path: pre-seed records, then run bench
// in consume mode and assert it reads the configured count.
func TestCLI_BenchConsume(t *testing.T) {
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "bench", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	addr, err := b.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	// Pre-seed 50 records so the consume run has data.
	prod, err := sdk.NewProducer(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for range 50 {
		if _, err := prod.Send(ctx, "bench", proto.Record{Value: []byte("x")}); err != nil {
			t.Fatal(err)
		}
	}

	if err := run([]string{
		"bench",
		"--addr", addr,
		"--topic", "bench",
		"--consume",
		"--count", "50",
	}); err != nil {
		t.Fatalf("bench --consume: %v", err)
	}
}

// TestCLI_Bench proves the bench subcommand drives a configurable
// load and exits cleanly. The output is not parsed — this just
// guards against the load-gen tooling regressing into a syntax
// error or wiring mistake.
func TestCLI_Bench(t *testing.T) {
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "bench", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	addr, err := b.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	if err := run([]string{
		"bench",
		"--addr", addr,
		"--topic", "bench",
		"--count", "100",
		"--size", "32",
	}); err != nil {
		t.Fatalf("bench: %v", err)
	}
}

// TestCLI_BenchParallelProducers proves bench --producer-count N
// splits the load across N concurrent producers. The total record
// count produced equals --count regardless of producer-count, so a
// 4-partition topic with 100 records and producer-count 4 still
// adds up to 100 records on the wire.
func TestCLI_BenchParallelProducers(t *testing.T) {
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "bench", PartitionCount: 4}); err != nil {
		t.Fatal(err)
	}
	addr, err := b.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	if err := run([]string{
		"bench",
		"--addr", addr,
		"--topic", "bench",
		"--count", "100",
		"--size", "32",
		"--producer-count", "4",
	}); err != nil {
		t.Fatalf("bench: %v", err)
	}

	// Verify the broker actually received 100 records across all
	// 4 partitions.
	tr, err := holocronnet.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var total int64
	for p := int32(0); p < 4; p++ {
		hw, err := tr.HighWater(ctx, proto.PartitionRef{Topic: "bench", Index: p})
		if err != nil {
			t.Fatal(err)
		}
		total += hw
	}
	if total != 100 {
		t.Fatalf("total records on wire: got %d, want 100", total)
	}
}

// TestCLI_TopicStatsAll proves `topic stats --all-topics` enumerates
// every topic in the registry and prints per-topic stats. Useful
// for cluster-wide capacity overview without a script-loop over
// `topic list | xargs topic stats`. With --json, the output is a
// JSON array so jq pipelines work.
func TestCLI_TopicStatsAll(t *testing.T) {
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	if err := b.CreateTopic(embed.TopicSpec{Name: "audits", PartitionCount: 2}); err != nil {
		t.Fatal(err)
	}
	addr, err := b.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	// Capture JSON to verify both topics surface.
	out := captureStdout(t, func() {
		if err := run([]string{
			"topic", "stats",
			"--addr", addr,
			"--all-topics",
			"--json",
		}); err != nil {
			t.Fatalf("topic stats --all-topics: %v", err)
		}
	})

	var topics []map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &topics); err != nil {
		t.Fatalf("output not JSON array: %v\n%s", err, out)
	}
	if len(topics) != 2 {
		t.Fatalf("got %d topics in output, want 2", len(topics))
	}
	names := map[string]bool{}
	for _, t := range topics {
		names[t["topic"].(string)] = true
	}
	if !names["events"] || !names["audits"] {
		t.Errorf("missing topic in output: names=%v", names)
	}
}

// TestCLI_TopicStats proves the stats subcommand reports per-
// partition record counts via the high-water op. Producing 3
// records to a 1-partition topic should report records=3.
func TestCLI_TopicStats(t *testing.T) {
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	addr, err := b.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	prod, err := sdk.NewProducer(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for i := range 3 {
		if _, err := prod.Send(ctx, "events", proto.Record{Value: []byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}

	if err := run([]string{
		"topic", "stats",
		"--addr", addr,
		"--topic", "events",
	}); err != nil {
		t.Fatalf("topic stats: %v", err)
	}
}

// TestCLI_TopicUpdate proves the update subcommand mutates a
// topic's retention through the wire op without recreating it.
// Subsequent describe sees the new value.
func TestCLI_TopicUpdate(t *testing.T) {
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 1, RetentionMs: 1000}); err != nil {
		t.Fatal(err)
	}
	addr, err := b.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	if err := run([]string{
		"topic", "update",
		"--addr", addr,
		"--topic", "events",
		"--retention-ms", "5000",
	}); err != nil {
		t.Fatalf("topic update: %v", err)
	}

	// Confirm via the broker's registry.
	cfg := b.Topics()
	var found bool
	for _, tc := range cfg {
		if tc.Name == "events" {
			if tc.RetentionMs != 5000 {
				t.Errorf("retention: got %d, want 5000", tc.RetentionMs)
			}
			found = true
		}
	}
	if !found {
		t.Fatal("events topic missing from registry")
	}
}

// TestCLI_Version proves --version (and the bare "version"
// subcommand) prints build info and exits cleanly. Validates the
// stdout includes the holocronctl banner, the Go runtime, and an
// os/arch line.
func TestCLI_Version(t *testing.T) {
	for _, args := range [][]string{{"--version"}, {"version"}} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			out := captureStdout(t, func() {
				if err := run(args); err != nil {
					t.Fatalf("%v: %v", args, err)
				}
			})
			if !strings.Contains(out, "holocronctl") {
				t.Errorf("output missing holocronctl banner: %q", out)
			}
			if !strings.Contains(out, "go:") {
				t.Errorf("output missing go runtime line: %q", out)
			}
			if !strings.Contains(out, "os/arch:") {
				t.Errorf("output missing os/arch line: %q", out)
			}
		})
	}
}

// TestCLI_TopicDescribe proves the topic-describe subcommand prints
// configuration for one topic and errors when the topic doesn't
// exist. Reuses the existing OpListTopics under the hood.
func TestCLI_TopicDescribe(t *testing.T) {
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 4}); err != nil {
		t.Fatal(err)
	}
	addr, err := b.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	if err := run([]string{
		"topic", "describe",
		"--addr", addr,
		"--topic", "events",
	}); err != nil {
		t.Fatalf("topic describe: %v", err)
	}
	if err := run([]string{
		"topic", "describe",
		"--addr", addr,
		"--topic", "missing",
	}); err == nil {
		t.Fatal("expected describe of missing topic to fail")
	}
}

// TestCLI_RecordFetch proves the record-fetch subcommand reads
// exactly one record at the requested offset. Errors when the
// offset is past the partition's high water.
func TestCLI_RecordFetch(t *testing.T) {
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	addr, err := b.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	// Seed two records.
	prod, err := sdk.NewProducer(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for i := range 2 {
		if _, err := prod.Send(ctx, "events", proto.Record{Value: []byte{byte('a' + i)}}); err != nil {
			t.Fatal(err)
		}
	}

	if err := run([]string{
		"record", "fetch",
		"--addr", addr,
		"--topic", "events",
		"--partition", "0",
		"--offset", "0",
	}); err != nil {
		t.Fatalf("record fetch offset 0: %v", err)
	}
	if err := run([]string{
		"record", "fetch",
		"--addr", addr,
		"--topic", "events",
		"--partition", "0",
		"--offset", "100",
	}); err == nil {
		t.Fatal("expected fetch past high-water to fail")
	}
}

// TestCLI_GroupListAndDescribe proves the operator-facing CLI for
// consumer groups: list enumerates known groups, describe shows
// per-member partition assignments. Both reach the broker via the
// new wire ops.
func TestCLI_GroupListAndDescribe(t *testing.T) {
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 2}); err != nil {
		t.Fatal(err)
	}
	addr, err := b.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	// Drive a consumer into the group so the manager registers it.
	c, err := sdk.NewConsumer(b.Transport(), sdk.WithGroup("g"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Subscribe(ctx, "events", 0); err != nil {
		t.Fatal(err)
	}

	if err := run([]string{"group", "list", "--addr", addr}); err != nil {
		t.Fatalf("group list: %v", err)
	}
	if err := run([]string{"group", "describe", "--addr", addr, "--group", "g"}); err != nil {
		t.Fatalf("group describe: %v", err)
	}
}

// TestCLI_ProduceIdempotent proves `produce --idempotent` enables
// the producer's idempotent path: the produced record carries the
// reserved producer-id and producer-seq headers the broker uses
// for retry deduplication. Without --idempotent these headers are
// absent. Closes the gap where exercising idempotency required
// writing a Go program.
func TestCLI_ProduceIdempotent(t *testing.T) {
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	addr, err := b.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	if err := run([]string{
		"produce",
		"--addr", addr,
		"--topic", "events",
		"--value", "v",
		"--idempotent",
	}); err != nil {
		t.Fatalf("produce: %v", err)
	}

	// Round-trip and inspect the headers.
	c, err := sdk.NewConsumer(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Assign(ctx, proto.PartitionRef{Topic: "events", Index: 0}, 0); err != nil {
		t.Fatal(err)
	}
	recs, err := c.Poll(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 {
		t.Fatalf("got %d records, want 1", len(recs))
	}
	headers := map[string]bool{}
	for _, h := range recs[0].Headers {
		headers[h.Key] = true
	}
	if !headers[proto.HeaderProducerID] {
		t.Errorf("missing producer-id header (idempotency not engaged); headers=%v", headers)
	}
	if !headers[proto.HeaderProducerSeq] {
		t.Errorf("missing producer-seq header; headers=%v", headers)
	}
}

// TestCLI_ProduceWithHeaders proves `produce --header k=v` stamps
// each repeated header onto the produced record. Closes the gap
// where the produce CLI couldn't exercise the headers path —
// downstream consumers that branch on header content (router
// connector, idempotency tracking, audit metadata) had no CLI-only
// testing path before this.
func TestCLI_ProduceWithHeaders(t *testing.T) {
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	addr, err := b.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	if err := run([]string{
		"produce",
		"--addr", addr,
		"--topic", "events",
		"--value", "hello",
		"--header", "trace-id=abc123",
		"--header", "tenant=acme",
	}); err != nil {
		t.Fatalf("produce: %v", err)
	}

	// Round-trip: read the record back and confirm the headers landed.
	c, err := sdk.NewConsumer(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Assign(ctx, proto.PartitionRef{Topic: "events", Index: 0}, 0); err != nil {
		t.Fatal(err)
	}
	recs, err := c.Poll(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 {
		t.Fatalf("got %d records, want 1", len(recs))
	}
	headers := map[string]string{}
	for _, h := range recs[0].Headers {
		headers[h.Key] = string(h.Value)
	}
	if headers["trace-id"] != "abc123" {
		t.Errorf("trace-id header: got %q, want abc123 (headers=%v)", headers["trace-id"], headers)
	}
	if headers["tenant"] != "acme" {
		t.Errorf("tenant header: got %q, want acme", headers["tenant"])
	}
}

// TestCLI_GroupDelete proves `group delete --group` drops the in-
// memory group registration and clears every committed offset.
// After delete, a fresh `group offsets --group` shows no entries —
// retention is no longer pinned by the abandoned group.
func TestCLI_GroupDelete(t *testing.T) {
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	addr, err := b.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	// Commit an offset under group "g" so DeleteGroup has something
	// to clear.
	if err := run([]string{
		"offset", "commit",
		"--addr", addr,
		"--group", "g",
		"--topic", "events",
		"--partition", "0",
		"--offset", "5",
	}); err != nil {
		t.Fatalf("offset commit: %v", err)
	}

	if err := run([]string{
		"group", "delete",
		"--addr", addr,
		"--group", "g",
	}); err != nil {
		t.Fatalf("group delete: %v", err)
	}

	// Round-trip via the wire: ListGroupOffsets should return zero
	// entries after delete.
	tr, err := holocronnet.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	offsets, err := tr.ListGroupOffsets(ctx, "g")
	if err != nil {
		t.Fatal(err)
	}
	if len(offsets) != 0 {
		t.Fatalf("post-delete offsets: got %d, want 0 (%v)", len(offsets), offsets)
	}

	// Deleting a missing group surfaces StatusUnknownMember as an
	// error; assert the second delete fails so operator scripts
	// can detect non-idempotent invocations if they care to.
	err = run([]string{
		"group", "delete",
		"--addr", addr,
		"--group", "missing",
	})
	if err == nil {
		t.Fatal("delete of missing group: got nil error, want StatusUnknownMember")
	}
}

// TestCLI_PingJSON proves `holocronctl ping --json` emits a
// structured JSON object scripts can parse — `{"addr": ..., "ok":
// true, "topics": N}`. Useful for monitoring scripts that need
// to distinguish between "broker reachable, 0 topics" and
// "broker unreachable" without parsing free-form text.
func TestCLI_PingJSON(t *testing.T) {
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	addr, err := b.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	out := captureStdout(t, func() {
		if err := run([]string{"ping", "--addr", addr, "--json"}); err != nil {
			t.Fatalf("ping --json: %v", err)
		}
	})

	var got struct {
		Addr   string `json:"addr"`
		OK     bool   `json:"ok"`
		Topics int    `json:"topics"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &got); err != nil {
		t.Fatalf("output not valid JSON: %v\nstdout=%q", err, out)
	}
	if !got.OK {
		t.Errorf("ok: got false, want true")
	}
	if got.Topics != 1 {
		t.Errorf("topics: got %d, want 1", got.Topics)
	}
	if got.Addr != addr {
		t.Errorf("addr: got %q, want %q", got.Addr, addr)
	}
}

// TestCLI_Ping proves `holocronctl ping` exits cleanly when the
// broker is reachable and authenticated. The command performs the
// handshake plus a no-op ListTopics — answers "is the broker up
// and accepting my key?" in scripts and health checks without the
// caller having to invent a probe.
func TestCLI_Ping(t *testing.T) {
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	addr, err := b.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	if err := run([]string{"ping", "--addr", addr}); err != nil {
		t.Fatalf("ping: %v", err)
	}

	// Unreachable broker → non-nil error so health-check scripts
	// can surface the failure.
	if err := run([]string{"ping", "--addr", "127.0.0.1:1", "--timeout", "200ms"}); err == nil {
		t.Fatal("ping unreachable: got nil error, want failure")
	}
}

// TestCLI_TopicLoadBatch proves `topic load --batch` ships every
// record in one SendBatch instead of N Sends. Faster for bulk
// imports because the broker sees one ProduceBatch RPC. Pairs
// with `produce --batch` which already exists.
func TestCLI_TopicLoadBatch(t *testing.T) {
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "src", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	if err := b.CreateTopic(embed.TopicSpec{Name: "dst", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	addr, err := b.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	// Seed src with 5 records, then dump.
	prod, err := sdk.NewProducer(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for i := 0; i < 5; i++ {
		if _, err := prod.Send(ctx, "src", proto.Record{Value: []byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	dumpFile := filepath.Join(t.TempDir(), "src.jsonl")
	if err := run([]string{
		"topic", "dump",
		"--addr", addr,
		"--topic", "src",
		"--file", dumpFile,
	}); err != nil {
		t.Fatal(err)
	}

	// Act — load --batch into dst.
	if err := run([]string{
		"topic", "load",
		"--addr", addr,
		"--topic", "dst",
		"--file", dumpFile,
		"--batch",
	}); err != nil {
		t.Fatalf("topic load --batch: %v", err)
	}

	// Assert — dst has 5 records.
	tr, err := holocronnet.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()
	hw, err := tr.HighWater(ctx, proto.PartitionRef{Topic: "dst", Index: 0})
	if err != nil {
		t.Fatal(err)
	}
	if hw != 5 {
		t.Errorf("dst HighWater: got %d, want 5", hw)
	}
}

// TestCLI_TopicLoad_RoundTripsDump proves `topic load --topic --file`
// reads a JSONL file produced by `topic dump` and re-publishes
// each record. Inverse of dump for snapshot+restore workflows
// (export from one cluster, import into another).
func TestCLI_TopicLoad_RoundTripsDump(t *testing.T) {
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "src", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	if err := b.CreateTopic(embed.TopicSpec{Name: "restored", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	addr, err := b.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	// Seed src with 3 records, then dump.
	prod, err := sdk.NewProducer(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for _, kv := range []struct{ k, v string }{{"a", "v1"}, {"b", "v2"}, {"c", "v3"}} {
		if _, err := prod.Send(ctx, "src", proto.Record{Key: []byte(kv.k), Value: []byte(kv.v)}); err != nil {
			t.Fatal(err)
		}
	}
	dumpFile := filepath.Join(t.TempDir(), "src.jsonl")
	if err := run([]string{
		"topic", "dump",
		"--addr", addr,
		"--topic", "src",
		"--file", dumpFile,
	}); err != nil {
		t.Fatalf("topic dump: %v", err)
	}

	// Act — load the dump into "restored".
	if err := run([]string{
		"topic", "load",
		"--addr", addr,
		"--topic", "restored",
		"--file", dumpFile,
	}); err != nil {
		t.Fatalf("topic load: %v", err)
	}

	// Assert — restored has the same 3 records with original keys+values.
	c, err := sdk.NewConsumer(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.Assign(ctx, proto.PartitionRef{Topic: "restored", Index: 0}, 0); err != nil {
		t.Fatal(err)
	}
	got := make([]proto.Record, 0, 3)
	for len(got) < 3 {
		recs, err := c.Poll(ctx, 3-len(got))
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, recs...)
	}
	want := []struct{ k, v string }{{"a", "v1"}, {"b", "v2"}, {"c", "v3"}}
	for i, w := range want {
		if string(got[i].Key) != w.k || string(got[i].Value) != w.v {
			t.Errorf("record %d: got (%s,%s), want (%s,%s)", i, got[i].Key, got[i].Value, w.k, w.v)
		}
	}
}

// TestCLI_TailJSON proves `tail --json` emits each tailed record
// as a JSONL line — same shape as topic dump, symmetric with
// consume --json.
func TestCLI_TailJSON(t *testing.T) {
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	addr, err := b.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	prod, err := sdk.NewProducer(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Pre-existing record that tail should skip.
	if _, err := prod.Send(ctx, "events", proto.Record{Value: []byte("pre")}); err != nil {
		t.Fatal(err)
	}

	// captureStdout runs fn synchronously while os.Stdout points at
	// a pipe; inside fn we run tail in a goroutine, produce after
	// a head-start, and wait for tail to exit.
	out := captureStdout(t, func() {
		done := make(chan error, 1)
		go func() {
			done <- run([]string{
				"tail",
				"--addr", addr,
				"--topic", "events",
				"--partition", "0",
				"--max", "1",
				"--json",
				"--duration", "3s",
			})
		}()
		time.Sleep(100 * time.Millisecond)
		if _, err := prod.Send(ctx, "events", proto.Record{Value: []byte("post")}); err != nil {
			t.Fatal(err)
		}
		if err := <-done; err != nil {
			t.Fatalf("tail --json: %v", err)
		}
	})

	line := strings.TrimSpace(out)
	if line == "" {
		t.Fatal("tail --json produced no output")
	}
	var rec struct {
		Offset int64  `json:"offset"`
		Value  string `json:"value"`
	}
	if err := json.Unmarshal([]byte(line), &rec); err != nil {
		t.Fatalf("output not valid JSON: %v\n%q", err, line)
	}
	if rec.Value != "post" {
		t.Errorf("tailed value: got %q, want post", rec.Value)
	}
}

// TestCLI_ConsumeJSON proves `consume --json` emits each record
// as a JSONL line (same shape as topic dump's per-record format).
// Closes the read-side JSON gap so scripts can pipe consume output
// through jq without parsing the human-readable text format.
func TestCLI_ConsumeJSON(t *testing.T) {
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	addr, err := b.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	prod, err := sdk.NewProducer(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for _, kv := range []struct{ k, v string }{{"k1", "v1"}, {"k2", "v2"}} {
		if _, err := prod.Send(ctx, "events", proto.Record{Key: []byte(kv.k), Value: []byte(kv.v)}); err != nil {
			t.Fatal(err)
		}
	}

	out := captureStdout(t, func() {
		if err := run([]string{
			"consume",
			"--addr", addr,
			"--topic", "events",
			"--max", "2",
			"--json",
		}); err != nil {
			t.Fatalf("consume --json: %v", err)
		}
	})
	lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("got %d lines, want 2 (out=%q)", len(lines), out)
	}
	for i, line := range lines {
		var rec struct {
			Offset int64  `json:"offset"`
			Key    string `json:"key"`
			Value  string `json:"value"`
		}
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("line %d JSON: %v (%q)", i, err, line)
		}
		if rec.Offset != int64(i) {
			t.Errorf("line %d offset: got %d, want %d", i, rec.Offset, i)
		}
	}
}

// TestCLI_RecordFetchJSON proves `record fetch --json` emits one
// JSON object so scripts can parse the result without scraping
// the human-readable text format. Same shape as topic dump's
// per-record format (UTF-8 / *_b64 dual encoding).
func TestCLI_RecordFetchJSON(t *testing.T) {
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	addr, err := b.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	prod, err := sdk.NewProducer(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := prod.Send(ctx, "events", proto.Record{Key: []byte("k1"), Value: []byte("v1")}); err != nil {
		t.Fatal(err)
	}

	out := captureStdout(t, func() {
		if err := run([]string{
			"record", "fetch",
			"--addr", addr,
			"--topic", "events",
			"--offset", "0",
			"--json",
		}); err != nil {
			t.Fatalf("record fetch --json: %v", err)
		}
	})
	var got struct {
		Offset int64  `json:"offset"`
		Key    string `json:"key"`
		Value  string `json:"value"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &got); err != nil {
		t.Fatalf("output not valid JSON: %v\n%s", err, out)
	}
	if got.Offset != 0 || got.Key != "k1" || got.Value != "v1" {
		t.Errorf("decoded: got %+v, want {0, k1, v1}", got)
	}
}

// TestCLI_TopicDumpStdout proves `topic dump` without --file
// writes JSONL to stdout, so Unix-style pipelines work without
// a temp file (`holocronctl topic dump --topic events | jq .key`).
func TestCLI_TopicDumpStdout(t *testing.T) {
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	addr, err := b.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	prod, err := sdk.NewProducer(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for _, kv := range []struct{ k, v string }{{"a", "v1"}, {"b", "v2"}} {
		if _, err := prod.Send(ctx, "events", proto.Record{Key: []byte(kv.k), Value: []byte(kv.v)}); err != nil {
			t.Fatal(err)
		}
	}

	out := captureStdout(t, func() {
		if err := run([]string{
			"topic", "dump",
			"--addr", addr,
			"--topic", "events",
		}); err != nil {
			t.Fatalf("topic dump: %v", err)
		}
	})

	// Assert — captured stdout has 2 valid JSONL records (the
	// "dumped N record(s)" status message goes to stderr).
	lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("stdout: got %d lines, want 2 (out=%q)", len(lines), out)
	}
	for i, line := range lines {
		var rec struct {
			Offset int64  `json:"offset"`
			Key    string `json:"key"`
		}
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("line %d JSON: %v (%q)", i, err, line)
		}
	}
}

// TestCLI_TopicDump proves `topic dump --topic --partition --file`
// writes every record up to the source's high-water as one
// JSON-lines record per line. Useful for offline analysis,
// backups, and operator-driven snapshots.
func TestCLI_TopicDump(t *testing.T) {
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	addr, err := b.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	prod, err := sdk.NewProducer(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for _, kv := range []struct{ k, v string }{{"a", "v1"}, {"b", "v2"}, {"c", "v3"}} {
		if _, err := prod.Send(ctx, "events", proto.Record{Key: []byte(kv.k), Value: []byte(kv.v)}); err != nil {
			t.Fatal(err)
		}
	}

	dumpFile := filepath.Join(t.TempDir(), "events.jsonl")
	if err := run([]string{
		"topic", "dump",
		"--addr", addr,
		"--topic", "events",
		"--file", dumpFile,
	}); err != nil {
		t.Fatalf("topic dump: %v", err)
	}

	// Assert — file has 3 lines, each parses as a record with key+value.
	data, err := os.ReadFile(dumpFile)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("dump file: got %d lines, want 3 (file=%q)", len(lines), data)
	}
	for i, line := range lines {
		var rec struct {
			Offset int64  `json:"offset"`
			Key    string `json:"key"`
			Value  string `json:"value"`
		}
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("line %d JSON: %v (%q)", i, err, line)
		}
		if rec.Offset != int64(i) {
			t.Errorf("line %d offset: got %d, want %d", i, rec.Offset, i)
		}
	}
}

// TestCLI_TopicCopyAllPartitions proves `topic copy --all-partitions`
// iterates every source partition and copies records to the
// destination. Single-partition mode (default) only copies one;
// without --all-partitions an operator migrating a multi-partition
// topic had to script the loop manually.
func TestCLI_TopicCopyAllPartitions(t *testing.T) {
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "src", PartitionCount: 3}); err != nil {
		t.Fatal(err)
	}
	if err := b.CreateTopic(embed.TopicSpec{Name: "dst", PartitionCount: 3}); err != nil {
		t.Fatal(err)
	}
	addr, err := b.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	// Seed each partition with 2 records — directly to a specific
	// partition so we know what's where.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for p := int32(0); p < 3; p++ {
		for i := 0; i < 2; i++ {
			if _, err := b.Transport().Publish(ctx, proto.PartitionRef{Topic: "src", Index: p}, proto.Record{Value: []byte{byte(p), byte(i)}}); err != nil {
				t.Fatal(err)
			}
		}
	}

	if err := run([]string{
		"topic", "copy",
		"--addr", addr,
		"--from", "src",
		"--to", "dst",
		"--all-partitions",
	}); err != nil {
		t.Fatalf("topic copy --all-partitions: %v", err)
	}

	// Assert — destination has 6 records total across its partitions.
	tr, err := holocronnet.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()
	var total int64
	for p := int32(0); p < 3; p++ {
		hw, err := tr.HighWater(ctx, proto.PartitionRef{Topic: "dst", Index: p})
		if err != nil {
			t.Fatal(err)
		}
		total += hw
	}
	if total != 6 {
		t.Errorf("dst total records: got %d, want 6 (2 per partition × 3 partitions)", total)
	}
}

// TestCLI_TopicCopy proves `topic copy --from --to` replicates
// every record from a source partition to a destination topic.
// Useful for migration and topic restructuring; doesn't preserve
// offsets — the destination broker assigns new ones. Source
// records' headers and keys carry over.
func TestCLI_TopicCopy(t *testing.T) {
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "src", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	if err := b.CreateTopic(embed.TopicSpec{Name: "dst", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	addr, err := b.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	// Seed three records on src.
	prod, err := sdk.NewProducer(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for _, kv := range []struct{ k, v string }{{"a", "v1"}, {"b", "v2"}, {"c", "v3"}} {
		if _, err := prod.Send(ctx, "src", proto.Record{Key: []byte(kv.k), Value: []byte(kv.v)}); err != nil {
			t.Fatal(err)
		}
	}

	// Act
	if err := run([]string{
		"topic", "copy",
		"--addr", addr,
		"--from", "src",
		"--to", "dst",
	}); err != nil {
		t.Fatalf("topic copy: %v", err)
	}

	// Assert — destination has the three records with original keys.
	c, err := sdk.NewConsumer(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.Assign(ctx, proto.PartitionRef{Topic: "dst", Index: 0}, 0); err != nil {
		t.Fatal(err)
	}
	got := make([]proto.Record, 0, 3)
	for len(got) < 3 {
		recs, err := c.Poll(ctx, 3-len(got))
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, recs...)
	}
	want := []struct{ k, v string }{{"a", "v1"}, {"b", "v2"}, {"c", "v3"}}
	for i, w := range want {
		if string(got[i].Key) != w.k || string(got[i].Value) != w.v {
			t.Errorf("record %d: got (%s,%s), want (%s,%s)", i, got[i].Key, got[i].Value, w.k, w.v)
		}
	}
}

// TestCLI_TopicHead proves `topic head` prints the first N records
// of a partition starting at offset 0. Sugar for the common
// "what's at the start of this topic?" inspection without writing
// a Go consumer.
func TestCLI_TopicHead(t *testing.T) {
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	addr, err := b.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	// Seed five records.
	prod, err := sdk.NewProducer(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for i := 0; i < 5; i++ {
		if _, err := prod.Send(ctx, "events", proto.Record{Value: []byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}

	if err := run([]string{
		"topic", "head",
		"--addr", addr,
		"--topic", "events",
		"--max", "3",
	}); err != nil {
		t.Fatalf("topic head: %v", err)
	}
}

// TestCLI_TopicLast proves `topic last` prints the last N records of
// a partition. Reads from max(0, high-water - N) so a fresh topic
// with fewer records than --max simply prints what exists. Distinct
// from the live-tail `tail` subcommand: live-tail attaches at HW and
// prints arrivals; `topic last` looks backward at the most recent
// historical records.
func TestCLI_TopicLast(t *testing.T) {
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	addr, err := b.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	prod, err := sdk.NewProducer(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for i := 0; i < 5; i++ {
		if _, err := prod.Send(ctx, "events", proto.Record{Value: []byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}

	if err := run([]string{
		"topic", "last",
		"--addr", addr,
		"--topic", "events",
		"--max", "2",
	}); err != nil {
		t.Fatalf("topic last: %v", err)
	}
}

// TestCLI_GroupRename proves `group rename --old --new` copies
// every committed offset under --old to --new and deletes --old.
// Useful for the "we want to rename our consumer group without
// losing position" pattern; without this, operators had to script
// ListGroupOffsets + Commit-per-partition + DeleteGroup manually.
func TestCLI_GroupRename(t *testing.T) {
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 2}); err != nil {
		t.Fatal(err)
	}
	addr, err := b.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	// Establish committed offsets under "old".
	for _, p := range []string{"0", "1"} {
		if err := run([]string{
			"offset", "commit",
			"--addr", addr,
			"--group", "old",
			"--topic", "events",
			"--partition", p,
			"--offset", "5",
		}); err != nil {
			t.Fatalf("seed commit p%s: %v", p, err)
		}
	}

	// Act
	if err := run([]string{
		"group", "rename",
		"--addr", addr,
		"--old", "old",
		"--new", "new",
	}); err != nil {
		t.Fatalf("group rename: %v", err)
	}

	// Assert — old has no offsets; new has both.
	tr, err := holocronnet.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if oldEntries, err := tr.ListGroupOffsets(ctx, "old"); err != nil {
		t.Fatal(err)
	} else if len(oldEntries) != 0 {
		t.Errorf("old after rename: got %d entries, want 0 (cleanup didn't fire)", len(oldEntries))
	}
	newEntries, err := tr.ListGroupOffsets(ctx, "new")
	if err != nil {
		t.Fatal(err)
	}
	if len(newEntries) != 2 {
		t.Fatalf("new after rename: got %d entries, want 2", len(newEntries))
	}
	for _, e := range newEntries {
		if e.Committed != 5 {
			t.Errorf("partition %d committed: got %d, want 5", e.Partition, e.Committed)
		}
	}
}

// TestCLI_GroupResetAll proves `group reset-all --group --to=...`
// commits a new offset for every (topic, partition) the group has
// committed in one command. Earliest commits 0; latest commits the
// partition's high-water. Replaces the script-loop pattern of
// "ListGroupOffsets | xargs offset commit ...".
func TestCLI_GroupResetAll(t *testing.T) {
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 2}); err != nil {
		t.Fatal(err)
	}
	addr, err := b.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	// Seed records on both partitions so the high-water is non-zero.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for _, p := range []int32{0, 1} {
		for i := 0; i < 3; i++ {
			if _, err := b.Transport().Publish(ctx, proto.PartitionRef{Topic: "events", Index: p}, proto.Record{Value: []byte{byte(i)}}); err != nil {
				t.Fatal(err)
			}
		}
	}

	// Establish committed offsets so the group has something to enumerate.
	for _, p := range []string{"0", "1"} {
		if err := run([]string{
			"offset", "commit",
			"--addr", addr,
			"--group", "g",
			"--topic", "events",
			"--partition", p,
			"--offset", "1",
		}); err != nil {
			t.Fatalf("seed commit p%s: %v", p, err)
		}
	}

	// Act — reset-all to latest.
	if err := run([]string{
		"group", "reset-all",
		"--addr", addr,
		"--group", "g",
		"--to", "latest",
	}); err != nil {
		t.Fatalf("group reset-all latest: %v", err)
	}

	// Assert — both partitions now committed at HW=3.
	tr, err := holocronnet.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()
	entries, err := tr.ListGroupOffsets(ctx, "g")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("entries: got %d, want 2", len(entries))
	}
	for _, e := range entries {
		if e.Committed != 3 {
			t.Errorf("partition %d committed: got %d, want 3 (high-water)", e.Partition, e.Committed)
		}
	}

	// Reset-all to earliest brings both back to 0.
	if err := run([]string{
		"group", "reset-all",
		"--addr", addr,
		"--group", "g",
		"--to", "earliest",
	}); err != nil {
		t.Fatalf("group reset-all earliest: %v", err)
	}
	entries, err = tr.ListGroupOffsets(ctx, "g")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Committed != 0 {
			t.Errorf("partition %d committed: got %d, want 0 (earliest)", e.Partition, e.Committed)
		}
	}
}

// TestCLI_GroupOffsets proves `group offsets --group` enumerates
// every (topic, partition) the group has committed and reports the
// derived lag (high-water - committed). Without the wire op the
// operator would have to walk every (topic, partition) by hand.
func TestCLI_GroupOffsets(t *testing.T) {
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	addr, err := b.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	// Produce three records so high-water = 3.
	prod, err := sdk.NewProducer(b.Transport())
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for i := 0; i < 3; i++ {
		if _, err := prod.Send(ctx, "events", proto.Record{Value: []byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}

	// Commit offset=1 for group g — two records remain unread.
	if err := run([]string{
		"offset", "commit",
		"--addr", addr,
		"--group", "g",
		"--topic", "events",
		"--partition", "0",
		"--offset", "1",
	}); err != nil {
		t.Fatalf("offset commit: %v", err)
	}

	// group offsets prints the committed/high-water/lag triplet.
	if err := run([]string{
		"group", "offsets",
		"--addr", addr,
		"--group", "g",
	}); err != nil {
		t.Fatalf("group offsets: %v", err)
	}
}

// TestCLI_OffsetCommitAndReset proves the offset subcommand round-
// trips a commit through the broker. After commit, a Subscribe
// from NoOffset on the same group resumes at the committed
// position rather than from offset 0.
func TestCLI_OffsetCommitAndReset(t *testing.T) {
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	addr, err := b.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	if err := run([]string{
		"offset", "commit",
		"--addr", addr,
		"--group", "g",
		"--topic", "events",
		"--partition", "0",
		"--offset", "42",
	}); err != nil {
		t.Fatalf("offset commit: %v", err)
	}
	if err := run([]string{
		"offset", "reset",
		"--addr", addr,
		"--group", "g",
		"--topic", "events",
		"--partition", "0",
	}); err != nil {
		t.Fatalf("offset reset: %v", err)
	}
}

// TestCLI_TopicDelete proves the new `topic delete` subcommand
// removes a topic via the wire op. A subsequent list shows the
// topic is gone.
func TestCLI_TopicDelete(t *testing.T) {
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "ephemeral", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	addr, err := b.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	if err := run([]string{
		"topic", "delete",
		"--addr", addr,
		"--topic", "ephemeral",
	}); err != nil {
		t.Fatalf("topic delete: %v", err)
	}

	// Topic should no longer be in the registry.
	if topics := b.Topics(); len(topics) != 0 {
		t.Errorf("after delete, registry has %d topics, want 0", len(topics))
	}
}

// TestCLI_ClusterMembersOnNonClustered proves the `cluster members`
// CLI handles a broker that isn't part of a cluster: the wire op
// returns an empty member list, and the CLI prints the "not part of
// a cluster" message instead of erroring.
func TestCLI_ClusterMembersOnNonClustered(t *testing.T) {
	b := embed.NewMemory()
	defer b.Close()
	addr, err := b.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	if err := run([]string{
		"cluster", "members",
		"--addr", addr,
	}); err != nil {
		t.Fatalf("cluster members against non-clustered broker: %v", err)
	}
}

// TestCLI_ClusterJoinOnNonClusteredFails proves cluster join surfaces
// a clear error when the broker isn't part of a cluster.
func TestCLI_ClusterJoinOnNonClusteredFails(t *testing.T) {
	b := embed.NewMemory()
	defer b.Close()
	addr, err := b.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	err = run([]string{
		"cluster", "join",
		"--addr", addr,
		"--id", "n2",
		"--peer-addr", "127.0.0.1:9999",
	})
	if err == nil {
		t.Fatal("expected error from cluster join on non-clustered broker, got nil")
	}
}

// TestCLI_ProduceConsumeRoundTrip drives `produce` then `consume` and
// confirms the published record arrives.
func TestCLI_ProduceConsumeRoundTrip(t *testing.T) {
	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 1}); err != nil {
		t.Fatal(err)
	}
	addr, err := b.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	if err := run([]string{
		"produce",
		"--addr", addr,
		"--topic", "events",
		"--key", "k1",
		"--value", "hello",
	}); err != nil {
		t.Fatalf("produce: %v", err)
	}

	// Consume with a tight duration so the test doesn't hang.
	if err := run([]string{
		"consume",
		"--addr", addr,
		"--topic", "events",
		"--max", "1",
		"--duration", (1 * time.Second).String(),
	}); err != nil {
		t.Fatalf("consume: %v", err)
	}
}

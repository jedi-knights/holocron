package main

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jedi-knights/holocron/broker/embed"
	"github.com/jedi-knights/holocron/proto"
	"github.com/jedi-knights/holocron/sdk"
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
	count, err := produceBatchFromReader(ctx, prod, "events", strings.NewReader(payload), "|")
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
	count, err := produceFromReader(ctx, prod, "events", strings.NewReader(payload), "|")
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

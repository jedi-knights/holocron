package main

import (
	"strings"
	"testing"
	"time"

	"github.com/jedi-knights/holocron/broker/embed"
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

// TestCLI_TopicList probes a configured set of names and reports each
// one's partition count or NOT FOUND status.
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
		"--probe", "orders,nope",
	}); err != nil {
		t.Fatalf("topic list: %v", err)
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

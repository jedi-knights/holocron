package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/jedi-knights/holocron/proto"
	holocronnet "github.com/jedi-knights/holocron/sdk/net"
)

// runTopic dispatches `topic <subcommand>`.
func runTopic(args []string) error {
	if len(args) == 0 {
		return errors.New("topic: subcommand required (copy | create | delete | describe | dump | head | last | list | load | stats | update)")
	}
	switch args[0] {
	case "copy":
		return runTopicCopy(args[1:])
	case "create":
		return runTopicCreate(args[1:])
	case "delete":
		return runTopicDelete(args[1:])
	case "describe":
		return runTopicDescribe(args[1:])
	case "dump":
		return runTopicDump(args[1:])
	case "head":
		return runTopicHead(args[1:])
	case "last":
		return runTopicLast(args[1:])
	case "list":
		return runTopicList(args[1:])
	case "load":
		return runTopicLoad(args[1:])
	case "stats":
		return runTopicStats(args[1:])
	case "update":
		return runTopicUpdate(args[1:])
	}
	return fmt.Errorf("topic: unknown subcommand %q", args[0])
}

// runTopicStats prints per-partition record counts for a topic.
// Counts are derived from each partition's high-water (the
// next-to-be-appended offset). Useful for capacity planning,
// load-balance inspection, and confirming production lands where
// expected. Byte-size estimates need broker-side segment stats
// that don't exist yet — listed as "n/a" for now.
func runTopicStats(args []string) error {
	fs := flag.NewFlagSet("topic stats", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:9092", "broker address")
	apiKey := fs.String("api-key", "", "API key for handshake")
	name := fs.String("topic", "", "topic name (required unless --all-topics)")
	allTopics := fs.Bool("all-topics", false, "report stats for every topic in the registry")
	jsonOut := fs.Bool("json", false, "emit machine-readable JSON")
	timeout := fs.Duration("timeout", 5*time.Second, "RPC timeout")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if !*allTopics && *name == "" {
		return errors.New("topic stats: --topic is required (or pass --all-topics)")
	}

	tr, err := dial(*addr, *apiKey)
	if err != nil {
		return err
	}
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	if *allTopics {
		topics, err := tr.ListTopics(ctx)
		if err != nil {
			return fmt.Errorf("list topics: %w", err)
		}
		out := make([]map[string]any, 0, len(topics))
		for _, t := range topics {
			s, total, err := topicStatsFor(ctx, tr, t.Name, t.PartitionCount)
			if err != nil {
				return err
			}
			out = append(out, map[string]any{
				"topic":      t.Name,
				"partitions": s,
				"total":      total,
			})
		}
		if *jsonOut {
			return printJSON(out)
		}
		for _, entry := range out {
			fmt.Printf("topic: %s (%d record(s) total)\n", entry["topic"], entry["total"])
		}
		return nil
	}

	parts, err := tr.PartitionsFor(ctx, *name)
	if err != nil {
		return fmt.Errorf("partitions for %q: %w", *name, err)
	}
	stats, total, err := topicStatsFor(ctx, tr, *name, parts)
	if err != nil {
		return err
	}

	if *jsonOut {
		return printJSON(map[string]any{
			"topic":      *name,
			"partitions": stats,
			"total":      total,
		})
	}
	fmt.Printf("topic: %s (%d partition(s), %d record(s) total)\n", *name, parts, total)
	for _, s := range stats {
		fmt.Printf("  partition %d: %d record(s) (high-water %d)\n", s.Partition, s.Records, s.HighWater)
	}
	return nil
}

// topicPartitionStat is the per-partition row returned by topicStatsFor.
type topicPartitionStat struct {
	Partition int32 `json:"partition"`
	HighWater int64 `json:"high_water"`
	Records   int64 `json:"records"`
}

// topicStatsFor walks every partition of a topic and collects
// per-partition record counts derived from each high-water. Shared
// by the single-topic and --all-topics paths.
func topicStatsFor(ctx context.Context, tr *holocronnet.Transport, name string, parts int32) ([]topicPartitionStat, int64, error) {
	stats := make([]topicPartitionStat, 0, parts)
	var total int64
	for i := int32(0); i < parts; i++ {
		hw, err := tr.HighWater(ctx, proto.PartitionRef{Topic: name, Index: i})
		if err != nil {
			return nil, 0, fmt.Errorf("high-water %s/%d: %w", name, i, err)
		}
		stats = append(stats, topicPartitionStat{Partition: i, HighWater: hw, Records: hw})
		total += hw
	}
	return stats, total, nil
}

// runTopicUpdate changes a topic's retention and/or segment-size
// settings without recreating it. --retention-ms and --segment-bytes
// default to 0 ("no change") so an operator can adjust one knob at
// a time. Partition count is immutable — recreate the topic if you
// need a different partition count.
func runTopicUpdate(args []string) error {
	fs := flag.NewFlagSet("topic update", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:9092", "broker address")
	apiKey := fs.String("api-key", "", "API key for handshake")
	name := fs.String("topic", "", "topic name (required)")
	retentionMs := fs.Int64("retention-ms", 0, "new retention in ms (0 = no change)")
	segmentBytes := fs.Int64("segment-bytes", 0, "new segment size in bytes (0 = no change)")
	timeout := fs.Duration("timeout", 5*time.Second, "RPC timeout")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *name == "" {
		return errors.New("topic update: --topic is required")
	}
	if *retentionMs <= 0 && *segmentBytes <= 0 {
		return errors.New("topic update: at least one of --retention-ms / --segment-bytes is required")
	}

	tr, err := dial(*addr, *apiKey)
	if err != nil {
		return err
	}
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	if err := tr.UpdateTopicConfig(ctx, *name, *retentionMs, *segmentBytes); err != nil {
		return fmt.Errorf("update topic %q: %w", *name, err)
	}
	fmt.Printf("topic %q updated", *name)
	if *retentionMs > 0 {
		fmt.Printf(" retention=%dms", *retentionMs)
	}
	if *segmentBytes > 0 {
		fmt.Printf(" segment=%dbytes", *segmentBytes)
	}
	fmt.Println()
	return nil
}

// runTopicDescribe prints the full TopicConfig for one topic —
// partition count, retention, and segment size. Reuses
// OpListTopics under the hood and filters to the requested name.
func runTopicDescribe(args []string) error {
	fs := flag.NewFlagSet("topic describe", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:9092", "broker address")
	apiKey := fs.String("api-key", "", "API key for handshake")
	name := fs.String("topic", "", "topic name (required)")
	jsonOut := fs.Bool("json", false, "emit machine-readable JSON")
	timeout := fs.Duration("timeout", 5*time.Second, "RPC timeout")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *name == "" {
		return errors.New("topic describe: --topic is required")
	}

	tr, err := dial(*addr, *apiKey)
	if err != nil {
		return err
	}
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	topics, err := tr.ListTopics(ctx)
	if err != nil {
		return fmt.Errorf("list topics: %w", err)
	}
	for _, t := range topics {
		if t.Name != *name {
			continue
		}
		if *jsonOut {
			return printJSON(t)
		}
		fmt.Printf("name:       %s\n", t.Name)
		fmt.Printf("partitions: %d\n", t.PartitionCount)
		if t.RetentionMs > 0 {
			fmt.Printf("retention:  %dms\n", t.RetentionMs)
		} else {
			fmt.Printf("retention:  (unbounded)\n")
		}
		if t.SegmentBytes > 0 {
			fmt.Printf("segment:    %d bytes\n", t.SegmentBytes)
		} else {
			fmt.Printf("segment:    (default)\n")
		}
		return nil
	}
	return fmt.Errorf("topic %q not found", *name)
}

// runTopicDelete removes the named topic and every record on it.
// Destructive — there is no undo. Cluster-mode brokers route the
// command through Raft so every node converges; followers redirect
// to the leader transparently.
func runTopicDelete(args []string) error {
	fs := flag.NewFlagSet("topic delete", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:9092", "broker address")
	apiKey := fs.String("api-key", "", "API key for handshake (empty = no auth)")
	name := fs.String("topic", "", "topic name (required)")
	timeout := fs.Duration("timeout", 5*time.Second, "RPC timeout")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *name == "" {
		return errors.New("topic delete: --topic is required")
	}

	tr, err := dial(*addr, *apiKey)
	if err != nil {
		return err
	}
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	if err := tr.DeleteTopic(ctx, *name); err != nil {
		return fmt.Errorf("delete topic %q: %w", *name, err)
	}
	fmt.Printf("topic %q deleted\n", *name)
	return nil
}

func runTopicCreate(args []string) error {
	fs := flag.NewFlagSet("topic create", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:9092", "broker address")
	apiKey := fs.String("api-key", "", "API key for handshake (empty = no auth)")
	name := fs.String("topic", "", "topic name (required)")
	partitions := fs.Int("partitions", 1, "partition count")
	timeout := fs.Duration("timeout", 5*time.Second, "RPC timeout")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *name == "" {
		return errors.New("topic create: --topic is required")
	}

	tr, err := dial(*addr, *apiKey)
	if err != nil {
		return err
	}
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	if err := tr.EnsureTopic(ctx, *name, int32(*partitions)); err != nil {
		return fmt.Errorf("ensure topic %q: %w", *name, err)
	}
	fmt.Printf("topic %q ready (%d partition(s))\n", *name, *partitions)
	return nil
}

func runTopicList(args []string) error {
	fs := flag.NewFlagSet("topic list", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:9092", "broker address")
	apiKey := fs.String("api-key", "", "API key for handshake")
	jsonOut := fs.Bool("json", false, "emit machine-readable JSON")
	timeout := fs.Duration("timeout", 5*time.Second, "RPC timeout")
	if err := fs.Parse(args); err != nil {
		return err
	}

	tr, err := dial(*addr, *apiKey)
	if err != nil {
		return err
	}
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	topics, err := tr.ListTopics(ctx)
	if err != nil {
		return fmt.Errorf("list topics: %w", err)
	}
	if *jsonOut {
		return printJSON(topics)
	}
	if len(topics) == 0 {
		fmt.Println("(no topics)")
		return nil
	}
	for _, t := range topics {
		fmt.Printf("%s\t%d partition(s)\n", t.Name, t.PartitionCount)
	}
	return nil
}

// dial opens a network transport with optional API-key auth. The
// CLI's shared connection helper.
func dial(addr, apiKey string) (*holocronnet.Transport, error) {
	if apiKey != "" {
		return holocronnet.Dial(addr, holocronnet.WithAPIKey(apiKey))
	}
	return holocronnet.Dial(addr)
}

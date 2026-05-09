package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/jedi-knights/holocron/proto"
)

// runGroup dispatches `group <subcommand>`.
func runGroup(args []string) error {
	if len(args) == 0 {
		return errors.New("group: subcommand required (list | describe | offsets | delete | reset-all | rename)")
	}
	switch args[0] {
	case "list":
		return runGroupList(args[1:])
	case "describe":
		return runGroupDescribe(args[1:])
	case "offsets":
		return runGroupOffsets(args[1:])
	case "delete":
		return runGroupDelete(args[1:])
	case "reset-all":
		return runGroupResetAll(args[1:])
	case "rename":
		return runGroupRename(args[1:])
	}
	return fmt.Errorf("group: unknown subcommand %q", args[0])
}

// runGroupList enumerates every consumer group registered with the
// broker's group manager and prints one line per group with its
// generation, member count, and subscribed topics.
func runGroupList(args []string) error {
	fs := flag.NewFlagSet("group list", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:9092", "broker address")
	apiKey := fs.String("api-key", "", "API key for handshake (empty = no auth)")
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
	groups, err := tr.ListGroups(ctx)
	if err != nil {
		return fmt.Errorf("list groups: %w", err)
	}
	if *jsonOut {
		return printJSON(groups)
	}
	if len(groups) == 0 {
		fmt.Println("(no groups)")
		return nil
	}
	for _, g := range groups {
		fmt.Printf("%s\tgen=%d\tmembers=%d\ttopics=%s\n",
			g.Name, g.Generation, g.MemberCount, strings.Join(g.Topics, ","))
	}
	return nil
}

// runGroupDescribe shows the per-member partition assignments for
// one consumer group. Useful for diagnosing rebalance state and
// uneven partition ownership.
func runGroupDescribe(args []string) error {
	fs := flag.NewFlagSet("group describe", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:9092", "broker address")
	apiKey := fs.String("api-key", "", "API key for handshake")
	group := fs.String("group", "", "group name (required)")
	jsonOut := fs.Bool("json", false, "emit machine-readable JSON")
	timeout := fs.Duration("timeout", 5*time.Second, "RPC timeout")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *group == "" {
		return errors.New("group describe: --group is required")
	}

	tr, err := dial(*addr, *apiKey)
	if err != nil {
		return err
	}
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	resp, err := tr.DescribeGroup(ctx, *group)
	if err != nil {
		return fmt.Errorf("describe group %q: %w", *group, err)
	}
	if *jsonOut {
		return printJSON(resp)
	}
	fmt.Printf("group %s (generation %d, topics=%s)\n",
		resp.Name, resp.Generation, strings.Join(resp.Topics, ","))
	if len(resp.Members) == 0 {
		fmt.Println("  (no members)")
		return nil
	}
	for _, m := range resp.Members {
		parts := make([]string, 0, len(m.Partitions))
		for _, p := range m.Partitions {
			parts = append(parts, fmt.Sprintf("%s:%d", p.Topic, p.Index))
		}
		fmt.Printf("  %s\t%s\n", m.MemberID, strings.Join(parts, ","))
	}
	return nil
}

// runGroupOffsets lists every (topic, partition) the named group has
// committed an offset on, alongside the partition's high-water and the
// derived lag (high-water - committed). Useful for spotting stuck
// consumers and right-sizing the consumer fleet — without lag there is
// no answer to "is this group keeping up?".
func runGroupOffsets(args []string) error {
	fs := flag.NewFlagSet("group offsets", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:9092", "broker address")
	apiKey := fs.String("api-key", "", "API key for handshake")
	group := fs.String("group", "", "group name (required)")
	jsonOut := fs.Bool("json", false, "emit machine-readable JSON")
	timeout := fs.Duration("timeout", 5*time.Second, "RPC timeout")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *group == "" {
		return errors.New("group offsets: --group is required")
	}

	tr, err := dial(*addr, *apiKey)
	if err != nil {
		return err
	}
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	entries, err := tr.ListGroupOffsets(ctx, *group)
	if err != nil {
		return fmt.Errorf("list group offsets %q: %w", *group, err)
	}
	if *jsonOut {
		// JSON shape includes the derived Lag so jq pipelines don't
		// need to recompute it.
		type entryJSON struct {
			Topic     string `json:"topic"`
			Partition int32  `json:"partition"`
			Committed int64  `json:"committed"`
			HighWater int64  `json:"high_water"`
			Lag       int64  `json:"lag"`
		}
		out := make([]entryJSON, 0, len(entries))
		for _, e := range entries {
			out = append(out, entryJSON{
				Topic:     e.Topic,
				Partition: e.Partition,
				Committed: e.Committed,
				HighWater: e.HighWater,
				Lag:       lagOf(e.Committed, e.HighWater),
			})
		}
		return printJSON(out)
	}
	if len(entries) == 0 {
		fmt.Printf("(group %q has no committed offsets)\n", *group)
		return nil
	}
	fmt.Printf("group %s\n", *group)
	for _, e := range entries {
		fmt.Printf("  %s/%d\tcommitted=%d\thigh-water=%d\tlag=%d\n",
			e.Topic, e.Partition, e.Committed, e.HighWater, lagOf(e.Committed, e.HighWater))
	}
	return nil
}

// runGroupDelete drops a consumer group from the broker and clears
// every committed offset under it. Cleanup workflow for abandoned
// groups whose committed offsets still pin retention but whose
// members are gone. Idempotent on the broker side: a missing group
// returns StatusUnknownMember which the CLI surfaces as an error
// the operator can inspect.
func runGroupDelete(args []string) error {
	fs := flag.NewFlagSet("group delete", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:9092", "broker address")
	apiKey := fs.String("api-key", "", "API key for handshake")
	group := fs.String("group", "", "group name (required)")
	timeout := fs.Duration("timeout", 5*time.Second, "RPC timeout")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *group == "" {
		return errors.New("group delete: --group is required")
	}

	tr, err := dial(*addr, *apiKey)
	if err != nil {
		return err
	}
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	if err := tr.DeleteGroup(ctx, *group); err != nil {
		return fmt.Errorf("delete group %q: %w", *group, err)
	}
	fmt.Printf("deleted group %q\n", *group)
	return nil
}

// runGroupResetAll commits a uniform offset for every (topic,
// partition) the group has touched. `--to=earliest` commits 0;
// `--to=latest` commits each partition's high-water (next
// to-be-appended offset). Replaces the script-loop pattern of
// "ListGroupOffsets | xargs offset commit ..." that was the only
// pre-batch-39 way to bulk-reset.
//
// The high-water comes from ListGroupOffsets's response — no extra
// per-partition HighWater calls.
func runGroupResetAll(args []string) error {
	fs := flag.NewFlagSet("group reset-all", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:9092", "broker address")
	apiKey := fs.String("api-key", "", "API key for handshake")
	group := fs.String("group", "", "group name (required)")
	to := fs.String("to", "", "earliest | latest (required)")
	timeout := fs.Duration("timeout", 10*time.Second, "RPC timeout (covers all partitions)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *group == "" {
		return errors.New("group reset-all: --group is required")
	}
	if *to != "earliest" && *to != "latest" {
		return errors.New("group reset-all: --to must be earliest or latest")
	}

	tr, err := dial(*addr, *apiKey)
	if err != nil {
		return err
	}
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	entries, err := tr.ListGroupOffsets(ctx, *group)
	if err != nil {
		return fmt.Errorf("list group offsets %q: %w", *group, err)
	}
	if len(entries) == 0 {
		fmt.Printf("(group %q has no committed offsets to reset)\n", *group)
		return nil
	}

	for _, e := range entries {
		newOffset := int64(0)
		if *to == "latest" {
			newOffset = e.HighWater
		}
		pref := proto.PartitionRef{Topic: e.Topic, Index: e.Partition}
		if err := tr.Commit(ctx, *group, pref, newOffset); err != nil {
			return fmt.Errorf("commit %s/%d: %w", e.Topic, e.Partition, err)
		}
		fmt.Printf("reset %s/%s/%d -> %d\n", *group, e.Topic, e.Partition, newOffset)
	}
	return nil
}

// runGroupRename copies every committed offset under --old to
// --new and deletes --old. The "rename my consumer group without
// losing position" pattern; without this, operators had to script
// ListGroupOffsets + Commit-per-partition + DeleteGroup themselves.
//
// Pure CLI orchestration over existing wire ops. The copy goes
// first; the delete only fires after every commit succeeds, so a
// failure mid-rename leaves both groups intact (caller can retry
// safely — Commit is idempotent for the same offset).
func runGroupRename(args []string) error {
	fs := flag.NewFlagSet("group rename", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:9092", "broker address")
	apiKey := fs.String("api-key", "", "API key for handshake")
	oldName := fs.String("old", "", "current group name (required)")
	newName := fs.String("new", "", "new group name (required)")
	timeout := fs.Duration("timeout", 10*time.Second, "RPC timeout (covers the full rename)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *oldName == "" || *newName == "" {
		return errors.New("group rename: --old and --new are required")
	}
	if *oldName == *newName {
		return errors.New("group rename: --old and --new must differ")
	}

	tr, err := dial(*addr, *apiKey)
	if err != nil {
		return err
	}
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	entries, err := tr.ListGroupOffsets(ctx, *oldName)
	if err != nil {
		return fmt.Errorf("list offsets %q: %w", *oldName, err)
	}
	if len(entries) == 0 {
		return fmt.Errorf("group rename: %q has no committed offsets to rename", *oldName)
	}

	for _, e := range entries {
		pref := proto.PartitionRef{Topic: e.Topic, Index: e.Partition}
		if err := tr.Commit(ctx, *newName, pref, e.Committed); err != nil {
			return fmt.Errorf("commit %s/%d to %q: %w", e.Topic, e.Partition, *newName, err)
		}
	}
	if err := tr.DeleteGroup(ctx, *oldName); err != nil {
		return fmt.Errorf("delete %q after copy: %w", *oldName, err)
	}
	fmt.Printf("renamed group %q -> %q (%d partitions)\n", *oldName, *newName, len(entries))
	return nil
}

// lagOf returns the records-behind count for one (committed,
// high-water) pair. If either is the -1 sentinel — uncommitted or
// missing partition — lag is -1 to make the unknown explicit rather
// than silently displaying a misleading value.
func lagOf(committed, highWater int64) int64 {
	if committed < 0 || highWater < 0 {
		return -1
	}
	return highWater - committed
}

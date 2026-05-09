package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"strings"
	"time"
)

// runGroup dispatches `group <subcommand>`.
func runGroup(args []string) error {
	if len(args) == 0 {
		return errors.New("group: subcommand required (list | describe)")
	}
	switch args[0] {
	case "list":
		return runGroupList(args[1:])
	case "describe":
		return runGroupDescribe(args[1:])
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

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"strings"
	"time"

	holocronnet "github.com/jedi-knights/holocron/sdk/net"
)

// runTopic dispatches `topic <subcommand>`.
func runTopic(args []string) error {
	if len(args) == 0 {
		return errors.New("topic: subcommand required (create | list)")
	}
	switch args[0] {
	case "create":
		return runTopicCreate(args[1:])
	case "list":
		return runTopicList(args[1:])
	}
	return fmt.Errorf("topic: unknown subcommand %q", args[0])
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
	probe := fs.String("probe", "",
		"comma-separated topic names to probe (broker has no enumeration API; the CLI probes each name via Metadata and prints the partition count or an error per topic)")
	timeout := fs.Duration("timeout", 5*time.Second, "RPC timeout")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *probe == "" {
		return errors.New("topic list: --probe is required (e.g., --probe=events,orders)")
	}

	tr, err := dial(*addr, *apiKey)
	if err != nil {
		return err
	}
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	for _, name := range strings.Split(*probe, ",") {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		n, err := tr.PartitionsFor(ctx, name)
		if err != nil {
			fmt.Printf("%s\tNOT FOUND (%v)\n", name, err)
			continue
		}
		fmt.Printf("%s\t%d partition(s)\n", name, n)
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

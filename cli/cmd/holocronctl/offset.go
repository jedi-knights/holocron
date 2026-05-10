package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/jedi-knights/holocron/cli/internal/clienttls"
	"github.com/jedi-knights/holocron/proto"
)

// runOffset dispatches `offset <subcommand>`. Operational tool for
// nudging consumer-group state when a group is stuck or needs to
// replay historical records.
func runOffset(args []string) error {
	if len(args) == 0 {
		return errors.New("offset: subcommand required (commit | reset)")
	}
	switch args[0] {
	case "commit":
		return runOffsetCommit(args[1:])
	case "reset":
		return runOffsetReset(args[1:])
	}
	return fmt.Errorf("offset: unknown subcommand %q", args[0])
}

// runOffsetCommit writes a committed offset for (group, topic,
// partition) directly. Use to skip past a poison record that's
// hanging the group, or to fast-forward a fresh group past
// historical data it shouldn't replay.
func runOffsetCommit(args []string) error {
	fs := flag.NewFlagSet("offset commit", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:9092", "broker address")
	apiKey := fs.String("api-key", "", "API key for handshake")
	group := fs.String("group", "", "consumer group (required)")
	topic := fs.String("topic", "", "topic name (required)")
	partition := fs.Int("partition", 0, "partition index")
	offset := fs.Int64("offset", -1, "committed offset (next-to-read)")
	timeout := fs.Duration("timeout", 5*time.Second, "RPC timeout")
	tlsCfg := clienttls.RegisterFlags(fs)
	credFile := fs.String("credential-file", os.Getenv("HOLOCRON_CREDENTIAL_FILE"), "path to a JWT file (mutually exclusive with --api-key)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *group == "" || *topic == "" || *offset < 0 {
		return errors.New("offset commit: --group, --topic, and --offset (>= 0) are required")
	}

	cfg, err := tlsCfg()
	if err != nil {
		return err
	}
	opts, err := credentialOpts(*credFile, *apiKey, dialOpts(cfg)...)
	if err != nil {
		return err
	}
	tr, err := dial(*addr, opts...)
	if err != nil {
		return err
	}
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	pref := proto.PartitionRef{Topic: *topic, Index: int32(*partition)}
	if err := tr.Commit(ctx, *group, pref, *offset); err != nil {
		return fmt.Errorf("commit %s/%s/%d=%d: %w", *group, *topic, *partition, *offset, err)
	}
	fmt.Printf("committed %s/%s/%d -> %d\n", *group, *topic, *partition, *offset)
	return nil
}

// runOffsetReset commits 0 — the group's next read on this
// partition starts from the beginning. Convenience over `commit
// --offset 0`.
func runOffsetReset(args []string) error {
	fs := flag.NewFlagSet("offset reset", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:9092", "broker address")
	apiKey := fs.String("api-key", "", "API key for handshake")
	group := fs.String("group", "", "consumer group (required)")
	topic := fs.String("topic", "", "topic name (required)")
	partition := fs.Int("partition", 0, "partition index")
	timeout := fs.Duration("timeout", 5*time.Second, "RPC timeout")
	tlsCfg := clienttls.RegisterFlags(fs)
	credFile := fs.String("credential-file", os.Getenv("HOLOCRON_CREDENTIAL_FILE"), "path to a JWT file (mutually exclusive with --api-key)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *group == "" || *topic == "" {
		return errors.New("offset reset: --group and --topic are required")
	}

	cfg, err := tlsCfg()
	if err != nil {
		return err
	}
	opts, err := credentialOpts(*credFile, *apiKey, dialOpts(cfg)...)
	if err != nil {
		return err
	}
	tr, err := dial(*addr, opts...)
	if err != nil {
		return err
	}
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	pref := proto.PartitionRef{Topic: *topic, Index: int32(*partition)}
	if err := tr.Commit(ctx, *group, pref, 0); err != nil {
		return fmt.Errorf("reset %s/%s/%d: %w", *group, *topic, *partition, err)
	}
	fmt.Printf("reset %s/%s/%d -> 0\n", *group, *topic, *partition)
	return nil
}

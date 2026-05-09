package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/jedi-knights/holocron/proto"
	"github.com/jedi-knights/holocron/sdk"
)

// runRecord dispatches `record <subcommand>`.
func runRecord(args []string) error {
	if len(args) == 0 {
		return errors.New("record: subcommand required (fetch)")
	}
	switch args[0] {
	case "fetch":
		return runRecordFetch(args[1:])
	}
	return fmt.Errorf("record: unknown subcommand %q", args[0])
}

// runRecordFetch reads exactly one record at (topic, partition,
// offset) and prints its key / value / headers. Useful for
// inspecting a specific record when debugging — the streaming
// consume subcommand is awkward when all you want is one record
// from a known position.
func runRecordFetch(args []string) error {
	fs := flag.NewFlagSet("record fetch", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:9092", "broker address")
	apiKey := fs.String("api-key", "", "API key for handshake")
	topic := fs.String("topic", "", "topic name (required)")
	partition := fs.Int("partition", 0, "partition index")
	offset := fs.Int64("offset", -1, "record offset (>= 0, required)")
	timeout := fs.Duration("timeout", 5*time.Second, "RPC timeout")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *topic == "" || *offset < 0 {
		return errors.New("record fetch: --topic and --offset (>= 0) are required")
	}

	tr, err := dial(*addr, *apiKey)
	if err != nil {
		return err
	}
	defer tr.Close()

	cons, err := sdk.NewConsumer(tr)
	if err != nil {
		return err
	}
	defer cons.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	if err := cons.Assign(ctx, proto.PartitionRef{
		Topic: *topic,
		Index: int32(*partition),
	}, *offset); err != nil {
		return fmt.Errorf("assign: %w", err)
	}
	records, err := cons.Poll(ctx, 1)
	if err != nil {
		return fmt.Errorf("poll: %w", err)
	}
	if len(records) == 0 {
		return fmt.Errorf("no record at %s/%d offset %d (high-water may be lower)",
			*topic, *partition, *offset)
	}
	r := records[0]
	fmt.Printf("offset:    %d\n", r.Offset)
	fmt.Printf("timestamp: %d\n", r.Timestamp)
	fmt.Printf("key:       %q\n", r.Key)
	fmt.Printf("value:     %q\n", r.Value)
	if len(r.Headers) > 0 {
		fmt.Println("headers:")
		for _, h := range r.Headers {
			fmt.Printf("  %s = %q\n", h.Key, h.Value)
		}
	}
	return nil
}

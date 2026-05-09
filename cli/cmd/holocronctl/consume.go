package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/jedi-knights/holocron/proto"
	"github.com/jedi-knights/holocron/sdk"
)

// runTail is `consume` rooted at the partition's current
// high-water — i.e., it skips historical records and prints only
// what arrives after attach. The live-tail pattern operators
// expect from log tooling.
//
// Differs from `consume --from-offset $(query)` only in
// convenience: the CLI resolves the high-water itself so the
// operator doesn't have to.
func runTail(args []string) error {
	fs := flag.NewFlagSet("tail", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:9092", "broker address")
	apiKey := fs.String("api-key", "", "API key for handshake")
	topic := fs.String("topic", "", "topic to tail (required)")
	partition := fs.Int("partition", 0, "partition index")
	max := fs.Int("max", 0, "stop after N records (0 = run until --duration elapses)")
	jsonOut := fs.Bool("json", false, "emit each record as JSONL (same shape as topic dump)")
	duration := fs.Duration("duration", 30*time.Second, "max time to wait")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *topic == "" {
		return errors.New("tail: --topic is required")
	}

	tr, err := dial(*addr, *apiKey)
	if err != nil {
		return err
	}
	defer tr.Close()

	resolveCtx, cancelResolve := context.WithTimeout(context.Background(), 5*time.Second)
	hw, err := tr.HighWater(resolveCtx, proto.PartitionRef{Topic: *topic, Index: int32(*partition)})
	cancelResolve()
	if err != nil {
		return fmt.Errorf("high-water lookup: %w", err)
	}

	cons, err := sdk.NewConsumer(tr)
	if err != nil {
		return err
	}
	defer cons.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()
	if err := cons.Assign(ctx, proto.PartitionRef{
		Topic: *topic, Index: int32(*partition),
	}, hw); err != nil {
		return fmt.Errorf("assign at high-water %d: %w", hw, err)
	}

	enc := json.NewEncoder(os.Stdout)
	count := 0
	for {
		batchSize := 32
		if *max > 0 && (*max-count) < batchSize {
			batchSize = *max - count
		}
		records, err := cons.Poll(ctx, batchSize)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				break
			}
			return err
		}
		for _, r := range records {
			if *jsonOut {
				if err := enc.Encode(toDumpRecord(r)); err != nil {
					return fmt.Errorf("encode offset %d: %w", r.Offset, err)
				}
			} else {
				fmt.Printf("offset=%d key=%q value=%q\n", r.Offset, r.Key, r.Value)
			}
			count++
			if *max > 0 && count >= *max {
				return nil
			}
		}
	}
	if !*jsonOut {
		fmt.Printf("tailed %d record(s) from offset %d\n", count, hw)
	}
	return nil
}

func runConsume(args []string) error {
	fs := flag.NewFlagSet("consume", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:9092", "broker address")
	apiKey := fs.String("api-key", "", "API key for handshake")
	topic := fs.String("topic", "", "source topic (required)")
	fromOffset := fs.Int64("from-offset", 0, "starting offset")
	max := fs.Int("max", 0, "stop after N records (0 = run until --duration elapses or ctrl-c)")
	jsonOut := fs.Bool("json", false, "emit each record as JSONL (same shape as topic dump)")
	duration := fs.Duration("duration", 10*time.Second, "max time to wait")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *topic == "" {
		return errors.New("consume: --topic is required")
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

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()
	if err := cons.Subscribe(ctx, *topic, *fromOffset); err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}

	enc := json.NewEncoder(os.Stdout)
	count := 0
	for {
		batchSize := 32
		if *max > 0 && (*max-count) < batchSize {
			batchSize = *max - count
		}
		records, err := cons.Poll(ctx, batchSize)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				break
			}
			return err
		}
		for _, r := range records {
			if *jsonOut {
				if err := enc.Encode(toDumpRecord(r)); err != nil {
					return fmt.Errorf("encode offset %d: %w", r.Offset, err)
				}
			} else {
				fmt.Printf("offset=%d key=%q value=%q\n", r.Offset, r.Key, r.Value)
			}
			count++
			if *max > 0 && count >= *max {
				return nil
			}
		}
	}
	if !*jsonOut {
		fmt.Printf("read %d record(s)\n", count)
	}
	return nil
}

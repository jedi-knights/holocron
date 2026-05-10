package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/jedi-knights/holocron/cli/internal/clienttls"
	"github.com/jedi-knights/holocron/proto"
	"github.com/jedi-knights/holocron/sdk"
)

// runTopicHead prints the first --max records of a partition starting
// at offset 0. The "what's at the start of this topic?" inspection
// without writing a Go consumer or learning the consume subcommand's
// flag set.
func runTopicHead(args []string) error {
	fs := flag.NewFlagSet("topic head", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:9092", "broker address")
	apiKey := fs.String("api-key", "", "API key for handshake")
	topic := fs.String("topic", "", "topic name (required)")
	partition := fs.Int("partition", 0, "partition index")
	max := fs.Int("max", 10, "maximum records to print")
	timeout := fs.Duration("timeout", 5*time.Second, "RPC timeout")
	tlsCfg := clienttls.RegisterFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *topic == "" {
		return errors.New("topic head: --topic is required")
	}
	if *max <= 0 {
		return errors.New("topic head: --max must be > 0")
	}
	cfg, err := tlsCfg()
	if err != nil {
		return err
	}
	return readPartitionRange(*addr, *apiKey, cfg, *topic, int32(*partition), 0, *max, *timeout)
}

// runTopicLast prints the last --max records of a partition. Reads
// from max(0, high-water - max) so a fresh topic with fewer records
// than --max prints what exists rather than failing. Distinct from
// the top-level `tail` subcommand (live-tail from HW): `topic last`
// looks backward at the most recent historical records.
func runTopicLast(args []string) error {
	fs := flag.NewFlagSet("topic last", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:9092", "broker address")
	apiKey := fs.String("api-key", "", "API key for handshake")
	topic := fs.String("topic", "", "topic name (required)")
	partition := fs.Int("partition", 0, "partition index")
	max := fs.Int("max", 10, "maximum records to print")
	timeout := fs.Duration("timeout", 5*time.Second, "RPC timeout")
	tlsCfg := clienttls.RegisterFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *topic == "" {
		return errors.New("topic last: --topic is required")
	}
	if *max <= 0 {
		return errors.New("topic last: --max must be > 0")
	}

	cfg, err := tlsCfg()
	if err != nil {
		return err
	}
	tr, err := dial(*addr, *apiKey, dialOpts(cfg)...)
	if err != nil {
		return err
	}
	defer tr.Close()

	resolveCtx, cancelResolve := context.WithTimeout(context.Background(), *timeout)
	hw, err := tr.HighWater(resolveCtx, proto.PartitionRef{Topic: *topic, Index: int32(*partition)})
	cancelResolve()
	if err != nil {
		return fmt.Errorf("high-water lookup: %w", err)
	}
	from := hw - int64(*max)
	if from < 0 {
		from = 0
	}
	tr.Close() // readPartitionRange opens its own dial.
	return readPartitionRange(*addr, *apiKey, cfg, *topic, int32(*partition), from, *max, *timeout)
}

// readPartitionRange reads up to maxRecords from the partition
// starting at fromOffset and prints each as
// `offset=N key=K value=V`. Shared by topic head and topic last.
// Returns nil when maxRecords have been read OR when the timeout
// elapses with the channel idle (whichever comes first) — partial
// reads are not an error since high-water - max can land before
// records exist.
func readPartitionRange(addr, apiKey string, tlsCfg *tls.Config, topic string, partition int32, fromOffset int64, maxRecords int, timeout time.Duration) error {
	tr, err := dial(addr, apiKey, dialOpts(tlsCfg)...)
	if err != nil {
		return err
	}
	defer tr.Close()

	cons, err := sdk.NewConsumer(tr)
	if err != nil {
		return err
	}
	defer cons.Close()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := cons.Assign(ctx, proto.PartitionRef{Topic: topic, Index: partition}, fromOffset); err != nil {
		return fmt.Errorf("assign: %w", err)
	}

	count := 0
	for count < maxRecords {
		batchSize := 32
		if maxRecords-count < batchSize {
			batchSize = maxRecords - count
		}
		records, err := cons.Poll(ctx, batchSize)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				break
			}
			return fmt.Errorf("poll: %w", err)
		}
		for _, r := range records {
			fmt.Printf("offset=%d key=%q value=%q\n", r.Offset, r.Key, r.Value)
			count++
			if count >= maxRecords {
				break
			}
		}
	}
	if count == 0 {
		fmt.Println("(no records)")
	}
	return nil
}

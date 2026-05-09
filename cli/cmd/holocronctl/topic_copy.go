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

// runTopicCopy reads every record from a source partition and
// re-publishes it to a destination topic. Useful for migration
// (rebuilding a topic with a different partition count or
// retention) and operator-side topic restructuring.
//
// Doesn't preserve broker offsets — the destination broker assigns
// new ones. Source-side keys, values, and headers carry over;
// timestamps are re-stamped by the destination broker on append.
//
// Reads bounded by the source partition's high-water at the start
// of the copy: records produced after the snapshot is taken are
// not copied (a steady-state copy needs a streaming variant we
// don't ship yet).
func runTopicCopy(args []string) error {
	fs := flag.NewFlagSet("topic copy", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:9092", "broker address")
	apiKey := fs.String("api-key", "", "API key for handshake")
	from := fs.String("from", "", "source topic (required)")
	to := fs.String("to", "", "destination topic (required, must already exist)")
	partition := fs.Int("partition", 0, "source partition index (ignored with --all-partitions)")
	allPartitions := fs.Bool("all-partitions", false, "copy every partition of source instead of just one")
	timeout := fs.Duration("timeout", 30*time.Second, "RPC timeout (covers the full copy)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *from == "" || *to == "" {
		return errors.New("topic copy: --from and --to are required")
	}
	if *from == *to {
		return errors.New("topic copy: --from and --to must differ")
	}

	tr, err := dial(*addr, *apiKey)
	if err != nil {
		return err
	}
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	if !*allPartitions {
		copied, err := copyPartition(ctx, tr, *from, *to, int32(*partition))
		if err != nil {
			return err
		}
		fmt.Printf("copied %d record(s) from %s/%d -> %s\n", copied, *from, *partition, *to)
		return nil
	}

	// All-partitions mode: enumerate the source's partitions and
	// loop. Each iteration is a self-contained copy so a transient
	// failure on partition N doesn't abort already-completed copies
	// on partitions 0..N-1.
	count, err := tr.PartitionsFor(ctx, *from)
	if err != nil {
		return fmt.Errorf("partitions for %s: %w", *from, err)
	}
	total := 0
	for p := int32(0); p < count; p++ {
		copied, err := copyPartition(ctx, tr, *from, *to, p)
		if err != nil {
			return fmt.Errorf("partition %d: %w", p, err)
		}
		total += copied
	}
	fmt.Printf("copied %d record(s) from %s -> %s (%d partitions)\n", total, *from, *to, count)
	return nil
}

// copyPartition runs the read-then-publish loop for one source
// partition, returning the count copied. Bounded by the source's
// HighWater snapshot at the start of the call.
func copyPartition(ctx context.Context, tr Transport, from, to string, partition int32) (int, error) {
	srcPref := proto.PartitionRef{Topic: from, Index: partition}
	hw, err := tr.HighWater(ctx, srcPref)
	if err != nil {
		return 0, fmt.Errorf("high-water on source: %w", err)
	}
	if hw == 0 {
		return 0, nil
	}

	cons, err := sdk.NewConsumer(tr)
	if err != nil {
		return 0, err
	}
	defer cons.Close()
	if err := cons.Assign(ctx, srcPref, 0); err != nil {
		return 0, fmt.Errorf("assign source: %w", err)
	}

	prod, err := sdk.NewProducer(tr)
	if err != nil {
		return 0, err
	}
	defer prod.Close()

	copied := 0
	for int64(copied) < hw {
		batch := 256
		if remaining := int(hw) - copied; remaining < batch {
			batch = remaining
		}
		records, err := cons.Poll(ctx, batch)
		if err != nil {
			return copied, fmt.Errorf("poll source: %w", err)
		}
		if len(records) == 0 {
			break
		}
		toSend := make([]proto.Record, 0, len(records))
		for _, r := range records {
			toSend = append(toSend, proto.Record{
				Key:     r.Key,
				Value:   r.Value,
				Headers: r.Headers,
			})
		}
		if _, err := prod.SendBatch(ctx, to, toSend); err != nil {
			return copied, fmt.Errorf("send to destination: %w", err)
		}
		copied += len(records)
	}
	return copied, nil
}

// Transport is the subset of net.Transport copyPartition needs —
// declared here so the helper can be tested with a fake without
// depending on the concrete net.Transport type.
type Transport interface {
	HighWater(ctx context.Context, p proto.PartitionRef) (int64, error)
	PartitionsFor(ctx context.Context, topic string) (int32, error)
	Publish(ctx context.Context, p proto.PartitionRef, r proto.Record) (int64, error)
	PublishBatch(ctx context.Context, p proto.PartitionRef, records []proto.Record) (int64, error)
	Subscribe(ctx context.Context, p proto.PartitionRef, fromOffset int64) (<-chan proto.Record, <-chan error, error)
	Commit(ctx context.Context, group string, p proto.PartitionRef, offset int64) error
	JoinGroup(ctx context.Context, group, memberID string, topics []string) (sdk.JoinResult, error)
	Heartbeat(ctx context.Context, group, memberID string, generation int32, maxWait time.Duration) (sdk.HeartbeatResult, error)
	LeaveGroup(ctx context.Context, group, memberID string) error
	Sync(ctx context.Context, p proto.PartitionRef) error
	Close() error
}

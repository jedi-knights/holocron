// Stage 1 end-to-end demo: a single process creates an in-memory broker,
// produces a small batch of records to a topic, then consumes them.
//
// This is the canonical Stage 1 acceptance test — if it prints all records
// it produced, the in-memory pub/sub path works end-to-end.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jedi-knights/holocron/broker/embed"
	"github.com/jedi-knights/holocron/proto"
	"github.com/jedi-knights/holocron/sdk"
)

const (
	demoTopic  = "orders.placed"
	partitions = 4
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	b := embed.NewMemory()
	defer b.Close()

	if err := b.CreateTopic(embed.TopicSpec{Name: demoTopic, PartitionCount: partitions}); err != nil {
		return err
	}

	producer, err := sdk.NewProducer(b.Transport())
	if err != nil {
		return err
	}
	defer producer.Close()

	consumer, err := sdk.NewConsumer(b.Transport(), sdk.WithGroup("demo"))
	if err != nil {
		return err
	}
	defer consumer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := consumer.Subscribe(ctx, demoTopic, 0); err != nil {
		return err
	}

	payloads := []string{"order-1", "order-2", "order-3", "order-4", "order-5"}
	for _, p := range payloads {
		offset, err := producer.Send(ctx, demoTopic, proto.Record{
			Key:   []byte(p),
			Value: []byte("placed"),
			Headers: []proto.Header{
				{Key: sdk.HeaderTraceID, Value: []byte("trace-" + p)},
			},
		})
		if err != nil {
			return err
		}
		fmt.Printf("produced %-8s -> offset %d\n", p, offset)
	}

	received := 0
	for received < len(payloads) {
		records, err := consumer.Poll(ctx, len(payloads))
		if err != nil {
			return err
		}
		for _, r := range records {
			fmt.Printf("consumed %-8s -> %q (offset %d)\n", string(r.Key), string(r.Value), r.Offset)
			received++
		}
	}
	return nil
}

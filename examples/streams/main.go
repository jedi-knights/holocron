// Stage 8 demo: a small streams topology counting clicks by key.
//
// Pipeline:
//
//	clicks → GroupByKey → Count("clicks") → counts-out
//
// Run with `go run ./examples/streams`. Produces five clicks (a/a/b/a/b)
// against an embedded broker, lets the topology process them, then
// prints the resulting counts.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jedi-knights/holocron/broker/embed"
	"github.com/jedi-knights/holocron/proto"
	"github.com/jedi-knights/holocron/sdk"
	"github.com/jedi-knights/holocron/streams"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	b := embed.NewMemory()
	defer b.Close()
	for _, name := range []string{"clicks", "counts-out"} {
		if err := b.CreateTopic(embed.TopicSpec{Name: name, PartitionCount: 1}); err != nil {
			return err
		}
	}

	top, err := streams.New(b.Transport())
	if err != nil {
		return err
	}
	top.Stream("clicks").
		GroupByKey().
		Count("clicks-by-key").
		To("counts-out")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := top.Start(ctx); err != nil {
		return err
	}
	defer top.Stop()

	producer, err := sdk.NewProducer(b.Transport())
	if err != nil {
		return err
	}
	defer producer.Close()
	for _, k := range []string{"a", "a", "b", "a", "b"} {
		if _, err := producer.Send(ctx, "clicks", proto.Record{Key: []byte(k)}); err != nil {
			return err
		}
	}

	// Wait for the topology to drain.
	consumer, err := sdk.NewConsumer(b.Transport())
	if err != nil {
		return err
	}
	defer consumer.Close()
	if err := consumer.Subscribe(ctx, "counts-out", 0); err != nil {
		return err
	}
	got := 0
	for got < 5 {
		records, err := consumer.Poll(ctx, 5)
		if err != nil {
			return err
		}
		for _, r := range records {
			fmt.Printf("counts-out: key=%s count=%d\n", string(r.Key), streams.DecodeCount(r.Value))
		}
		got += len(records)
	}

	store := top.Store("clicks-by-key")
	fmt.Println("---- final state ----")
	a, _ := store.Get([]byte("a"))
	b2, _ := store.Get([]byte("b"))
	fmt.Printf("a = %d\nb = %d\n", streams.DecodeCount(a), streams.DecodeCount(b2))
	return nil
}

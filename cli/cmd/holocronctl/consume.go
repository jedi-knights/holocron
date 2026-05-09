package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/jedi-knights/holocron/sdk"
)

func runConsume(args []string) error {
	fs := flag.NewFlagSet("consume", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:9092", "broker address")
	apiKey := fs.String("api-key", "", "API key for handshake")
	topic := fs.String("topic", "", "source topic (required)")
	fromOffset := fs.Int64("from-offset", 0, "starting offset")
	max := fs.Int("max", 0, "stop after N records (0 = run until --duration elapses or ctrl-c)")
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
			fmt.Printf("offset=%d key=%q value=%q\n", r.Offset, r.Key, r.Value)
			count++
			if *max > 0 && count >= *max {
				return nil
			}
		}
	}
	fmt.Printf("read %d record(s)\n", count)
	return nil
}

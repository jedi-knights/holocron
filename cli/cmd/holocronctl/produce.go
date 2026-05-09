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

func runProduce(args []string) error {
	fs := flag.NewFlagSet("produce", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:9092", "broker address")
	apiKey := fs.String("api-key", "", "API key for handshake")
	topic := fs.String("topic", "", "destination topic (required)")
	key := fs.String("key", "", "record key (optional)")
	value := fs.String("value", "", "record value (required)")
	timeout := fs.Duration("timeout", 5*time.Second, "RPC timeout")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *topic == "" || *value == "" {
		return errors.New("produce: --topic and --value are required")
	}

	tr, err := dial(*addr, *apiKey)
	if err != nil {
		return err
	}
	defer tr.Close()

	prod, err := sdk.NewProducer(tr)
	if err != nil {
		return err
	}
	defer prod.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	rec := proto.Record{Value: []byte(*value)}
	if *key != "" {
		rec.Key = []byte(*key)
	}
	off, err := prod.Send(ctx, *topic, rec)
	if err != nil {
		return fmt.Errorf("send: %w", err)
	}
	fmt.Printf("topic=%s offset=%d\n", *topic, off)
	return nil
}

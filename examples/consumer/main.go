// Standalone network consumer. Connects to a running holocrond and
// prints every record it receives from the configured topic.
//
// Run a broker and the producer first; then in another terminal:
//
//	go run ./examples/consumer
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/jedi-knights/holocron/sdk"
	holocronnet "github.com/jedi-knights/holocron/sdk/net"
)

func main() {
	addr := flag.String("addr", envOrDefault("HOLOCRON_BROKER", "127.0.0.1:9092"), "broker address")
	topic := flag.String("topic", "orders.placed", "topic to consume")
	group := flag.String("group", "examples-consumer", "consumer group name")
	flag.Parse()

	if err := run(*addr, *topic, *group); err != nil {
		log.Fatal(err)
	}
}

func run(addr, topic, group string) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	t, err := holocronnet.Dial(addr)
	if err != nil {
		return err
	}
	defer t.Close()

	c, err := sdk.NewConsumer(t, sdk.WithGroup(group))
	if err != nil {
		return err
	}
	defer c.Close()

	if err := c.Subscribe(ctx, topic, 0); err != nil {
		return err
	}

	fmt.Printf("subscribed to %s; press Ctrl-C to exit\n", topic)
	for {
		records, err := c.Poll(ctx, 32)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		for _, r := range records {
			fmt.Printf("offset=%d key=%-10s value=%q\n", r.Offset, string(r.Key), string(r.Value))
		}
	}
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

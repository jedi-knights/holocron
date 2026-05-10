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

	"github.com/jedi-knights/holocron/examples/internal/clienttls"
	"github.com/jedi-knights/holocron/sdk"
	holocronnet "github.com/jedi-knights/holocron/sdk/net"
)

func main() {
	addr := flag.String("addr", envOrDefault("HOLOCRON_BROKER", "127.0.0.1:9092"), "broker address")
	topic := flag.String("topic", "orders.placed", "topic to consume")
	group := flag.String("group", "examples-consumer", "consumer group name")
	tlsCA := flag.String("tls-ca", os.Getenv("HOLOCRON_TLS_CA"), "PEM CA bundle for verifying the broker's cert (enables TLS)")
	tlsSkipVerify := flag.Bool("tls-skip-verify", false, "enable TLS without certificate verification (lab use only)")
	flag.Parse()

	if err := run(*addr, *topic, *group, clienttls.Options{
		CAFile:     *tlsCA,
		SkipVerify: *tlsSkipVerify,
	}); err != nil {
		log.Fatal(err)
	}
}

func run(addr, topic, group string, tlsOpts clienttls.Options) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	dialOpts, err := buildDialOpts(tlsOpts)
	if err != nil {
		return err
	}
	t, err := holocronnet.Dial(addr, dialOpts...)
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

// buildDialOpts converts clienttls.Options into the holocronnet.Option
// slice the SDK's Dial accepts. Returns an empty slice when TLS is off.
func buildDialOpts(tlsOpts clienttls.Options) ([]holocronnet.Option, error) {
	cfg, err := clienttls.Config(tlsOpts)
	if err != nil {
		return nil, err
	}
	if cfg == nil {
		return nil, nil
	}
	return []holocronnet.Option{holocronnet.WithTLS(cfg)}, nil
}

// Standalone network producer. Connects to a running holocrond and
// publishes a few records to the configured topic.
//
// Run a broker first:
//
//	go run ./broker/cmd/holocrond --data-dir /tmp/holo --listen :9092
//
// Then in another terminal:
//
//	go run ./examples/producer
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jedi-knights/holocron/examples/internal/clienttls"
	"github.com/jedi-knights/holocron/proto"
	"github.com/jedi-knights/holocron/sdk"
	holocronnet "github.com/jedi-knights/holocron/sdk/net"
)

func main() {
	addr := flag.String("addr", envOrDefault("HOLOCRON_BROKER", "127.0.0.1:9092"), "broker address")
	topic := flag.String("topic", "orders.placed", "topic to produce to")
	partitions := flag.Int("partitions", 4, "partitions to create (ignored if topic exists)")
	count := flag.Int("count", 5, "number of records to send")
	tlsCA := flag.String("tls-ca", os.Getenv("HOLOCRON_TLS_CA"), "PEM CA bundle for verifying the broker's cert (enables TLS)")
	tlsSkipVerify := flag.Bool("tls-skip-verify", false, "enable TLS without certificate verification (lab use only)")
	flag.Parse()

	if err := run(*addr, *topic, int32(*partitions), *count, clienttls.Options{
		CAFile:     *tlsCA,
		SkipVerify: *tlsSkipVerify,
	}); err != nil {
		log.Fatal(err)
	}
}

func run(addr, topic string, partitions int32, count int, tlsOpts clienttls.Options) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dialOpts, err := buildDialOpts(tlsOpts)
	if err != nil {
		return err
	}
	t, err := holocronnet.Dial(addr, dialOpts...)
	if err != nil {
		return err
	}
	defer t.Close()

	if err := t.CreateTopic(ctx, topic, partitions); err != nil && !proto.IsStatus(err, proto.StatusInternal) {
		// "already exists" comes back as StatusInvalidRequest; we tolerate it.
		fmt.Printf("note: %v\n", err)
	}

	p, err := sdk.NewProducer(t)
	if err != nil {
		return err
	}
	defer p.Close()

	for i := range count {
		key := fmt.Sprintf("order-%d", i+1)
		offset, err := p.Send(ctx, topic, proto.Record{
			Key:   []byte(key),
			Value: []byte("placed"),
			Headers: []proto.Header{
				{Key: sdk.HeaderTraceID, Value: []byte("trace-" + key)},
			},
		})
		if err != nil {
			return err
		}
		fmt.Printf("produced %-9s -> offset %d\n", key, offset)
	}
	return nil
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

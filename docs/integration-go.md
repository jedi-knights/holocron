# Integrating Holocron into a Go project

This guide gets you from zero to a working producer + consumer in about ten minutes. It covers both deployment shapes Holocron supports: **embedded** (broker runs inside your process — perfect for tests, demos, and single-process apps) and **network** (a separate `holocrond` daemon serving SDK clients over TCP).

For background on what event streaming is and why you'd want it, read [`eda.md`](eda.md) first.

## Requirements

- Go **1.23 or newer**

## Install the SDK

```bash
go get github.com/jedi-knights/holocron/sdk@latest
```

If you want the embedded broker for tests:

```bash
go get github.com/jedi-knights/holocron/broker@latest
```

If you want the binaries (`holocrond` daemon + `holocronctl` operator CLI), build them from a clone:

```bash
git clone https://github.com/jedi-knights/holocron.git
cd holocron
make build  # produces ./bin/holocrond and ./bin/holocronctl
```

## Hello world — embedded broker

The fastest way to see Holocron work is to run the broker inside your own process. No daemon, no network, no ports. Useful for tests and single-process tools.

```go
package main

import (
	"context"
	"fmt"

	"github.com/jedi-knights/holocron/broker/embed"
	"github.com/jedi-knights/holocron/proto"
	"github.com/jedi-knights/holocron/sdk"
)

func main() {
	// 1. Bring up an in-memory broker.
	b := embed.NewMemory()
	defer b.Close()

	// 2. Create a topic with 4 partitions.
	_ = b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 4})

	// 3. Produce.
	p, _ := sdk.NewProducer(b.Transport())
	defer p.Close()
	offset, _ := p.Send(context.Background(), "events", proto.Record{
		Key:   []byte("user-42"),
		Value: []byte(`{"action":"login"}`),
	})
	fmt.Println("produced at offset", offset)

	// 4. Consume.
	c, _ := sdk.NewConsumer(b.Transport())
	defer c.Close()
	_ = c.Subscribe(context.Background(), "events", 0)
	records, _ := c.Poll(context.Background(), 32)
	for _, r := range records {
		fmt.Printf("consumed: key=%s value=%s\n", r.Key, r.Value)
	}
}
```

This produces and consumes on the same partition (because `user-42` always hashes to the same partition) and you'll see one record on the consumer side. Done.

## Hello world — network broker

For multi-process work, run the broker as a daemon and dial it over the wire.

**Terminal 1 — start the broker:**

```bash
./bin/holocrond --memory --listen 127.0.0.1:9092
# or, with disk persistence:
./bin/holocrond --data-dir /tmp/holocron --listen 127.0.0.1:9092
```

**Terminal 2 — your producer:**

```go
package main

import (
	"context"
	"fmt"

	"github.com/jedi-knights/holocron/proto"
	"github.com/jedi-knights/holocron/sdk"
	holocronnet "github.com/jedi-knights/holocron/sdk/net"
)

func main() {
	tr, err := holocronnet.Dial("127.0.0.1:9092")
	if err != nil {
		panic(err)
	}
	defer tr.Close()

	ctx := context.Background()
	_ = tr.CreateTopic(ctx, "events", 4)

	p, _ := sdk.NewProducer(tr)
	defer p.Close()
	offset, _ := p.Send(ctx, "events", proto.Record{
		Key:   []byte("user-42"),
		Value: []byte(`{"action":"login"}`),
	})
	fmt.Println("produced at offset", offset)
}
```

**Terminal 3 — your consumer:**

```go
package main

import (
	"context"
	"fmt"

	"github.com/jedi-knights/holocron/sdk"
	holocronnet "github.com/jedi-knights/holocron/sdk/net"
)

func main() {
	tr, _ := holocronnet.Dial("127.0.0.1:9092")
	defer tr.Close()

	c, _ := sdk.NewConsumer(tr, sdk.WithGroup("my-app"))
	defer c.Close()
	_ = c.Subscribe(context.Background(), "events", 0)

	for {
		records, _ := c.Poll(context.Background(), 32)
		for _, r := range records {
			fmt.Printf("offset=%d key=%s value=%s\n", r.Offset, r.Key, r.Value)
		}
	}
}
```

The SDK API is identical between embedded and network shapes — only the `Transport` you build at startup differs. Code that uses `Producer` and `Consumer` is portable.

## The model in three sentences

- **Topic** — a named stream of records, split into a fixed number of **partitions**.
- **Partition** — an ordered, append-only log; the same key always lands on the same partition; consumers read by **offset**.
- **Consumer group** — a set of consumers that share a topic's partitions cooperatively; each partition is owned by exactly one group member at a time.

For the full data-model reference (records, segments, indexes, retention), see [`data-model.md`](data-model.md).

## Producer — common patterns

```go
// Synchronous send (default): waits for the broker to acknowledge.
offset, err := p.Send(ctx, "events", proto.Record{Key: k, Value: v})

// Fire-and-forget: returns immediately, errors surface via p.AsyncErrors().
p.SendNoWait(ctx, "events", proto.Record{Key: k, Value: v})

// Batch a slice of records in one RPC.
offsets, err := p.SendBatch(ctx, "events", records)

// Functional options at construction time.
p, _ := sdk.NewProducer(tr,
    sdk.WithLinger(5 * time.Millisecond),       // batch records arriving close together
    sdk.WithCompression(proto.CodecLZ4),        // wire-level compression
    sdk.WithMaxInFlight(64),                    // cap concurrent SendNoWait
    sdk.WithIdempotency("orders-svc-instance-1"), // broker-side dedup on retry
)
```

## Consumer — common patterns

```go
// Self-managed (you assign partitions explicitly).
c, _ := sdk.NewConsumer(tr)
_ = c.Subscribe(ctx, "events", 0)              // partition 0 from offset 0
_ = c.Subscribe(ctx, "events", -1)             // partition 0 from latest

// Group-managed (broker assigns partitions; rebalances as members join/leave).
c, _ := sdk.NewConsumer(tr, sdk.WithGroup("billing-svc"))
_ = c.Subscribe(ctx, "events", 0)              // partition arg is ignored — group decides

records, err := c.Poll(ctx, 32)               // up to 32 records
for _, r := range records {
    handle(r)
}
_ = c.CommitAll(ctx)                          // mark progress
```

`Consumer.Run(ctx, handler)` is also available for the common "loop forever calling a handler" pattern, with built-in error handling and auto-commit options. See `sdk/consumer.go` for the full surface.

## Stream processing

Holocron ships a Kafka-Streams-shaped DSL in the `streams/` module:

```go
import "github.com/jedi-knights/holocron/streams"

topology := streams.NewTopology(tr).
    Stream("orders.placed").
    Filter(func(r proto.Record) bool { return len(r.Value) > 0 }).
    Map(func(r proto.Record) proto.Record { /* transform */ return r }).
    To("orders.normalised")

go topology.Run(ctx)
```

Stateful operators (`Count`, `Aggregate`, `Sum`, windowed aggregations, joins) are supported with pluggable state stores. See [`stage-8.md`](stage-8.md) and [`sustaining.md`](sustaining.md) for the full feature set.

## Bundled examples

The repository ships runnable demos. Clone and `go run` them:

```bash
git clone https://github.com/jedi-knights/holocron.git
cd holocron

# Single-process: embedded broker + producer + consumer in one binary
go run ./examples/inproc

# Multi-process: broker daemon + standalone producer/consumer
./bin/holocrond --memory --listen 127.0.0.1:9092 &
go run ./examples/producer --addr 127.0.0.1:9092 --count 5
go run ./examples/consumer --addr 127.0.0.1:9092

# Connect tier: file source -> broker -> file sink
go run ./examples/connect

# Stream processing topology
go run ./examples/streams
```

## TLS

When the broker is running with TLS, point the SDK at its CA bundle:

```go
import (
    "crypto/tls"
    "crypto/x509"
    "os"
)

caPEM, _ := os.ReadFile("/etc/holocron/ca.pem")
pool := x509.NewCertPool()
pool.AppendCertsFromPEM(caPEM)

tr, err := holocronnet.Dial("broker.example.com:9092", holocronnet.WithTLS(&tls.Config{
    RootCAs:    pool,
    ServerName: "broker.example.com",
    MinVersion: tls.VersionTLS13,
}))
```

For the full TLS configuration surface (mTLS, cluster TLS, cert generation), see [`tls.md`](tls.md).

## Where to go next

- [Operator guide](operator-guide.md) — running the broker daemon
- [Comparison vs alternatives](comparison.md) — when to pick what
- [Data model](data-model.md) — records, segments, retention internals
- [Architecture](architecture.md) — module boundaries and design rules

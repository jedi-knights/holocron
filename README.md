# Holocron

A single-binary, Go-native distributed log broker — NATS JetStream's operational simplicity with the per-partition ordering and Kafka-shaped log model JetStream lacks.

![Maturity](https://img.shields.io/badge/maturity-pre--alpha-orange)
![Go](https://img.shields.io/badge/go-1.23+-00ADD8?logo=go)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

> **Status:** Pre-alpha. The on-disk format, wire protocol, and public APIs change without notice until the first tagged release. Production gaps remain — see the [Roadmap](docs/roadmap.md).

## Table of contents

- [Overview](#overview)
- [Why Holocron?](#why-holocron)
- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Usage](#usage)
- [Configuration](#configuration)
- [Development](#development)
- [Documentation](#documentation)
- [Contributing](#contributing)
- [License](#license)

## Overview

Holocron is a **distributed log** — an append-only, replayable, partitioned event store that sits between your services. Producers publish records to topics; consumers subscribe and read them by offset, individually or as cooperating consumer groups.

If "distributed log" doesn't already mean something to you, read [**What is event-driven architecture?**](docs/eda.md) first.

## Why Holocron?

Three reasons to pick Holocron over the alternatives:

- **Operationally simple.** One Go binary. No JVM, no ZooKeeper-or-KRaft, no operator, no sidecar. Drop it on a host and it runs. Same shape as NATS JetStream.
- **Real per-partition ordering.** Strict ordering is a broker primitive, not publisher-side hashing. Records with the same key always land on the same partition and stay in order. NATS JetStream's truncation behaviour ([Jepsen 2.12.1](https://jepsen.io/analyses/nats-2.12.1)) is the bug you don't get here.
- **Kafka-shaped where it matters, smaller where it doesn't.** Topics, partitions, offsets, segments, consumer groups, replication, schema registry, stream-processing DSL, connector framework — all the Kafka primitives you actually use, in a project small enough to read end-to-end.

For the full feature matrix and when-to-pick-what guidance against Kafka, NATS JetStream, Redpanda, WarpStream, and Pulsar, see [**Holocron vs the alternatives**](docs/comparison.md).

## Features

What's shipped today:

- Topics, partitions, per-partition strict ordering, replayable consumers
- Disk-backed segmented log with sparse offset index, atomic recovery, time + size retention
- Network wire protocol (TCP, length-prefixed binary framing) with full `produce` / `fetch` / `metadata` / `commit` / `create-topic` opcodes
- Consumer groups with range-assignment rebalancing
- Multi-node clusters with Raft leader election and replication (`--cluster`)
- TLS — server, optional or required mTLS for clients, mandatory mTLS for inter-node Raft
- Idempotent producer with persistent broker-side dedup
- Connect-style source/sink connector framework with reference connectors
- Optional schema registry (Confluent-shape HTTP API)
- Stream-processing DSL with stateful operators, windows, joins
- `holocronctl` operator CLI with full inspection + management surface, `--json` everywhere

For what's coming next (auth, KV/Object store, non-Go SDKs, geo-replication), see the [Roadmap](docs/roadmap.md).

## Requirements

- Go **1.23 or newer**

## Installation

```bash
git clone https://github.com/jedi-knights/holocron.git
cd holocron
make build
```

This produces `bin/holocrond` (broker daemon) and `bin/holocronctl` (operator CLI).

For the SDK only:

```bash
go get github.com/jedi-knights/holocron/sdk@latest
```

## Usage

### 60-second quickstart in Go

Embed the broker in your own program — no daemon, no ports:

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
    b := embed.NewMemory()
    defer b.Close()

    _ = b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 4})

    p, _ := sdk.NewProducer(b.Transport())
    defer p.Close()
    offset, _ := p.Send(context.Background(), "events", proto.Record{
        Key:   []byte("user-42"),
        Value: []byte(`{"action":"login"}`),
    })
    fmt.Println("produced at offset", offset)

    c, _ := sdk.NewConsumer(b.Transport())
    defer c.Close()
    _ = c.Subscribe(context.Background(), "events", 0)
    records, _ := c.Poll(context.Background(), 32)
    for _, r := range records {
        fmt.Printf("consumed: key=%s value=%s\n", r.Key, r.Value)
    }
}
```

### Run a standalone broker

```bash
./bin/holocrond --memory --listen 127.0.0.1:9092
```

For producer + consumer connecting over the network, consumer groups, stream processing, TLS, and the bundled `examples/` walkthroughs, see [**Integrating Holocron in Go**](docs/integration-go.md).

For non-Go projects: only Go is supported today; see [**Integrating from non-Go projects**](docs/integration-other-languages.md) for the roadmap and current options.

## Configuration

The daemon reads each setting from a flag, with a `HOLOCRON_*` environment variable as fallback. The most important ones:

| Variable | Default | Purpose |
|---|---|---|
| `HOLOCRON_DATA_DIR` | `/var/lib/holocron` | Disk-backed storage location. |
| `HOLOCRON_LISTEN` | `:9092` | Wire-protocol bind address. |
| `HOLOCRON_TLS_CERT` / `HOLOCRON_TLS_KEY` | — | Enable TLS on the wire listener. |

Cluster mode, Raft TLS, retention, and the full configuration surface are documented in the [**Operator guide**](docs/operator-guide.md). For TLS specifically, see [**TLS**](docs/tls.md).

## Development

```bash
make build          # compile broker, sdk, cli into ./bin
make test           # run unit tests across all modules
make lint           # gofmt + go vet + staticcheck
make tidy           # go mod tidy in each module
```

Run the full suite with the race detector before merging:

```bash
go test -race ./broker/... ./sdk/... ./proto/... ./examples/... ./cli/...
```

Container builds:

```bash
docker compose --profile single up      # single broker on :9092
docker compose --profile cluster up     # 3-node Raft cluster on :9091/:9092/:9093
```

## Documentation

Background, design, integration, and operations docs all live in [`docs/`](docs/):

- [**What is event-driven architecture?**](docs/eda.md) — start here if logs and brokers are new
- [**Comparison vs alternatives**](docs/comparison.md) — Holocron vs Kafka, JetStream, Redpanda, Pulsar, WarpStream
- [**Integrating in Go**](docs/integration-go.md) — full producer / consumer / streams tutorial
- [**Integrating from non-Go projects**](docs/integration-other-languages.md) — current state and roadmap
- [**Operator guide**](docs/operator-guide.md) — running the broker
- [**TLS**](docs/tls.md) — full TLS surface (server + mTLS + cluster)
- [**Architecture**](docs/architecture.md) — module boundaries and design rules
- [**Data model**](docs/data-model.md) — records, segments, indexes, retention
- [**Roadmap**](docs/roadmap.md) — what's coming next

Per-stage design notes for the foundational work live in [`docs/stage-N.md`](docs/) and [`docs/sustaining.md`](docs/sustaining.md).

## Contributing

Issues and pull requests are welcome. Please read [`CONTRIBUTING.md`](CONTRIBUTING.md) before opening a PR. The short version:

- One PR, one concern. Conventional Commits for the title and body.
- Add tests with the change. Integration tests for storage and broker; unit tests everywhere else.
- Update `docs/` when behaviour changes. Stale documentation is worse than missing documentation.

## License

[MIT](LICENSE).

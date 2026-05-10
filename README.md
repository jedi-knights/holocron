# Holocron

A from-scratch, append-only event streaming platform — built to understand how systems like Apache Kafka, AWS SQS/SNS, Google Pub/Sub, and AWS EventBridge work beneath the surface.

![Status](https://img.shields.io/badge/status-feature--frozen-blue)
![Stage](https://img.shields.io/badge/stage-8%20%2B%20sustaining-blue)
![Go](https://img.shields.io/badge/go-1.23+-00ADD8?logo=go)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

> **Status:** Feature-frozen at end of the sustaining era (batches 21–52); Stage 9 (fresh-follower record catch-up) is **partially complete and paused** — see [`docs/stage-9.md#implementation-status`](docs/stage-9.md#implementation-status). The eight roadmap stages are complete and ~96 polish/ergonomics items have landed across SDK, CLI, streams, and observability surface. The remaining backlog items are large architectural pieces (exactly-once, per-partition Raft, sendfile, continuous replication completion, multiplexed connections, richer schema parsing) documented in [`docs/sustaining.md#deferred-work`](docs/sustaining.md#deferred-work) but not on a roadmap. The on-disk format, wire protocol, and public APIs will change without notice until the first tagged release.

## Table of contents

- [Overview](#overview)
- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Usage](#usage)
- [Configuration](#configuration)
- [Development](#development)
- [Architecture](#architecture)
- [Project layout](#project-layout)
- [Roadmap](#roadmap)
- [Non-goals](#non-goals)
- [Contributing](#contributing)
- [License](#license)
- [Acknowledgements](#acknowledgements)

## Overview

Modern applications rarely run as a single process on a single machine. They are constellations of services — orders, payments, inventory, notifications, analytics — that communicate without each one knowing intimately about the others. Two patterns emerge for that communication:

1. **Synchronous request/response** — service A calls service B and waits. Simple, but couples availability, deployment cadence, and forces every consumer of an event to be discovered up front.
2. **Asynchronous event passing** — service A publishes "order placed" to a broker; payments, inventory, and notifications independently subscribe. A doesn't know who listens; the broker absorbs throughput differences between producers and consumers.

The second pattern is the foundation of **event-driven architecture**. The infrastructure that makes it work — the piece sitting between producers and consumers — is called **messaging middleware** or, in its modern log-centric form, an **event streaming platform**:

| System | What it is |
|---|---|
| Apache Kafka, AWS Kinesis, Apache Pulsar, Redpanda | Distributed append-only logs with replayable consumers |
| AWS SQS, RabbitMQ queues, ActiveMQ | Point-to-point message queues with at-least-once delivery |
| AWS SNS, Google Pub/Sub | Fan-out pub/sub brokers |
| AWS EventBridge, NATS JetStream | Event buses with content-based routing |

Holocron is a single-binary Kafka-style broker, written in Go and built incrementally — each stage adds one production-grade concept and is fully runnable on its own. The name comes from the holocrons of Star Wars lore: crystalline, append-only repositories of knowledge, indexed and replayable by anyone with access.

The platform's primitive is the **distributed log** (row 1 above), and the other three categories — message queue, pub/sub fan-out, event bus — are realized on top of it via consumer groups (Stage 4) and the Connect tier (Stage 6) without giving up the "broker stays dumb" architectural rule. See [`docs/architecture.md`](docs/architecture.md#coverage-of-the-messaging-middleware-taxonomy) for the per-category mapping.

This is a **learning project first** and a **functional broker second**. The goal is not to compete with Kafka. The goal is a codebase where every architectural decision is small enough to read and obvious enough to justify.

## Features

- **Single binary, zero external dependencies at runtime.** The broker process owns its data directory; nothing else.
- **Idiomatic Go.** Small interfaces at the consumer side, functional options for configuration, channels for fan-out, `context.Context` everywhere it belongs.
- **Stage-by-stage growth.** Each stage on the [roadmap](#roadmap) produces a working system. No half-built scaffolding for features that aren't here yet.
- **Stable SDK surface.** `Producer` and `Consumer` look the same in Stage 1 (in-process) as they will in Stage 3 (network) — only the `Transport` strategy underneath changes.
- **Throughput-first defaults.** Per-partition single-writer locking, sticky partitioning, length-prefixed binary framing (Stage 3+), zero-copy reads from sealed segments (Stage 3+).
- **Honest about trade-offs.** When holocron differs from Kafka or SQS, [`docs/`](docs/) explains *why*, not just *what*.

## Requirements

- Go **1.23 or newer** (uses `range` over integers and `min`/`max` builtins).
- Optional: Docker and `docker compose` to run the broker in a container.

## Installation

Clone the repository and build the binaries:

```bash
git clone https://github.com/jedi-knights/holocron.git
cd holocron
make build
```

This produces `bin/holocrond` (the broker daemon) and `bin/holocronctl` (the operator CLI).

## Usage

### Run the Stage 1 end-to-end demo

A single-process producer → broker → consumer round trip:

```bash
go run ./examples/inproc
```

Expected output:

```
produced order-1  -> offset 0
produced order-2  -> offset 0
produced order-3  -> offset 0
produced order-4  -> offset 0
produced order-5  -> offset 1
consumed order-1  -> "placed" (offset 0)
consumed order-2  -> "placed" (offset 0)
consumed order-3  -> "placed" (offset 0)
consumed order-4  -> "placed" (offset 0)
consumed order-5  -> "placed" (offset 1)
```

Offsets are partition-local. The keys (`order-1` through `order-5`) hash to different partitions; same-partition records get sequential offsets.

### Run the standalone broker + clients

```bash
# Terminal 1 — broker
./bin/holocrond --memory --listen 127.0.0.1:9092

# Terminal 2 — producer
go run ./examples/producer --addr 127.0.0.1:9092 --count 5

# Terminal 3 — consumer
go run ./examples/consumer --addr 127.0.0.1:9092
```

The producer publishes records to topic `orders.placed` (creating it if
absent); the consumer subscribes and prints records as they arrive.

### Embed the broker in your own program

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
    fmt.Println("offset:", offset)
}
```

## Configuration

Stage 1 has no runtime configuration to speak of — the in-memory broker accepts no flags or environment variables. The variables below are reserved for later stages so that environment-driven deployment patterns are stable from the start.

| Variable | Default | Stage | Purpose |
|---|---|---|---|
| `HOLOCRON_DATA_DIR` | `/var/lib/holocron` | 2+ | Directory the broker writes its segmented log into. |
| `HOLOCRON_LISTEN` | `:9092` | 3+ | Address the broker's network listener binds to. |

## Development

```bash
make build          # compile broker, sdk, cli into ./bin
make test           # run unit tests across all modules
make lint           # gofmt + go vet + staticcheck
make run            # start the broker locally (Stage 1: in-memory, no network)
make tidy           # go mod tidy in each module
```

Run the full test suite with the race detector:

```bash
go test -race ./broker/... ./sdk/... ./proto/... ./examples/... ./cli/...
```

Container build:

```bash
docker compose --profile single up      # single broker on :9092
docker compose --profile registry up    # broker + schema registry on :8081
docker compose --profile cluster up     # 3-node Raft cluster on :9091/:9092/:9093
```

## Architecture

The broker accepts records, durably orders them within partitions, and serves them to consumers. It does not transform, filter, or route by content — those responsibilities live in clients or in a dedicated Connect-style worker tier (Stage 6).

```
            ┌────────────┐         ┌────────────┐
            │  Producer  │         │  Consumer  │
            │   (SDK)    │         │   (SDK)    │
            └─────┬──────┘         └──────▲─────┘
                  │ Publish               │ Subscribe / Poll
                  ▼                       │
            ┌─────────────────────────────┴─────┐
            │           Holocron broker         │
            │  ┌─────────────┐  ┌────────────┐  │
            │  │   Topic     │  │ Pub/Sub    │  │
            │  │  Registry   │──│  Fan-out   │  │
            │  └──────┬──────┘  └────────────┘  │
            │         ▼                         │
            │  ┌─────────────┐                  │
            │  │   Storage   │  Strategy:       │
            │  │  (Store IF) │  Memory | File   │
            │  └─────────────┘                  │
            └───────────────────────────────────┘
                          │
                          ▼  (Stage 2+)
                   /var/lib/holocron
                   ├── orders.placed/
                   │   └── 0/
                   │       ├── 00000000000000000000.log
                   │       └── 00000000000000000000.idx
                   └── ...
```

For the durable architectural decisions and module boundaries, see [`docs/architecture.md`](docs/architecture.md). For the data model — record, topic, partition, offset, segment, index, retention — see [`docs/data-model.md`](docs/data-model.md). For the per-stage design notes, see [`docs/stage-N.md`](docs/).

## Project layout

Holocron is a Go workspace stitching eight modules together (`proto`, `sdk`, `broker`, `cli`, `connect`, `registry`, `streams`, `examples`). Each has a single, clear responsibility.

```
holocron/
├── go.work                          # workspace stitching all modules
├── docker-compose.yml               # broker + opt-in demo profile
├── docs/                            # design notes, data model, per-stage docs
│
├── broker/                          # the holocron daemon
│   ├── cmd/holocrond/               # main entry point
│   ├── embed/                       # public: in-process broker handle
│   ├── inproc/                      # public: sdk.Transport over a Broker
│   └── internal/
│       ├── broker/                  # publish/subscribe core
│       ├── log/                     # append-only log: segments + index (Stage 2)
│       ├── server/                  # transport layer (Stage 3)
│       ├── storage/                 # Store interface; Memory + File implementations
│       └── topic/                   # topic + partition registry
│
├── sdk/                             # public Go client — what users import
├── proto/                           # shared types between broker and sdk
├── connect/                         # Stage 6: source/sink connector framework
│   ├── connector.go                 # interfaces
│   ├── worker.go                    # runtime
│   └── file/                        # reference file source + file sink
├── registry/                        # Stage 7: optional schema registry
│   ├── registry.go                  # service kernel
│   ├── http.go                      # Confluent-shaped HTTP API
│   └── cmd/holocron-registry/       # standalone daemon
├── streams/                         # Stage 8: stream processing library
│   ├── topology.go                  # DSL + runtime
│   └── store.go                     # StateStore + MemoryStore
├── cli/                             # holocronctl: inspect topics, tail logs
└── examples/                        # producer + consumer + connect demos
    ├── inproc/                      # Stage 1: end-to-end in one process
    ├── producer/                    # Stage 3+: standalone network producer
    ├── consumer/                    # Stage 3+: standalone network consumer
    └── connect/                     # Stage 6: file source → broker → file sink
```

The split exists because:

- `sdk` must be importable by external users. Its `go.mod` cannot drag in broker internals.
- `examples` imports `sdk` exactly as a downstream user would, which catches accidental coupling during development.
- `proto` holds types both `broker` and `sdk` depend on, isolated to prevent import cycles.

## Roadmap

Each stage produces a working, runnable system. Each lands with a `docs/stage-N.md` design note explaining the *why*.

- [x] **Stage 1 — In-memory pub/sub.** Single process. Topics, partitions, fan-out subscribers. Establishes the core abstractions and the `sdk.Transport` boundary.
- [x] **Stage 2 — Persistent append-only log.** File-backed segmented log with sparse offset index, atomic index persistence, torn-tail recovery, time-based retention. Survives restarts. `holocrond --data-dir <path>`.
- [x] **Stage 3 — Network protocol.** TCP listener with hand-rolled length-prefixed binary framing. `sdk/net.Dial(addr)` returns an `sdk.Transport` that speaks v1 wire protocol. Long-poll Fetch, version-checked handshake, full opcode coverage for produce / fetch / metadata / commit / create-topic. Standalone `examples/producer` and `examples/consumer` now run against a real broker.
- [x] **Stage 4 — Consumer groups and partition rebalancing.** Multiple consumers in one group cooperatively share a topic's partitions via range assignment. Generation-based heartbeats; rejoin happens in the heartbeat goroutine so revoked pumps are cancelled before new generations fetch. Offsets stored broker-durably (JSON file in the data directory; internal-topic-backed store deferred to a later stage).
- [x] **Stage 5 — Replication and leader election.** Multi-node clusters via `hashicorp/raft`. Cluster mode is opt-in (`--cluster`); produces and topic-create operations replicate through Raft Apply. Followers redirect SDK clients to the leader's wire address with `StatusNotLeader`. Disk-backed Raft log + stable store (BoltDB), file snapshot store. End-to-end test: 3 in-process nodes, SDK dials a follower, automatic redirect, replicated to all three.
- [x] **Stage 6 — Connect-style framework.** Source/sink connector interfaces and a `Worker` runtime in a new `connect/` module. Reference connectors: `file.Source` (tail a file → produce) and `file.Sink` (consume → append to a file). Sink tasks share a consumer group, so partition assignment scales horizontally for free (Stage 4). End-to-end demo and test prove the source → broker → sink path through the SDK.
- [x] **Stage 7 — Optional schema registry.** Standalone `holocron-registry` daemon backed by a holocron topic (`__holocron_schemas`). Service kernel + Confluent-shaped HTTP API (`/subjects`, `/schemas/ids/{id}`, `/compatibility/...`). State recovers on startup by replaying the topic. Idempotent register, monotonic globally-unique IDs, single-partition-for-total-order, opaque schema text.
- [x] **Stage 8 — Stream processing library.** Kafka Streams analog in a new `streams/` module. Fluent DSL: `Stream(topic).Filter().Map().GroupByKey().Count(store).To(topic)`. Stateless ops (Filter / Map / FlatMap), stateful ops (Count, Aggregate) with a `StateStore` interface and an in-memory implementation. One goroutine per pipeline. Windowing, joins, and changelog-backed stores deferred to follow-ons (compaction is the dependency).
- [x] **Sustaining (batches 21–52).** Polish era: per-partition state stores with multi-task parallelism, changelog-backed stores with per-partition isolation, tumbling/hopping/session windows, stream-stream and stream-table joins (inner/left/outer), idempotent producer + persistent broker dedup, server-pushed rebalance via long-poll heartbeat, full operator-CLI surface (`group`, `topic`, `record`, `cluster`, `bench`, `tail`, `consume`, `produce`, `ping`, `offset` with `--json` everywhere), Producer/Consumer observability (`Stats`, `SendCount`, `Position`, `Lag`, `TotalLag`), pipeline-level error handling (`WithErrorHandler`, `WithDLQ`), DLQ + retry + transform connectors, schema-registry config + delete + Confluent-shape compat. See [`docs/sustaining.md`](docs/sustaining.md) for the per-theme breakdown.

## Non-goals

- Production durability guarantees on par with Kafka. Holocron is for learning and small workloads.
- Wire-protocol compatibility with Kafka, SQS, or any other system. Holocron speaks its own protocol.
- A managed service, a UI dashboard, or a Kubernetes operator. Those are separate problems.
- Exactly-once semantics in the distributed-transaction sense. At-least-once with idempotent consumers is the target.
- Content-based routing or transformation in the broker. Those live in clients or in the Connect tier.

## Contributing

Issues and pull requests are welcome. Please read [`CONTRIBUTING.md`](CONTRIBUTING.md) before opening a PR. The short version:

- One PR, one concern. Conventional Commits for the title and body.
- Add tests with the change. Integration tests for storage and broker; unit tests everywhere else.
- Update `docs/` when behavior changes. Stale documentation is worse than missing documentation.

## License

[MIT](LICENSE).

## Acknowledgements

Holocron's design is informed by Apache Kafka, NATS JetStream, Redpanda, and the LogDevice paper. Where this codebase makes a choice that differs from those systems, the surrounding docs explain why.

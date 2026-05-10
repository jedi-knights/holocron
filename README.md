# Holocron

A single-binary, Go-native distributed log broker — NATS JetStream's operational simplicity with the per-partition ordering and Kafka-shaped log model JetStream lacks.

![Maturity](https://img.shields.io/badge/maturity-pre--alpha-orange)
![Go](https://img.shields.io/badge/go-1.23+-00ADD8?logo=go)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

> **Status:** Pre-alpha. The foundation (Stages 1–8 plus the sustaining era — see [`docs/sustaining.md`](docs/sustaining.md)) ships disk-backed segmented storage, a network wire protocol, consumer groups, Raft-replicated clusters, a Connect tier, a schema registry, and a stream-processing library. Stage 9 (fresh-follower record catch-up) is **partially complete and paused** — see [`docs/stage-9.md#implementation-status`](docs/stage-9.md#implementation-status). Production gaps remain — TLS, authentication, multi-tenancy, and SDKs beyond Go — and are tracked in the [Roadmap](#roadmap) below. The on-disk format, wire protocol, and public APIs will change without notice until the first tagged release.

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
- [Comparison: Holocron vs NATS JetStream](#comparison-holocron-vs-nats-jetstream)
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

Holocron is a single-binary, Go-native distributed log broker. The name comes from the holocrons of Star Wars lore: crystalline, append-only repositories of knowledge, indexed and replayable by anyone with access.

The platform's primitive is the **distributed log** (row 1 above), and the other three categories — message queue, pub/sub fan-out, event bus — are realized on top of it via consumer groups and the Connect tier without giving up the "broker stays dumb" architectural rule. See [`docs/architecture.md`](docs/architecture.md#coverage-of-the-messaging-middleware-taxonomy) for the per-category mapping.

**Where Holocron fits in the market.** Holocron is positioned specifically against **NATS JetStream**, the only mainstream broker that shares its constraints — Go, single binary, no JVM, no external coordinator, no Kafka wire protocol. JetStream is excellent at messaging-and-light-persistence; it is weak at strict per-partition ordering (its model is publisher-side subject hashing; the Jepsen 2.12.1 analysis surfaced a truncation-related data-loss case, and "strict ordering" is still an open feature request upstream). Holocron's bet is to keep JetStream's deployment story and out-execute it on the log primitive itself. The full feature-by-feature comparison is in [Comparison](#comparison-holocron-vs-nats-jetstream).

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

The daemon reads each setting from a flag, falling back to a `HOLOCRON_*` environment variable, then to the default. See [`docs/tls.md`](docs/tls.md) for the full TLS guide.

| Variable | Default | Purpose |
|---|---|---|
| `HOLOCRON_DATA_DIR` | `/var/lib/holocron` | Directory the broker writes its segmented log into. |
| `HOLOCRON_LISTEN` | `:9092` | Address the broker's wire-protocol listener binds to. |
| `HOLOCRON_TLS_CERT` | — | PEM cert chain. Presence enables TLS on the wire listener. |
| `HOLOCRON_TLS_KEY` | — | PEM private key matching `HOLOCRON_TLS_CERT`. |
| `HOLOCRON_TLS_CLIENT_CA` | — | PEM CA bundle for client-cert verification (optional mTLS). |
| `HOLOCRON_TLS_REQUIRE_CLIENT_CERT` | `false` | Reject clients without a verified cert (requires `HOLOCRON_TLS_CLIENT_CA`). |
| `HOLOCRON_TLS_MIN_VERSION` | `1.3` | Minimum TLS version: `1.2` or `1.3`. |
| `HOLOCRON_NODE_ID` | — | Node identifier in cluster mode. |
| `HOLOCRON_RAFT_LISTEN` | `:9192` | Raft RPC bind address in cluster mode. |
| `HOLOCRON_PEERS` | — | Cluster membership: `id=raft-addr=wire-addr,...` |

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

## Comparison: Holocron vs NATS JetStream

Holocron is positioned specifically against [NATS JetStream](https://docs.nats.io/nats-concepts/jetstream). The matrix below is current as of pre-alpha and will be re-published with each milestone. "Parity" means feature equivalence at a quality bar suitable for a Go-native deployment, not exhaustive feature-for-feature mirroring.

| Capability | NATS JetStream | Holocron | Status |
|---|---|---|---|
| Single static binary, no external dependencies | Yes | Yes | Parity |
| Go-native server (no JVM, no sidecars) | Yes | Yes | Parity |
| Persistent streams (file + memory backends) | Yes | Yes | Parity |
| **Strict per-partition ordering as a broker primitive** | Weak (publisher-side subject hashing; Jepsen 2.12.1 found data loss) | Yes (per-partition single-writer, broker-enforced) | **Holocron win** |
| **Kafka-shaped log model** (replayable by offset, segmented, indexed) | No (subject-keyed message store) | Yes | **Holocron win** |
| Replication and leader election | Raft per-stream | Raft per-cluster (per-partition Raft on roadmap) | Parity |
| Consumer groups with partition rebalancing | Pull/push consumers, queue groups | Yes (range assignment, generation heartbeats) | Parity |
| Idempotent producer / broker-side dedup | Yes (message-ID dedup window) | Yes (persistent dedup) | Parity |
| Operator CLI | `nats` | `holocronctl` (full surface, `--json` everywhere) | Parity |
| Native stream-processing library | None | Yes (DSL, stateful ops, windows, joins) | **Holocron win** |
| Source/sink connector framework | None | Yes (`connect/` module + reference connectors) | **Holocron win** |
| Schema registry | None native | Yes (Confluent-shape HTTP API, broker-backed) | **Holocron win** |
| KV store layered on the log | Yes | No | **Gap** |
| Object store layered on the log | Yes | No | **Gap** |
| TLS for client and intra-cluster traffic | Yes | No | **Gap (production blocker)** |
| Authentication (JWT / NKey / accounts) | Yes | No | **Gap (production blocker)** |
| Multi-tenancy / account isolation | Yes | No | **Gap** |
| Mirror / source streams (cross-stream replication) | Yes | No | **Gap** |
| Leaf nodes (edge / IoT connectivity) | Yes | No | **Gap** |
| Cross-region / super-cluster geo-replication | Yes | No | **Gap** |
| SDKs beyond Go | Many (TS, Python, Rust, Java, .NET, …) | Go only | **Gap** |

The gaps drive the [Roadmap](#roadmap). The wins drive the positioning.

## Roadmap

The foundation has shipped. Going forward, work is **capability-driven** — grouped into waves prioritized by competitive gap with NATS JetStream and by what production users actually need. Each capability lands when it crosses production-quality thresholds (correct under partition, `-race` clean, documented, tested), not when scaffolding is in place.

### Shipped

The eight foundational stages plus the sustaining era are complete. Per-stage design notes are in [`docs/stage-N.md`](docs/) and the polish-era breakdown is in [`docs/sustaining.md`](docs/sustaining.md).

- [x] **Stages 1–3 — In-memory pub/sub → persistent segmented log → network wire protocol.** Topics, partitions, fan-out, file-backed segments with sparse index, hand-rolled length-prefixed framing, full opcode coverage for produce / fetch / metadata / commit / create-topic.
- [x] **Stage 4 — Consumer groups and rebalancing.** Range assignment, generation-based heartbeats, broker-durable offsets.
- [x] **Stage 5 — Raft replication and leader election.** `hashicorp/raft`, opt-in `--cluster`, follower redirect on `StatusNotLeader`.
- [x] **Stage 6 — Connect-style framework.** Source/sink connector interfaces, `Worker` runtime, reference file connectors.
- [x] **Stage 7 — Schema registry.** Standalone `holocron-registry` backed by a holocron topic, Confluent-shape HTTP API.
- [x] **Stage 8 — Stream processing library.** Fluent DSL, stateless and stateful operators, `StateStore` interface.
- [x] **Sustaining era.** Per-partition state stores with multi-task parallelism, changelog-backed stores, tumbling/hopping/session windows, stream-stream and stream-table joins, idempotent producer + persistent broker dedup, server-pushed rebalance, full operator-CLI surface, Producer/Consumer observability (`Stats`, `Position`, `Lag`), pipeline-level error handling, DLQ + retry + transform connectors.

### Wave 1 — Production-readiness (in flight)

Without these, no real production user can deploy Holocron. This wave is the gate to the first tagged release.

- [ ] **Stage 9 — Fresh-follower record catch-up.** Currently paused after M4; offset-gap scenario tracked in [`docs/stage-9.md`](docs/stage-9.md).
- [ ] **TLS** for client and intra-cluster traffic.
- [ ] **Authentication** — JWT/NKey-style credentials, scoped tokens for producers and consumers.
- [ ] **ACLs** — per-topic publish/subscribe authorization.
- [ ] **Multi-tenancy isolation** — account/namespace-level resource limits and topic visibility.

### Wave 2 — Layered stores (closes NATS's most-used API surface)

NATS JetStream's KV and Object store are heavily used in practice; closing this gap removes the most common reason a team picks JetStream over a Kafka-shaped log.

- [ ] **KV store** built on a compacted holocron topic, with the same get/put/watch shape JetStream users expect.
- [ ] **Object store** for large blobs, chunked across log records.

### Wave 3 — Ecosystem reach

- [ ] **Python SDK** (priority: largest non-Go user base).
- [ ] **TypeScript SDK** (priority: edge and full-stack apps).
- [ ] Stable, documented v1 wire protocol — the contract for non-Go clients.

### Wave 4 — Distribution and edge

- [ ] **Mirror / source streams** for cross-cluster replication.
- [ ] **Leaf nodes** for edge connectivity, mirroring NATS's leaf-node model.
- [ ] **Super-cluster-style geo-replication.**
- [ ] **Per-partition Raft** (currently per-cluster) for write-throughput scale-out.

### Wave 5 — Hardening (continuous)

- [ ] **Jepsen-grade fault injection** with public, reproducible reports — the bar Holocron must clear to credibly claim a correctness advantage over JetStream.
- [ ] **Published latency and throughput benchmarks** vs. NATS JetStream and Redpanda on the same hardware, refreshed per release.
- [ ] **Sendfile / zero-copy on the fetch path**, multiplexed connections, and other deferred performance items from the sustaining-era backlog ([`docs/sustaining.md#deferred-work`](docs/sustaining.md#deferred-work)).

## Non-goals

- **Wire-protocol compatibility with Kafka.** Holocron speaks its own protobuf-framed protocol. Targeting the Kafka API would put Holocron in Redpanda's market on Redpanda's terms, against a multi-year head start.
- **Diskless / S3-tiered storage as a v1 differentiator.** That axis is already crowded (WarpStream, Bufstream, AutoMQ); Holocron's local-disk segment work would become throwaway. May revisit post-v1 as an opt-in backend.
- **JVM-ecosystem connectors as launch features** — Kafka Connect, Kafka Streams, Flink. Holocron has its own native connect tier and stream-processing library.
- **A managed service, UI dashboard, or Kubernetes operator** at this stage. Self-hosted single-binary first; managed offerings are downstream of v1.
- **Exactly-once semantics in the distributed-transaction sense.** Idempotent producer + at-least-once delivery + consumer-side dedup is the target shape.
- **Content-based routing or transformation in the broker.** Those live in clients, the Connect tier, or the streams library — never in the broker process.

## Contributing

Issues and pull requests are welcome. Please read [`CONTRIBUTING.md`](CONTRIBUTING.md) before opening a PR. The short version:

- One PR, one concern. Conventional Commits for the title and body.
- Add tests with the change. Integration tests for storage and broker; unit tests everywhere else.
- Update `docs/` when behavior changes. Stale documentation is worse than missing documentation.

## License

[MIT](LICENSE).

## Acknowledgements

Holocron's design is informed by Apache Kafka, NATS JetStream, Redpanda, and the LogDevice paper. The competitive thesis specifically targets NATS JetStream — see [Comparison](#comparison-holocron-vs-nats-jetstream) — and where Holocron's choices diverge from JetStream or from any of the systems above, the surrounding docs explain why.

# Holocron vs the alternatives

> **TL;DR** — Holocron is for **Go-native services** that want the operational simplicity of NATS JetStream (one binary, no JVM, no coordinator) with the per-partition ordering JetStream lacks. If you're already running Kafka at scale or you need its ecosystem (Connect, Streams, Flink, Schema Registry — third-party), stay on Kafka. If you need diskless S3-tiered storage, look at WarpStream / Bufstream / AutoMQ.

## Quick decision matrix

| If you want… | Pick |
|---|---|
| Go-native, single-binary, strict per-partition ordering | **Holocron** |
| Kafka wire-protocol compatibility, deepest ecosystem, mature ops tooling | **Apache Kafka** |
| Kafka-compatible but JVM-free, with a commercial vendor | **Redpanda** |
| Lightweight messaging with KV / Object / WebSocket / leaf nodes | **NATS JetStream** |
| Diskless S3-backed storage, BYOC pricing | **WarpStream / Bufstream / AutoMQ** |
| Geo-replication, tiered storage, queue-and-stream in one product | **Apache Pulsar** |

## Holocron vs NATS JetStream — the head-to-head

NATS JetStream is the alternative Holocron is positioned against most directly. They share an operational profile (Go, single binary, no JVM, no coordinator). They differ on the log primitive itself.

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
| Wire-protocol TLS (server + mTLS) | Yes | Yes | Parity |
| Inter-node TLS | Yes | Yes (mandatory mTLS) | Parity |
| Operator CLI | `nats` | `holocronctl` (full surface, `--json` everywhere) | Parity |
| Native stream-processing library | None | Yes (DSL, stateful ops, windows, joins) | **Holocron win** |
| Source/sink connector framework | None | Yes (`connect/` module + reference connectors) | **Holocron win** |
| Schema registry | None native | Yes (Confluent-shape HTTP API, broker-backed) | **Holocron win** |
| KV store layered on the log | Yes | No | **Gap** |
| Object store layered on the log | Yes | No | **Gap** |
| Authentication (JWT / NKey / accounts) | Yes | No | **Gap** |
| Multi-tenancy / account isolation | Yes | No | **Gap** |
| Mirror / source streams (cross-stream replication) | Yes | No | **Gap** |
| Leaf nodes (edge / IoT connectivity) | Yes | No | **Gap** |
| Cross-region / super-cluster geo-replication | Yes | No | **Gap** |
| SDKs beyond Go | Many (TS, Python, Rust, Java, .NET, …) | Go only | **Gap** |

The gaps drive the [Roadmap](roadmap.md). The wins drive the positioning.

## Holocron vs Apache Kafka

**Pick Kafka when:**
- You need the ecosystem — Kafka Connect, Kafka Streams, Flink, ksqlDB, Confluent Schema Registry, mirror-maker
- You're hiring against a Kafka-fluent talent pool
- You're at scale (millions of messages/sec across hundreds of brokers) where Kafka's hardening is decades ahead
- You have an SRE team comfortable with the JVM, ZooKeeper-or-KRaft mode, and partition rebalancing

**Pick Holocron when:**
- You're a Go shop and want native interop without a Kafka bridge
- You want a single static binary, not a JVM stack with a metadata coordinator
- You don't need the ecosystem — your event flow lives between your own services
- You'd rather start small and grow into features than configure an enterprise broker for a 5-service deployment

Holocron does **not** speak the Kafka wire protocol. If Kafka API compatibility is mandatory, look at Redpanda, WarpStream, Bufstream, or AutoMQ — those compete on the Kafka-API axis explicitly.

## Holocron vs Redpanda

Redpanda is Kafka-API-compatible, written in C++ for low-latency throughput. Pick Redpanda when you need Kafka API compatibility without the JVM. Holocron does not compete on this axis — it has its own protobuf-framed protocol because chasing Kafka API compatibility would put a small project in a multi-year war against a well-funded incumbent.

## Holocron vs WarpStream / Bufstream / AutoMQ

These are diskless brokers that store records in S3 (or S3-compatible object storage) instead of local disk. The pitch is dramatic cost reduction at scale by eliminating inter-AZ replication traffic. Pick them when:

- You're cloud-only with high cross-AZ data charges
- Your throughput is high enough that S3 latency is acceptable

Holocron uses local-disk segments. Diskless storage may land as an opt-in backend post-v1, but the local-disk path is the primary mode and the one the rest of the design is built around.

## Holocron vs Apache Pulsar

Pulsar offers a unified model for queues + streams plus tiered storage and geo-replication. Pick Pulsar when you need all of that in one product. Pulsar's adoption stalled in 2026 and the deployment model (broker tier + BookKeeper tier + ZooKeeper-or-etcd) is heavy compared to Holocron's single binary.

## Non-goals

Holocron deliberately does not target:

- **Wire-protocol compatibility with Kafka.** Holocron speaks its own protobuf-framed protocol. Targeting the Kafka API would put Holocron in Redpanda's market on Redpanda's terms.
- **Diskless / S3-tiered storage as a v1 differentiator.** That axis is already crowded; Holocron's local-disk segment work would become throwaway. May revisit post-v1 as an opt-in backend.
- **JVM-ecosystem connectors as launch features** — Kafka Connect, Kafka Streams, Flink. Holocron has its own native connect tier and stream-processing library.
- **A managed service, UI dashboard, or Kubernetes operator** at this stage. Self-hosted single-binary first; managed offerings are downstream of v1.
- **Exactly-once semantics in the distributed-transaction sense.** Idempotent producer + at-least-once delivery + consumer-side dedup is the target shape.
- **Content-based routing or transformation in the broker.** Those live in clients, the Connect tier, or the streams library — never in the broker process.

## Where to go next

- [Integration in Go](integration-go.md) — get productive
- [Roadmap](roadmap.md) — what's coming next

# Roadmap

The foundation has shipped. Going forward, work is **capability-driven** — grouped into waves prioritized by competitive gap with NATS JetStream and by what production users actually need. Each capability lands when it crosses production-quality thresholds (correct under partition, `-race` clean, documented, tested), not when scaffolding is in place.

For *why* this is the priority order, see [`comparison.md`](comparison.md).

## Shipped

The eight foundational stages plus the sustaining era are complete. Per-stage design notes are in `stage-N.md`; the polish-era breakdown is in [`sustaining.md`](sustaining.md).

- [x] **Stages 1–3 — In-memory pub/sub → persistent segmented log → network wire protocol.** Topics, partitions, fan-out, file-backed segments with sparse index, hand-rolled length-prefixed framing, full opcode coverage for produce / fetch / metadata / commit / create-topic.
- [x] **Stage 4 — Consumer groups and rebalancing.** Range assignment, generation-based heartbeats, broker-durable offsets.
- [x] **Stage 5 — Raft replication and leader election.** `hashicorp/raft`, opt-in `--cluster`, follower redirect on `StatusNotLeader`.
- [x] **Stage 6 — Connect-style framework.** Source/sink connector interfaces, `Worker` runtime, reference file connectors.
- [x] **Stage 7 — Schema registry.** Standalone `holocron-registry` backed by a holocron topic, Confluent-shape HTTP API.
- [x] **Stage 8 — Stream processing library.** Fluent DSL, stateless and stateful operators, `StateStore` interface.
- [x] **Sustaining era.** Per-partition state stores with multi-task parallelism, changelog-backed stores, tumbling/hopping/session windows, stream-stream and stream-table joins, idempotent producer + persistent broker dedup, server-pushed rebalance, full operator-CLI surface, Producer/Consumer observability (`Stats`, `Position`, `Lag`), pipeline-level error handling, DLQ + retry + transform connectors.

## Wave 1 — Production-readiness (in flight)

Without these, no real production user can deploy Holocron. This wave is the gate to the first tagged release.

- [ ] **Stage 9 — Fresh-follower record catch-up.** Currently paused after M4; offset-gap scenario tracked in [`stage-9.md`](stage-9.md).
- [x] **TLS** for client and intra-cluster traffic. Server TLS, optional/required mTLS for clients, mandatory mTLS for Raft. See [`tls.md`](tls.md).
- [x] **Authentication** — Ed25519-signed JWT credentials, scoped tokens for producers and consumers, denylist with SIGHUP reload, `holocronctl auth issue` / `inspect`. See [`auth.md`](auth.md).
- [x] **ACLs** — per-topic publish/subscribe + admin authorization driven by JWT `holocron.scopes` claims. Three verbs (produce/consume/admin), prefix wildcards, deny by default when auth is configured. See [`authorization.md`](authorization.md).
- [ ] **Multi-tenancy isolation** — account/namespace-level resource limits and topic visibility. The `holocron.account` claim is already plumbed; this wave adds enforcement.
- [ ] **Cert hot-reload on `SIGHUP`** — small follow-on to the TLS work; closes the rotation gap.

## Wave 2 — Layered stores (closes NATS's most-used API surface)

NATS JetStream's KV and Object store are heavily used in practice; closing this gap removes the most common reason a team picks JetStream over a Kafka-shaped log.

- [ ] **KV store** built on a compacted holocron topic, with the same get/put/watch shape JetStream users expect.
- [ ] **Object store** for large blobs, chunked across log records.

## Wave 3 — Ecosystem reach

- [ ] **Stable, documented v1 wire protocol** — the contract for non-Go clients. Gates everything in this wave.
- [ ] **Python SDK** (priority: largest non-Go user base).
- [ ] **TypeScript SDK** (priority: edge and full-stack apps).

## Wave 4 — Distribution and edge

- [ ] **Mirror / source streams** for cross-cluster replication.
- [ ] **Leaf nodes** for edge connectivity, mirroring NATS's leaf-node model.
- [ ] **Super-cluster-style geo-replication.**
- [ ] **Per-partition Raft** (currently per-cluster) for write-throughput scale-out.

## Wave 5 — Hardening (continuous)

- [ ] **Jepsen-grade fault injection** with public, reproducible reports — the bar Holocron must clear to credibly claim a correctness advantage over JetStream.
- [ ] **Published latency and throughput benchmarks** vs. NATS JetStream and Redpanda on the same hardware, refreshed per release.
- [ ] **Sendfile / zero-copy on the fetch path**, multiplexed connections, and other deferred performance items from the sustaining-era backlog ([`sustaining.md`](sustaining.md#deferred-work)).
- [ ] **Built-in Grafana dashboard** for the daemon's metrics surface.

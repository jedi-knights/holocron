# Architecture

This document captures the durable architectural decisions that govern
holocron's structure. New code that contradicts these decisions should
either be reshaped to fit them or trigger a deliberate decision to amend
this document — never both at once.

## The broker stays dumb

The broker accepts records, durably orders them within partitions, and
serves them to consumers. It does **not**:

- transform, filter, or enrich records,
- evaluate user-supplied expressions over record payloads,
- run user-supplied code in the broker process,
- coordinate distributed transactions across topics.

All of that lives either in the producer/consumer SDKs or in a separate
Connect-style worker tier (Stage 6). This is the same separation Kafka
makes; the reasoning is the same:

1. **Stable hot path.** The broker's append + index + fan-out path is small
   and CPU-predictable. Mixing transformation work in destabilizes p99
   latency for everyone.
2. **Independent scale.** Transformation workloads are CPU-heavy and
   bursty; broker workloads are I/O-heavy and steady. Conflating them
   couples capacity planning across two very different shapes.
3. **Fault isolation.** A buggy filter shouldn't crash the broker.
4. **Multi-language clients.** If transformations live in clients, every
   language can run them. If they live in the broker, only the broker's
   language can.

## Module boundaries

```
proto       ── shared data types          (no other deps)
  │
  ├── sdk   ── public client surface       (depends on proto)
  │     │
  │     └── examples (downstream)         (depends on sdk + proto)
  │
  └── broker
        ├── cmd/holocrond                  (the daemon binary)
        ├── embed                          (public: in-process broker handle)
        ├── inproc                         (public: sdk.Transport over a Broker)
        └── internal/                      (private to the broker module)
              ├── storage  (Store + MemoryStore + FileStore)
              ├── log      (segmented append-only log — Stage 2)
              ├── topic    (Registry)
              ├── broker   (publish/subscribe core)
              └── server   (transport — Stage 3)
```

Two boundaries are load-bearing and must not be crossed:

1. **`broker/internal/...` is private.** External callers (SDK, examples,
   CLI, tests in other modules) must go through `broker/embed` or
   `broker/inproc`. Internal packages are free to reshape; the public
   surface is stable.
2. **The SDK does not import the broker.** The Producer and Consumer talk
   to a `sdk.Transport` interface. Stage 1 ships an in-process
   implementation; Stage 3 will ship a network one. Neither implementation
   is an SDK concern.

## The Strategy boundary at `sdk.Transport`

`sdk.Transport` is the single seam that lets the same SDK serve an
in-process broker (Stage 1) and a remote broker (Stage 3) without code
changes. Each method on it is a candidate to be reshaped during the
network design — but Producer/Consumer code, and any user code on top of
them, is not.

When evaluating an SDK change, the question is: *will this still make
sense when Transport is a network call with batching, compression, and
retries?* If not, push it into Transport.

## Stages and what they add

Each stage lands one production-grade concept end to end. No stage
introduces scaffolding for a later stage.

| Stage | Adds |
|---|---|
| 1 | In-memory pub/sub. Topics, partitions, fan-out subscribers. |
| 2 | Persistent segmented log on top of `storage.Store`. Same broker API. |
| 3 | Network protocol. Real TCP/gRPC transport replaces `inproc`. |
| 4 | Consumer groups + partition rebalancing. Broker-side committed offsets. |
| 5 | Replication and leader election (Raft). |
| 6 | Connect-style framework for source/sink connectors. |
| 7 | Optional schema registry. |
| 8 | Stream processing library — Kafka Streams analog with stateful ops backed by compacted internal topics. |
| Sustaining | Polish era. ~96 features across 32 batches: per-partition state, multi-task pipelines, persistent dedup, windowing, joins, full CLI surface, observability, pipeline error handling. See [`sustaining.md`](sustaining.md). |

## Deferred work

The eight stages are complete; the sustaining era is closed. Six items
remain in the backlog, all large enough to be individual stages of
their own:

| Area | What's deferred | Why deferred |
|---|---|---|
| Streams | Exactly-once across produce + commit-offset | Needs broker-side transactions; substantial new wire ops + recovery story |
| Schema registry | Avro / Protobuf parsing with type/default/nesting compatibility | Needs a real schema parser; batch 27 ships a structural required-field check that catches the common cases |
| Broker storage | Linux `sendfile(2)` fast path | Disk and wire formats already align (batch 5); needs a build-tagged splice path with compression-aware fallback |
| Cluster | Per-partition Raft | Decouples write throughput from a single leader; meaningful refactor of the cluster module |
| Cluster | Continuous follower replication | Batch 22 ships a one-shot snapshot; long-lived replication needs a streaming follower mode tracking the leader's log |
| Wire protocol | Multiplexed connections (correlation IDs, ordered demux) | One-RPC-per-connection today; multiplexing reduces connection churn at the cost of demux complexity |

These are documented as known shape, not roadmap commitments. The
codebase is at a natural pause point and any of them is a multi-week
project on its own.

## Coverage of the messaging-middleware taxonomy

Messaging middleware (Message-Oriented Middleware, MOM) splits into four
recognized categories. Holocron's primitive is the **distributed log** —
the first row below — and the other three are realized on top of it
without giving up the "broker stays dumb" rule.

| Category | Examples | How holocron covers it | Stage |
|---|---|---|---|
| Distributed log / event streaming | Kafka, Kinesis, Pulsar, Redpanda | Native primitive: topics, partitions, replayable offsets, time/size retention. | 1 – 2 |
| Message queue (point-to-point) | SQS, RabbitMQ queues, ActiveMQ | Single-partition topic + a single consumer group. `Poll` reads, `Commit` advances the group's offset. Visibility timeout = held-but-uncommitted offset; if the holder dies, rebalance reassigns the partition. Dead-letter = a separate topic. | 4 |
| Pub/Sub broker (fan-out) | SNS, Google Pub/Sub, RabbitMQ topics | Same topic, **N consumer groups** (one per subscriber). Each group reads independently with its own committed offset. The fan-out shape today (multiple subscribers in `embed_test.go`) becomes proper pub/sub at Stage 4. | 4 |
| Event bus / router | EventBridge, NATS JetStream | A **router connector** in the Connect tier — reads from a source topic, evaluates rules, writes matches to target topics. Routing is *not* in the broker. | 6 |

This split is deliberate. The alternative — putting routing rules in the
broker, the way EventBridge does — would violate the "broker stays dumb"
rule above. Routing is user-supplied logic; user-supplied logic
destabilizes the broker hot path. Connect is where it belongs.

### Conceptual distinctions

The taxonomy makes more sense when the underlying axes are explicit:

- **Queue vs log.** A queue deletes records after acknowledgment; a log
  retains them for the configured retention window and lets consumers
  seek by offset. **Replayability** is the headline difference. Holocron
  is a log; queue semantics are recovered by adding consumer groups
  (Stage 4) and treating "consumed and committed" as the equivalent of
  "deleted from a queue."
- **Push vs pull.** SNS pushes records to subscribers; Kafka and
  Holocron pull. Push is simpler for the consumer; pull handles
  backpressure better and lets the broker stay stateless about consumer
  liveness. The Stage-4 SDK can add a push-style helper (callback-driven
  consume loop) as sugar over `Poll`, but the wire protocol stays pull.
- **Topic vs subject vs stream.** Different vendors, same idea — a
  named, durable channel of records. Holocron uses **topic** throughout.

## Header conventions

The broker treats all headers as opaque, but the SDK and connector
ecosystem rely on the following keys (declared in `sdk/headers.go`):

- `holocron.schema` — schema name or ID for the record value.
- `holocron.idempotency-key` — producer-assigned dedup key for downstream
  sinks.
- `holocron.trace-id` — distributed-trace identifier.

New conventions are added here only when they need to be standard across
multiple connectors. Application-specific headers stay in the application.

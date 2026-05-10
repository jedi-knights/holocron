# Event-driven architecture, in 10 minutes

This page is for developers who are evaluating Holocron and want a grounded answer to *what is this for?* before reading any API docs. If you already know what an event log is and why you'd want one, skip to [`integration-go.md`](integration-go.md).

## The problem: services need to talk

Modern applications are rarely a single process. You have an orders service, a payments service, an inventory service, a notifications service, and an analytics service. They all need to know about each other's events.

Two patterns dominate:

1. **Synchronous request/response** — orders calls payments and waits. Simple, but it couples availability (payments down → orders down), forces orders to know every service that cares about a new order, and makes refactoring painful.
2. **Asynchronous event passing** — orders publishes "order placed" to a broker. Payments, inventory, notifications, and analytics independently subscribe. Orders doesn't know who listens. The broker absorbs throughput differences. Adding a new consumer is a deployment, not a code change in orders.

The second pattern is **event-driven architecture (EDA)**. The infrastructure that makes it work — the piece sitting between producers and consumers — is called **messaging middleware** or, in its modern log-centric form, an **event streaming platform**.

## Four shapes of broker

Not every broker is the same shape. The category you need depends on what you want the broker to *be*:

| Shape | Examples | What it does |
|---|---|---|
| **Distributed log** | Apache Kafka, AWS Kinesis, Apache Pulsar, Redpanda, **Holocron** | An append-only log per topic. Consumers read by offset and can replay history. |
| **Point-to-point queue** | AWS SQS, RabbitMQ queues, ActiveMQ | Each message is delivered to exactly one consumer. No replay. |
| **Pub/sub fan-out** | AWS SNS, Google Pub/Sub | Each event is fanned out to every subscriber. Usually no persistence. |
| **Event bus with routing** | AWS EventBridge, NATS JetStream | Pub/sub plus content-based routing rules. |

Holocron is in the first row. Its primitive is the **distributed log**: an ordered, durable, replayable sequence of records. The other three shapes can be built on top of a log (queue = consumer group with one member; fan-out = consumer group per subscriber; bus = a routing layer in front), which is why Kafka-shaped systems eat the rest of the market over time.

## Why a log, specifically?

Three properties you only get from a log:

1. **Replay.** A new analytics service can read every order from the beginning of time. A queue threw those messages away the moment they were consumed.
2. **Per-key ordering.** Records with the same key (e.g. `user-42`) land on the same partition and stay in order. Critical for things like "balance updated" events.
3. **Decoupled producers and consumers.** A burst of 100,000 orders/sec doesn't crash your downstream services — they consume at their own rate.

The trade-off: logs are *not* the right shape for one-shot RPC, fire-and-forget commands, or workflows that need transactional commit across services. Use the right tool.

## Where Holocron fits

Holocron is a single-binary, Go-native distributed log broker. It targets the same operational profile as **NATS JetStream** — one binary, no JVM, no external coordinator — while providing the per-partition ordering and Kafka-shaped log model JetStream lacks.

Concretely: if you're a Go shop building event-driven services and you've been weighing JetStream against running Kafka, Holocron is the third option. See [`comparison.md`](comparison.md) for the matrix.

## Where to go next

- [Integration in Go](integration-go.md) — your first producer and consumer
- [Comparison vs alternatives](comparison.md) — when to pick Holocron, Kafka, JetStream, or Redpanda
- [Operator guide](operator-guide.md) — running the broker

# Stage 1 — In-memory pub/sub

Stage 1 establishes the core abstractions: topics, partitions, records,
producer/consumer, and the Transport boundary between SDK and broker.
Persistence and the network are deliberately deferred — both add concerns
that obscure the underlying model.

Demo: `go run ./examples/inproc`. Tests: `make test`.

## What ships

| Component | Package | Role |
|---|---|---|
| Shared data types | `proto` | `Record`, `Header`, `TopicConfig`, `PartitionRef`. |
| Topic registry | `broker/internal/topic` | Source of truth for topic existence and partition counts. |
| Storage interface | `broker/internal/storage` | `Store` (Strategy boundary) and `MemoryStore` (in-memory implementation). |
| Pub/sub core | `broker/internal/broker` | Append + fan-out, per-partition single-writer. |
| Embedded handle | `broker/embed` | Public façade bundling the above. The only way in for callers outside the broker module. |
| In-process transport | `broker/inproc` | Implements `sdk.Transport` against a local `broker.Broker`. |
| SDK client surface | `sdk` | Stable `Producer` and `Consumer`, identical at every later stage. |
| Demo | `examples/inproc` | Single-process producer → broker → consumer. |

## Design decisions

### Why a Transport interface in the SDK

The SDK does not import the broker. It depends only on `proto` and on its
own `Transport` interface. Stage 1 ships `broker/inproc` as the in-process
implementation; Stage 3 will ship a network implementation. The Producer
and Consumer types do not change.

This is the **Strategy pattern**: `Transport` is the strategy, with one
implementation today and another later. The point of the pattern is not
elegance in the abstract — it's that user code written against the SDK
today will keep working when the broker grows a network.

### Why per-partition single-writer locking

Each partition has its own mutex. `Publish` acquires that mutex for the
duration of `store.Append` plus fan-out to subscribers; `Subscribe`
acquires it briefly to snapshot the high water and register itself. This
yields three properties:

- **Append order = offset order.** No two records can be assigned the same
  offset, and an observer reading the partition sees offsets in the order
  records were appended.
- **Cross-partition parallelism.** Different partitions have independent
  locks; a slow consumer on partition A does not stall publishes to
  partition B.
- **Consistent subscriber registration.** A new subscriber observes a
  high-water snapshot taken under the same lock that gates new appends, so
  no record can slip between catch-up and live-tail.

The trade-off: a slow subscriber blocks publishes to the partition it's
subscribed to (its bounded channel fills, the broker's send blocks, the
partition lock is held). That's correct backpressure for at-least-once
delivery. Production brokers add per-subscriber overflow policies (drop,
disconnect-the-laggard, etc.); Stage 1 keeps the simplest correct
behavior.

### Why catch-up + live-tail subscribe shape

`Subscribe(ctx, partition, fromOffset)` is two phases:

1. **Catch-up.** Read records `[fromOffset, hwAtRegister)` from the store.
2. **Live tail.** Forward records appended after registration via the
   live-fanout channel.

The phases share the partition lock at the moment of registration: under
the same lock that gates new appends, the broker reads the high water and
appends the new subscriber to the fan-out list. This guarantees no record
appended after registration is missed, and no record appended before
registration is delivered twice.

The pump goroutine does the catch-up reads (potentially blocking on a slow
consumer) and then transitions to receiving from its private fan-out
channel for live records.

### Why the SDK fans in to a single channel

`Consumer` opens one `Subscribe` per assigned partition and pumps each
into a single internal `fanIn` channel. `Poll` reads from `fanIn`. The
alternative — `reflect.Select` over many partition channels — works but is
heavyweight and obscure.

The trade-off: per-partition order is preserved within a partition, but
the consumer sees no global ordering across partitions. That matches the
guarantees the broker offers and the standard Kafka consumer behavior.

### Why FNV-1a for the default partitioner

The hash function used to route records by `Key` must be stable across
producers — every producer that hashes `Key` "user-42" must land on the
same partition. FNV-1a is small, allocation-free, and fast. It is **not
the wire-stable choice**: `docs/data-model.md` open question tracks
Murmur2 vs xxhash vs FNV for the Stage 3 wire protocol. Until then, any
consistent choice works.

## Acceptance

Stage 1 is "done" when:

- `make build` succeeds.
- `make test` and `go test -race ./...` both pass.
- `go run ./examples/inproc` round-trips records through producer →
  broker → consumer.

## Known limitations (intentional)

These are absent on purpose. Reaching for them is a sign the work belongs
in a later stage.

- No persistence — restarting the process drops all data.
- No retention or compaction.
- No network listener — producers and consumers must live in the same
  process as the broker (via `broker/embed`).
- No batching on the wire (no wire yet).
- No `acks` levels — every append is treated as durable-to-the-store.
- No consumer groups — each consumer tracks its own offsets.
- No metrics, no admin API. The CLI prints a placeholder.

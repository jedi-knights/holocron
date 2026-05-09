# Stage 8 — Stream processing library

Stage 8 closes the roadmap. The broker stores events, Connect moves
them between systems, the registry types them — and now `streams` turns
them into computations. This is the Kafka Streams analog: a small DSL
for filter / map / aggregate pipelines that read from holocron topics,
maintain state in local stores, and write results back to holocron
topics.

## What ships

| Component | Package | Role |
|---|---|---|
| Topology builder | `streams/topology.go` | `New(transport)`, `Stream(topic)`, fluent operator chaining, `Run`/`Start`/`Stop`. |
| Stateless ops | `streams/topology.go` | `Filter`, `Map`, `FlatMap`. |
| Stateful ops | `streams/topology.go` | `GroupByKey().Count(store)` and `GroupByKey().Aggregate(store, fn)`. |
| State stores | `streams/store.go` | `StateStore` interface; in-memory `MemoryStore` implementation. |
| Sinks | `streams/topology.go` | `.To(topic)` writes back to a topic; `.ForEach()` is a terminal that only updates state. |
| End-to-end test | `streams/topology_test.go` | Filter/map → topic, count by key, running aggregate. |
| Demo | `examples/streams/main.go` | Counts five clicks across two keys; prints both the changelog stream and the final state. |

## The DSL in one example

```go
top, _ := streams.New(transport)

// Cleaning pipeline.
top.Stream("clicks-raw").
    Filter(func(r proto.Record) bool { return len(r.Value) > 0 }).
    Map(func(r proto.Record) proto.Record {
        return proto.Record{Key: r.Key, Value: []byte("clean:" + string(r.Value))}
    }).
    To("clicks-clean")

// Aggregation pipeline.
top.Stream("clicks-clean").
    GroupByKey().
    Count("clicks-by-key").
    To("counts-out")

top.Run(ctx)
```

Two pipelines, one topology, one Run. Each pipeline becomes a goroutine
that consumes from the source topic, applies the operator chain, and
either produces to the sink topic or terminates with a state-store
update.

## Design decisions

### Why a fluent DSL instead of a Processor API

Two common shapes exist:

1. **Fluent DSL** — `stream.Filter(...).Map(...).To(...)`.
   Optimized for the common case; reads top-down.
2. **Processor API** — explicitly add named processors and edges.
   More general; lets a single processor have multiple inputs and
   outputs.

Stage 8 ships the DSL because it's the shape every Streams tutorial
shows first; the lesson is "stream processing as composition of small
operators." A Processor API would slot underneath cleanly later — the
DSL would compile down to it.

### Why operators are inline transformations, not goroutines

A more abstract design would spawn one goroutine per operator and
connect them with channels. That generalizes to fan-out and fan-in
trivially, but multiplies goroutine count by pipeline depth.

For Stage 8 V1, every operator is a pure function (`op` type) applied
inline in the source consumer's poll loop. Each pipeline is exactly
one goroutine — one source, a chain of transforms, one optional sink.
Fan-out is achievable by registering multiple pipelines with the same
source topic; the underlying SDK consumer handles it.

### Why state stores are in-memory and not changelog-backed

Real Kafka Streams writes every state-store update to a **changelog
topic** (compacted, single-partition-per-task) so a restarting task
can rebuild local state by replaying the changelog. Stage 8 V1 ships
in-memory stores only; state is lost on restart.

The trick is straightforward to add: each `Put` produces a record to
`<store-name>-changelog`; on `Start`, replay that topic into the store
before processing inputs. Two reasons it's deferred to a follow-on:

1. **Compaction matters here.** The changelog grows with every
   update; without compaction, replays scan unbounded data. Holocron's
   log compaction is on TODO.md.
2. **Stage 8 still teaches the model.** The DSL, operator chaining,
   and state-store interface are all visible; durability is a layer
   that slots in without changing the surface.

### Why GroupByKey is a no-op marker

In Kafka Streams, `groupByKey()` semantically partitions the stream so
records with the same key co-locate on the same task. With the default
sdk Partitioner (FNV-1a hash of key, deterministic), that's already
true — same-keyed records always land on the same partition. Holocron
does not yet have per-partition tasks (Stage 8 V1 is single-task), so
`GroupByKey` exists only to gate `Count` / `Aggregate`. Once
per-partition parallelism lands, `GroupByKey` becomes meaningful as
the marker for re-keying via internal repartition topics.

### Why Count returns a Stream of changelog updates

Two choices:
- Count returns a `Table` (logical: a current view per key).
- Count returns a `Stream` of changelog records (every update is a
  record).

Holocron returns the Stream. It's the simpler shape, downstream
operators chain naturally (`.Count(store).To(topic)`), and the table
view is recoverable from the state store directly via `topology.Store
(name).Range(fn)`. Real Kafka Streams has both abstractions; the
simpler one is enough to demonstrate the lesson.

### Why output count values are 8-byte big-endian

`Count` and `Aggregate` need a wire format for their output values.
Eight big-endian bytes is the smallest fixed-size encoding that holds
any plausible count, and big-endian sorts naturally for downstream
range queries. `EncodeCount` / `DecodeCount` are exported so external
producers and consumers can match the format.

## Acceptance

- `make build && go test -race ./...` is green.
- `streams/topology_test.go::TestStream_FilterMapToTopic`: filter +
  map produces the expected records on the output topic.
- `streams/topology_test.go::TestStream_GroupByKeyCount`: 5 inputs
  produce 5 changelog records and the right per-key counts.
- `streams/topology_test.go::TestStream_AggregateRunningSum`: per-key
  running sum reaches the expected value.
- `go run ./examples/streams` prints the changelog stream then the
  final per-key counts (`a = 3`, `b = 2`).

## Known limitations (intentional)

- **Single-task per pipeline.** No per-partition parallelism. Fine
  pedagogically; production scale needs partition-aware task
  assignment via consumer groups (the same machinery Stage 4 already
  has — wiring it in is a follow-on).
- **State stores are in-memory only.** No changelog topic, no
  recovery on restart. The follow-on is straightforward but waits on
  log compaction.
- **No windowing.** `Count` and `Aggregate` are over the entire
  history of a key. Tumbling, hopping, and session windows all
  require time semantics (event time vs processing time, watermarks)
  which is a stage of its own.
- **No joins.** Stream-stream and stream-table joins need windowing
  and time semantics; both deferred.
- **Offsets reset on restart.** The runtime subscribes from offset 0;
  the broker already has consumer groups for resumption (Stage 4),
  but the streams runtime doesn't yet wire `WithGroup`. One-line fix
  once the consumer-group / state-store recovery story is unified.
- **No exactly-once.** Each record is processed at-least-once.
  Exactly-once would require broker-side transactions across produce
  + commit-offset boundaries — a meaningful broker addition.

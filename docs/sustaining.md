# Sustaining era (batches 21–52)

After Stage 8 closed the original eight-stage roadmap, the codebase
entered a polish/sustaining phase: ~96 features across 32 batches,
each adding three coherent items end-to-end with tests under `-race`.

This document organizes the sustaining work by theme. The
batch-by-batch detail lives in `git log` and the historical entries
in `TODO.md`. The point of this doc is the *shape* of what landed,
not a comprehensive enumeration.

## Themes

### Streams API breadth

The Stage 8 V1 DSL was Filter / Map / FlatMap + GroupByKey().Count or
.Aggregate. The sustaining era expanded it into a full Kafka-Streams
analog:

- **Per-partition tasks** with `WithMaxTasks(n)` so pipelines parallelize
  across partitions, plus per-partition state stores so two records
  with the same key on different partitions don't collide.
- **Changelog-backed stores** with per-partition isolation
  (`<store>-changelog` topics, one partition per task), so a
  rebalance-driven reassignment rebuilds local state from the
  partition's own history without leaking other partitions' writes.
- **Windowing**: tumbling, hopping, session — driven by event-time
  watermarks with idle-detection so quiet streams keep advancing.
- **Joins**: stream-stream (inner), LeftJoin, OuterJoin — all with
  window-close-and-emit so an outer no-match fires after the window
  closes, not on first arrival. Stream-table joins via KTable +
  `JoinTable`. `Stream.ToTable` materializes a transformed stream
  into a KTable in-place.
- **Operator surface**: SelectKey, MapValues, MapKeyValue, GroupBy,
  Reduce, Branch, Through, FilterNot, Distinct, Skip, Take, Sample,
  Throttle, Tap, Peek, ForEachFunc, Print/PrintTo, Sum.
- **Error handling**: `WithErrorHandler` catches op panics and
  reports them live; `WithDLQ` routes panicked records to a
  dead-letter topic with a `holocron.dlq.error` header.

### Producer ergonomics + observability

- **Idempotency**: `WithIdempotency()` stamps `holocron.producer.id`
  + `holocron.producer.seq` headers; broker dedupes retries by the
  pair, persisted across restarts via the `__holocron_dedup` topic
  with `WithDedupTopic`. TTL eviction (`WithDedupTTL`) bounds memory.
- **Send paths**: `Send`, `SendBatch`, `SendNoWait` (fire-and-forget)
  with `WithMaxInFlight(n)` semaphore-bounded concurrency.
- **Hooks**: `WithOnSent` fires per successful record;
  `WithOnAsyncError` fires per SendNoWait failure.
- **Compression**: `WithCompression(LZ4)` + `WithCompressionLevel`
  (0 = fast, 1..9 = LZ4-HC for ratio).
- **Retry**: `WithRateLimitRetry(tries, wait)` + `WithRetryOn(statuses
  ...)` to widen retry beyond StatusRateLimited.
- **Observability**: `Stats()` returns `{SendCount, AsyncErrors,
  Pending, BatcherCount}` — one call instead of four accessors.

### Consumer ergonomics + observability

- **Subscription**: `Subscribe`, `SubscribeMany` (single JoinGroup
  for N topics), `Assign` for self-managed.
- **Position control**: `Pause` / `Resume` per partition, `PauseAll`
  / `ResumeAll`, `Seek(p, offset)`, `SeekToBeginning`, `SeekToEnd`.
- **Introspection**: `Position(p)`, `Lag(ctx, p)`, `TotalLag(ctx)`,
  `Assignment()`, `Topics()`, `Stats() {Topics, Assignment,
  PolledCount, PerPartition}`.
- **Commit**: `Commit(p, offset)`, `CommitAll(ctx)`, `WithAutoCommit
  (d)` — background goroutine ticks CommitAll every d.
- **Group-coordinated**: long-poll heartbeats deliver server-pushed
  rebalance via dedicated heartbeat connection; sticky member-IDs
  via `WithMemberID` skip LeaveGroup on Close so a quick restart
  rejoins without churn.

### Operator-facing CLI surface

The CLI grew from ~5 subcommands at end of Stage 8 to a full operator
toolkit. Every read-side command emits JSON via `--json`:

| Group | Subcommands |
|---|---|
| `topic` | create, delete, describe, list, stats (`--all-topics`), update, head, last, copy (`--all-partitions`), dump (file or stdout, JSONL with UTF-8/base64 dual encoding), load (`--batch`) |
| `record` | fetch (`--json`) |
| `produce` | single, stdin mode (`--key-sep`), batch mode (`--batch`), headers (`--header`, repeatable), idempotent (`--idempotent`) |
| `consume` | full (`--json`), live-tail (`--json`) |
| `group` | list, describe, offsets, delete, reset-all (`--to=earliest|latest`), rename |
| `offset` | commit, reset |
| `cluster` | status, members, join, leave |
| `bench` | produce mode, `--consume`, `--producer-count N` for parallel load |
| `ping` | dial+ListTopics health probe (`--json`) |

### Connect tier

- **Source/sink connectors** with retry + DLQ (`WithRetry`,
  `WithDLQ`).
- **Reference connectors**: `file.Source`, `file.Sink`, HTTP source +
  sink, router (content-based routing with rich predicates including
  `HeaderExists`, `KeyRegex`, `Any`), transform.
- **Distributed coordination**: `WithSourceCoordTopic` makes source
  tasks consumer-group members so partition assignment scales
  horizontally for free.
- **Source-offset persistence**: `OffsetStore` interface with
  `MemoryOffsetStore` and `TopicOffsetStore` backed by
  `__connect_offsets`; restart-resume with no double-production.
- **Per-task observability**: `Worker.Stats()` returns
  `[]TaskStats{Connector, TaskIndex, Records, Bytes}`.

### Schema registry

- **Confluent-shape HTTP API**: `/subjects`, `/schemas/ids/{id}`,
  `/compatibility/...`, `/config/{subject}`, with delete on subjects
  and individual versions.
- **Compatibility modes**: structural BACKWARD/FORWARD/FULL
  enforcement on JSON-Schema-shaped payloads with required-field
  inspection. Per-subject compatibility config persists via
  header-marked records on the schemas topic.
- **API-key auth**: `WithAPIKeys` on the HTTP handler.
- **Multi-instance**: schema IDs are broker-assigned (offset-derived)
  so two registry instances sharing a broker can't collide.

### Broker capabilities

- **TLS** on the wire, **API-key** authentication on the handshake,
  **per-topic ACLs** via `WithACL(map[apiKey]ACL{Produce, Consume})`.
- **Quotas** per API key (produce-bandwidth + fetch-bandwidth).
- **Per-key compression** with codec-aware fetch responses.
- **Retention**: time-based AND size-based, plus log compaction
  (Kafka-style keep-latest-per-key).
- **Cluster**: `hashicorp/raft` with disk-backed log + stable store
  + file snapshots. Leader redirect, AddVoter / RemoveVoter wire
  ops, ClusterStatus, ClusterMembers.
- **Topic operations**: create, delete (replicated through Raft on
  clustered brokers), update (retention, segment-size — partition
  count immutable), list.
- **Snapshot/bootstrap**: chunked `OpListSegments` +
  `OpFetchSegmentChunk` lets a fresh follower seed its data dir from
  a peer in bounded-memory chunks, including the active segment.
- **Observability**: Prometheus metrics endpoint, `Broker.Stats()`,
  per-partition `HighWater`, `ListGroupOffsets` with high-water in
  one round-trip.

### Wire protocol

Wire version went from v3 → v9 across the era. New ops added:

- v4: API-key handshake.
- v5: Symmetric Fetch-side compression (was produce-only).
- v6: Long-poll heartbeat (`MaxWaitMs`) for server-pushed rebalance.
- v7: Chunked partition snapshot (replacing one-shot).
- v8: `ListGroupOffsets` with high-water in one round-trip.
- v9: `DeleteGroup`.

Plus: ListSegments, FetchSegmentChunk, DeleteTopic, ListTopics,
ListGroups, DescribeGroup, ClusterStatus, UpdateTopicConfig.

## What changed about the project

The changes above shifted the codebase from "8 stages of pedagogy" to
"a working learning platform with operator surface." The single-binary
broker can be exercised end-to-end from the shell:

```bash
holocronctl topic create --topic events --partitions 4
holocronctl produce --topic events --value hello --header trace=abc
holocronctl topic dump --topic events | jq .value
holocronctl group offsets --group g --json
holocronctl bench --topic events --count 100000 --producer-count 4
holocronctl topic copy --from events --to events-v2 --all-partitions
```

Every read-side subcommand emits JSON; every write-side has an
idempotent path; every operator surface has both a single-target and
a bulk variant where it makes sense.

## Deferred work

Six items remain in the backlog. Each is large enough to be a stage
of its own. They are documented in
[`architecture.md#deferred-work`](architecture.md#deferred-work):

1. Exactly-once across produce + commit-offset (needs broker txns)
2. Avro / Protobuf schema parsing for richer registry compatibility
3. Linux `sendfile(2)` fast path for sealed-segment Fetch
4. Per-partition Raft for write-throughput scaling
5. Continuous cluster replication for records (streaming follower)
6. Multiplexed connections with correlation IDs

These are known shape, not roadmap commitments. The codebase is at a
natural pause point and any of them is a multi-week project on its
own.

## Why freeze here

The polish surface is exhausted in the sense that any remaining easy
or medium-sized ergonomic gaps are increasingly hard to find. The
last several batches leaned heavily on aliases (Tap), pre-canned
sugar (Sum), and `--json` variants — diminishing returns on
ergonomics rather than substantive new behavior.

The next meaningful work is one of the six deferred items. They each
require enough design + implementation that a "sustaining batch"
shape doesn't fit them. Freezing the polish era and treating any
follow-on as a new stage with its own design note (the existing
`stage-N.md` pattern) is the cleaner path.

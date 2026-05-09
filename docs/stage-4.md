# Stage 4 — Consumer groups + rebalancing

Stage 4 turns the broker into a coordinator. Multiple consumers can now
share a topic's partitions cooperatively (consumer groups), commit their
progress to the broker (durable, group-scoped offsets), and rebalance
when peers come and go.

## What ships

| Component | Package | Role |
|---|---|---|
| Group state machine | `broker/internal/groups` (`manager.go`) | Membership, generation tracking, rebalance triggering. |
| Range assignment | `broker/internal/groups` (`assign.go`) | Deterministic, contiguous partition splits across members. |
| Offset store interface | `broker/internal/groups` (`offsets.go`) | `OffsetStore` Strategy. Memory + JSON-file implementations. |
| New wire ops | `proto/wire.go` | `OpJoinGroup`, `OpHeartbeat`, `OpLeaveGroup`. `OpCommit` now writes through. |
| Server handlers | `broker/internal/server` | Decode, dispatch to `Manager`, encode response. |
| Transport extensions | `sdk` (`transport.go`) + `inproc` + `net` | `JoinGroup` / `Heartbeat` / `LeaveGroup` on the public Transport interface. |
| Group-mode consumer | `sdk/consumer.go` | Subscribe with `WithGroup` joins, fetches at committed offsets, heartbeats, rejoins on rebalance. |

## The protocol, in one diagram

```
consumer A           broker            consumer B
   |                   |                   |
   |--- JoinGroup --->|                   |
   |<-- assign [0..3] --|                   |
   |--- Fetch p0 ---->|                   |
   |                   |<-- JoinGroup ---|
   |                   |<-- assign [2,3]-|
   |                   | (gen ↑, A is stale)
   |--- Heartbeat --->|                   |
   |<-- RebalanceNeeded |                   |
   |--- JoinGroup --->|                   |
   |<-- assign [0,1] --|                   |
   |--- Fetch p0,p1 -->|--- Fetch p2,p3 -->|
```

A and B never overlap once both are stable: rebalance happens before
records are fetched at the new generation.

## Design decisions

### Why a separate group manager, not bolted onto the broker

`broker.Broker` is the publish/subscribe core. Group coordination is a
distinct responsibility (membership, liveness, assignment math) and
benefits from being testable in isolation. The broker holds an optional
`*groups.Manager` and exposes it via `Groups()`. Brokers without one
function as Stage 1–3 brokers — useful for tests that don't need the
overhead.

### Why range assignment

Three strategies are common in Kafka:

1. **Range** — each member gets a contiguous slice. Simplest. Works well
   when partition counts are similar across topics.
2. **Round-robin** — interleave partitions across members. Slightly
   better balance when topic partition counts differ.
3. **Sticky** — try to preserve previous assignments across rebalances
   to minimize state-loss in stateful consumers.

Stage 4 ships range only. The `assign.go` interface is small enough that
adding a strategy is a one-file change.

### Why broker-side commits go to a JSON file (not an internal topic)

Real Kafka stores commits in a special compacted topic (`__consumer_offsets`).
That trick — the broker uses its own log to store its own metadata — is
the textbook "self-hosting" pattern, and would be educationally great
to implement.

We deferred it because:

1. **It is genuinely complex.** Boot ordering (load topic before serving
   requests), compaction (eventually keeping only the latest entry per
   key), consumer-of-internal-topic (the broker subscribes to itself).
2. **Stage 5 (replication) makes it more interesting.** Once partition
   logs are replicated, the offsets topic is replicated for free, and
   the lesson lands more clearly.
3. **JSON file is "broker-durable" enough for Stage 4's claim.** Each
   commit `os.Rename`s a temp file, which is atomic on POSIX. The file
   survives restarts; that's all Stage 4 promises.

The `OffsetStore` interface (`offsets.go`) is the seam. A
`TopicBackedOffsetStore` lands in a later stage without changing the
manager or the wire.

### Why "committed = next to read" (not "highest delivered")

Two conventions exist; both work. Kafka chose "committed = the offset
the next read should start at." A consumer that just processed offset N
calls `Commit(N+1)`. We adopted this so callers' Kafka instinct works.

### Why heartbeats trigger rejoin in the heartbeat goroutine

The first cut signaled a rebalance flag and let `Poll` do the rejoin.
That left a window where old partition pumps kept fetching after the
broker had already reassigned them. Two consumers ended up reading the
same partitions until `Poll` next ran.

Moving rejoin into the heartbeat goroutine (the only goroutine that
mutates group state) closes the window. Pumps for revoked partitions are
cancelled before more records can arrive on the new generation. This is
the same shape Kafka's Java client uses: the heartbeat thread owns
group-state transitions; the user's poll loop just consumes.

### Generation numbers

Every rebalance increments the group's generation. Heartbeats carry the
caller's generation; a mismatch means the caller is stale and must
rejoin. This is the same mechanism Kafka uses to fence zombies — old
members can't continue acting on stale assignments after the group has
moved on.

## Acceptance

- `make build && make test` and `go test -race ./...` are both green.
- `embed_test.go::TestConsumerGroup_SharesPartitionsWithoutOverlap` runs
  two consumers in one group and verifies no record key is delivered
  twice.
- `embed_test.go::TestConsumerGroup_CommitSurvivesRestart` verifies a
  fresh consumer with the same group ID resumes from the committed
  offset.
- `holocrond` + two `examples/consumer` instances with `--group=demo` +
  `examples/producer` round-trips over real TCP without overlap.

## Known limitations (intentional)

- **JSON offset store, not the Kafka internal-topic trick.** The lesson
  lands cleanly when replication does (Stage 5).
- **Range assignment only.** Round-robin and sticky are one-file adds.
- **No pre-rebalance offset commit.** Real Kafka clients optionally
  commit before relinquishing a partition during rebalance to avoid
  duplicate processing. Stage 4 leaves this to user code.
- **No member ID stickiness across restarts.** A returning consumer
  gets a new member ID and is treated as a brand-new member. Sticky
  rejoin is a stretch goal.
- **Session timeout is fixed at 15s.** Configurable via
  `groups.WithSessionTimeout`, but not currently plumbed through `embed`.
- **No consumer-side pre-fetch buffer trim on rebalance.** Records that
  the consumer pulled into its `fanIn` channel before the rebalance
  signal still get delivered to `Poll`. They were legitimately fetched
  at the previous generation; documenting this is the Stage-4 trade-off.

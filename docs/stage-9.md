# Stage 9 — Fresh-follower record catch-up

Stage 9 closes a gap in the Stage 5 cluster: a node joining a
long-running cluster cannot reliably catch up on historical records
once the leader's Raft log has been snapshot-truncated. The fix is
a record-aware bootstrap path that fetches segments from a peer
before the new follower joins the live Raft Apply stream.

## What already works

Stage 5 plus sustaining-era polish gets us most of the way:

1. **Steady-state replication.** Every produce on the leader becomes
   a `CmdAppend` Raft log entry. Each follower's `FSM.Apply` calls
   `store.Append(...)` on its local file-backed Store. Records are
   on every voter once the entry commits.
2. **Topic metadata snapshots.** `FSM.Snapshot` serializes the
   registry. A new follower restoring from a snapshot can immediately
   serve metadata reads.
3. **One-shot peer bootstrap.** Batch 22 ships `OpListSegments` +
   `OpFetchSegmentChunk` and `embed.BootstrapPartitionFromPeer` —
   the operator manually copies a partition's sealed segments from a
   donor before opening the recipient broker. Useful for migration;
   not integrated with the Raft join path.

## The gap

`FSM.Snapshot` deliberately omits records (line 117 of `fsm.go`):
records live in the file-backed Store, not in the FSM, and inlining
gigabyte-class segment data into the snapshot stream would defeat
the purpose of having a separate durable Store.

Combined with Raft's standard log-truncation behavior (entries past
the snapshot threshold are GC'd), this means:

- **Day 1**: cluster boots, Raft log holds the first N produces.
- **Day 7**: cluster takes a snapshot, truncates pre-snapshot
  entries. Records produced on day 1 are now ONLY in the file-backed
  Store on each existing voter — not in the Raft log, not in the
  FSM snapshot.
- **Day 14**: a new node joins via `Cluster.AddVoter`. Raft sends
  it the day-7 snapshot (registry only) plus log entries from day 7
  forward. The new node's local Store has no day-1 records, and
  no replay path exists.

Existing clients that fetch from the new node will see the cluster
as if those records never existed. Stage 9 fixes that.

## Design

### Approach

When a follower joins, it runs a **record sync** phase before it
starts serving reads:

1. Snapshot install completes (registry hydrated, log entries
   pending).
2. Follower compares its per-(topic, partition) local high-water
   against the leader's. Each partition with a deficit enters sync.
3. Follower streams the missing segments from the leader using the
   existing chunked OpListSegments / OpFetchSegmentChunk wire ops.
4. Once the local Store catches up to a watermark `W` per partition,
   the follower starts applying Raft entries from offset `W+1`
   forward — entries below `W` are deduped (the local Store already
   has them from the segment sync).
5. Follower marks itself ready to serve reads.

The cutover is safe because the leader's Raft Apply stream and the
segment sync are operating on the same underlying log: any record
produced after the segment sync starts will show up in either the
sync stream (if it landed in a sealed segment before the segment
sync read it) or the Raft log (if it landed after). Dedup at the
follower's Apply path prevents double-application.

### Wire protocol

No new wire ops needed in V1. Reuse:

- `OpListSegments` (0x10) — leader returns segment manifest.
- `OpFetchSegmentChunk` (0x11) — leader returns byte ranges.
- `OpHighWater` (0x0C) — follower reads leader's high-water for
  each (topic, partition).

### FSM hook

`FSM.Apply` for `CmdAppend` gains a guard:

```go
func (f *FSM) applyAppend(body []byte) any {
    cmd := DecodeAppend(body)
    pref := proto.PartitionRef{Topic: cmd.Topic, Index: cmd.Partition}
    // During catch-up, skip records the segment sync already wrote.
    if hw, _ := f.store.HighWater(ctx, pref); cmd.Offset <= hw - 1 {
        return hw - 1 // already applied
    }
    return f.store.Append(ctx, pref, cmd.Record)
}
```

This requires `CmdAppend` to carry the broker-assigned offset — a
new field on the existing command. The leader assigns the offset
before submitting the command (today the leader's FSM.applyAppend
runs Append and returns the new offset). The wire format gains one
int64 per AppendCommand.

### Segment sync orchestration

A new `(*Cluster).syncRecords(ctx)` method, called from the broker's
post-join hook:

1. Enumerate every (topic, partition) the registry knows about.
2. For each, query `leader.HighWater(p)`.
3. If `local.HighWater(p) < leader.HighWater(p)`, run
   `BootstrapPartitionFromPeer` against the leader, but loop until
   `local.HighWater(p) >= some_target`. The loop is needed because
   the leader keeps producing during sync.
4. Once every partition is within `acceptable_lag` of the leader,
   start the Raft Apply pump.

`acceptable_lag` is tuned so the Raft log entries that arrive during
the gap are guaranteed to overlap with what segment sync just
fetched. A conservative value: leader's snapshot threshold (1 GiB
of records) divided by the produce rate. In practice, 5 minutes of
lag gives Raft Apply plenty of overlap.

## Subtleties

### Truncation race during sync

If the leader takes a snapshot mid-sync (so log entries the follower
needs are truncated before the follower receives them), the follower
falls back to re-running segment sync. This requires a retry loop in
the orchestrator, not a protocol change.

### Leader change during sync

If the current leader steps down mid-sync, the follower restarts
sync against the new leader. Segment sync is idempotent — partial
data on the recipient is overwritten by re-fetched chunks.

### Active segment

The leader's active segment grows during sync. The current
`OpListSegments` captures sizes under the partition mutex, so the
recipient reads up to the listed size — records appended after
listing aren't transferred, but they will arrive via Raft Apply
once the recipient catches up.

### Partition added after sync start

If a topic is created (or a partition added) after sync starts, the
follower learns via the FSM `CmdCreateTopic`. The orchestrator runs
sync against newly-known partitions before completing.

## Testing strategy

Three end-to-end scenarios:

1. **Cold start, mid-life cluster.** Build a 2-node cluster,
   produce 10k records, force a Raft snapshot + log truncation,
   add a 3rd node, verify it serves all 10k records via fetch.
2. **Continuous load during join.** Like (1), but produce records
   continuously throughout the join. Verify the new node reaches
   parity and serves the full record set.
3. **Truncation during sync.** Like (1), but force another snapshot
   mid-sync to trigger the truncation-race retry path.

Each scenario gets a test in `broker/internal/cluster/sync_test.go`
or `broker/embed/embed_test.go` (whichever module's surface the
test exercises).

## Out of scope

- **Multi-leader / leaderless replication.** Stage 9 stays
  Raft-based with one leader.
- **Cross-cluster / geo replication.** Stage 9 is one cluster.
- **Tiered storage** (offload sealed segments to S3 / GCS). The
  segment sync protocol could carry a backend hint in a later
  stage; not Stage 9.
- **Bandwidth throttling for sync.** Sync runs at wire speed in
  V1; an operator who needs throttling configures the wire-level
  rate-limit (existing quota machinery).

## Acceptance

- 3-node cluster passes the three test scenarios above with
  `-race`.
- A new follower joining a 1-million-record cluster catches up
  in bounded time (target: < 30s for 1 GiB of records on local
  storage).
- No regression in the existing Stage 5 cluster tests.
- Wire format change (CmdAppend gains an offset field) is
  backwards-incompatible — bumps the cluster's command-version
  byte but does not affect the SDK wire protocol (different
  surface).

## Estimate

Three to four sustaining-batch-equivalents of work, but
non-decomposable into batches because the cutover semantics need
to be designed end-to-end:

- Wire / FSM changes (CmdAppend offset field, FSM.applyAppend
  dedup guard): ~1 day.
- syncRecords orchestrator: ~2 days.
- Tests (3 scenarios with race + truncation forcing): ~2 days.
- Documentation: ~0.5 day.

Roughly two weeks of focused work — substantially more than a
single sustaining batch, which is why Stage 9 gets its own design
note before any code lands.

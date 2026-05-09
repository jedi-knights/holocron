# Stage 5 — Replication + leader election

Stage 5 is the hardest one. The broker becomes a distributed system: a
cluster of N nodes that replicate every produce and topic-create through
Raft consensus, elect a leader, and survive single-node failures.

Cluster mode is **opt-in behind `--cluster`**. Without it, holocron is
the same single-node Stage 4 broker — every existing test and demo still
works untouched.

## What ships

| Component | Package | Role |
|---|---|---|
| Command codec | `broker/internal/cluster` (`command.go`) | Wire format for replicated operations: `CmdAppend`, `CmdCreateTopic`. |
| Raft FSM | `broker/internal/cluster` (`fsm.go`) | Applies committed Raft entries to local `storage.Store` + `topic.Registry`. |
| Cluster | `broker/internal/cluster` (`cluster.go`) | Wraps `hashicorp/raft` with a Bolt-backed log + stable store and a TCP transport. |
| Broker integration | `broker/internal/broker` (`broker.go`) | `WithCluster` option; `Publish`/`CreateTopic` route through `Cluster.Apply` on the leader and return `ErrNotLeader` on followers. |
| Wire status | `proto/wire.go` | `StatusNotLeader` (0x40); error message body holds the leader's wire address. |
| Server | `broker/internal/server` | Translates `ErrNotLeader` into `StatusNotLeader` on the response. |
| SDK redirect | `sdk/net/transport.go` | On `StatusNotLeader`, swaps the dial target to the leader's wire address and retries. |
| Embedded API | `broker/embed/embed.go` | `embed.WithCluster(ClusterConfig{...})` plumbs Raft into `NewDisk`. `IsLeader` / `LeaderAddr` / `WaitForLeader` accessors. |
| Daemon flags | `broker/cmd/holocrond/main.go` | `--cluster --node-id=NID --raft-listen=:9192 --peers=ID=RAFT=WIRE,... --bootstrap`. |

## The model in one diagram

```
                     Producer (sdk/net)
                          │
                          │ Publish
                          ▼
              ┌─────────────────────────┐
              │  any node's wire :9092  │
              └────────────┬────────────┘
                           │
                  ┌────────┴───── follower? ─────┐
                  │                              │
                  │ I am leader                  │ I am follower
                  ▼                              ▼
        cluster.Apply(EncodeAppend) ───┐   StatusNotLeader(leaderWireAddr)
                                       │   ─→ SDK redials → leader
                  ┌────────────────────┼────────────────────┐
                  ▼                    ▼                    ▼
           ┌────────────┐       ┌────────────┐       ┌────────────┐
           │ Node A FSM │       │ Node B FSM │       │ Node C FSM │
           │  Apply →   │       │  Apply →   │       │  Apply →   │
           │ FileStore  │       │ FileStore  │       │ FileStore  │
           └────────────┘       └────────────┘       └────────────┘
              ▲                                           ▲
              └───── Raft replication (TCP, msgpack) ─────┘
```

## Design decisions

### Why one global Raft cluster (not per-partition)

Real Kafka's KRaft mode uses one Raft for cluster metadata + per-partition
replication. Pulsar uses per-partition replication via BookKeeper. Both
are more complex than holocron's choice: **one Raft cluster per broker**
that replicates everything.

Trade-off: leadership is a single point of write throughput. Real
production systems spread leadership across nodes per partition for
horizontal scale. Holocron picks the simpler model so the lesson is
"this is how Raft replicates a log" rather than "this is how to multiplex
many Rafts." Per-partition Raft is a clear future stage.

### Why cluster mode is opt-in

The standard "single node, no replication" path is the right default for
local development, the in-process tests, and small deployments. Cluster
mode adds a Raft listener on a separate port, doubles the disk write
amplification (Raft log + partition log), and requires multi-node
configuration. Forcing every Stage 5+ user through that would be wrong.

`embed.WithCluster(ClusterConfig{...})` flips it on; absence of the
option preserves Stage 4 behavior bit-for-bit.

### Why the Raft log and the partition log both store records

`hashicorp/raft` is a generic consensus library: it expects user
operations to live in its own log so they can be replayed by the FSM on
each node. Each `produce` therefore lands in two places — the Raft log
(replicated, append-only, used for replay/recovery) and the partition
log (the FSM's persistent output, where consumers Fetch from).

This duplicates writes, which Kafka's KRaft and Pulsar's BookKeeper avoid
by making the partition log itself be the consensus log. That's a deeper
restructure. Stage 5 accepts the duplication and documents it as the
price of using `hashicorp/raft` as a library.

### Why the SDK redirects rather than the broker proxying

Two patterns exist for "wrong node received the request":

1. **Broker proxies.** A follower forwards the request to the leader,
   gets the answer, returns it. The client never knows.
2. **Broker redirects.** A follower replies "not leader, try $LEADER";
   the client redials.

Kafka and most modern systems use redirect. Reasons:

- The leader sees the producer's own connection identity (useful for
  quotas, throttling, audit).
- Followers don't have to maintain in-flight state for proxied requests.
- Failures are simpler: a redirect that hits a dead leader is just one
  more redirect cycle.

Holocron's SDK retries up to 4 times following redirects. Beyond that,
the call returns the last error.

### Why the leader address in StatusNotLeader is the wire address

The Raft library knows leader IDs and Raft addresses. The SDK needs the
**broker's wire-protocol address** (the port producers and consumers
talk to). The two are deliberately on different ports — Raft is
inter-node only.

`Peer{ID, Addr, WireAddr}` carries both. The cluster's `LeaderWireAddr()`
maps a leader ID to its wire address via the static peer config. The
broker's `ErrNotLeader.LeaderAddr` field holds the wire address; the
server writes it as the StatusNotLeader response message.

### Why offset commits don't go through Raft (yet)

Stage 4 stores consumer-group offsets in a JSON file on each node. In
cluster mode each node has its own JSON file, which means offsets are
**not replicated**. A consumer that commits to one node's broker and
then fails over to another will see different committed offsets.

Fixing this means moving the offset store to either:
- a special compacted internal topic (`__holocron_offsets`) that flows
  through the same Raft, or
- a Raft-replicated KV inside the FSM.

Both are real work. Stage 5 leaves it as a known limitation; the
follow-on is tracked in TODO.md.

### Why no snapshots

The FSM does not capture state in a Raft snapshot — `Snapshot()` returns
an empty stream and `Restore()` discards. This is a deliberate Stage 5
simplification with two consequences:

1. **The Raft log grows unbounded.** Set a very high `SnapshotThreshold`
   so Raft doesn't try to snapshot prematurely.
2. **A new follower joining can't catch up via snapshot transfer.** It
   has to replay every Raft log entry. Fine for the educational scale;
   not viable at production data volumes.

Real snapshotting would need to capture the entire `FileStore` state
(every partition log, every segment, every index) into the snapshot
sink. That's a significant exercise in its own right.

## Acceptance

- `make build && make test` and `go test -race ./...` are both green.
- `cluster/cluster_test.go::TestCluster_ThreeNodeReplicatesAppends`:
  3 in-process nodes elect a leader and replicate appends so every
  node's local store has the same records.
- `embed/embed_test.go::TestCluster_ThreeNodeReplicatesViaNetworkSDK`:
  end-to-end SDK → follower → redirect → leader → Raft → all nodes.
- `holocrond --cluster --node-id=n1 --bootstrap --peers=n1=...,n2=...,n3=...`
  starts a cluster member that the SDK can connect to.

## Known limitations (intentional)

- **No snapshotting.** Raft log grows unbounded; new joiners replay from
  scratch.
- **Offsets not replicated.** Per-node JSON file. Consumer failover
  across cluster nodes will see inconsistent committed offsets.
- **One global Raft, not per-partition.** Leader is the write
  bottleneck; throughput does not scale with cluster size.
- **Static cluster membership.** Adding/removing nodes at runtime
  (`raft.AddVoter` / `RemoveServer`) is supported by the underlying
  library but not exposed in `holocrond` CLI; bring up the cluster with
  the final membership in `--peers`.
- **No TLS on either the wire or Raft port.** Plaintext.
- **Cluster mode disables in-process subscribe fanout.** `inproc`
  subscribers don't see records that arrive via Raft Apply because the
  fanout path runs only in the non-cluster `Publish`. This isn't a real
  problem in practice — cluster mode exists for the network use case —
  but `examples/inproc` should not be combined with `WithCluster`.
- **No produce-side acks tuning.** Every produce in cluster mode is
  effectively `acks=replicated` (Raft commit waits for majority quorum).
  `acks=local` and `acks=durable` only matter in single-node mode.

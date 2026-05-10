# Operator guide

This guide is for whoever runs the Holocron broker — either as a daemon on a server or as part of a Kubernetes deployment. For application developers integrating against a running broker, see [`integration-go.md`](integration-go.md).

## Build

```bash
git clone https://github.com/jedi-knights/holocron.git
cd holocron
make build
```

This produces:
- `bin/holocrond` — the broker daemon
- `bin/holocronctl` — the operator CLI

Requirements: Go 1.23 or newer.

## Run a single broker

```bash
# In-memory (data lost on shutdown — fine for testing)
./bin/holocrond --memory --listen 127.0.0.1:9092

# Disk-backed (the production shape)
./bin/holocrond --data-dir /var/lib/holocron --listen 0.0.0.0:9092
```

The startup banner reports the deployment shape, the bind address, and whether TLS is on:

```
holocrond — stage 5 (cluster)
backend: disk at /var/lib/holocron
listening on 0.0.0.0:9092 (wire v1, plain)
press Ctrl-C to exit
```

## Configuration

Every flag has a `HOLOCRON_*` environment-variable fallback. Flags win when both are set.

### Core

| Flag | Env | Default | Purpose |
|---|---|---|---|
| `--data-dir` | `HOLOCRON_DATA_DIR` | `/var/lib/holocron` | Directory for the segmented log + Raft state. |
| `--listen` | `HOLOCRON_LISTEN` | `:9092` | TCP address the wire listener binds to. Empty disables the listener. |
| `--memory` | — | `false` | In-memory store (testing only — data is lost on restart). |

### Retention

| Flag | Default | Purpose |
|---|---|---|
| `--retention` | `0` (off) | Delete sealed segments older than this duration. |
| `--retention-bytes` | `0` (off) | Delete oldest sealed segments while a partition exceeds this byte size. |

Both can be set; both run on the same sweeper interval.

### Cluster mode

Multi-node deployments use Raft for leader election and replication.

| Flag | Env | Default | Purpose |
|---|---|---|---|
| `--cluster` | — | `false` | Enable Raft-replicated cluster mode. |
| `--node-id` | `HOLOCRON_NODE_ID` | — | Unique identifier for this node. Required in cluster mode. |
| `--raft-listen` | `HOLOCRON_RAFT_LISTEN` | `:9192` | Raft RPC bind address. |
| `--peers` | `HOLOCRON_PEERS` | — | Cluster membership: `id=raft-addr=wire-addr,...` |
| `--bootstrap` | — | `false` | Bootstrap as the first node. Set on exactly one node when standing up a new cluster. |

Example three-node startup, run on each host:

```bash
# Node 1 (the bootstrap node)
holocrond --data-dir /var/lib/holocron \
  --listen 0.0.0.0:9092 \
  --cluster --node-id n1 --raft-listen 0.0.0.0:9192 \
  --peers 'n1=10.0.0.1:9192=10.0.0.1:9092,n2=10.0.0.2:9192=10.0.0.2:9092,n3=10.0.0.3:9192=10.0.0.3:9092' \
  --bootstrap

# Nodes 2 and 3 — same --peers, omit --bootstrap, change --node-id
```

SDK clients can dial any node; followers redirect with a `StatusNotLeader` reply that the SDK transparently follows.

### TLS

Both the wire-listener and the Raft transport can be wrapped in TLS. See [`tls.md`](tls.md) for the full surface — there are enough flags and design choices that they live in their own document.

## `holocronctl` — the operator CLI

`holocronctl` is the inspect-and-modify counterpart to the daemon. Every subcommand accepts `--addr` (broker address) and the standard TLS flags.

```bash
# Topic management
holocronctl topic list --addr broker:9092
holocronctl topic create --topic events --partitions 4 --addr broker:9092
holocronctl topic describe --topic events --addr broker:9092
holocronctl topic stats --topic events --addr broker:9092 --json

# Records (read-only)
holocronctl tail --topic events --partition 0 --addr broker:9092
holocronctl consume --topic events --group ops --addr broker:9092
holocronctl record fetch --topic events --partition 0 --offset 42 --addr broker:9092

# Consumer groups
holocronctl group list --addr broker:9092
holocronctl group describe --group billing-svc --addr broker:9092
holocronctl offset commit --group billing-svc --topic events --partition 0 --offset 100 --addr broker:9092

# Cluster
holocronctl cluster status --addr broker:9092
holocronctl cluster members --addr broker:9092
holocronctl cluster join --peer-id n4 --peer-addr 10.0.0.4:9192 --addr broker:9092

# Liveness probe (cheap)
holocronctl ping --addr broker:9092 --json
```

Run `holocronctl <command> -h` for command-specific options. Every read-side command supports `--json` for scripting.

## Storage layout

Disk-backed brokers organise data under `--data-dir` like this:

```
/var/lib/holocron/
├── topics.json                 # topic registry (atomic-write rotated)
├── orders.placed/              # one directory per topic
│   ├── 0/                      # one subdirectory per partition
│   │   ├── 00000000000000000000.log     # segment data
│   │   └── 00000000000000000000.idx     # sparse offset index
│   └── 1/
│       └── ...
└── raft/                       # only present in --cluster mode
    ├── raft-log.db
    ├── raft-stable.db
    └── snapshots/
```

For the full data-model reference (segment rollover, sparse-index format, retention semantics, compaction), see [`data-model.md`](data-model.md).

## Container deployment

The repository ships a `docker-compose.yml` with three profiles:

```bash
docker compose --profile single up      # single broker on :9092
docker compose --profile registry up    # broker + schema registry on :8081
docker compose --profile cluster up     # 3-node Raft cluster on :9091/:9092/:9093
```

These are reference deployments — production users are expected to write their own manifests against the published image.

## Observability

The daemon exposes Prometheus-style metrics through the `metrics` package. The SDK's `Producer.Stats()` and `Consumer.Stats()` return point-in-time snapshots of throughput, lag, and per-partition state — see the SDK reference in [`integration-go.md`](integration-go.md).

A built-in Grafana dashboard is not provided yet; that's tracked under Wave 5 (hardening) on the [roadmap](roadmap.md).

## Limitations and roadmap

Holocron is **pre-alpha**. The on-disk format, wire protocol, and public APIs change without notice until the first tagged release. Production-readiness gaps still open:

- Authentication beyond mTLS (JWT / accounts) — Wave 1
- KV and Object store layers — Wave 2
- Non-Go SDKs — Wave 3
- Cross-cluster mirror / leaf nodes / geo-replication — Wave 4

See [`roadmap.md`](roadmap.md) for the full sequence.

## Where to go next

- [TLS configuration](tls.md) — full TLS surface (server + mTLS + cluster)
- [Architecture](architecture.md) — module boundaries and design rules
- [Data model](data-model.md) — records, segments, indexes, retention
- [Roadmap](roadmap.md) — what's coming

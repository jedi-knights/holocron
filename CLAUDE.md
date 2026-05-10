# CLAUDE.md

Project-specific rules for working in this repo. Global rules from `~/.claude` apply on top.

## What this project is

Holocron is a **single-binary, Go-native distributed log broker** positioned as an alternative to **NATS JetStream** — the same operational simplicity (one binary, no JVM, no external coordinator) plus the per-partition ordering and Kafka-shaped log model that JetStream lacks. The product thesis:

> *NATS JetStream, but with real per-partition ordering you can rely on — still in one Go binary.*

Decisions about scope, abstractions, and trade-offs are judged against that thesis: does this make Holocron a better choice than NATS JetStream for someone choosing a Go-native broker today? Stages 1–8 plus the sustaining era shipped the foundation. From here the roadmap is **capability-driven**, prioritized by competitive gap with NATS JetStream and by what production users actually need (TLS, auth, multi-tenancy, KV/Object store layers, more SDKs). The capability matrix in `README.md` is the contract.

## Architectural rules that override defaults

1. **The broker stays dumb.** No transformation, filtering, content-based routing, or user-supplied code in the broker process. Those live in the SDK or in the Stage-6 Connect tier. If a change proposes "let's just add a small hook in the broker," the answer is no.
2. **`broker/internal/...` is private.** No imports from `sdk`, `examples`, `cli`, or any module other than `broker`. Outside callers go through `broker/embed` (public façade) or `broker/inproc` (Transport adapter).
3. **The SDK does not import the broker.** `sdk` imports only `proto`. Producers and Consumers talk to the `sdk.Transport` interface; concrete transports live in `broker/inproc` (Stage 1+) and `broker/internal/server` (Stage 3+).
4. **Capability-driven growth.** The capability matrix in `README.md` is the contract; new abstractions land with the capability that needs them. Do not introduce types, abstractions, or scaffolding for hypothetical future capabilities. A capability ships when it crosses production-quality thresholds (correct under partition / `-race` clean / documented / tested), not when scaffolding is in place.
5. **Per-partition ordering only.** No global ordering across partitions. Code that assumes cross-partition order is incorrect.
6. **Backwards compatibility is not a goal yet.** Pre-alpha — the on-disk format, wire protocol, and public APIs change between stages without compatibility shims. Do not add deprecation aliases or version flags before the first tagged release.

## Module layout

```
proto/         # shared data types — depended on by everything
sdk/           # public Go client; imports only proto
broker/        # daemon module
  cmd/holocrond/
  embed/       # PUBLIC: in-process broker handle (use this from tests/demos)
  inproc/      # PUBLIC: sdk.Transport over a Broker
  internal/    # PRIVATE — storage, log, topic, broker, server
connect/       # source/sink connector framework + reference connectors
registry/      # standalone schema registry (broker-backed)
streams/       # stream processing library (DSL + runtime)
cli/           # holocronctl
examples/      # demos; imports sdk exactly as a downstream user would
```

## Workspace quirks

- `go.work` stitches the modules. `./...` from the workspace root does **not** expand — use the explicit list (`./broker/... ./sdk/... ./proto/... ./connect/... ./registry/... ./streams/... ./cli/... ./examples/...`) or run `make` targets.
- New constructors use **functional options**. Pattern: `sdk.NewProducer(transport, sdk.WithPartitioner(...))`, `embed.NewDisk(dir, embed.WithRetention(...))`.
- The publish hot path is **per-partition single-writer**. Do not introduce broker-wide locks across partitions.
- **`Store` is a Strategy.** New durability backends are new implementations of `storage.Store`, not new methods on existing ones.

## Testing

- `make test` runs everything; always re-run with race before merging: `go test -race ./broker/... ./sdk/...`.
- **Storage and broker** packages must have integration tests; SDK and helpers get unit tests.
- `broker/embed/embed_test.go` is the public-surface acceptance test — new end-to-end behavior should land a test there too.
- Disk tests use `t.TempDir()`; never write to fixed paths.
- A failing test under `-race` blocks merge even if it passes without race. Don't paper over with retries.

## Running

```bash
make build                                    # bin/holocrond + bin/holocronctl
go run ./examples/inproc                      # Stage 1 demo (in-memory, single process)
./bin/holocrond --data-dir /tmp/holocron-foo  # Stage 2 daemon (disk-backed)
```

## Where things live

| Topic | File |
|---|---|
| Project framing, install, usage | `README.md` |
| Durable architectural rules + taxonomy coverage | `docs/architecture.md` |
| Data model (record, topic, partition, offset, segment, index, retention) | `docs/data-model.md` |
| Per-stage design notes (the *why* of each stage) | `docs/stage-N.md` |
| Active backlog | `TODO.md` (gitignored) |

## Pre-merge checklist

1. Does this change advance a capability on the roadmap in `README.md`, or close a documented gap vs. NATS JetStream?
2. Does it respect the broker-stays-dumb rule?
3. Are no `broker/internal/...` imports introduced from outside the broker module?
4. Does the SDK surface still make sense over a network `Transport` (the default deployment shape since Stage 3)?
5. Do tests pass with `-race`?
6. Behavior change → `docs/` updated in the same PR (per project doc-discipline rule)?

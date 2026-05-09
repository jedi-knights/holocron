# CLAUDE.md

Project-specific rules for working in this repo. Global rules from `~/.claude` apply on top.

## What this project is

Holocron is a **learning-first, Kafka-style event streaming platform** — a single-binary broker plus an SDK in a Go workspace with five modules. Stages are the contract: each one ships an end-to-end working system and lands with its own `docs/stage-N.md` design note.

The "learning-first" framing matters: prefer clarity over cleverness, prefer small working stages over large unfinished ones, and make the *why* of each architectural choice visible in code or docs.

## Architectural rules that override defaults

1. **The broker stays dumb.** No transformation, filtering, content-based routing, or user-supplied code in the broker process. Those live in the SDK or in the Stage-6 Connect tier. If a change proposes "let's just add a small hook in the broker," the answer is no.
2. **`broker/internal/...` is private.** No imports from `sdk`, `examples`, `cli`, or any module other than `broker`. Outside callers go through `broker/embed` (public façade) or `broker/inproc` (Transport adapter).
3. **The SDK does not import the broker.** `sdk` imports only `proto`. Producers and Consumers talk to the `sdk.Transport` interface; concrete transports live in `broker/inproc` (Stage 1+) and `broker/internal/server` (Stage 3+).
4. **Stage-by-stage growth.** Do not introduce types, abstractions, or scaffolding for a future stage in a current-stage PR. The roadmap in `README.md` is the contract; new abstractions land with the stage that needs them.
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
cli/           # holocronctl
examples/      # demos; imports sdk exactly as a downstream user would
```

## Workspace quirks

- `go.work` stitches the modules. `./...` from the workspace root does **not** expand — use the explicit list (`./broker/... ./sdk/... ./proto/... ./cli/... ./examples/...`) or run `make` targets.
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

1. Does this change belong to the current stage in `README.md`?
2. Does it respect the broker-stays-dumb rule?
3. Are no `broker/internal/...` imports introduced from outside the broker module?
4. If the SDK surface changes, does it still make sense when `Transport` becomes a network call (Stage 3+)?
5. Do tests pass with `-race`?
6. Behavior change → `docs/` updated in the same PR (per project doc-discipline rule)?

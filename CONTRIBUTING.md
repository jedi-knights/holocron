# Contributing to Holocron

Thanks for your interest. Holocron is a learning-first project; the bar for clarity and small, well-justified changes is high.

## Ground rules

- **One PR, one concern.** If your branch touches storage *and* the SDK *and* a CLI flag, split it. Use Conventional Commits — `feat(storage):`, `fix(sdk):`, `docs(broker):`, etc.
- **Stage discipline.** Each roadmap stage produces a working system. Do not introduce scaffolding for a future stage in a current-stage PR.
- **Tests with the change.** Storage and broker get integration tests; everything else gets unit tests. A bug fix without a regression test will be asked to add one.
- **Update `docs/` when behavior changes.** Stale documentation is worse than missing documentation.

## Repository layout

Holocron is a Go workspace. Each module has a single responsibility:

| Module | Responsibility |
|---|---|
| `proto/` | Shared data and wire types — depended on by everything else. |
| `broker/` | The `holocrond` daemon. `internal/` packages are not importable from outside. |
| `sdk/` | The public Go client. Imports only `proto`. |
| `cli/` | `holocronctl` — operator tooling. |
| `examples/` | Reference producer and consumer. Imports `sdk` exactly as a downstream user would. |

The split exists so that `sdk` cannot accidentally pull in broker internals. If you find yourself wanting to add a `broker/internal/...` import to the SDK, that's a signal something belongs in `proto/` instead.

## Local development

```bash
make build     # compile broker, sdk, cli into ./bin
make test      # run unit tests across all modules
make lint      # gofmt + go vet + staticcheck
make run       # start the broker locally
```

The Go workspace (`go.work`) means you can `cd` into any module and `go build ./...` will resolve sibling modules from local paths automatically — no `replace` directives needed.

## Style

- Idiomatic Go. Small interfaces defined at the consumer side; functional options for configuration; `context.Context` first parameter; channels for fan-out.
- Default to no comments. A comment earns its place by explaining *why*, never *what*.
- Errors get wrapped with `fmt.Errorf("...: %w", err)`. Never `errors.New("...")` for an error returned from an internal call.

## Reporting bugs

Open an issue with: the smallest reproducer you can produce, the broker version (`holocrond --version`), and the OS/architecture. Attach the broker data directory only if it does not contain real data — Holocron does not encrypt records at rest.

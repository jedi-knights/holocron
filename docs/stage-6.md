# Stage 6 — Connect framework

Stage 6 is the platform layer the project came for. The broker is the
substrate; Connect is what turns it into infrastructure people use to
move data between systems. Source connectors pull events from databases,
files, and APIs; sink connectors push events into warehouses, indexes,
and message queues. The lesson is the same one Kafka Connect teaches:
**most ETL/ELT work is configuration, not code**, once the pipeline
primitives exist.

## What ships

| Component | Package | Role |
|---|---|---|
| Connector interfaces | `connect/connector.go` | `SourceConnector`/`SourceTask` and `SinkConnector`/`SinkTask`. |
| Worker runtime | `connect/worker.go` | Hosts tasks, owns lifecycle, drives the source poll-produce-commit and sink consume-put-flush loops. |
| File source | `connect/file/source.go` | Tails a line-oriented file, publishes each line as a record. |
| File sink | `connect/file/sink.go` | Consumes a topic, appends each record's value as a line. |
| End-to-end test | `connect/worker_test.go` | File source → embedded broker → file sink, verified line-for-line. |
| Demo | `examples/connect/main.go` | Single-process pipeline driving the same path. |

## The model in one diagram

```
                   external systems                       holocron broker
                        (DB, file,                         (Stage 1–5)
                       API, queue)
                            │                                  │
                            ▼                                  ▼
   ┌──────────────────────────────────────────┐   ┌───────────────────────┐
   │              connect.Worker              │   │                       │
   │                                          │   │   topics, partitions, │
   │  ┌────────────────┐  ┌────────────────┐  │   │   consumer groups,    │
   │  │ source goroutine │ │ sink goroutine │  │   │   committed offsets    │
   │  │                │  │                │  │   │                       │
   │  │  Poll → Send → │──┼──→  Subscribe →│──┼──→│                       │
   │  │       Commit   │  │  Put → Flush → │  │   │                       │
   │  └────────────────┘  └────────────────┘  │   │                       │
   └──────────────────────────────────────────┘   └───────────────────────┘
              ▲                       ▲                       ▲
              │                       │                       │
              │           uses sdk.Producer / sdk.Consumer    │
              │                                               │
              │      uses sdk.Transport (inproc OR network)   │
              └───────────────────────────────────────────────┘
```

## Design decisions

### Why Connect is a separate module, not a broker package

`connect/` imports only `proto` + `sdk`. It does not reach into broker
internals. That preserves two properties:

1. **Connect runs anywhere the SDK runs.** A Worker pointed at
   `sdk.net.Dial("broker:9092")` is identical to one pointed at an
   embedded `inproc` transport — same code, different deployment.
2. **The Connect tier scales independently.** Connector hosts are CPU-
   heavy and bursty; brokers are I/O-heavy and steady. Conflating them
   would couple capacity planning across two very different shapes.

Both are restatements of `docs/architecture.md`'s "broker stays dumb"
rule. Connect is what runs *on top of* the broker.

### Why source produces directly via sdk.Producer

A source task could write directly to the broker's local store, but
that's only possible when the worker shares a process with the broker.
Going through the SDK Producer makes the pipeline **transport-agnostic**
and forces source connectors through the same backpressure path as any
other producer.

### Why sink uses a consumer group

Stage 4's consumer groups give Connect everything it needs for
horizontal scaling for free:

- Multiple sink tasks of the same connector share one group.
- The broker spreads the topic's partitions across them.
- Tasks join/leave; partitions rebalance.
- Committed offsets persist on the broker, so a restarting task resumes
  exactly where it stopped.

The sink task's contract is "Put then Flush." The Worker calls `Flush`
on a timer; once Flush returns nil, the Worker can commit the broker
offset (Stage 6 V1 leaves explicit commit to the consumer's auto-flow —
see Limitations below).

### Why the file source returns a single task

File sources can't be partitioned — splitting a file across N readers
would interleave bytes. Returning exactly one task is the honest model.
Connectors that *can* parallelize (database sharding, S3 prefix
partitioning) return up to `maxTasks`.

### Why source offsets live in the source record, not in a separate API

Each `SourceRecord` carries a `SourceOffset map[string]any` — file byte
position, database LSN, API cursor — opaque to the framework. The
Worker, in a real implementation, persists the highest offset per task
once the record is durably published. Stage 6 V1 ships the carrier
without the persistence: source connectors that need durable resume
must persist offsets themselves (the file source does this implicitly
by reading from the OS file position; nothing extra to store).

The Kafka trick — store offsets in a special compacted topic
(`connect-offsets`) — is the natural evolution and is tracked in
TODO.md alongside the same trick for consumer-group offsets.

## Acceptance

- `make build && go test -race ./...` is green.
- `connect/worker_test.go::TestEndToEnd_FileSourceToFileSink` writes 5
  lines to a source file, runs a Worker hosting the file source and
  file sink connectors, and verifies the sink file contains exactly the
  same lines in the same order.
- `go run ./examples/connect` prints the same 5 lines from the sink
  file.

## Known limitations (intentional)

- **Source offsets are not durable across restarts in V1.** A new
  worker process won't pick up where the last one left off unless the
  source connector itself persists offsets externally. The framework's
  `SourceOffset` plumbing is in place; storage isn't.
- **Sink offset commit is implicit.** The SDK consumer commits via
  `consumer.Commit`; the Worker's `Flush` returns nil but doesn't yet
  trigger a commit. Auto-commit (every Flush, or every N records) is a
  small follow-on.
- **No connector reconfiguration at runtime.** Add connectors before
  `Start`; reconfiguring requires `Stop` + new Worker.
- **No distributed worker coordination.** Multiple workers running the
  same connector configuration coordinate through the broker's consumer
  groups (sink side) but not on the source side. Two workers running
  the same single-task source will both produce — duplicate records.
  Real Kafka Connect's distributed mode adds a coordination protocol
  for this; Stage 6 V1 expects the operator to run sources singletonly.
- **No router connector yet.** The EventBridge-style content-based
  routing would land cleanly here as a `connect/router` package; tracked
  in TODO.md.
- **No retry policy / dead-letter queue.** A sink Put failure currently
  fails the task. Production connectors need configurable retry and
  DLQ behavior; Stage 6 V1 keeps the lesson on the lifecycle.

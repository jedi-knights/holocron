# Stage 3 — Network protocol

Stage 3 turns the broker into a real network service. Producers and
consumers in separate processes connect over TCP using a hand-rolled
length-prefixed binary protocol. The SDK surface does not change —
the `sdk.Transport` interface gains a network implementation that
slots in alongside `inproc`.

## What ships

| Component | Package | Role |
|---|---|---|
| Wire types & framing | `proto/wire.go` | OpCodes, status codes, request/response encoders, frame I/O. |
| TCP server | `broker/internal/server` | Accept loop, per-connection request dispatch. |
| Network transport | `sdk/net` | `Dial(addr)` returns an `sdk.Transport` that speaks the wire protocol. |
| Listener wiring | `broker/embed` | `Broker.Listen(addr)` starts a server alongside an embedded broker. |
| Daemon | `broker/cmd/holocrond` | `--listen :9092` (default). Starts disk store + listener. |
| Standalone clients | `examples/producer`, `examples/consumer` | Real network demos. |

## Wire protocol v1

Every frame on the wire is:

```
+---------------------+--------+----------+
| 4-byte length BE u32 | opcode | payload  |
+---------------------+--------+----------+
```

- `length` is the number of bytes that follow (including opcode).
- `opcode` is one byte; the same opcode appears in the matching response.
- `payload` is opcode-specific.

Responses additionally begin with a 1-byte **status**:

```
payload = [status u8] [body | error-message]
```

`status == 0` means OK and the body is the success payload. Non-OK
responses carry a length-prefixed UTF-8 error message instead.

### OpCodes

| Op | Code | Purpose |
|---|---|---|
| `OpHandshake` | `0x06` | First message on every connection. Carries the client's wire version. |
| `OpProduce` | `0x01` | Append one record to a partition. Server returns the assigned offset. |
| `OpFetch` | `0x02` | Long-poll read. Client supplies `fromOffset`, `maxRecords`, `maxWaitMs`. |
| `OpMetadata` | `0x03` | Returns the partition count for a topic. |
| `OpCreateTopic` | `0x04` | Registers a topic with the broker. |
| `OpCommit` | `0x05` | No-op through Stage 3; lights up at Stage 4 with consumer groups. |

### Status codes

| Status | Code | Meaning |
|---|---|---|
| `StatusOK` | `0x00` | Success. |
| `StatusUnknownTopic` | `0x10` | Referenced topic is not registered. |
| `StatusInvalidPartition` | `0x11` | Partition index is out of range. |
| `StatusInvalidRequest` | `0x12` | Malformed body or wrong opcode for state. |
| `StatusVersionMismatch` | `0x20` | Handshake wire version disagreement. |
| `StatusInternal` | `0xFF` | Broker-internal error; check the message. |

## Why long-poll Fetch instead of server push

The broker could keep a connection open and stream records as they
arrive. We chose long-poll Fetch (the Kafka model) because:

1. **Backpressure stays with the consumer.** A slow consumer simply
   doesn't call Fetch for a while. The broker never has to estimate
   "is this consumer keeping up?"
2. **Stateless connections.** A Fetch is a single RPC; the broker
   stores no per-connection subscription state. Recovery from broker
   restart is trivial — the client just reconnects and Fetch resumes.
3. **Simpler protocol.** No need for per-stream sequencing, flow
   control, or out-of-band cancellation messages.

The `MaxWaitMs` field on `FetchRequest` lets the server hold the
connection until records arrive, polling internally every 25 ms. This
gives push-like latency without push-like protocol complexity.

## Why one connection per RPC kind

Each `Transport` instance holds one shared connection for unary RPCs
(produce, metadata, commit, create-topic), guarded by a mutex. Each
`Subscribe` call dials a **separate** connection because long-poll
Fetch blocks the connection for up to `MaxWaitMs`; sharing would
serialize all consumes through one slow request.

This keeps the protocol synchronous and the implementation easy to
reason about. Multiplexing would let one connection serve many
in-flight RPCs but requires per-frame correlation IDs and ordered
demux — Stage 4+ if benchmarks justify it.

## Why hand-rolled framing instead of gRPC / protobuf

The platform's stated philosophy is "single binary, zero external
dependencies at runtime." gRPC works but pulls in protobuf codegen as
a build dep and HTTP/2 framing at runtime. For a learning project the
hand-rolled format **is** the lesson — every byte on the wire is
explicit and editable. Stage 5 (replication) may revisit this if Raft
integration suggests gRPC anyway.

## Acceptance

- `make build && make test` is green; `go test -race ./...` is green.
- `embed_test.go::TestListen_NetworkRoundTrip` produces and consumes
  records through a real TCP listener.
- `./bin/holocrond --memory --listen 127.0.0.1:9092` plus
  `go run ./examples/producer` and `go run ./examples/consumer` works
  in three terminals.

## Known limitations (intentional)

- **No TLS.** Plaintext over TCP. Stage 5 or a separate hardening
  stage owns this.
- **No authentication or authorization.** Anyone who can dial the
  port can produce and consume.
- **No batching on the wire.** `Produce` carries exactly one record;
  the data-model `SendBatch` API still serializes record-by-record.
  Sticky-partitioner linger and batched produce land as a backlog
  item once the SDK has metrics to tune them.
- **No compression.** LZ4 batch compression is on the backlog; adds a
  dep and is best added once batching exists.
- **No `sendfile(2)` zero-copy path.** Reads decode-and-re-encode
  through Go buffers. Optimization for sealed segments is on the
  backlog.
- **`acks=durable` not exposed at the producer.** Storage layer
  supports `Sync()`; SDK still treats every send as page-cache-durable.
  Lights up after sticky batching is implemented.
- **No multiplexing.** One outstanding RPC per connection.

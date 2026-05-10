# Integrating Holocron from non-Go projects

**Today:** Holocron ships only a Go SDK. There is no Python, TypeScript, Java, or Rust client yet.

If you are evaluating Holocron for a non-Go project, this is the page to read. The honest answer is "not yet, but here's the plan."

## Why Go-only today

Holocron is pre-alpha. Every release between now and the first tagged version may change the wire protocol, the on-disk format, or the SDK surface. Maintaining a single canonical SDK while the contract is in motion keeps the surface area small enough to iterate on.

Once the wire protocol stabilises (the gate to the first tagged release), additional SDKs are next.

## What's coming, in order

This is the public roadmap. See [`roadmap.md`](roadmap.md) for the full set of waves.

1. **Stable v1 wire protocol** — the documented contract that non-Go SDKs target. Lands as part of Wave 3.
2. **Python SDK** — first non-Go client. Targets the largest non-Go user base for event streaming.
3. **TypeScript SDK** — for edge and full-stack apps.
4. **Other languages** (Rust, Java, .NET, etc.) — community-driven once the wire protocol is documented.

There is no published date for any of these. They are gated on Wave 1 (production-readiness) and Wave 2 (KV / Object store) finishing first.

## Today's options for non-Go projects

If you need to integrate Holocron now from a non-Go service, you have three paths:

### 1. Wait

The most honest answer. If you can defer adoption until the SDKs ship, do that. Pre-alpha software with no client SDK is not a production-ready dependency.

### 2. Use a sidecar

Run a small Go process alongside your service that uses the Holocron SDK and exposes a thin HTTP/gRPC façade your service consumes. This works today, but it pushes operational complexity onto you (a second process per pod, your own protocol design, error handling, retries).

### 3. Roll your own client against the wire protocol

Holocron's wire protocol is a hand-rolled length-prefixed binary framing over TCP, with payloads encoded as protobuf messages defined in the `proto/` module. The opcode set covers `produce`, `fetch`, `metadata`, `commit`, and `create-topic` (Stage 3 and later add more).

The protocol is **not yet stable** — between now and the first tagged release, framing, opcodes, and message shapes will change. If you build a custom client today, expect to update it when the v1 contract lands. The relevant source files:

- [`proto/`](https://github.com/jedi-knights/holocron/tree/main/proto) — message definitions
- [`broker/internal/server/`](https://github.com/jedi-knights/holocron/tree/main/broker/internal/server) — the canonical wire decoder; reading this is currently the only authoritative reference for the protocol shape
- [`sdk/net/`](https://github.com/jedi-knights/holocron/tree/main/sdk/net) — the Go client implementation, which is the closest thing to a reference client

If you go this route and want to upstream the result once the v1 protocol stabilises, open an issue first.

## When the SDKs land

This page will be updated with installation and usage guides for each SDK as it ships. Watch the repository or the [Roadmap](roadmap.md) for status.

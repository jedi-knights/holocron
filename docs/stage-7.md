# Stage 7 — Schema registry

Stage 7 is the platform's metadata service: a place where producers
declare the shape of the records they intend to write, consumers fetch
those schemas to deserialize records, and the registry assigns each
(subject, version) tuple a globally unique integer ID so on-the-wire
records carry only the ID. The lesson is the **broker as its own
metadata store**: registry state lives in a holocron topic, recovered
on startup by replaying the log from offset 0.

## What ships

| Component | Package | Role |
|---|---|---|
| Service kernel | `registry/registry.go` | Subject + version + ID tables; `Register`, `GetByID`, `GetVersion`, `GetLatest`, `ListSubjects`, `ListVersions`, `CheckCompatibility`. |
| Replay | `registry/registry.go` (`replay`) | On `Start`, drain `__holocron_schemas` and apply every record into in-memory tables. |
| HTTP handler | `registry/http.go` | Confluent-shaped REST surface — same paths existing schema-registry clients already speak. |
| Daemon | `registry/cmd/holocron-registry/main.go` | Connects via `sdk/net.Dial`, ensures the topic exists, serves HTTP on `--listen`. |
| Tests | `registry/registry_test.go`, `registry/http_test.go` | Subject/version/ID semantics, idempotency, restart-replay, HTTP routes. |

## HTTP API

```
POST   /subjects/{subject}/versions       — register a schema; returns {"id": N}
GET    /subjects                          — list subjects
GET    /subjects/{subject}/versions       — list versions for a subject
GET    /subjects/{subject}/versions/{v}   — fetch (v: number or "latest")
GET    /schemas/ids/{id}                  — fetch by global ID
POST   /compatibility/subjects/{subject}/versions/latest — check compat
                                            (mode=NONE works; others
                                             return 501 in V1)
```

Routes match Confluent Schema Registry so existing clients work without
changes. JSON shapes match too: `{"id": N}` on register, `{"schema": "..."}`
on GetByID, `[1, 2, 3]` on ListVersions.

## Design decisions

### Why store state on the broker

The registry is itself a service that holds critical metadata. Three
options for where that metadata lives:

1. **Local disk on the registry host.** Simple but doesn't survive
   host failure; high-availability requires bespoke replication.
2. **An external KV store (etcd, Consul, a database).** Adds an
   operational dependency the broker users don't already have.
3. **A holocron topic.** The broker is already replicated, durable,
   and operational. The registry uses what's there.

Option 3 is what Kafka's Confluent Schema Registry does, and what
Kafka itself does for cluster metadata in KRaft mode. The registry
becomes one of the broker's clients; nothing structurally new gets
introduced. It's the same trick as Connect's planned offset topic and
the broker's own consumer-group offset store: **the broker hosts its
ecosystem's metadata.**

### Why one partition

The schemas topic uses a single partition. This guarantees a total order
across all subjects, which makes ID assignment trivially monotonic — the
service just bumps `nextID` as it applies records. Multiple partitions
would require either a two-phase ID assignment or a globally
synchronized counter.

The cost is that all schema writes funnel through a single broker
partition leader. For a metadata service this is fine — the workload
is low-rate.

### Why subject + version + ID are all distinct

- **Subject** is the namespace — typically `<topic>-value` or
  `<topic>-key` so producers and consumers have a stable convention.
- **Version** is monotonically increasing per subject. Lets clients
  pin to "the v3 of orders-value" explicitly.
- **ID** is globally unique across all (subject, version) pairs. Lets
  on-the-wire records carry a tiny integer instead of a full schema.

Confluent uses the same triplet for the same reasons. Holocron mirrors
the convention so existing producer/consumer libraries written against
that contract translate cleanly.

### Why register-with-same-schema is idempotent

Production deployments routinely re-run their startup logic — a pod
restart, a deployment rollout, a configuration replay. Returning a new
ID for the same exact schema text every time would let bugs leak in
silently (a producer that thinks it's on schema 5 sends with ID 5, but
the consumer fetched 7 because of a redundant register).

The Service compares the new schema text to the latest version of the
subject; if they match, it returns the existing ID without writing.
This is what Confluent does too.

### Why compatibility modes return errors instead of silent passes

V1 implements only `NONE`. The other modes (`BACKWARD`, `FORWARD`,
`FULL`) require parsing the schema language to compare new vs. existing
fields, which means picking a schema language. Holocron's registry
doesn't (yet) — schemas are opaque text.

A pragmatic alternative would be to silently return `true` for all
modes. We don't, because that would be **misleading**: callers that
asked for `BACKWARD` and got `true` would believe the registry checked.
Returning an explicit "not implemented in V1" lets the caller choose:
fall back to `NONE`, or carry the check out-of-band.

### Why the registry is not part of the broker binary

A separate binary preserves the architectural rule: the broker stays
dumb. Schema concerns are a layer above the broker, not inside it.
Operators who don't need a registry don't run one; teams with multiple
registries (e.g., per environment) run multiple instances.

## Acceptance

- `make build && go test -race ./...` is green.
- `registry_test.go::TestStartReplaysExistingTopic`: a fresh Service
  pointed at a broker with prior registrations rebuilds the full state.
- `http_test.go::TestHTTP_*`: every Confluent-shaped route returns the
  right shape on success and the right status on missing resources.
- Smoke test: `holocrond` + `holocron-registry` + `curl` POST/GET
  round-trip across the wire works.

## Known limitations (intentional)

- **Single-instance only.** Two registries pointed at the same broker
  topic would each generate the same IDs (replay sees both, and they'd
  conflict). The broker side has no leader election for the registry.
  Multi-instance HA needs a coordination scheme (the registry that
  successfully appends gets the ID) or to delegate to the broker's own
  consensus once it's exposed for non-broker uses.
- **No compatibility enforcement beyond NONE.** Picking a schema
  language (Avro / JSON Schema / Protobuf) opens that door but is a
  Stage-7.5 project of its own.
- **No subject deletion.** Add-only. Real Confluent supports soft and
  hard deletes; both require tombstone records, which want log
  compaction (already in TODO.md).
- **No authentication or authorization on the HTTP API.** Plaintext,
  open. Production deployments would put it behind a reverse proxy or
  add API-key auth (also in TODO.md alongside broker-side auth).
- **The replay drain timeout is fixed at 200ms.** Fine for in-memory
  and local-disk brokers; a network broker that's pumping a large
  schemas topic might want to track the topic's high water at subscribe
  time and read up to it explicitly. Tracked as a follow-up.

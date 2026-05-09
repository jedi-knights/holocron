# Data model

This document defines the core data abstractions in holocron and the reasoning behind each choice. Read it before reading code; the names and invariants here govern every package.

The model is intentionally close to Apache Kafka's. Where holocron diverges, the divergence is called out explicitly with the *why*.

## Vocabulary at a glance

| Term | One-line definition |
|---|---|
| [Record](#record) | The atomic unit of data — an immutable, ordered event. |
| [Topic](#topic) | A named, durable stream of records. |
| [Partition](#partition) | An ordered, independently-appended subdivision of a topic. |
| [Offset](#offset) | A monotonically increasing position within a partition. |
| [Segment](#segment) | A bounded file holding a contiguous range of a partition's records. |
| [Index](#index) | A sparse offset-to-byte-position map for a segment. |
| [Producer](#producer) | A client that appends records to a topic. |
| [Consumer](#consumer) | A client that reads records from a partition starting at an offset. |
| [Consumer group](#consumer-group) | A set of consumers that cooperatively share a topic's partitions. |
| [Retention](#retention) | The policy controlling how long records remain readable. |

## Record

A **record** is the atomic unit of data in holocron. It is immutable once written.

```
Record {
    Offset    int64       // assigned by the broker on append; never reused
    Timestamp int64        // unix nanoseconds; producer-supplied or broker-assigned
    Key       []byte       // optional; used for partition routing and log compaction
    Value     []byte       // the payload — opaque bytes from the broker's perspective
    Headers   []Header     // optional key/value metadata
}

Header {
    Key   string
    Value []byte
}
```

### Field-level rules

- **`Offset`** is assigned by the broker, not the producer. A producer that wants idempotency uses headers or a deduplication key in `Key`, not the offset.
- **`Timestamp`** defaults to the broker's wall clock at append time if the producer omits it. Producer-supplied timestamps are accepted but never trusted for ordering — ordering is determined by offset alone.
- **`Key`** is opaque bytes. Two records with the same key are routed to the same partition (see [partition routing](#partition-routing)). The broker does not interpret keys.
- **`Value`** is opaque bytes. Holocron does not parse, validate, or transform payloads. Schema enforcement is a job for the SDK or an external schema registry.
- **`Headers`** are for application-level metadata: trace IDs, content-type hints, idempotency keys. Header keys are UTF-8 strings; values are bytes.

### Why immutable

An append-only log derives most of its useful properties — replay, durability, simple replication — from records being immutable. Mutation would force every downstream consumer to either re-read history or maintain its own diff state, which destroys the point of the log.

## Topic

A **topic** is a named, durable stream of records. Topics are the unit at which producers and consumers address data.

```
Topic {
    Name           string         // e.g. "orders.placed"
    Partitions     []Partition    // 1..N; fixed at creation, growable later
    RetentionMs    int64          // see Retention
    SegmentBytes   int64          // target rollover size for segments
}
```

### Naming

Topic names are case-sensitive UTF-8 strings, matching the regex `[a-zA-Z0-9._-]{1,249}`. The 249-byte limit and the allowed character set match Kafka's conventions so that topic names remain safe to use as filesystem path components.

### Why a fixed partition count

The partition count of a topic is fixed at creation. Increasing it later is supported but renders any [key-based routing](#partition-routing) non-deterministic for keys appended before the change. Holocron makes this trade-off visible in the API: increasing partitions requires explicit acknowledgement of the routing impact.

Decreasing the partition count is **not** supported. The records are still there, and silently dropping a partition would silently drop data.

## Partition

A **partition** is an ordered, independently-appended subdivision of a topic. Partitions are the unit of parallelism: producers append to partitions in parallel, and consumers read from partitions in parallel.

```
Partition {
    TopicName  string
    Index      int32       // 0..N-1 within a topic
    Log        Log          // the on-disk append-only log (segments + indexes)
    HighWater  int64        // offset of the next record to be appended
}
```

### Ordering guarantee

Within a single partition, records are strictly ordered by offset. This is the only ordering guarantee holocron makes. **There is no global ordering across partitions** — and there cannot be one without serializing all writes through a single node, which would defeat the purpose of partitioning.

If your application needs ordering for a logical entity (a user, an order, a tenant), route all events for that entity to the same partition by setting `Key`.

### Partition routing

The producer SDK routes records to partitions using these rules, in order:

1. If the producer specifies an explicit partition index, use it.
2. Else if `Key` is non-empty, route by `partition = hash(Key) mod len(partitions)`. The hash function is documented in `docs/protocol.md` and is fixed once the broker reaches v0.1.
3. Else round-robin across partitions, with sticky batching (a producer holds a partition for a small window to improve batch fill rate).

## Offset

An **offset** is a `int64` position within a partition. Offsets are monotonically increasing and never reused, even after retention deletes a record.

- The first record in a fresh partition has offset `0`.
- After a record at offset `N` is appended, the partition's high-water mark becomes `N+1`.
- After retention deletes records `[0, K)`, the partition's *log start offset* moves to `K`. Offsets `0..K-1` no longer exist; reading them returns an error.

### Who tracks offsets

Through stage 3, **consumers track their own offsets**. A consumer is a stateless reader — it tells the broker "give me records starting at offset X" and the broker complies. This is deliberately simpler than Kafka, where consumer offsets are stored in the broker.

In stage 4 (consumer groups), the broker becomes the source of truth for committed offsets per group. See [Consumer group](#consumer-group).

## Segment

A partition's log is not one giant file. It is a sequence of **segments** — bounded files holding contiguous offset ranges.

```
broker-data/
└── orders.placed/
    └── 0/                              # partition 0
        ├── 00000000000000000000.log    # records 0..123,456
        ├── 00000000000000000000.idx    # index for the above
        ├── 00000000000000123457.log    # records 123,457..245,910
        ├── 00000000000000123457.idx
        └── 00000000000000245911.log    # active segment (open for append)
            ...
```

The filename is the **base offset** — the offset of the first record in the segment, zero-padded to 20 digits so that lexical sort matches numeric sort.

### Why segmented

Segmenting solves three problems at once:

1. **Retention by deletion of whole files.** Removing one segment is `unlink(2)`. Trimming individual records out of a giant file would require rewriting it.
2. **Bounded recovery time.** On startup the broker only needs to validate the tail of the active segment, not the entire history.
3. **Bounded index memory.** Indexes can be loaded per active segment instead of one enormous index per partition.

### Rollover

A new segment is opened when the active one exceeds `Topic.SegmentBytes` (default: 1 GiB). The previous segment is closed and its index is finalized. Rollover is the only time a segment file is closed for writing.

## Index

Each segment has a companion **index** file mapping offsets to byte positions within the segment.

```
IndexEntry {
    RelativeOffset uint32    // offset relative to the segment's base offset
    Position       uint32    // byte position in the .log file
}
```

The index is **sparse**: not every record has an entry. The broker writes an entry every `IndexIntervalBytes` (default: 4 KiB) of log data. Lookups binary-search the index to find the nearest preceding entry, then scan forward in the log file to the exact record.

### Why sparse

A dense index doubles disk and memory cost for marginal lookup benefit. A sparse index trades a small linear scan (typically a few KiB) for an order-of-magnitude size reduction. This is the same trade-off Kafka makes.

## Producer

A **producer** is a client that appends records to a topic. The SDK exposes:

- `Send(record)` — append a single record; returns `(offset, error)`.
- `SendBatch([]record)` — append a batch atomically to a single partition.
- `Flush()` — block until all in-flight batches are acknowledged.

Producers are responsible for choosing partitions (directly or by setting `Key`). Producers are **not** responsible for assigning offsets or timestamps.

### Acknowledgement levels

Producers configure how durable an append must be before `Send` returns:

| Level | Meaning |
|---|---|
| `acks=none` | Fire-and-forget. Lowest latency, no durability guarantee. |
| `acks=local` | Wait for the leader broker to write to its OS page cache. Default. |
| `acks=durable` | Wait for the leader to `fsync` the segment to disk. |
| `acks=replicated` | (Stage 5+) Wait for a quorum of replicas to acknowledge. |

## Consumer

A **consumer** is a client that reads records from a partition starting at an offset. The SDK exposes:

- `Subscribe(topic)` — subscribe to all partitions of a topic.
- `Assign(partition, offset)` — pin to a specific partition at a starting offset.
- `Poll(maxRecords, timeout)` — fetch up to `maxRecords` available records.
- `Commit(offset)` — (stage 4+) commit a consumed offset to the broker for the consumer's group.

### Read positions

A consumer can start reading from any of these positions:

- **Earliest** — the partition's log start offset (which advances as retention deletes segments).
- **Latest** — the partition's high-water mark; only sees records appended after the consumer joined.
- **Specific offset** — any valid offset in `[log start, high water]`.
- **Timestamp** — (planned) the offset of the first record at or after a given timestamp; resolved using segment metadata.

## Consumer group

> **Status:** Specified for stage 4. Not yet implemented.

A **consumer group** is a set of consumers that cooperatively share the partitions of a topic. Each partition is assigned to exactly one consumer in the group at a time. When consumers join or leave, the group rebalances.

```
ConsumerGroup {
    ID          string
    Members     []ConsumerID
    Assignment  map[PartitionID]ConsumerID
    Committed   map[PartitionID]int64    // last committed offset per partition
}
```

The group's **committed offsets** are stored in a special internal topic (`__holocron_offsets`), which is itself a partitioned, replicated log. This is the same trick Kafka uses: the broker is its own source of truth.

## Retention

**Retention** controls how long records remain readable. Two policies, configurable per topic:

| Policy | Trigger |
|---|---|
| Time-based | Delete segments where the youngest record is older than `RetentionMs`. |
| Size-based | Delete oldest segments until the partition's total size is below `RetentionBytes`. |

Both policies operate at segment granularity. **Individual records are never deleted** — only whole segments past the retention threshold. The active segment is never deleted.

### Why segment-granular

Deleting individual records would rewrite the segment file, invalidate the index, and break offset stability. Holocron's design holds offsets stable for the lifetime of a segment; the only way an offset becomes unreadable is when its entire segment is dropped.

### Compaction

> **Status:** Planned for a later stage.

For topics that represent the latest state of a keyed entity (configuration, materialized views), retention by time is wrong — you want the latest record per key kept indefinitely. **Log compaction** rewrites segments, dropping superseded records but preserving the latest record for each key. This is the same model as Kafka's compacted topics.

## Invariants

These statements are true at all times for every running broker. Code that violates an invariant is a bug.

1. Within a partition, offsets are dense and monotonically increasing.
2. A record, once acknowledged at any `acks` level above `none`, is never silently lost. Failure to deliver is always reported as an error.
3. The active segment of a partition is the only segment open for writing.
4. The base offset in a segment's filename equals the offset of its first record.
5. A consumer that reads offset `N` and offset `N+1` consecutively from the same partition observes them in append order.
6. No global ordering exists across partitions; consumers cannot assume any.

## Open questions

These are intentionally undecided and will be resolved as the corresponding stages land. Tracking them here avoids re-litigating them ad-hoc in PRs.

- **Hash function for key-based partitioning.** Murmur2 (Kafka), xxhash, or FNV. Decision deferred to stage 3 alongside the wire protocol.
- **Index entry size.** 8 bytes (current spec) vs. 16 bytes for absolute offsets. Trade-off is index size vs. simpler lookup logic.
- **Header value encoding on the wire.** Length-prefixed bytes vs. fixed-size types. Decision deferred to stage 3.
- **Maximum record size.** Currently unbounded; will be capped before stage 3 to bound broker memory per request.

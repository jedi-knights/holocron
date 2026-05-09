# Stage 2 — Persistent append-only log

Stage 2 swaps the in-memory store for a disk-backed segmented log.
Records survive process restarts. The broker API, the SDK, and the
demo do not change — only the store underneath does.

Demo: `go run ./examples/inproc -dir /tmp/holocron-stage2`.
Daemon: `./bin/holocrond --data-dir /tmp/holocron-stage2`.

## What ships

| Component | Package | Role |
|---|---|---|
| Record framing | `broker/internal/log` (`frame.go`) | Length-prefixed records with CRC32C trailers. |
| Sparse index | `broker/internal/log` (`index.go`) | 8-byte entries (`relativeOffset`, `position`) every 4 KiB. |
| Segment | `broker/internal/log` (`segment.go`) | One `.log` + `.idx` pair; rolls at 1 GiB by default. |
| Partition log | `broker/internal/log` (`partition.go`) | Multi-segment composer; serves cross-segment reads. |
| Disk store | `broker/internal/storage` (`file.go`) | `FileStore` implements `Store` over `PartitionLog`. |
| Topic persistence | `broker/internal/topic` | `topics.json` written atomically on every change. |
| Disk handle | `broker/embed` (`NewDisk`) | Wires it all together; optional retention sweeper. |
| Daemon | `broker/cmd/holocrond` | Defaults to disk; honors `HOLOCRON_DATA_DIR`. |

## On-disk layout

```
<data-dir>/
├── topics.json                              # registered topics + their config
├── orders.placed/
│   └── 0/                                   # partition 0
│       ├── 00000000000000000000.log         # records 0 .. K-1
│       ├── 00000000000000000000.idx         # sparse index for above
│       ├── 00000000000000000000K.log        # active segment (open for append)
│       └── 00000000000000000000K.idx        # written at seal time
└── orders.placed/1/...                      # partition 1
```

Segment filenames are zero-padded to 20 digits so lexical sort matches
numeric sort. The number is the **base offset** — the offset of the first
record in the segment.

## Frame format

Each record on disk is a self-contained frame:

```
+--------------------+-----------+-------------+
| 4-byte body length |   body    | 4-byte CRC  |
+--------------------+-----------+-------------+
```

Body layout:

```
offset:        int64 BE       (assigned by the broker)
timestamp:     int64 BE       (broker wall clock if zero on Append)
key:           int32 len + bytes
value:         int32 len + bytes
header count:  uint32 BE
per header:    int32 keylen + key bytes (UTF-8) + int32 vallen + value bytes
```

The trailing CRC is **CRC32C** (Castagnoli) — same polynomial Kafka uses,
hardware-accelerated on modern CPUs.

## Index format

Each sparse index entry is 8 bytes:

```
+---------------------+----------------------+
| 4-byte rel. offset  | 4-byte log position  |
+---------------------+----------------------+
```

The index is **sparse** — one entry every 4 KiB of `.log` data, written
on append and persisted on segment seal. To read offset `O`:

1. Find the segment whose base offset is the largest ≤ `O`.
2. Binary search the index for the entry with the largest relative offset
   ≤ `O - baseOffset`.
3. Seek to that byte position in `.log`.
4. Scan forward, decoding records, until `r.Offset == O`.

The expected scan distance is at most the index interval (4 KiB) — small,
predictable, and avoids the cost of a dense index.

## Recovery

On `OpenPartition(dir)`:

1. List `*.log` files; sort by base offset ascending.
2. Open every non-active segment read-only; the highest-base one is the
   active segment, opened read-write.
3. For the active segment: scan records sequentially, validating each
   CRC. If a record fails (torn write from a crash), `Truncate` the file
   to the last good byte position and stop. The broker's high-water mark
   becomes the offset after the last good record.
4. Rebuild the active segment's index by scanning. Sealed segments load
   their `.idx` from disk; missing or empty index files yield an empty
   in-memory index, which still works — lookups will scan from position 0.

## Rollover

When the active segment exceeds `SegmentBytes`, the partition log:

1. `flush` + `fsync` the active segment.
2. Persist its index to `<base>.idx.tmp`, then `rename` to `<base>.idx`
   (atomic — a crash never leaves a half-written index).
3. Mark the segment read-only.
4. Open a new active segment whose base offset is the current high water.

Subsequent appends go to the new active segment.

## Retention

Time-based retention is opt-in via `embed.WithRetention(d)` (or
`holocrond --retention=24h`). A background sweeper runs every 5 minutes
(configurable) and, for each partition, deletes whole sealed segments
whose **last** record is older than the cutoff. The active segment is
never deleted.

Segment-granular only: individual records are never rewritten or
removed. This keeps offsets stable for a segment's lifetime; the only way
an offset becomes unreadable is when its entire segment is dropped.

Size-based retention is **not yet implemented**. It is a small extension
of the same sweeper and lands when needed.

## Durability

Stage 2 commits to **`acks=local` semantics**: an `Append` returns once
the record is in the OS page cache. The kernel flushes to disk on its
schedule, which gives strong durability for graceful shutdowns and weak
durability for hard kernel crashes / power loss.

`acks=durable` (`fsync` per write) is supported by `PartitionLog.Sync()`
and `FileStore` exposes nothing extra — Stage 3 lights this up at the
producer API once the network protocol is in place.

Trade-off: page-cache durability is the throughput-friendly default.
`fsync` per record collapses throughput by 1–2 orders of magnitude on
spinning disks and ~10× on NVMe. The data-model spec explicitly makes
this a per-producer choice rather than a broker default.

## Acceptance

Stage 2 is "done" when:

- `make build` succeeds.
- `go test -race ./...` passes.
- The end-to-end test `embed_test.go::TestNewDisk_PersistsTopicsAndDataAcrossRestart`
  produces records, closes the broker, reopens against the same data dir,
  and reads every record back.
- `holocrond --data-dir /tmp/holocron-foo` starts, recovers any topics,
  and reports clean shutdown on `SIGINT`.

## Known limitations (intentional)

- `acks=durable` API is plumbed at the storage layer only; SDK still
  treats every send as page-cache-durable. Lights up at Stage 3.
- No size-based retention.
- No log compaction (Kafka's "keep latest per key" mode).
- No segment pre-allocation; we let the OS extend files.
- No mmap on the read path; explicit `read` syscalls are the simpler
  starting point. mmap can come once profiling justifies it.
- Recovery rebuilds active-segment indexes by full scan. Fine for
  segments under ~1 GiB; a faster path can resume from the persisted
  index when needed.

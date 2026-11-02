# Native disk node path

This page describes the native disk path of a format 8 home.
The [memory and disk routes](lastdb-rehydrate.md) describe the logical model.
The [disk format v2 engine](../vendor/laststore/README.md) describes the run files and the background merge.
The [V2 compatibility contract](lastdb-disk-format-v2.md) records the accepted file versions, encryption, limits, and crash rules.

Disk format v2 is the sole native LastDB engine.
Normal startup and direct FoldDB constructors use it without an activation flag.
An empty home initializes directly. Startup gives the generic failure for any other format before logical namespace access. It performs no conversion.
Legacy Cloud Sync is removed. No configuration can activate it.
The V2 byte backup has a separate gate that defaults off. Its source availability does not establish production cloud readiness.

## Memory and write acknowledgements

The core model owns current schema definitions and fetched records.
A fetch adds the full record at one key. Alternate entries stay on disk.
Records use one warm byte budget, the same source controls, and a whole-record purge.
Private write preparation retains complete changes until disk commit.
The commit then publishes clean records. Older calls retain their earlier values and memory charges until release.
The recency list selects clean records for purge. It does not hold a second set of values.
The warm set has a byte limit, 256 MiB by default. It has no record count limit.
Separate allowances cover definitions, private writes, and temporary work.
Those allowances do not define a whole-process memory maximum.

Every native write waits for its complete disk commit and clean core publication before the ack.
No native route returns an early memory ack.
Logical requests carry no placement log across task
scopes and blocking calls. Local file-blob puts retain their explicit store flush.
When a complete value cannot enter the warm set, the commit clears affected records; a later read fetches them from disk.
Native status exposes accepted and completed sequences and pending requests.
The warm set keeps decoded records only. Status fields of the retired path memory stay on the wire for installed clients and read 0.
Normal status uses the core counts for its record count, holds, and purge total. Its warm lookup counters also come from the core.
Normal status returns a saved sample, normally refreshed every 60 seconds; native status returns the current counters.
The separate background purge counters describe queued work, without another value store.

The first accepted request starts the disk worker immediately. There is no 500 ms collection timer.
A FIFO lock lets callers already ahead of the worker share one complete disk commit.
Later callers wait behind the worker. Each caller waits for its own completed sequence.
A private write queue retains prepared bytes and resource owners until completion.
Ordinary reads cannot inspect that queue. Private write preparation can inspect its ordered pending changes.
The [eager queue design](lastdb-eager-durable-queue.md) defines the order, limits, and merge handoff.
`POST /api/db/flush` on the owner socket waits until every earlier accepted local write is durable on disk. It takes an empty body or `{}` and does no cloud work.
`LASTDB_DISK_CAP_BYTES` caps the file bytes of the main run home (`disk-run`). It has no default, and an unset value means no cap.
A value that is not a positive byte count stops the node open.
The node checks space when it admits a write. The filesystem must keep 128 MiB of reserve after the batch.
With a cap, the home bytes, the batch, and 64 MiB of replacement reserve must also stay within the cap.
A refused write gets a no-space error alone. It does not fail a commit.

Compaction reserves its output bytes before each file write and retains publication space through the required barriers.
The shared filesystem allowance covers both run homes. The configured disk cap applies to `disk-run`; `disk-cas` keeps its default uncapped policy.
Compaction can use the 64 MiB replacement allowance, but retains 64 MiB of free filesystem space and the hard home cap.
A large step can still require more free space or a larger configured cap.

An admission refusal preserves the previous readable root. Output owners attempt to remove unpublished files; reopen removes any unnamed leftovers.

## Home fence

Every node home is a format 8 home: disk format v2 with typed ranges and one record per key.
Its node marker file `laststore-pack-node-v1` holds `format=8\nhome=disk-run\ncas=disk-cas\n`, beside the `disk-run` and `disk-cas` run homes.
The run homes are complete before the marker appears.
Startup gives the generic failure for a home with any other marker. It does not convert the files.

## One read boundary

One call captures an immutable typed core view and a committed disk root.
Until the next complete publication, ordinary reads retain the previous committed state.
Queries in a batch share the call's work budget and temporary buffers.
Each query item retains its own result or explicit failure.
A failed native read cannot become an empty success through a fallback.
The call releases its file handles, pages, and buffers after completion.

A complete record can come directly from the captured core view.
A miss uses the captured disk root and can admit the fetched record into the core model.
An incomplete dependency receipt permits body reuse but requires a normal current record read.
Read errors do not become confirmed absence.

A range read reads the record keys under one hash.
The warm set can remain partial. A partial set of records cannot prove range completeness.
The current flat and compound key forms share one physical prefix union; they are not selectable older layouts.

## One complete mutation

One native transaction collects its disk entries and covered typed effects before publication.
The write gate selects the current comparison state for conditional writes.
Records become visible together with their alternate entry changes.
Purge operations use the same boundary.

All requests remain private until disk commit, including raw, administrative, and replay operations.
The commit updates the alternate entries, invalidates changed sources, and installs covered clean records under one read boundary.
An uncovered change clears affected core records before success.
This queue is disk output and resource lifetime state, rather than an alternate source for ordinary reads.

## Backup boundary

The V2 byte backup copies the run files of both run homes.
Capture saves the exact cut and its named hold before hashing or network work.
Resume uses that saved cut. It does not select a newer root.
Restore verifies the files before it publishes a fresh destination.

The [V2 byte transport design](designs/cloud-byte-transport-v2.md) and the [V2 client design](designs/cloud-run-v2-client.md) describe the jobs, transfer, and restore.
`LASTDB_BYTE_BACKUP_V2_ENABLED` controls that route and defaults off.
Legacy Cloud Sync is removed. A local capture does not prove deployed cloud transfer or cloud recovery.

Sources: [request completion](../fold_db/crates/core/src/storage/disk_pack/transaction/request.rs),
[queue](../fold_db/crates/core/src/storage/disk_pack/queue.rs),
[clean publication](../fold_db/crates/core/src/storage/disk_pack/view.rs),
and [backup capture](../fold_db/crates/core/src/storage/disk_backup/byte_backup/capture.rs).

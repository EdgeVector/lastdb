# Native disk node path

Current source: main `8a0a40b4f`, reviewed 2026-10-24.
The [memory and disk routes](lastdb-rehydrate.md) describe the logical model.
The [storage parts guide](lastdb-storage-parts.md) describes packed files and automatic maintenance.

The native packed engine is the sole logical LastDB engine.
Normal startup and direct FoldDB constructors use it without an activation flag.
An empty home initializes directly. Startup refuses an unsupported older format before logical namespace access and performs no conversion.
The disk format v1 removal deleted the separate owner command `lastdb-native-transfer` on 2026-10-26.
Legacy Cloud Sync activation receives an explicit error.
Native backup has a separate, disabled-by-default source gate; its source availability does not establish production cloud readiness.

The [cutover results](https://github.com/EdgeVector/lastdb/blob/0b0a79d4c10949e2f0570e82d9bc4fdb9513a285/docs/proofs/core-clarity-20261018/RESULTS.md) identify the exact source, releases, and fresh-data checks.
The [logical warm-set plan](lastdb-logical-warm-set-plan.md) records the current component boundaries.
The primary and its archive do not participate in these checks.

## Memory and write acknowledgements

The core model owns current schemas, molecules, keys, selected tips, confirmed absence, and fetched atom values.
A selected tip can remain without its value after a call that needs no value.
A value fetch adds the full atom to that same logical record.
The warm set preserves an existing full value when an unchanged tip arrives.
Both positive states use the same warm byte budget, source controls, and whole-record purge.
Private write preparation retains complete changes until disk commit.
The commit then publishes clean logical records. Older calls retain their earlier values and memory charges until release.
The recency list selects clean logical records for purge. It does not hold a second set of values.
The warm set defaults to a 256 MiB byte limit. The former 10,000-record cap is retired.
Separate allowances cover definitions, complete selected keys, private writes, and temporary work.
Those allowances do not define a whole-process memory maximum.

Every native write waits for its complete disk commit and clean core publication before the ack.
No native route returns an early memory ack.
Logical requests no longer carry the legacy LastStore placement log across task
scopes and blocking calls. The serialized `touched_group_ids` field remains empty
for native writes. Local file-blob puts retain their explicit store flush.
When a complete value cannot enter the warm set, the commit clears affected records; a later read fetches them from disk.
Native status exposes accepted and completed sequences and pending requests.
The retained dirty-record fields remain zero for compatibility.
`core_warm_tip_only_paths` counts retained positive paths whose values are not in memory.
Normal status uses these same core counts for its logical-record count, holds, dirty records, and purge total.
Its tip-only count and warm lookup counters also come from the core.
Normal status returns a saved sample, normally refreshed every 60 seconds; native status returns the current counters.
The separate background purge counters describe queued work, without another value store.

The first accepted request starts the disk worker immediately. There is no 500 ms collection timer.
A FIFO lock lets callers already ahead of the worker share one complete disk commit.
Later callers wait behind the worker. Each caller waits for its own completed sequence.
A private write queue retains prepared bytes and resource owners until completion.
Ordinary reads cannot inspect that queue. Private write preparation can inspect its ordered pending changes.
The [eager queue design](lastdb-eager-durable-queue.md) defines the order, limits, and compaction handoff.

## Home fence

Since disk format v2 phase V3, every new node home is v2.
Its `laststore-pack-node-v1` marker holds `format=3\nhome=disk-run\ncas=disk-cas\n`, beside the `disk-run` and `disk-cas` run homes.
The rest of this section describes the v1 fence of an older home, which still opens as v1.

The node store contains `laststore-pack-node-v1`, with the exact bytes `format=1\nhome=disk-pack\n`.
The native physical home resides in its fixed `disk-pack` child directory.
Both the node store and native child contain a downgrade refusal in `laststore-layout-v1`:
`version=2\nlayout_mode=disk_pack_v1\n`.
Native startup verifies that refusal and the node marker.
The public constructor also recognizes a direct native physical home.
Neither entry selects the older engine or converts its files.

## One read boundary

One call captures an immutable typed core view and a committed disk root.
Until the next complete publication, ordinary reads retain the previous committed state.
Queries in a batch share the call's work budget and temporary buffers.
Each query item retains its own result or explicit failure.
A failed native read cannot become an empty success through a fallback.
The call releases its file handles, pages, and buffers after completion.

A complete logical record can come directly from the captured core view.
A miss uses the captured disk root and can admit the fetched logical record into the core model.
An incomplete dependency receipt permits body reuse but requires normal current-tip resolution.
Read errors do not become confirmed absence.

Selected HashRange schemas can preload complete molecule keys for fast range selection.
The preload occurs on first use and remains useful while that complete key set stays ready.
The body warm set can remain partial. A partial set of bodies cannot prove range completeness.
The current flat and compound key forms share one physical prefix union; they are not selectable older layouts.

## One complete mutation

One native transaction collects its physical records and covered typed effects before publication.
The write gate selects the current comparison state for conditional writes.
Logical values and confirmed absence become visible together with their key-membership changes.
Purge operations use the same boundary.

All requests remain private until disk commit, including raw, administrative, and replay operations.
The commit updates key membership, invalidates changed sources, and installs covered clean values under one read boundary.
An uncovered change clears affected core records before success.
This queue is disk output and resource lifetime state, rather than an alternate source for ordinary reads.

## Backup boundary

Local backup exports the selected snapshot and exact dependency files. Restore verifies them before it publishes a fresh destination.
The native capture code also saves an exact source home, snapshot generation, frontier, and durable file hold before network work.
Resume uses that saved cut. It does not select a newer root.

The [native backup design](designs/native-cloud-backup-mini-v1.md) describes separate owner jobs, transfer, restore, and release gates.
`LASTDB_NATIVE_BACKUP_ENABLED` controls that source route and defaults off.
Legacy Cloud Sync remains disabled. Local capture and export do not prove deployed cloud transfer or cloud recovery.

Sources: [request completion](../fold_db/crates/core/src/storage/disk_pack/transaction/request.rs),
[queue](../fold_db/crates/core/src/storage/disk_pack/queue.rs),
[clean publication](../fold_db/crates/core/src/storage/disk_pack/view.rs),
and [backup capture](../fold_db/crates/core/src/storage/disk_pack/backup_capture/capture.rs).

## Historical receipts

The [October 15 receipt](https://github.com/EdgeVector/lastdb/blob/0b0a79d4c10949e2f0570e82d9bc4fdb9513a285/docs/proofs/disk-native-node-20261015/report.md) records the earlier development path and its debug comparison.
That step required explicit engine activation. Its controls and measurements describe only its recorded source.
The [7110c7f5 receipt](https://github.com/EdgeVector/lastdb/blob/0b0a79d4c10949e2f0570e82d9bc4fdb9513a285/docs/proofs/layer2-completion-20261016/node-final-off-summary.json) measures a later historical candidate.
The [October 16 status report](lastdb-disk-rollout-status.md) describes that candidate's earlier limits.
Neither receipt establishes current performance, primary behavior, or upgrade readiness.

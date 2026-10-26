# Native disk work status, October 16, 2026

CAUTION: This historical candidate report does not describe current startup, acknowledgements, limits, or the disk format.
Use the [current native route summary](lastdb-disk-native-node.md), [memory and disk routes](lastdb-rehydrate.md),
and [storage parts guide](lastdb-storage-parts.md).
Current native writes wait for disk commit and clean memory publication, with no collection timer or early memory ack.
The source identities, measurements, and limits below describe the October 16 candidate.

The local engine work is complete within the limits below.
Both benchmark versions start with empty homes, write the same synthetic input, and read it back.

Tom selected completion of the original native LastStore packed-file engine.
R5′ finds file positions through directory and locator pages.
R6′ reads the required bytes from packed files.
The candidate keeps existing atom and tip records, with complete batches as the write boundary.
It does not implement the later direct-position tip format or a new body-key codec.

The final product source revision is `7110c7f5`, before the final receipts and status documents.
The daemon, CLI, disk-home, recovery, and maintenance programs use that same clean source.
The [completion receipts](https://github.com/EdgeVector/lastdb/tree/0b0a79d4c10949e2f0570e82d9bc4fdb9513a285/docs/proofs/layer2-completion-20261016/) identify each exact executable and configuration.
The [final fresh-data report](https://github.com/EdgeVector/lastdb/blob/0b0a79d4c10949e2f0570e82d9bc4fdb9513a285/docs/proofs/layer2-completion-20261016/semantics-7110.md) records a new 256-record write, reads, updates, conditions, delete, recreate, and flush.
That final check uses one empty home, with no import, copy, restore, or reopen.

The [earlier batch component report](https://github.com/EdgeVector/lastdb/blob/0b0a79d4c10949e2f0570e82d9bc4fdb9513a285/docs/proofs/layer2-completion-20261016/batch-semantics.md) records full responses, partial memory results, and ordered change entries on `b47`.
The [earlier record component report](https://github.com/EdgeVector/lastdb/blob/0b0a79d4c10949e2f0570e82d9bc4fdb9513a285/docs/proofs/layer2-completion-20261016/semantics.md) retains its original memory ack, condition, delete, and recreate results.
The [earlier recovery component report](https://github.com/EdgeVector/lastdb/blob/0b0a79d4c10949e2f0570e82d9bc4fdb9513a285/docs/proofs/layer2-completion-20261016/recovery.md) records shared atoms, local backup, restore, and conversion on its stated source.
The [benchmark report](https://github.com/EdgeVector/lastdb/blob/0b0a79d4c10949e2f0570e82d9bc4fdb9513a285/docs/proofs/layer2-completion-20261016/benchmarks.md) separates physical engine costs from node costs and memory acknowledgements.

## Candidate source

| Area | Candidate result |
|---|---|
| Batch reads | Exact, batch, and bounded range reads; call-local page and file state; secondary atom requests across fields use one batch per storage scope. |
| Complete writes | One private native request, complete output reservation, complete journal publication, and a durable receipt. |
| Memory ack | Explicit write mode accepts eligible complete batches in memory. The idle flush delay defaults to 500 ms and cannot exceed 500 ms. |
| Captured reads | One disk root and accepted memory view, including schema names and field metadata. Native data choices bypass the legacy memory graph. |
| Warm set | Complete logical field paths, with a 10000-record cap. Repeated complete owner queries can reuse one key or an explicit key batch. |
| Optional metadata | Complete memory coverage precedes value copies. Optional request metadata can decline admission without a failed data read. |
| Change list | One ordered event vector uses bounded event-and-position writes, rather than a separate write for each event. |
| Write preparation | Changed keys use batch reads for tips, delete barriers, and molecule pointers. One exact write owner holds one 40,000-key work budget. Queries retain 10,000 keys. |
| File lifetime | Read and backup holds protect required files. Compaction and reclaim remain inside LastStore. |
| Packed payloads | Conversion and compaction share one payload writer across hash groups. Per-group locator trees remain separate. |
| New hash groups | A new group takes a fresh part in an existing locator object, as an edit of the empty tree at page zero. Only the first groups of an empty directory make an object. A batch adds four parts. |
| Complete batch size | Fresh native homes require feature `0x20` before admission and permit 16,384 stored operations. Existing homes retain 4,096. |
| Disk cap | `LASTDB_DISK_CAP_BYTES` connects write, backup, and replacement reservations. Normal writes preserve 64 MiB of replacement headroom. |
| Local recovery | Owner backup and flush routes, exact bundle validation, source-free local restore, and explicit forward and reverse conversion. |
| Current status | `GET /api/db/native-status` reports accepted work and the durable boundary without a forced flush. |

The 500 ms interval starts disk work.
Disk completion also includes the publication cost.
`POST /api/db/flush` waits for earlier accepted batches and returns the durable boundary.
An unset disk cap keeps the existing uncapped mode.
A configured cap must be positive; admission counts current files and reserves replacement space.
Unsupported or excessive shared paths use the durable fallback.
Fresh empty LastDB homes use the version-2 schema-reference proof and feature bit `0x10`.
Generic conversions preserve exact bytes but do not grant this derived authority from an imported proof.

## Supported limits

Physical read batches retain a 10,000-slot limit, including write preparation.
Larger write work uses ordered chunks inside the same held call and complete pending batch.
The call never resets its budget after an error.
Write preparation retains the normal page, block, and byte limits.
The warm set retains its separate 10,000-logical-record cap.
One logical mutation can require many stored operations for its atoms, tips, shared references, and delete work.
The 16,384-operation feature keeps the same 32-bit operation numbers and 128 MiB batch byte cap.
Older readers refuse the feature before data access. This change does not upgrade existing homes automatically.

Compaction copies outside the write gate and publishes after a bounded catch-up.
Automatic runs can copy recent tiers. Explicit compaction defaults to a full copy.
See `docs/lastdb-concurrent-compaction.md`.
Its quote permits at most 65,536 coordinates and 1,000,000 records.
It requires replacement space under a configured disk cap.
Payload files share packed data; each hash group retains its locator files.
The engine preserves opaque field bytes but refuses the separate physical encrypted-frame format.
The small generated homes do not establish behavior at the full warm-set cap.

The final compaction receipt reports 2 payload files, with 4,019,991 bytes.
It also reports 2,662 locator files, with 109,920,256 bytes.
The receipt reports 2,772 total files and 115,459,898 bytes after release and reclaim.
Payload packing succeeds, but locator metadata dominates this dataset.
The whole-home compaction takes 40.801 seconds; the benchmark report separates this operation from quiet latency measurements.
Large durable writes still take seconds, and the final node range measurements remain slower than the earlier native baseline.

## Adoption boundary

The default backend remains legacy.
The native path requires explicit selection or a valid existing native home.
The primary, primary archive, shared services, and cloud configuration stay unchanged.
Native cloud upload and bootstrap remain separate Layer 3 work.
The native startup gate still refuses cloud configurations.
Local backup success does not establish cloud recovery or primary upgrade readiness.

## Earlier receipts

The October 15 receipts remain historical evidence with their original source and limits.
They do not establish the final candidate's performance or recovery result.

- [Initial native node](https://github.com/EdgeVector/lastdb/blob/0b0a79d4c10949e2f0570e82d9bc4fdb9513a285/docs/proofs/disk-native-node-20261015/report.md)
- [Native read holds](https://github.com/EdgeVector/lastdb/blob/0b0a79d4c10949e2f0570e82d9bc4fdb9513a285/docs/proofs/disk-native-read-holds-20261015/report.md)
- [File lifetime and compaction](https://github.com/EdgeVector/lastdb/blob/0b0a79d4c10949e2f0570e82d9bc4fdb9513a285/docs/proofs/disk-file-lifetime-20261016/report.md)
- [Earlier batch read and write comparison](https://github.com/EdgeVector/lastdb/blob/0b0a79d4c10949e2f0570e82d9bc4fdb9513a285/docs/proofs/layer2-batch-reads-writes-20261016/report.md)

Use the final completion receipts for the final executable hashes, measurements, and limits.
Use generated data and product commands for operational evidence.
Keep build, format, lint, and security checks. Add no tests or fixture harness.

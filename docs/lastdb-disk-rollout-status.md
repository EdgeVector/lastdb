# Native disk work status, October 16, 2026

Tom selected completion of the original native LastStore packed-file engine.
R5′ finds file positions through directory and locator pages.
R6′ reads the required bytes from packed files.
The candidate keeps existing atom and tip records, with complete batches as the write boundary.
It does not implement the later direct-position tip format or a new body-key codec.

The final product source revision is `08409a22`, before the final receipts and status documents.
The [completion receipts](proofs/layer2-completion-20261016/) identify each exact executable and configuration.
The [read and write report](proofs/layer2-completion-20261016/semantics.md) records complete responses, memory ack, conditions, delete, flush, and reopen.
The [recovery report](proofs/layer2-completion-20261016/recovery.md) records shared atoms, local backup, restore, and conversion.
The [benchmark report](proofs/layer2-completion-20261016/benchmarks.md) separates physical engine costs from node costs and memory acknowledgements.

## Candidate source

| Area | Candidate result |
|---|---|
| Batch reads | Exact, batch, and bounded range reads; call-local page and file state; secondary atom requests across fields use one batch per storage scope. |
| Complete writes | One private native request, complete output reservation, complete journal publication, and a durable receipt. |
| Memory ack | Explicit write mode accepts eligible complete batches in memory. The idle flush delay defaults to 500 ms and cannot exceed 500 ms. |
| Captured reads | One disk root and accepted memory view, including schema names and field metadata. Native data choices bypass the legacy memory graph. |
| Warm set | Complete logical field paths, with a 10000-record cap. Positive owner exact queries can reuse complete warm responses. |
| File lifetime | Read and backup holds protect required files. Compaction and reclaim remain inside LastStore. |
| Packed payloads | Conversion and compaction share one payload writer across hash groups. Per-group locator trees remain separate. |
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

- [Initial native node](proofs/disk-native-node-20261015/report.md)
- [Native read holds](proofs/disk-native-read-holds-20261015/report.md)
- [File lifetime and compaction](proofs/disk-file-lifetime-20261016/report.md)
- [Earlier batch read and write comparison](proofs/layer2-batch-reads-writes-20261016/report.md)

Use the final completion receipts for the final executable hashes, measurements, and limits.
Use generated data and product commands for operational evidence.
Keep build, format, lint, and security checks. Add no tests or fixture harness.

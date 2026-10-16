# Native read-state receipt, October 15, 2026

The native reader now excludes live graph tips, atoms, and schema deletion marks from its captured disk state.
A selected local tip without an atom returns a server error instead of successful partial fields.
Legacy graph-first reads remain available.
The [read policy](../../lastdb-disk-read-state-routing.md) describes the source change and limits.

## Candidate and data

- Optimized build source: `a4ec6440`, above the flush fix `94ffbd3a`.
- Daemon SHA-256: `9afec0a3dad9fef7e238fe8b39a8877486e793fbf5f0ccafbfb31dd67028be0c`.
- Reported version: `0.23.4-pre.0-194-ga4ec64405`.
- Build: `cargo build -p lastdb_node --bin lastdbd --release --locked`.
- The branch then rebases onto merged PR #205, `c5b4c774`. Its read source becomes `de5d2a3b` and `a77e27eb`.
- A source comparison confirms no product-source differences across that rebase in `fold_db`, `lastdb_node`, or `lastdb_host`.

The native home starts empty and uses `LASTDB_DISK_PACK=1`, `LASTDB_RESIDENT_MODE=off`, and cloud sync off.
Four product batches create 256 records in `ReadReceiptProof`.
The schema uses hash `bucket`, range `id`, and fields `bucket`, `id`, `title`, `body`.
The keys are `generated` plus `record-000000` through `record-000255`.
Initial body N contains 512 repetitions of `abcdefgh` followed by N.
The commands use the isolated schema route and normal product APIs from [the receipt recipe](../../lastdb-read-receipts.md#direct-commands-on-generated-data).

## Writes and complete fields

Four 64-record update batches and eight three-item query batches run through four independent clients.
The query batches contain one exact key, several exact keys with an absence, and a six-record hash range.
All 256 updates succeed. All 24 read items return status 200, complete expected fields, correct counts, and zero unresolved records.
All retained read receipts report zero group opens, loader calls, legacy replay, journal replay, footer bytes, and directory reads.
See [writes](read-native-writes.json) and [reads](read-native-reads.json).

Two more collected batches set the first 64 values, create an optional-field record and an independent Hash-schema record, and delete `record-000255`.
The Hash schema uses distinct field descriptions to avoid unintended protein membership.
The five-item query batch verifies:

- Exact key zero returns the complete final title and body.
- The optional record returns its title without a body field. The absent key remains absent.
- The Hash key returns its complete expected fields.
- The deleted key returns no record.
- The bounded hash range returns all 255 expected keys and full field bytes, with no duplicates and `has_more=false`.

See [final mutations](read-native-final-mutations.json) and [full data checks](read-native-data.json).
This covers 257 distinct live keys across both schemas, plus the deleted and absent keys.

## Capacity and reopen

Four concurrent product calls each contain 64 query items.
Every item requests one existing key and 128 distinct absent keys.
The workload uses the real 10,000-key budget.

- 38 items succeed and retain the complete expected record.
- 218 items return 503 `read_resource_limit`, with the explicit held-capacity message.
- A later five-item batch succeeds and returns the expected complete values and valid absences.
- Settled status reports 10,000 keys, zero held keys, zero dirty keys, zero over-cap keys, zero queued persist jobs, and zero persist failures.

See [capacity results](read-native-pressure.json) and [settled status](read-native-status.json).
The status sample does not establish the exact maximum concurrent memory size.

A normal close and reopen retains every checked field, optional absence, Hash value, and deletion.
The keyed count remains 255; its ten-record page reports `has_more=true` and zero unresolved records.
See [reopen](read-native-reopen.json), [count](read-native-count.json), and [reopened count](read-native-reopen-count.json).

The same final binary also opens the earlier fresh generated legacy home in `write` mode.
All 256 complete records match the first fix's saved result.
Four more 64-record update batches and twelve read items then succeed with complete fields and zero unresolved records.
See [legacy compatibility](read-legacy-compat.json), [legacy writes](read-legacy-writes.json), and [legacy reads](read-legacy-concurrent.json).

## Checks and limits

Workspace library/binary Clippy, optimized build, format, diff, source/function size, new-allow, storage-write, URL, artifact, redaction, byte-slice, and secret checks pass.
No tests, fixture harness, fault injection, or artificial data corruption runs.
The missing-local-atom refusal and direct library count wrapper receive source checks; no damaged-data or direct-library harness exercises them.
The ordinary concurrent commands do not force the exact previously unsafe instruction interleaving.
The source policy removes the live-graph selection that enabled that path.

This receipt does not establish primary behavior, the final speed gate, schema-cache versions per query, or complete native memory-ack admission.
No primary, primary copy, archive, factory, board, CI configuration, or shared service participates.
Both native runs and the final legacy run close normally. Only compact generated-data receipts enter the repository.
The owned temporary homes can leave after these receipts and the PR body are saved.

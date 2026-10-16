# Generated native schema-cache receipt, October 15, 2026

The final daemon serves generated records, updates, deletes, batches, and ranges after the cache publication change.
The same data remains correct after a normal close and reopen.
This receipt completes one preparation unit. The native activation gates remain closed.

## Candidate and scope

- Source commits: `7ef4f7be` and `672321cc`.
- Debug daemon SHA-256: `929a4e33a49cdf898de5c3a51b2aa99dcfede80e702f2c4cdfda168dcb4e4c74`.
- The node starts with an empty owned temporary home and creates its own identity.
- `LASTDB_DISK_PACK=1` selects native storage. `LASTDB_RESIDENT_MODE=off` remains required.
- Cloud sync stays off. No primary, primary copy, archive, factory, board, or shared service participates.
- Both owned daemon runs close normally. The temporary home and request files are removed after receipt preservation.

The generated `ReadReceiptProof` schema uses hash `bucket` and range `id`.
One product batch creates eight explicit keys from `generated / record-0` through `generated / record-7`.
Each body contains 8,198 bytes. The commands follow [the existing generated-data product recipe](../../lastdb-read-receipts.md#direct-commands-on-generated-data).
The body is `body-N` followed by 1,024 repetitions of `abcdefgh`.

## Product result

The first query batch reads one exact key, three explicit keys including an absence, one range, and record seven.
Every item returns status 200. Full field bytes match the generated request values.
The response includes the hash field with the requested title and body fields.

One product mutation batch changes record zero and deletes record seven.
The next query returns `updated-title-0` and `updated-body-0` for zero and no record for seven.
The range `[record-2, record-6)` with limit two returns records two and three.
It reports `has_more=true` and cursor `record-3`.

After normal close and reopen, one eight-key product call returns seven records.
Every remaining body matches its expected full bytes. The deleted record remains absent.
See [the observations](observations.json).

All retained query-item receipts report zero loader calls, hash-group opens, legacy replay, journal replay, footer bytes, and directory reads.
The all-key read has the same zero counters in [its response header](all-reopen.headers).
The repeat query leaves the complete CURRENT digest unchanged.
See [the read receipts](read-receipts.json) and [the root receipt](root-and-time.json).

The create call takes 3.687137 seconds. The update/delete call takes 1.669492 seconds.
The reply reports `background_tasks_drained=true`, `convergence_pending=false`, and the existing `durability=queued` label.
These debug observations do not establish the speed completion gate or a memory-first acknowledgement contract.

## Source checks and limits

Workspace library/binary Clippy, the daemon build, format, diff, source size, function size, new-allow, storage-write, URL, artifact, redaction, byte-slice, and secret checks pass.
No test suite, fixture harness, cancellation probe, or fault injection runs.
The ownership and deferred cache order receive source checks. This receipt does not prove interruption or allocation failure behavior.

The source moves uniquely held bodies into the journal batch and retains a copy fallback for a shared record.
The source also retains the write gate and deferred cache actions inside submitted blocking work.
The operational commands do not measure allocator copies or cancellation behavior.

The [status receipt](status.json) reports the existing 10,000-key budget and zero over-cap keys.
This small generated node does not establish complete logical-path admission or a process memory bound.
Versioned schema reads, complete physical and logical capacity, conversion, backups, reverse conversion, and release-mode speed remain required.
The primary upgrade remains unprepared.

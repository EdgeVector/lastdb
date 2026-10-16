# Flush cleanup lock, October 15, 2026

PR #204 removes the graph tip after a flush. Its cleanup released the slot lock before the dirty flags and key entry left.
A newer write could publish in that gap and lose its protection to the older cleanup.
The fix keeps the slot write lock through every keyed removal.
The tip map lock ends after the matching tip leaves.
Atom cleanup and budget enforcement follow the slot lock release.
The existing purge path uses the same lock order and cleanup helpers.

## Candidate

- Source: `94ffbd3a`, based on merged PR #204, `155ca98b`.
- Optimized daemon SHA-256: `5c459ebc5ce680b1f6c9309e360c1b7d389abec4e9e474a44368201741b5fbde`.
- Build: `cargo build -p lastdb_node --bin lastdbd --release --locked`.
- Fresh generated legacy home, `LASTDB_RESIDENT_MODE=write`, cloud sync off.
- The isolated product schema route creates `ReadReceiptProof` with hash `bucket`, range `id`, and fields `bucket`, `id`, `title`, `body`.
- No primary, primary copy, archive, factory, board, or shared service participates.

## Product results

Four independent product batches create 256 named keys, 64 records per call.
Each initial body contains 512 repetitions of `abcdefgh` followed by its record number.
The client collects each complete mutation batch before the call.

Thirty-two update batches and thirty-two read batches then run through four independent clients.
Each update changes the same 64 keys and receives a queued acknowledgement.
Each read batch contains an exact key, several exact keys with one absence, and a six-record hash range.

- All 2,048 updates are accepted.
- All 96 read items return status 200 and the expected record counts.
- Every returned record contains its complete expected title and body bytes from an accepted value.
- The reads report zero unresolved records.
- The final bounded hash range returns all 256 expected keys and values with `has_more=false`.
- A normal close and reopen retains those complete values for all 256 records.

The status observation reports the 10,000-key budget, 3,522 keys, zero over-cap keys, and zero dirty keys.
The persist queue still contains 15 jobs at that observation. Both owned daemon runs then close normally.
This small workload does not exercise the key-budget boundary.
The receipts retain counts and comparisons; private node identities and full logs remain outside the repository.

See [writes](flush-writes.json), [reads](flush-reads.json), [data](flush-data.json), [status](flush-status.json), and [reopen](flush-reopen.json).

## Validation limits

Workspace library/binary Clippy, the optimized daemon build, format, diff, source/function size, new-allow, storage-write, URL, artifact, redaction, byte-slice, and secret checks pass.
The product workload exercises ordinary concurrent calls and normal shutdown.
It does not force or measure the exact previously unsafe instruction interleaving.
The lock-lifetime source review establishes that the newer revision cannot enter the keyed cleanup boundary.
No tests, fixture harness, or fault injection runs.

The separate native read-state fix remains necessary.
The generated home stays private for that follow-up check, then its owned temporary data can leave.

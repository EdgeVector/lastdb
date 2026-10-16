# Native read holds and optimized-node receipt, October 15, 2026

The native reader now protects fetched records until the shared read scope closes.
Generated product commands exercise the real 10,000-key cap and return explicit capacity errors when active calls hold that capacity.
Both native and legacy nodes return identical full field bytes for 2,048 generated keys after normal close and reopen.
This receipt does not complete the logical-path, conversion, recovery, or primary upgrade gates.

## Candidate and generated data

- Source: `770957ad`, after PR #202.
- Optimized daemon SHA-256: `a4536a62aca84ea39ccad5b66e4ca605478b1475a997c5ba399e5d9ba19f4e95`.
- Build: `cargo build -p lastdb_node --bin lastdbd --release --locked`.
- Both nodes use the same daemon, `LASTDB_RESIDENT_MODE=off`, and separate fresh generated homes.
- The native node selects `LASTDB_DISK_PACK=1`. The legacy node uses the default backend.
- Cloud sync stays off. The nodes run one at a time. No owned build runs during the read measurements.
- No primary, primary copy, archive, board, factory, or shared service participates.

The schema is `ReadReceiptProof`, with hash `bucket`, range `id`, and fields `bucket`, `id`, `title`, and `body`.
All 2,048 keys use hash `generated` and ranges `record-000000` through `record-002047`.
Title N is `title-N`. Body N is `body-N:` followed by 16 repetitions of `abcdefgh`.
The load collects every mutation first, then sends 32 product batches of 64 records.
The commands use the isolated schema route and product APIs from [the existing recipe](../../lastdb-read-receipts.md#direct-commands-on-generated-data).

## Correct data and held capacity

After normal close and reopen, each node answers the same first exact query and 34-item query batch.
The batch contains 32 distinct exact queries, a five-key query with one absence, and a bounded range.
Every item returns status 200 and the expected full field bytes.
The range returns 32 records from `record-001024` and cursor `record-001055`, with `has_more=true`.
See [the mixed results](mixed-results.json).

Thirty-two bounded range calls then return all 2,048 keys.
Every returned key and field byte matches the generated input on both nodes.
The normalized records produce SHA-256 `761fe45f36af96fba619ff9fa13407fd801b590f4269fb2818c4b1343835bf8d` on both nodes and the generated input.
See [native data](native-data.json), [legacy data](legacy-data.json), and [the digest](root-and-data-digest.json).

Four independent native calls run concurrently. Each contains 64 query items.
Each item requests one existing key and 128 distinct absent keys.
The requests use separate client processes and the normal 10,000-key budget.

- 40 query items succeed and retain the complete expected record.
- 216 items return 503 `read_resource_limit`, with the message that active calls hold the native logical-record capacity.
- No successful item returns an empty result or the wrong body.
- A later ordinary exact query succeeds and returns the expected complete body.
- Final status reports 10,000 keys, zero held keys, and zero over-cap keys.

The status sampler uses a one-second interval. The concurrent calls finish in about 0.55 seconds.
The during-call sample does not capture the held-key peak; the explicit capacity errors establish the observed refusal path.
This receipt does not claim a measured peak hold count.
See [capacity results](pressure.json), [the subsequent read](after-pressure.json), and [status](status.json).

CURRENT retains its digest across the first query, mixed batch, full export, capacity workload, and subsequent exact query.
All retained native cost receipts report zero hash-group opens, legacy replay, journal replay, footer bytes, and directory reads.

## Read cost

These are query-scope times. They exclude some request and response work.
The first exact query follows a normal reopen. Filesystem page state remains unknown.
The 32 item times come from one mixed product batch with its normal shared root and worker limit.
Percentiles use the nearest rank. They do not form an independent distribution of cold-device reads.

| Observation | Native | Legacy |
|---|---:|---:|
| First exact query | 16.742792 ms | 20.737083 ms |
| First exact successful bytes | 771,788 | 20,138,888 |
| First exact file opens | 55 | 28 |
| First exact legacy replay bytes | 0 | 20,133,608 |
| 32-item p50 | 6.214042 ms | 7.881583 ms |
| 32-item p95 | 8.644459 ms | 8.392292 ms |
| 32-item p99 | 8.869333 ms | 9.162334 ms |
| 32-item successful byte sum | 12,489,904 | 418,958,253 |
| 32-item file-open sum | 908 | 685 |

The native first query and median improve in this observation. The native p95 is slightly slower.
The native reader also opens more files despite the large byte reduction.
This does not establish the final speed gate or the separate small storage-key p95 goal.
Host activity and filesystem cache state remain uncontrolled.
See [native costs](native-costs.json) and [legacy costs](legacy-costs.json).

## Load limits and source checks

The native client's multi-transfer load completes 31 requests and times out before one connection.
Server telemetry reports 31 successful mutation batches and no mutation errors at that point.
A separate request repeats the missing 64-key batch and succeeds.
The full record comparison then confirms all 2,048 records.
The client multi-transfer behavior does not establish four active server requests, so this receipt makes no comparative write-rate claim.

Native replies report pending convergence for 31 batches. The legacy replies report no pending convergence.
Native telemetry attributes about 163.3 seconds to mutation work and 150.1 seconds to the background-task wait across the first 31 batches.
The node log reports remaining background tasks. The task kind is not established here.
This gap remains open before activation; the phase name alone does not identify search as the cause.
See [load observations](load-observations.json).

Workspace library/binary Clippy, the optimized daemon build, format, diff, source/function size, new-allow, storage-write, URL, artifact, redaction, byte-slice, and secret checks pass.
No test suite, fixture harness, cancellation probe, or fault injection runs.
The complete schema–field–molecule–atom contract, dirty-write capacity, versioned schema reads, pack replacement, conversion, backup restore, and rollback remain open.

All four owned daemon runs close normally.
The stopped generated homes remain private for the required conversion and restore work in the same task.
They contain no primary data. Cleanup follows the last required conversion receipt.

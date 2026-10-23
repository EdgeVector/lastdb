# Durable writes after the merge review, October 22, 2026

The final ordinary single-client average is **17.00 ms**, below the 20 ms target.
Its P99 is **40.10 ms**.
The source review finds no remaining merge blocker in this change.
The required CI gate must pass on the final PR head before merge.

## Changes and integration

Delete and Purge now reserve memory after the notification stage.
The request boundary also checks the final memory quote before it uses the retained owner.
Insufficient reserved memory uses the existing disk-first path.

Native notices above 1,024 events no longer enter the separate background queue because of their count.
Bounded notification chunks share one private transaction and one tip owner.
The existing 32 MiB notice bound and native operation limits remain active.
A complete oversized stage refuses before acceptance.

The branch includes main through `bd517062e`, with the disabled cloud-backup feature.
It preserves main's exact-file compaction protections, conditional root publication, physical reads, and atom lifecycle work.
The old compaction prototype is removed.
The total performance change includes these main changes; the two review fixes do not explain the whole gain.

The publication branch starts from current main and retains the reviewed final tree.
The original local branch keeps the earlier history.
Five oversized historical proof blobs use their existing lossless gzip copies in the publication tree.
The artifact range gate passes without an override.

## Complete request latency

The baseline is the previously reviewed **durable** branch.
It is not main's memory-only acknowledgement.
Each client rotates through distinct schemas; the four clients use separate schema sets.

| Source and mode | Requests | Clients | Records per request | Average | P99 |
|---|---:|---:|---:|---:|---:|
| Reviewed durable branch | 1,000 | 1 | 1 | 26.44 ms | 82.71 ms |
| Initial integration, diagnostics off | 1,000 | 1 | 1 | 17.89 ms | 54.72 ms |
| Initial integration, diagnostics on | 1,000 | 1 | 1 | 19.39 ms | 51.68 ms |
| **Final integration, diagnostics off** | **1,000** | **1** | **1** | **17.00 ms** | **40.10 ms** |
| Final integration, diagnostics on | 1,000 | 1 | 1 | 22.13 ms | 51.12 ms |
| Reviewed branch, concurrent compaction attempt | 256 | 4 | 8 | 269.59 ms | 4,171.98 ms |
| Initial integration, concurrent compaction attempt | 256 | 4 | 8 | 115.59 ms | 359.49 ms |
| Final integration, concurrent compaction attempt | 256 | 4 | 8 | 127.22 ms | 500.64 ms |

The final ordinary average improves by 35.7% against the matched durable baseline.
The final concurrent batch average improves by 52.8%.
These are complete request times, without division by the record count.
The concurrent batch average does not meet 20 ms.
The diagnostic run also exceeds 20 ms; its timings must not replace the ordinary request result.

Each mode uses a new generated home, 64 schemas, 64-byte values, and eight socket workers.
Automatic maintenance stays on, with a zero start delay and a 4,096-part trigger.
The host lock excludes cooperative compiler work during every latency capture.
Other host work remains uncontrolled, and the captures run in sequence.
These results do not establish a fixed tail bound or production-load guarantee.

All explicit concurrent compaction attempts return a bounded retryable refusal during client writes.
Each retry after client writes completes.
Automatic compaction also starts during the single-client captures, but it does not complete before their final measured request.
The report does not claim a compaction publication during those measured intervals.

## Remaining time and locks

The final profiled single-client averages include:

| Step | Average |
|---|---:|
| Logical data, capacity, acceptance | 3.48 ms |
| Payload files | 0.78 ms |
| Prior recovery receipt | 0.21 ms |
| Lookup files | 3.39 ms |
| New recovery receipt | 0.68 ms |
| Root publication | 4.77 ms |
| Final disk drain | 6.71 ms |
| Memory publication | 0.08 ms |
| Queue, response, other work | 2.03 ms |

The final trace retains one full device flush per measured client commit.
It contains no separate background notification commit within the measured client intervals.
The final concurrent trace also contains zero such commits and zero admission overlap with them.
Its mean admission wait is still 93.40 ms because client commits share the writer and root boundary.
Compaction still requires a short conditional root boundary; filesystem work can also compete for disk capacity.

Unmatched expanded notices retain their existing fallback queue.
Erase author-clock work can also use a separate durable scope.
The result does not promise that every background operation disappears or that every request takes less than 20 ms.

## Disk and recovery evidence

All **5,768 measured requests** return durable receipts, with zero write errors.
Each of the eight modes checks its known values after SIGKILL and restart.
Each mode also checks author-clock advancement and its final clean disk flush.
All **11,144 measured notices**, plus the eight sentinels, survive exactly once.
The storage journal directory and tail remain absent.
The change adds no journal or disk format.

The final binary also accepts **1,025 distinct Hash keys** in one request.
Its 1,025 notices survive an immediate forced stop.
Its product data and notices use one native commit with 5,128 stored operations and one device flush.
Known first and last values survive.
The [large-commit receipt](large-commit-proof.json) retains the exact commit counts.

Two earlier repeated-key shapes reach existing read-work limits.
Their receipts remain under `boundaries/` and `boundaries-compact/`.
The first refusal leaves no data or notices after reopen.
No resource limit is disabled to obtain the accepted distinct-key result.

An early erase check omits the required empty field map and receives a grammar refusal.
That helper error remains in the receipts; it does not verify an erase path.
The corrected [erase check](final-erases/) creates two keys, deletes both, recreates both, then purges both.
After a forced stop at each erase boundary, both keys stay absent.
All eight mutation notices survive exactly once, and the final clean flush succeeds.
This correctness check records no latency and does not hold the compiler exclusion.

These are real daemon operations on generated data, not physical power-cut experiments.
No retired tests, fixtures, or test suite run.
The primary and its archive remain untouched.
Every owned node stops, and the owned temporary data is removed after log verification.

## Validation and next steps

The optimized final daemon build, strict core/daemon Clippy, full workspace Clippy, and both off-lane binary Clippy checks pass.
Format, source size, new allows, atom-writer, redaction, and artifact checks pass.
PR #278 also passes the required CI gate before the final receipt update.
The final receipt update must pass that same gate before merge.

The next performance work targets the concurrent client queue and slow file-metadata operations.
A longer sustained workload should measure compaction progress and disk-size control during continuous writes.
These tasks must retain the final device flush and the disk-before-ack rule.

Exact source and binary hashes remain in [final-identity.json](final-identity.json).
The [summary](summary.json), phase summaries, raw samples, and compressed logs retain all latency results.

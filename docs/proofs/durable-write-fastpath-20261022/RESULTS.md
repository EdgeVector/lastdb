# Durable client writes, October 22, 2026

The final single-client average falls from **40.61 ms to 25.64 ms**, a 36.9% reduction.
The **20 ms average target still fails**.
The final P99 stays near 85 ms and does not improve against this fresh baseline.
No code merges or pushes occur.

The change puts each ordinary native notification in its client's existing disk commit.
It removes the second notification commit that delayed the next client write.
It also reuses immutable directory pages and removes repeated work in the generation receipt.
The disk format and final disk flush stay the same.
The [design notes](DESIGN.md) describe the exact scope and the remaining costs.

## Complete request results

The baseline uses the previous journal-free durable branch, before these changes.
It does not use the old in-memory ack or the removed storage journal.

| Capture | Requests | Clients | Records per call | Average | Median | P99 | Maximum |
|---|---:|---:|---:|---:|---:|---:|---:|
| Before, diagnostics off | 1,000 | 1 | 1 | 40.61 ms | 38.45 ms | 84.73 ms | 428.32 ms |
| Initial change, diagnostics on | 1,000 | 1 | 1 | 27.69 ms | 24.73 ms | 87.07 ms | 391.55 ms |
| Initial change, diagnostics off | 1,000 | 1 | 1 | 25.37 ms | 24.16 ms | 55.28 ms | 165.62 ms |
| Final change, diagnostics off | 1,000 | 1 | 1 | **25.64 ms** | **23.96 ms** | **86.12 ms** | **517.72 ms** |
| Final change, schema aliases, diagnostics on | 1,000 | 1 | 1 | 27.06 ms | 25.04 ms | 86.96 ms | 120.68 ms |
| Before, concurrent compaction attempt | 256 | 4 | 8 | 170.43 ms | 124.55 ms | 873.10 ms | 942.17 ms |
| Initial change, concurrent compaction attempt | 256 | 4 | 8 | 172.84 ms | 144.00 ms | 548.48 ms | 853.38 ms |

The final common-path average repeats the earlier 25.37 ms control.
The P99 varies from 55.28 ms to 87.07 ms across the changed-code captures.
One lower P99 does not establish a stable single-client improvement.

The concurrent batch average does not improve.
Its P99 falls by 37.2%, and throughput rises from 172.92 to 181.45 records per second.
These times describe complete batch requests; they are not divided by eight.
The concurrent captures use the initial notification change, before the alias-match correction.
The final alias correction does not change their canonical-name request path's commit structure.

Each client rotates through its own schemas.
The four clients never write the same product schema.
Both explicit compaction attempts return the bounded eight-catch-up refusal during client writes.
Both compaction calls complete after the client writes end.
The report does not present that later success as a compaction publication during client writes.

## Background work and metadata

The fresh single-client baseline accepts **2,000 native write scopes** for 1,000 client calls.
The final change accepts **1,000**, all complete when the last client receipt returns.
The baseline still has its last notification commit incomplete at the measurement endpoint.

The final alias trace contains **zero separate background commits** during all 1,000 measured requests.
Its average native writer-admission wait is 0.000021 ms.
The earlier initial-change trace also contains zero background commits and zero overlap with the admission wait.
These traces confirm the result for the measured canonical and alias request paths.
The fallback paths in the design notes remain outside that claim.

The concurrent trace changes from 49 separate background commits to zero within the complete measured intervals.
Their average overlap with client admission falls from 17.96 ms to zero.
Average admission remains 126.69 ms because other client commits and the root handoff still share the writer.

The single-client recovery list starts with 720 dependency parts in both versions.
It ends with 13,974 before the change and 9,408 in the final capture.
This lower count follows the removal of the extra commits; it does not remove the existing flat recovery list.
Compaction still needs a quiet final boundary to replace that list.

## Remaining request cost

This budget describes the final alias trace, with diagnostics on.
The exclusive averages add to its complete 27.06 ms request average.
The P99 column describes one actual request, number 365; it does not add separate phase percentiles.

| Step | Average | Actual P99 request |
|---|---:|---:|
| Writer admission and part room | 0.00 ms | 0.00 ms |
| Logical records, capacity, acceptance | 6.06 ms | 6.62 ms |
| Payload files | 0.81 ms | 7.76 ms |
| Prior recovery receipt | 1.04 ms | 0.77 ms |
| Lookup files and dependencies | 5.10 ms | 41.34 ms |
| New recovery receipt | 2.09 ms | 14.36 ms |
| Root publication | 4.30 ms | 9.55 ms |
| Final disk drain | 5.61 ms | 4.80 ms |
| Memory publication | 0.09 ms | 0.11 ms |
| Queue, response, other work | 1.97 ms | 1.66 ms |
| **Complete request** | **27.06 ms** | **86.96 ms** |

The remaining P99 request spends most of its time in file and metadata work, with no writer-admission delay.
The measured phases do not isolate the cause of each slow filesystem operation.
They do not support a claim that the remaining tail comes from the notification lock.
Each final alias commit still contains one full device flush.
The change does not weaken that flush to reach the target.

## Method and disk proof

- Each capture uses a fresh generated home with 64 distinct schemas and distinct field identities.
- Each value contains 64 bytes. Each node uses eight socket workers.
- Automatic maintenance stays on, with a zero start delay and a 4,096-part threshold.
- The host lock excludes cooperative compiler jobs throughout each capture.
- Other host work remains uncontrolled. The captures run in sequence on this machine.
- The baseline binary has SHA-256 `b9506269d266b4b51c688a817c0dca293c83c696cf4e6b180cd2e38e7937408c`.
- The final binary has SHA-256 `18b61d57b703ecb0b676fcfd52aa1e2604bf87bba6b273e3e9e9587abdc70871`.
- The final product source is `abab443a97f6c4aedbe1eb469c21dbc16f519427`, with an empty product-source diff at capture.

All **5,512 measured requests** return successful durable receipts, with zero write errors.
Each mode also acknowledges a final sentinel, stops with SIGKILL, and checks its known data after restart.
Every mode recovers the expected latest values and advances its author clock on the next durable write.
Every mode completes its final clean shutdown and reports a successful disk flush.
Every mode confirms that the storage journal directory and tail file are absent.

All **6,048 measured notifications** from the changed binaries appear exactly once after the forced stops.
The five final sentinel notifications also survive.
The alias capture confirms the caller's original schema spelling, key, operation, and mutation identity.
This is a process-stop recovery check; it does not include a physical power-cut experiment.

The optimized build, strict Clippy, format, source-size, new-allow, redaction, and write-path source checks pass.
No tests, test suite, or test coverage gate runs.
The temporary nodes stop and the owned generated homes are removed after the proof receipts are saved.
The primary and its archive remain untouched.

The [machine summary](summary.json), [initial phase trace](trial1/phase-summary.json), and [final alias trace](aliases/phase-summary.json) retain the exact values.
The [concurrent summary](after-concurrent/concurrent-summary.json) retains all 256 native scope budgets.
The [cleanup receipt](cleanup-complete.json) records removal of the owned temporary root.
Older oversized proof files use lossless gzip copies so the existing artifact gate can pass.
Their [receipt](prior-proof-normalization.json) retains the original and compressed hashes.

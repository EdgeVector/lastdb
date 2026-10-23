# Independent atom lifecycle scale review

Date: 2026-10-22. Reviewer: the independent Codex scale agent.

The three actual product captures completed on fresh generated homes.
Every owned node stopped. Exact ownership checks preceded removal of all three temporary homes.
The primary, its archive, installed programs, and shared services remained unchanged.
No tests ran.

## Source and scope

The independent build uses frozen source `43ca120feb20ae6b95ab36dd3e2a088366522369`.
The daemon SHA-256 is `54b090091922cfe5c55c8a1f81a96ad3ffa6c8bc89a5afffb16a40158321e794`.
`build.json` records the command, toolchain, successful result, and executable hashes.

The review found an output-descriptor defect and a long-key space-quote defect.
The parent corrected those paths in `c1413d3d0`, `d5985afd0`, and `c11e6ad8c`.
`SOURCE_REVIEW.md` records the source analysis and the separate review of each correction.
Frozen tier times exclude the new descriptor closes and quote metadata reads.
They are not timing results for the final corrected binary.

The large capture also demonstrates a successful pack with a higher selected part count.
That result exposed a soft-limit retry defect in the post-copy policy.
The parent corrected that path in `bb7a4a7f5219265cb660ba8084b72fdbb2833554`.
The source review approves that revision, subject to its green CI and the parent's successful final product check.
`APPROVAL_ADDENDUM.md` records the completed corrected-binary check and final source/product approval.

## Workload

The small and large homes contain 4,096 and 100,000 user records.
Each record has four fields and a generated 1,024-byte body.
Seed calls contain at most 128 creates and request durable acknowledgement.
The first full-copy receipts count 49,198 and 1,200,046 native entries.
User records and native entries are different counts.

Each home receives a full copy before the adjacent writer phases.
The baseline writer runs for 20 seconds without an explicit copy.
The next writer runs while an explicit full compact request is in flight.
Each call contains 16 updates across a fixed set of 64 keys and requests durability.
The client pauses 10 ms between calls.
The later recent rounds each change another 64 keys.

The readback covers every changed user key and a fixed distributed sample of unchanged keys.
It does not read every unchanged user value.
The final product verifier covers the complete counter tally.
The workload contains no file blobs.

The standard host lock excludes other compliant heavy builds and benchmark windows.
Filesystem cache and other host activity remain uncontrolled.
The adjacent phases are not randomized or repeated matched trials.
They establish observed delays, not a speedup against main or a production latency bound.

## Observed write delays

All numbers below are milliseconds unless stated otherwise.
Completion gaps include the client pause.

| Home | Phase | Calls | Median | p95 | p99 | Maximum | Largest completion gap |
|---|---|---:|---:|---:|---:|---:|---:|
| 4,096 user records | Baseline | 329 | 41.11 | 70.87 | 186.75 | 302.50 | 318.07 |
| 4,096 user records | Concurrent full compact | 79 | 48.40 | 97.65 | 348.91 | 348.91 | 365.10 |
| 100,000 user records | Baseline | 260 | 57.66 | 102.16 | 146.79 | 194.22 | 206.23 |
| 100,000 user records | Concurrent full compact | 335 | 54.12 | 87.48 | 109.52 | 879.67 | 892.76 |

The compact API intervals lasted 5.400 and 24.217 seconds.
During those intervals, 73 and 329 durable batch acknowledgements completed.
Those intervals include copy, handoff, and reclaim.
The largest observed handoffs were 299 and 844 ms.
The full requests did not stop all writers for the duration of their copy.

The small concurrent pack caught 10,722 changed entries across nine rounds.
The large concurrent pack caught 34,039 changed entries across nine rounds.
These round counts include the final bounded handoff round.

## Copy cost and selected parts

The first large full copy took 63.195 seconds, then reclaimed files for 8.099 seconds.
It reduced selected parts from 5,296 to 107.
Measured physical bytes fell from 15,970,149,635 to 876,976,387.

The large concurrent full copy took 22.194 seconds, then reclaimed files for 1.899 seconds.
It copied 1,208,542 payload entries and 17,024 tombstones.
Selected parts increased from 1,279 to 1,540 as concurrent changes and their dependencies joined the replacement.
Physical bytes also increased during that active writer phase.
These receipts do not establish immediate part or byte reduction under active writes.

The second fixed recent round copied 645 payload entries on the small home and 646 on the large home.
The reported pack times were 69 and 152 ms.
The next large recent round copied 645 payload entries in 161 ms.
This work did not copy the full 1.2 million-entry base.
The first recent round also includes changes that arrived near the preceding handoff.

Recent page work can include old tombstones within rewritten leaves.
The small home reached its conservative changed-entry threshold and selected a full copy in the last round.
That full copy took 631 ms.

The observed node peaks were 195,493,888 bytes for the small run and 295,469,056 bytes for the large run.
Those peaks cover seed and every later phase.
They are measurements of these runs, not process-memory cap guarantees.

## Automatic worker

The separate `automatic` home uses the supported 128-part high-water setting and zero start delay.
These settings keep the ordinary trigger operation small.
The worker first refused a final delta above its bounded budget.
It retried after its back-off and completed a full copy.
The successful run reduced 269 selected parts to 12.
A later durable 64-key update succeeded; the final count was 24 parts and the writer remained healthy.

The complete verifier reported zero mismatches and a complete tally in all three runs.
The large run read 600,101 counters and 400,000 tips across 1,010 pages.
The small and automatic runs each read 24,582 counters and 16,384 tips.

The automatic run did not reach the real 85-percent soft limit.
It proves the ordinary trigger and retry operation, including one bounded catch-up refusal.
It does not prove recovery at the 85-percent threshold.
No file edits, smaller hard limit, or fault-only knob created that condition.

## Receipts and reproduction

`measure.py` is the actual product operation driver.
It uses the existing capture helper for exact home ownership, request transport, and graceful shutdown receipts.
Each run contains inputs, recipe, raw request hashes, full compact receipts, counter results, node output, and cleanup facts.
The `writer-samples.json` files retain individual request durations and completion times.
The cleanup receipts retain the realpath, UID, inode, marker, and stopped process facts.

Use a fresh mode-0700 path from `mktemp` under `TMPDIR` for every invocation.
Run the driver through `scripts/ci/with-fold-host-cargo-lock.sh`.
Select `--records 4096` or `--records 100000`, `--seed-batch 128`, and `--baseline-seconds 20`.
The automatic operation also uses `--automatic-worker`.
Pass the exact binary, frozen revision, output directory, and scratch path explicitly.
After a complete capture, use the existing `snapshot-commits-20261022/cleanup.py --run PATH` command.

The results establish no power-loss proof, configured disk-cap stress proof, or 85-percent recovery proof.

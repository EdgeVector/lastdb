# Minimal compaction locks: product evidence

The candidate removes the operation-wide maintenance mutex. Exact file guards protect each copy's output and reused inputs.
The shared write gate covers conditional publication, required durability barriers, and the matching read view.
The [design](../../lastdb-compaction-minimal-locks.md) gives the complete lifetime and retry rules.

## Captures

| Capture | Source | Result |
|---|---|---|
| `lifecycle-run1` | `6560d567382bbc8d9b5b22523e7217eb8816cbb8` | Complete; exact temporary home removed |
| `large-run1` | `b9e7434e48a5a4b752b5c94dde74af317deb9155` | Complete; exact temporary home removed |
| `lifecycle-integrated` | `b9e7434e48a5a4b752b5c94dde74af317deb9155` | Complete; exact temporary home removed |
| `automatic-integrated` | `b9e7434e48a5a4b752b5c94dde74af317deb9155` | Complete; exact temporary home removed |

Each capture names the executable hash, source, driver hashes, exact owned home, process IDs, requests, read results, and cleanup.
Every home starts empty with generated data. The primary and its archive remain untouched.
No tests run. A process interruption does not establish power-loss behavior.

## Lifecycle and unchanged-value overlap

The first capture includes durable writes during a copy, recent tiers, long keys, deletes, and complete counter checks.
It also includes graceful reopen, forced process exit, interrupted private output, data-only backup restore, TTL, and schema removal.
Exact reads and complete key continuations agree with all 4,090 accepted live records at the end.
Both complete counter checks report zero mismatches.

Two equal-frontier copies publish generations 159 and 160 with zero and one publication retries.
Each result selects 12 dependency files. A third copy receives `compaction_busy` with HTTP 503.
Sixteen cleanup calls overlap active copies. One removes 16 files with two jobs before and after the call.
The maximum recorded shared boundary is 20 ms in this capture.
This is an observation, not a latency guarantee. Filesystem durability can delay that boundary.

The resource sampler records 37 valid samples, a peak of 182,517,760 memory bytes, and at most 99 file handles.
The sampler cannot establish a strict memory or file-handle bound between samples.
This capture does not establish orphan removal during a copy or cleanup invalidation after partial file removal.
Its receipts retain `false` and zero for those observations.

## Large integrated home

The generated home contains 100,000 ordinary records and one long-key record.
The initial full copy processes 1,200,054 native entries. The user record count is 100,001.
Four complete readbacks agree with accepted values after interruption, both overlap phases, and final reopen.
Three complete counter checks each read 600,108 counters and bodies plus 400,004 tips.
Every tally is complete, with zero mismatches and zero suspect atoms.

| Window | Copies | Catch-up entries per copy | Publication retries | Selected dependency files |
|---|---|---|---|---|
| Concurrent writes | 21.134 / 21.220 seconds | 27,047 | 0 / 1 | 1,318 / 1,318 |
| Unchanged values | 11.972 / 12.005 seconds | 0 | 0 / 1 | 108 / 108 |

The finite writer completes 261 durable batches during its 20-second overlap window.
Of 220 cleanup calls, 218 complete without deferral while copies remain active.
Cleanup removes 2,155 files during active copies.
Retained overlap status reports show a maximum shared boundary of 11 ms.
The final reopen resets those measures; the final status does not retain the earlier maximum.

The worker log records three orphan files and 1,540,096 bytes removed at `02:18:18.926130Z`.
Adjacent status receipts bracket that event with two active jobs.
This observation establishes orphan removal during concurrent copies.
The driver's stronger all-post-reopen-path result stays `false`.
That collected set is not proof that every path is an orphan.
No capture observes cleanup invalidation after a partial removal; the original count stays zero.
The source review establishes the epoch checks. These operations do not establish every possible race order.

The resource sampler records 723 valid samples, a peak of 628,572,160 memory bytes, and at most 170 file handles.
These are observations on the shared host, not strict resource bounds.
The baseline phase includes 283 writer batches and two later worker-start batches.
The original `comparison.json` retains all 285 baseline-phase samples and 261 overlap samples.
It supports no paired speed claim. The driver now names the worker-start phase separately.

The large receipt stream exceeds the repository's 2 MiB blob cap.
`receipts.jsonl.gz` preserves its exact bytes; `receipt-archive.json` records both hashes and the equality check.
The raw stream was compressed only after supported ownership cleanup succeeded.

## Integrated lifecycle and automatic worker

The integrated lifecycle repeats the complete small-home operations with main's exact-key changes.
The automatic-worker capture uses a 128-file trigger and actual durable writes.
The worker completes a copy and leaves a healthy writer, zero maintenance failures, and zero worker restarts.
Its observed shared boundary is 7 ms. Both generated homes receive ownership-checked cleanup.

## Final capacity-error correction

A later source review found that sparse allocation retried every capacity refusal.
Eight path-budget refusals could therefore become a generic configuration error.
The correction retries only an exact active-deletion conflict.
It preserves the path-budget `compaction_behind` refusal and removes the exact new empty file before its local guard ends.
The correction changes no successful copy or publication path.
The records above retain their original source and driver hashes.

## Local checks

- Workspace and vendored Rust format checks pass for the reviewed source.
- Package and full-workspace clippy pass with warnings as errors at integrated source `b9e7434e`.
- The optimized daemon, recovery command, and maintenance command build for source `6560` and integrated source `b9e7434e`.
- File size, function size, and new-allow checks pass against main after integration.
- Two independent reviewers approve source `6560` and its unchanged integration at `b9e7434e48a5a4b752b5c94dde74af317deb9155`.

The final correction build, current-binary lifecycle and automatic operations, final reviewer reports, and current-head CI remain completion gates.

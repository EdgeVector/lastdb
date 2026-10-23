# Snapshot receipt part removal: disk growth and write latency

Date: 2026-10-22, Pacific time.

This comparison isolates receipt part removal from the already journal-free writer.
The baseline is `08d8436ccce9e2493e187a6b0133dc93f47c410f`.
The candidate product source is `a1941e3a8742d4da4743a02db99153119bdf034b`.
Later report commits do not change that measured product source.

The change works, but I do not recommend a merge in this form.
It reduces file growth while it increases batch write latency in all three pairs.
The remaining directory and locator work still dominates the short update workload.
PR 273 stays a draft; the primary stays unchanged.

## Measured costs

Each cell is the median of three per-run values.
Growth is KiB per logical update; latency is milliseconds per complete request.
A batch request contains 32 updates.

| Workload | Growth before → after | Request p50 before → after | Request p95 before → after |
|---|---:|---:|---:|
| Durable singles | 668.35 → 640.36 | 28.98 → 30.65 | 48.97 → 69.19 |
| Durable batches | 144.71 → 141.78 | 99.96 → 164.65 | 116.40 → 189.19 |
| Queued singles | 737.32 → 637.32 | 20.62 → 24.10 | 25.13 → 45.76 |
| Queued batches | 165.64 → 154.64 | 126.22 → 179.83 | 150.37 → 232.43 |
| Durable singles after compaction | 550.54 → 550.54 | 26.84 → 29.94 | 44.46 → 39.16 |

The median paired growth reductions are 4.09%, 2.22%, 13.56%, and 6.29% for the first four workloads.
The durable-batch p50 increases by 40.8% through 64.7% across the three pairs; the median paired increase is 54.9%.
These paired percentages differ from a ratio of the two column medians.
The post-compaction workload shows no consistent growth benefit.

| Durable-single cost | Before | After |
|---|---:|---:|
| Logical file growth per update | 668.35 KiB | 640.36 KiB |
| Allocated file growth per update | 674.63 KiB | 646.69 KiB |
| Process write bytes per update | 719.25 KiB | 691.63 KiB |
| New directory bytes per update | 151.75 KiB | 152.25 KiB |
| New locator bytes per update | 332.25 KiB | 332.25 KiB |
| New receipt bytes per update | 145.00 KiB | 118.25 KiB |
| New payload and map bytes per update | 38.35 KiB | 38.61 KiB |

Each family is a separate median, so its values need not sum to the median total.
The receipt reduction accounts for the short workload's benefit.
Directory and locator bytes remain essentially unchanged.
The later queued-single phase saves more receipt bytes: 262.63 → 162.25 KiB per update.
The [file-family receipt](file-family-costs.json) retains all per-run values and source hashes.

| Separate operation | Before | After |
|---|---:|---:|
| Compaction wall time | 2.59 s | 2.89 s |
| Reported pack time | 1.160 s | 1.489 s |
| Reported reclaim time | 1.448 s | 1.250 s |
| Focused worst durable write | 1.198 s | 1.314 s |
| Single-delete request p50 | 31.87 ms | 34.99 ms |
| One 129-delete batch request | 271.02 ms | 439.54 ms |

The focused worst write increases in every pair.
The 129-delete workload has one request per run; it does not supply a useful within-run percentile.
Warm reads remain responsive during compaction, but their maxima and percentiles vary.
These observations establish no shorter compaction write pause.

The matched and focused phases report peak process memory medians of 49.49 → 49.36 MiB.
All captured phases, including the later deletes, report 55.49 → 54.86 MiB.
The all-phase ranges are 47.89–56.27 MiB before and 49.36–61.28 MiB after.
These are observed process peaks at captured checkpoints, not a hard memory cap or a maximum-size workspace result.
The [independent numeric audit](final-analysis-audit.json) records both scopes and recomputes all 351 paired statistics.

## Change

The old publisher copies all prior dependency parts into each new receipt.
The candidate retains only parts with positive current reference counts.
A shared part remains until its last current reference goes.
Older receipts, read holds, backup references, and the concurrent deletion floor retain their existing protection.

The complete counts reside in the immutable receipt, before the atomic `CURRENT` publication.
The change adds no journal, mutable side file, or recovery replay.
It keeps the existing directory, locator, payload, block-map, generation, and root files.
It also keeps the 16 KiB search pages and their write cost.

Receipt removal alone does not remove a physical file.
The automatic worker now schedules ordinary reclaim after a 30-second idle interval, even below the compaction trigger.
Its orphan sweep also repeats for parts that no receipt names.
The sweep interval is 30 seconds through ten minutes, with a ten-minute default.
The configured file age, grace, retained-root rules, and generation floor remain deletion conditions.
Active work, holds, and backoff can delay either cleanup path.

## Method

Both variants use the same compiler, lockfile, release commands, and build target directory.
The accepted candidate reports `lastdbd 0.23.4-pre.0-274-ga1941e3a8`.
Its SHA-256 is `910a91836d709c9667d513c6410403fef524cfbe28597e030a8f056678d7fd0e`.
The [build checks](final-build-checks.json) verify ten node, CLI, and physical-command copies across both versions.

One standard host lock covers the final operational checks, six benchmark runs, and analysis.
Each run uses a fresh generated database with 512 records, four fields, and a 1 KiB body.
The order is before 1, after 1, after 2, before 2, before 3, and after 3.
Each process uses four socket workers, a 16 MiB thread stack, and the same cleared product environment.

Automatic maintenance is off in both measured variants.
The flush interval remains 500 ms.
This isolates foreground costs; it does not measure default automatic-maintenance performance.
The separate operational captures cover automatic cleanup.

| Phase | Offered work |
|---|---|
| Durable singles | 64 separate updates |
| Durable batches | 16 requests with 32 updates each |
| Queued singles | 256 separate updates, followed by an explicit flush |
| Queued batches | Eight requests with 32 updates each, followed by an explicit flush |
| Compaction | One compaction with a concurrent warm read and durable write |
| Post-compaction singles | 24 separate durable updates |
| Separate focused compaction | 64 warm reads and 64 durable writes at fixed nominal start times |
| Separate deletes | 64 single deletes, then one 129-delete request; 319 records remain |

Every phase checks all known keys and ordered range pages under one hash.
Each run checks graceful reopen.
The focused probes use sequential request streams; a delayed request can delay later starts.
Their recorded start delay remains separate from each request's latency.

The report uses three independent pairs.
Each reported p50 or p95 first comes from one run; the comparison uses the median across those three run statistics.
Requests from different runs are not pooled.
Internal native commit counts can vary with request speed and flush groups.
The workload includes that internal work and does not claim a fixed cost per native commit.

Logical growth measures file lengths; allocated growth measures filesystem allocation.
Process write counters are operating-system accounting, not physical SSD writes.
The filesystem cache is uncontrolled.
The host lock excludes cooperating benchmark and build jobs, not all host activity.
Three pairs do not establish statistical significance.

## Operational results

The [six-check manifest](operational-runs.json) completes on the exact candidate source.
These commands use five fresh generated homes; the older-program refusal shares the large-value home.

| Operation | Observed result |
|---|---|
| Retained snapshot and backup | Old dependency hashes remain intact during a retained-root reclaim. Release permits obsolete-part removal without compaction. Earlier backup restore returns the earlier values. |
| Crash reopen | A forced stop after an explicit flush preserves the expected keys and ranges after reopen. |
| Large values and shared parts | Sixteen random 768 KiB values cross an 8 MiB data-part boundary. A real crossing record and shared map/data parts remain readable through partial deletion. Final reference removal permits physical cleanup. |
| Older format | The older supported snapshot collects counts at writer open. Its first new write publishes the counted format. |
| Older program refusal | The baseline binary refuses both reads and writes of feature `512`; exact physical file hashes remain unchanged. |
| External keys | The actual locator includes parent and leaf pages with external key references. Deletes, replacements, reclaim, and reopen preserve the required shared overflow pages. |
| Automatic ordinary reclaim | Files that remain live after startup become obsolete after later writes. Periodic reclaim removes them below the default compaction trigger, without a pack or manual reclaim. |
| Repeated orphan sweep | Real native Put/Delete batches create two unused 768 KiB payloads. Startup removes the older payload and retains the younger payload below the age cutoff. The same process later removes the younger payload on the repeat sweep. |

The orphan capture advances the selected generation beyond the younger payload before startup.
Its initial retention therefore exercises the age cutoff, rather than the generation floor alone.
The payload predates startup; this capture does not claim a raw physical writer operates beside a live node.
The capture checks exact generated payload bytes, two automatic sweep log events, live keys, ranges, and reopen.
It uses no manual reclaim or compaction request.
Ordinary automatic cleanup completes 28.28 seconds after its update burst.
The repeated orphan capture removes its younger payload after 29.26 seconds of observation.
Both automatic operations use the default 4,096-part trigger, zero grace, and zero startup delay.
The orphan operation uses a ten-second file age; the product default remains ten minutes.
These times describe the offered operations, not a guaranteed cleanup deadline.

These operations do not establish power-loss behavior, every interruption edge, corruption recovery, cloud sync, or a primary result.
They do not reach payload map part one, the maximum home size, or the count-workspace ceiling.
See the [source review](SOURCE-REVIEW.md) for format, memory, and work limits.

## Source explanation

Exact counts add synchronous work before root publication.
Each changed stored key creates a fresh resolver for its old and new payload extents.
Related keys can therefore reread the same block-map pages and part headers, with their SHA-256 checks.
A delete still resolves the old payload extent.
The publisher also rereads fresh search pages and updates bounded count maps.

The old route already reads the previous receipt and writes a complete new receipt.
The additional count-map construction and count lookups add work to those existing steps.
Compaction adds a complete metadata and payload-map count pass after replacement output, before root publication.
That pass occurs within the compaction write pause.
The count path does not decode payload bodies.

These source paths explain plausible latency costs; they are not a measured profile attribution.
The periodic cleanup timers do not run in either matched variant because automatic maintenance is off.

The next performance step should reuse a bounded resolver cache across related keys within one batch.
It should preserve the current workspace bounds and complete receipt publication.
A profile and another matched comparison must establish its benefit before a merge recommendation.

## Checks and cleanup

Full workspace Clippy, format, URL lint, changed-file size, and changed-function size checks pass on the final product source.
All PR build and lint checks pass at `a1941e3a8`.
The [independent operational audit](independent-operational-audit.json) verifies the accepted binary and helper hashes before binary cleanup.

All eleven final generated homes and the ten earlier intermediate homes are removed after their owned nodes stop.
The earlier failed-capture home is also removed; its original incomplete summary and failure hashes remain unchanged.
The exact build-root cleanup removes all 21 owned binary copies.
The supported Git command removes the clean detached baseline source worktree.
The current DEV worktree and the review receipts remain.

Cleanup receipts: [build scratch](build-scratch-cleanup.json), [failed capture](failed-retention-operational-envelope-1/failed-scratch-cleanup.json), and [baseline source](baseline-worktree-cleanup.json).
Each successful capture also retains its own exact-path cleanup receipt.

## Earlier captures

The first candidate build reports the baseline version and fails the version guard before any node starts.
The [failed build receipt](failed-build-1.json) remains marked incomplete.
A package cache refresh produces a correctly stamped intermediate binary without a source change.

The next retention capture stops at a helper response-envelope error.
It establishes no product failure and supplies no benchmark sample.
Its [failure diagnosis](failed-retention-operational-envelope-1/failure-diagnosis.json) and original incomplete summary remain intact.

The complete `fa0695505` round passes five operational checks and three matched pairs.
A later source review finds the startup-only orphan sweep gap.
The [intermediate archive](intermediate-fa06955-archive.json) preserves its original file hashes, build receipts, and exact frozen helper bytes.
Its results remain separate from the final candidate results.

## Evidence

- [Measurement plan](METHOD.md) and [source review](SOURCE-REVIEW.md).
- [Final build checks](final-build-checks.json), [baseline build](before-build.json), and [candidate build](after-build.json).
- [Final review window](review-runs.json), [pair order](pair-runs.json), [numeric summary](summary.json), and [full text statistics](summary.txt).
- [Retained-root operation](retention-operational/summary.json), [large-value operation](edge-operational/summary.json), and [external-key operation](overflow-operational/summary.json).
- [Automatic reclaim](auto-reclaim-operational/summary.json) and [repeated orphan sweep](orphan-repeat-operational/summary.json).
- [Independent numeric audit](final-analysis-audit.json), [operational audit](independent-operational-audit.json), and [post-package analysis](post-packaging-analysis.json).

Exact compression round trips pass, and all fourteen original final-window summaries retain their hashes after the repeated analysis.
The packaged analysis reproduces every metric and the full statistics text.

The primary, its archive, cloud configuration, shared services, and deployment settings remain unchanged.
No tests or test coverage commands run.

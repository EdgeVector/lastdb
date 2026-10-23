# Receipt pruning and bounded cache: follow-up results

This report compares the original journal-free writer, the receipt-pruning draft, and the draft with one bounded cache per commit.
Receipt pruning and periodic reclaim reduce retained disk bytes by 86.0% in the default-maintenance comparison.
The bounded cache removes most of the draft's extra CPU cost, but batch and forced-compaction delays remain higher than the baseline.
Keep PR 273 as a draft for general release; retain the space reduction as useful work.
The primary, archive, cloud activation, and shared services stay unchanged.

## Scope

| Source | Meaning | Evidence in this round |
|---|---|---|
| `08d8436cc` | Original journal-free writer | Fresh release build, profile, three controlled runs, one default-maintenance run |
| `a1941e3a8` | Receipt pruning with a new resolver per changed key | Exact prior release executable, profile, one default-maintenance run |
| `e79b71151` | Receipt pruning with one bounded reference cache per commit | Fresh release build, profile, three controlled runs, six product operations, one default-maintenance run |

The [method](METHODS.md) defines the fixed workloads, build checks, limits, and cleanup procedure.
The earlier [draft comparison](../receipt-part-pruning-benchmark-20261022/RESULTS.md) remains a separate historical result.
Its three draft pairs are not new samples in this round.
The draft executable matches its original accepted hash.
The [baseline build](before-build.json) and [draft copy](draft-copy.json) receipts identify those executables.

An initial shared-target build linked old storage libraries behind a new `e79` version stamp.
The first product operation found the old receipt feature and stopped before any controlled or default-maintenance run.
That profile contributes no cache result.
The [rejection receipt](rejected-cache-build-1.json) and [byte archive](rejected-build-1-archive.json) preserve the failed attempt.
The replacement protocol uses an empty target directory for each source and verifies real node and tool writes before acceptance.

## What the profile establishes

Each diagnostic sends the same 256 durable requests with 32 logical updates per request.
The node processes 8,192 offered updates on a fresh 512-key home.
Automatic maintenance stays off.
The native sampler observes part of this work; the CPU counters cover the whole fixed workload and final flush.
The CPU counters cover only the node process and use the captured Mach timebase.
The profile timings are diagnostic and stay separate from the normal latency results.

| Diagnostic | Node CPU seconds | Fixed-work driver seconds | Requests that overlap the native sample |
|---|---:|---:|---:|
| Baseline | 30.14080 | 45.73880 | 123 |
| Draft | 38.73273 | 52.30384 | 109 |
| Cache | 31.12728 | 46.34776 | 121 |

The cache uses 19.6% less node CPU than the draft and 3.3% more than the fresh baseline.
Its fixed-work driver time is 1.3% above the fresh baseline.
Those diagnostic values do not replace the unprofiled paired latency comparison.
The draft sample shows 759 live-part context weights within 2,583 commit-context weights, or 29.4%.
The count path contains 425 terminal checksum weights and 248 terminal file-call weights.
Those are wall-stack weights, not CPU percentages.
The cache sample has 219 live-part context weights within 2,472 commit-context weights, or 8.9%; the baseline has none.
Payload-resolver weights fall from 574 to 16; the fresh-metadata pass stays near its prior size, at 182 and 196.
These are path-presence weights, not call counts or CPU attribution.
The [independent profile review](PROFILE-METHOD-REVIEW.md) and saved analyses preserve the nonduplicate attribution and exact raw sample hashes.

The source discards checked payload-map pages and part headers after each changed key in the draft.
The cache retains that checked immutable metadata within the same publication.
It retains no value bodies, adds no global cache, and adds no disk file.
The [source review](SOURCE-REVIEW.md) found no source blocker.
Its bounds are 26 pages, 25 checked part identities, and 32 file handles.
The page bytes total 416 KiB, plus administration; the cache leaves memory before final count and receipt allocations.

## Controlled results

Three fresh-home pairs compare the isolated baseline and cache executables with automatic maintenance off.
The workload has 512 keys, four fields, and a 1 KiB body.
Each offered update changes the title and body at one key.
Tables show medians across the three runs, without pooled requests.
The [complete numerical report](summary.txt) includes all run ranges and paired changes.
The [independent raw audit](matched-numeric-audit.json) verifies all 351 paired distributions.

| Metric | Baseline | Cache |
|---|---:|---:|
| Durable-single file growth per update | 670.86 KiB | 639.35 KiB |
| Queued-single file growth per update | 738.95 KiB | 638.19 KiB |
| Durable batch32 file growth per update | 144.62 KiB | 141.40 KiB |
| Durable-single process write bytes per update | 722.13 KiB | 690.25 KiB |
| Durable-single request p50 / p95 | 30.42 / 49.96 ms | 30.94 / 48.38 ms |
| Durable batch32 request p50 / p95 | 113.33 / 144.19 ms | 131.24 / 164.89 ms |
| Queued batch32 request p50 | 136.41 ms | 157.33 ms |
| One durable 129-delete request | 267.76 ms | 261.08 ms |
| Compaction wall time | 2.83 s | 3.03 s |
| Compaction pack / reclaim time | 1.267 / 1.519 s | 1.560 / 1.490 s |
| Focused largest write delay per run | 1.249 s | 1.488 s |

The durable-single growth reduction is about 4.7% from the table columns; the median paired reduction is 4.55%.
The queued-single reduction is about 13.6%.
Most saved bytes come from smaller receipts; directory and locator page costs remain similar.
The cache does not remove the large search-page write cost.

The durable batch32 p50 rises 15.8% from the run medians; the median paired increase is 16.46%.
Two pairs record slower batch medians; one records a faster median.
Batch p95 results are mixed, with one large regression; the after run range is 132.19 through 370.30 ms.
All three controlled compaction pairs record a longer pack and a larger focused maximum write delay.
Single-write results are mixed.
These results support a space tradeoff rather than a general speed improvement.

## Memory observations

| Scope: median of observed per-run peaks | Baseline | Cache |
|---|---:|---:|
| Matched workload plus focused compaction, kernel memory charge | 54.89 MiB | 53.05 MiB |
| All captured phases including deletes, kernel memory charge | 57.03 MiB | 55.22 MiB |
| All captured phases including deletes, sampled resident size | 158.36 MiB | 146.83 MiB |

The kernel charge uses `ri_lifetime_max_phys_footprint`; resident size is a different metric.
The full-phase kernel-charge ranges are 56.81–58.72 MiB before and 53.05–56.49 MiB after.
The median memory observations fall in both scopes; individual values vary.
These small homes fit the existing warm-data budget; the run does not establish behavior at its limit.
The 256 MiB warm budget, read admission, and protected dirty-record rules remain unchanged.

## Product checks

All six product operations pass on the final isolated cache executable.
They cover retained roots and reclaim, earlier backup restore, and reopen after a forced stop following flush.
They also cover shared large data and map parts, a record across a part boundary, and external leaf and parent keys.
Older-format count admission and older-program refusal pass without unintended file changes.
Ordinary automatic reclaim and repeated orphan cleanup pass their separate operational checks.
Those checks use their recorded short delay and age settings; they do not establish default-maintenance performance.
All owned operational nodes stop, and their homes leave disk after the receipts are saved.
The separate long runs retain the real default delay and age settings.

## Default automatic maintenance

Each variant receives the same 330 durable batch32 requests over 660 nominal seconds, followed by 60 seconds from the final ack.
The run keeps the default 30-second startup delay, 4,096-part initial trigger, zero grace, and ten-minute orphan age.
It sends no manual pack or reclaim request.
Periodic exact reads, ordered ranges, final stable idle inventories, and reopen establish the retained result.
Each variant has one run; these observations do not establish steady-state size or a latency distribution across runs.

All three variants complete every request with the expected data, ordered ranges, and a correct reopen.
The ordered input digest matches across all variants; each sends 10,560 offered updates.
Their actual active durations are 660.126, 660.181, and 660.174 seconds.
The [baseline](auto-before-numeric-audit.json), [draft](auto-draft-numeric-audit.json), and [cache](auto-after-numeric-audit.json) audits preserve exact values and hashes.

| One observed run per variant | Baseline | Draft | Cache |
|---|---:|---:|---:|
| Logical bytes after active writes | 1,253.27 MiB | 230.90 MiB | 230.90 MiB |
| Largest sampled logical inventory | 1,253.27 MiB | 233.84 MiB | 234.78 MiB |
| Logical bytes after quiet tail | 1,253.27 MiB | 173.02 MiB | 175.05 MiB |
| Allocated bytes after quiet tail | 1,254.51 MiB | 173.67 MiB | 175.69 MiB |
| Files after quiet tail | 4,474 | 3,038 | 3,046 |
| Request p50 / p95 | 127.46 / 173.58 ms | 186.94 / 246.33 ms | 137.18 / 175.72 ms |
| Largest request | 380.86 ms | 855.13 ms | 878.36 ms |
| Requests above 500 ms / one second | 0 / 0 | 5 / 0 | 2 / 0 |
| Maximum cadence delay | 14.82 ms | 12.55 ms | 12.69 ms |
| Completed automatic packs | 1 | 0 | 0 |
| Observed net logical growth per active second | 1.887 MiB/s | 0.338 MiB/s | 0.338 MiB/s |
| Process write-counter increase over sample span | 1,674.93 MiB | 1,660.98 MiB | 1,658.04 MiB |
| Captured lifetime peak kernel memory charge | 53.78 MiB | 51.53 MiB | 51.61 MiB |
| Largest sampled resident size | 205.56 MiB | 202.97 MiB | 230.03 MiB |

The cache retains 86.0% fewer logical bytes and 86.0% fewer allocated bytes than the baseline after the quiet tail.
Its request p50 rises 7.6%; p95 rises 1.2%.
Periodic reclaim reduces its logical inventory by 58,562,048 bytes during the quiet tail without a pack.
The baseline inventory stays unchanged during its tail.
The cache still retains about 2 MiB more than the draft at this finite endpoint.
That difference does not establish a long-term size difference between those variants.

| Final logical bytes by file family | Baseline | Cache |
|---|---:|---:|
| Directory | 281,280,512 | 4,554,752 |
| Locator | 864,419,840 | 123,076,608 |
| Generation receipts | 121,339,904 | 262,144 |
| Packs | 47,099,120 | 55,648,105 |

Pruning permits reclaim of obsolete file parts; the directory and locator files still supply searchable access.
This result changes the short-run space assessment: little immediate growth reduction can coexist with a large retained-space reduction after reclaim.
The baseline completes one automatic pack; no request overlaps its approximate pack interval.
The draft and cache complete no pack, so their requests above 500 ms cannot be assigned to compaction.
No orphan-removal log event appears in any run; this observation does not prove that a sweep never executes.
The counters report no maintenance failure, reclaim failure, or worker restart.
The warm bytes remain below the 256 MiB policy; these small homes do not exercise its limit.
The memory counters move in different directions: the kernel charge falls, while sampled resident size rises against the baseline.
The process write-counter increase falls about 1.0%; the 86.0% retained-space reduction is a different result.
Neither counter measures physical SSD writes.
The [size derivative](auto-retained-size-derived.json) records per-metric sample peaks, interval timestamps, and signed tail changes.
Samples can miss a larger peak, and the observed net rate describes this finite interval rather than steady-state growth or byte production.

## Other source findings

The [next-change review](NEXT-CHANGE-REVIEW.md) identifies a separate native publication for app notification metadata after a durable HTTP batch.
The author clock already reserves a range; a larger clock reservation cannot remove that app feed publication.
The observed whole-root commit counts fit this source path, but do not establish exact per-publication attribution.
The app feed, author clock, and sync mutation log have different purposes.
No app feed or clock code changes in this trial.

The [scale review](SCALE-REVIEW.md) separates physical directory coordinates from logical records and file parts.
Both sources already limit compaction to 65,536 directory coordinates and 67,108,864 locator records, with separate per-coordinate bounds.
The candidate adds an older-root count pass before writer admission, with a 67,108,864-step work ceiling.
That pass can refuse a valid older home that the prior writer can still serve.
The small-home evidence does not establish the support envelope for larger homes or all older snapshots.

## Recommendation

Keep PR 273 as a draft for a general release.
The default result supports receipt pruning as a space improvement; it does not support a general write-speed improvement.
The cache removes most repeated reference work, while slower batch medians and forced-compaction write delays remain.
The independent source review also identifies older-home admission limits that need a declared support envelope.

Use the existing durable batch API when independent updates are already available.
Add no wait to collect future updates and keep the existing durable acknowledgement.
This is the smallest immediate action because it needs no new storage contract.
The measured batch phases support that direction, but they do not isolate a causal effect from batch use alone.
All keys in a batch still wait for the complete request; amortized time is not per-key latency.

The next small engine trial should measure and reduce the separate app feed publication.
First identify actual publication owners; the current whole-root counters supply only a source-consistent explanation.
A bounded independent queue deadline is the smaller candidate; preserve its caps, order, shutdown drain, and subscriber delay contract.
Atomic feed and product publication needs a separate failure-boundary and lock-order review.

Before a general release, use a representative larger generated home with the intended keys, hashes, values, schemas, and update/delete history.
Verify older-root admission, actual compaction, exact reads, ordered ranges, reopen, and further durable writes afterward.
Record the supported geometry, memory, refusal limits, space reserve, and write pauses.
Use fresh owned data; the primary and archive stay outside this work.

## Validation and evidence preservation

The frozen product source passes full-workspace Clippy, format, source-size, and new-allow checks.
The isolated release builds, source-path acceptance, and real writer format gates pass for both accepted variants.
No test suite or coverage command runs.
Later changes contain documentation and receipts only.

Packaging preserves 24 original summary files byte for byte and checks each compression round trip.
The [numeric reproduction](post-packaging-analysis.json) reproduces all 351 controlled distributions and the exact summary text.
The [profile reproduction](post-package-profile-audit.json) reproduces all three canonical analyses from their committed gzip samples.
Historical and rejected captures remain excluded from accepted results.
Full response bodies remain in ignored local compressed receipts; their exact raw checks occur before packaging.
The committed compact evidence reproduces the numeric report in this recorded checkout.
A fresh checkout cannot repeat every full-response check without those excluded body receipts; some original helper keys also contain absolute paths.

All owned generated nodes stop and their homes leave disk.
The [build cleanup](build-scratch-cleanup.json) removes 25 copied binaries, both isolated targets, and their owned temporary roots.
The [finalization receipt](finalization-window.json) preserves the failed-home cleanup and package order.
The rejected original summary remains false; its status is never rewritten as success.
The three clean source-only worktrees also leave disk through the supported Git command; their [cleanup receipt](source-worktree-cleanup.json) records identity and result.
The primary, archive, cloud activation, shared services, CI policy, and deployment settings stay unchanged.

## Measurement limits

The controlled result uses three fresh-home pairs and does not establish statistical significance.
The default-maintenance result uses one finite run per variant and does not establish steady-state size.
The active interval is nominally 660 seconds, followed by 60 seconds from the actual final durable ack.
The driver retains every late request.
Native inventories during writes have interval bounds and can observe concurrent file removal.

The pack log follows pack and reclaim; its duration covers only the pack call.
Any reconstructed overlap window is approximate and does not identify the exact internal write-lock interval.
The exact request intervals establish request latency independently of that attribution.
Process write counters do not measure physical SSD writes.
Process memory observations do not establish a hard memory cap.
Forced process stops do not establish power-loss behavior.

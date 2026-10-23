# Journal review and repeat measurements — October 22, 2026

The source review removes the remaining erase recovery records and journal-based cloud repair.
The searchable snapshot owns native local recovery.
The sync mutation log retains its sync role.
The [source receipt](SOURCE-REVIEW.md) lists the removed paths and the retained boundaries.

The repeat confirms lower disk growth and process write bytes in every measured update phase.
Single writes complete sooner in all three pairs.
Batch latency and compaction pauses show mixed results.
The first report's broad latency gains do not repeat in this set.

## Exact comparison

The baseline is `6d8aa0d573e91a94fd4fd308a7f0eacca022e852`, before native journal removal.
The final candidate is `5919abbfbf2263993ea99700b73bd9c8d43d689b`.
The [before build](before-build.json), [after build](after-build.json), and [final build checks](final-build-checks.json) identify their binaries.
Both use the same compiler, locked dependencies, default features, and release profile.
Both include the disk-barrier and concurrent-reclaim changes from PRs 266 and 267.

This comparison measures the complete change from the original journal-backed writer.
It does not isolate the extra erase-meter and cloud-repair removal from PR 268.
PR 268 merged as `4118c7baa`; PR 271 contains this follow-up review.

Each fresh home contains 512 generated HashRange records, four fields, and 1 KiB bodies.
The original six measured phases retain their inputs, cost boundaries, and state checks.
The order is before-1, after-1, after-2, before-2, before-3, after-3.
One owned node runs at a time under the standard host Cargo lock.
Automatic maintenance stays off; the flush interval stays 500 ms.
Operating system caches and other host activity remain uncontrolled.

A separate compaction phase follows the original sequence and its graceful reopen.
It sends 64 warm reads and 64 durable writes, with nominal starts at 20 ms plus 50 ms per ordinal.
Each role uses one sequential request stream.
A blocked request delays later starts; the receipts retain that delay and the actual request intervals.
Overlap means overlap with the compaction request, without a claim about the internal write gate.

The final separate phase sends 64 durable single deletes, then one durable request with 129 deletes.
It checks all 512 known keys, deleted absence, ordered range pages, and a graceful reopen with 319 survivors.
The delete input records the existing value bytes separately from the empty delete payload.

## Write time and disk growth

Each value below is the median of three run-level results.
The [full summary](summary.json) retains all values, ranges, and matched pair changes.
The median of pair changes can differ from the change between two medians.

| Workload | Before request p50 | After request p50 | Before request p95 | After request p95 |
|---|---:|---:|---:|---:|
| Durable single | 86.11 ms | 48.91 ms | 167.13 ms | 88.55 ms |
| Durable batch of 32 | 195.21 ms | 197.04 ms | 399.26 ms | 305.19 ms |
| Queued single | 50.02 ms | 34.33 ms | 95.93 ms | 68.94 ms |
| Queued batch of 32 | 233.70 ms | 193.65 ms | 354.42 ms | 293.35 ms |
| Durable single after compaction | 92.07 ms | 46.15 ms | 133.47 ms | 82.25 ms |

Durable-single p50 improves in all three pairs, with reductions from 3.8% to 65.7%.
Durable-batch p50 worsens in two pairs and improves in one.
Queued-batch p50 also worsens in two pairs, despite its lower median across runs.
These observations support no general batch-latency improvement claim.
Queued acks retain the memory contract and can wait for author-clock metadata.

The next values describe one logical update, including its related internal work.
Growth means new or expanded file bytes before compaction.
Net growth remains a separate metric in the summary.
All sizes use KiB, or 1,024 bytes.

| Workload | Logical growth before → after | Allocated growth before → after | Process write bytes before → after |
|---|---:|---:|---:|
| Durable single | 836.1 → 671.6 KiB | 841.0 → 678.0 KiB | 912.0 → 723.3 KiB |
| Durable batch of 32, per record | 155.6 → 145.2 KiB | 155.8 → 145.3 KiB | 159.4 → 148.0 KiB |
| Queued single | 1,028.6 → 745.1 KiB | 1,029.8 → 747.7 KiB | 1,067.6 → 772.2 KiB |
| Queued batch of 32, per record | 196.5 → 167.9 KiB | 196.7 → 168.0 KiB | 200.3 → 170.8 KiB |

Process write bytes fall in all three pairs for all four update phases.
The median paired reductions are 20.7%, 7.1%, 27.5%, and 14.6%, in the order above.
Each frontier advance uses two generation publications before and one after.
Native queue coalescence stays close to its previous level; frontier counts still differ slightly.

Directory, locator, and generation files retain 96.5% of new file bytes across the candidate's four update phases and three runs.
That percentage uses the combined byte totals, not a median of percentages.
The remaining metadata cost is large relative to each 1 KiB body.

## Compaction, deletes, and memory

| Observation | Before median | After median |
|---|---:|---:|
| Original compaction request | 4.565 s | 4.522 s |
| Original pack phase | 1.868 s | 2.133 s |
| Original file removal phase | 2.612 s | 2.361 s |
| Original concurrent durable write | 1.953 s | 2.202 s |
| Focused phase: worst durable write per run | 2.185 s | 2.234 s |
| Focused phase: warm-read p50 | 0.411 ms | 0.402 ms |
| Sequence of 64 durable deletes, through final flush | 5.194 s | 3.672 s |
| One durable request with 129 deletes | 492.41 ms | 397.47 ms |
| Observed process memory peak | 52.8 MiB | 53.3 MiB |

Total compaction time improves in one pair and worsens in two.
File removal completes sooner in all three pairs, but the complete request does not show a consistent gain.
The focused phase records four overlapping writes per baseline run and three per candidate run.
Their worst delays range from 1.517–2.726 seconds before and 1.971–2.740 seconds after.
This set establishes no compaction write-pause improvement.
The small overlap counts do not support a tail-latency claim.
Across all focused read requests, p95 stays between 0.438–1.007 ms before and 0.498–0.766 ms after.

The 64-delete sequence completes sooner in all three pairs.
Its process writes fall from 1,176.9 to 951.8 KiB per deleted key.
The 129-delete request improves in two pairs and worsens in one.
Its process writes fall from 87.2 to 82.3 KiB per deleted key.
The summary retains existing source-value bytes separately from each empty delete payload.
One batch request per run does not establish a batch p95.

The memory values are lifetime peaks observed at the original sequence's checkpoints, including seed and compaction.
They do not include final shutdown or prove a peak for every later extra phase.
The byte limits require separate evidence at a larger workload.

## Verification and reproducibility

All six runs verify complete exact-key values and ordered range results after each original cost boundary.
All six verify the original, focused, and final erase states after graceful reopen.
The extra erase phases verify 448 survivors, then 319 survivors, plus absence for every deleted key.
All owned nodes stop and all six owned database homes leave disk.
The [run manifest](pair-runs.json) links the exact cleanup receipts.

An independent receipt review reconstructs all mutation inputs and checks every saved raw query response.
It recomputes all 351 paired distributions from the three independent pairs.
The fourteen original and round-two helpers stay unchanged throughout all six runs.
The final [analysis driver](analyze.py) also retains the original build commands verbatim.
Its [command receipt](build-command-equivalence.json) permits only the known host wrapper and extra CLI build distinction.
It changes no build receipt or measurement.

The [evidence index](evidence-index.json) links compact request times, response summaries, and exact payload hashes.
Full generated payload captures remain local under the repository evidence policy.
The [compression receipt](compression.json) confirms exact decompression of the original receipts and file inventories.
The [extra compression receipt](extra-compression.json) confirms exact focused and erase inventories.
The analysis driver reproduces the same numerical results from the committed compact evidence.

## Erase recovery and removed repair routes

The separate [final erase capture](erase-operational-3/summary.json) uses the final node and CLI binaries.
The [physical reader receipt](disk-build.json) identifies its separate build.
Its [source equivalence receipt](disk-source-equivalence.json) confirms identical final vendor source bytes.

The capture creates 256 generated records in each of two schemas with shared fields.
Each erase request supplies both known key layouts explicitly in one batch.
The first request removes 129 keys per schema; the second removes the remaining 127.
Record counts follow 256 → 127 → 0 per schema.
Molecule slots follow 1,024 → 508 → 0 per schema.
Unique atom counts follow 769 → 382 → 0.

After the first erase and explicit flush, the capture forces a process stop.
On reopen, the owner meter APIs return identical numeric values before any value fetch.
The meter trust state correctly reports `stale_after_unclean_stop`.
Exact native reads find the encrypted aggregate header and schema slices, without the retired aggregate recovery key.
The inventory contains no native recovery journal file.
The capture does not decrypt identity keys or meter bodies.
The version-2 encoding claim remains a source fact; the ordinary APIs verify the restored values.

The removed HTTP route returns status 404 with a nine-byte plain-text body.
Both removed CLI commands exit with status 2 and an unrecognized-subcommand error.
The node refuses a generated retired repair marker.
The [refusal receipt](erase-operational-3/retired-state-refusal.json) compares 417 protected metadata entries before and after that refusal.
The selected snapshot, retired repair tree, and identity file metadata stay unchanged.
Ordinary diagnostics remain outside that comparison.
The capture then verifies a final empty-state reopen and removes its [owned scratch](erase-operational-3/cleanup.json).

Two earlier captures failed because of helper assumptions, not established product defects.
The [first](erase-operational/summary.json) expected a Hash peer delete without its explicit key.
The [second](erase-operational-2/summary.json) expected JSON from the removed route.
Their failed summaries remain unchanged, and separate cleanup-only receipts remove their stopped owned homes.
The [operational evidence index](operational-evidence-index.json) retains exact payload hashes and compact receipts for all three attempts.

## Recommendation

Keep the journal removal for its simpler local recovery and consistent reduction in disk writes.
Treat batch latency and compaction write delay as unresolved.
Next, reduce the changed directory, locator, and generation bytes in one small change, then repeat this workload.
Do not start a broader storage rewrite from these three pairs alone.

## Scope and limits

Three pairs provide three run-level observations.
Requests from different runs are not pooled.
Process write counters do not measure physical SSD writes.
The small generated workload does not exercise the 256 MiB warm budget or the new erase refusal limits.
This report establishes no power-loss, primary, cloud, or default automatic-maintenance result.

The native journal writer, native replay, erase recovery records, and journal-based cloud repair are removed.
Final delete audit facts and authenticated incoming sync outcomes remain; neither drives local recovery.
Legacy HashGroup restore, maintenance, and general LastStore APIs can still write authoritative `.seg` data files.
Old native journal homes and interrupted cloud-repair state receive an explicit refusal.
This change includes no old-native converter and does not enable native cloud sync.

The primary, its archive, installed services, and shared configuration stay unchanged.

# Journal removal: before and after, October 22, 2026

Journal removal improves write cost in this controlled workload.
Every matched write phase completes sooner and records fewer process write bytes in all three pairs.
The change removes a second root publication per stored commit.
The compaction write delay remains about one second in five of the six probes.

## Comparison

Before: main `6d8aa0d573e91a94fd4fd308a7f0eacca022e852`, with the internal native journal.
After: `3b53ece33324f75a0cb81b668964b3e782640838`, with direct snapshot commits in PR 268.
Both sources include the earlier disk-barrier and concurrent-reclaim fixes from PRs 266 and 267.
Both binaries use the same compiler, dependencies, default features, and locked release build.
The [before build](before-build.json) and [after build](after-build.json) record exact executable hashes and version labels.

The comparison uses three fresh-home pairs on one Mac16,5 with 36 GiB memory and macOS 26.5.2.
Run order is before/after, after/before, before/after, with one node at a time.
Each home contains 512 generated HashRange records, four fields, and 1 KiB bodies.
Updates change the title and body. All pairs use identical ordered input bytes.
Automatic maintenance is off; the flush interval stays 500 ms; the node uses four socket workers.
This is a controlled comparison, rather than a default automatic-maintenance workload.

Each run seeds 16 durable batches of 32 records.
It then measures 64 durable singles, 16 durable batches of 32, 256 queued singles, and eight queued batches of 32.
One explicit compaction follows, then 24 durable singles.
Queued phases have no flush between requests. One final flush establishes their complete durable boundary.
The host Cargo lock excludes cooperative builds throughout all six runs.
Operating system caches and other host work remain uncontrolled.

## Write time

Each number below is the median of three run-level results.
Request p50 and p95 describe calls within each run; the calls are not independent repeats.
The full [summary](summary.json) contains all three values, their ranges, and paired changes.

| Workload | Before request p50 | After request p50 | Before request p95 | After request p95 |
|---|---:|---:|---:|---:|
| Durable single, before compaction | 47.86 ms | 29.31 ms | 53.58 ms | 31.25 ms |
| Durable batch of 32 | 124.41 ms | 97.44 ms | 144.62 ms | 121.64 ms |
| Queued single | 29.76 ms | 20.01 ms | 38.77 ms | 23.05 ms |
| Queued batch of 32 | 171.49 ms | 106.00 ms | 196.16 ms | 112.66 ms |
| Durable single, after compaction | 58.79 ms | 28.92 ms | 83.14 ms | 36.56 ms |

The paired reduction in durable-single request p50 ranges from 36.3% to 61.0%.
The corresponding batch-of-32 reduction ranges from 14.0% to 32.5%.
These ranges reflect the three observed pairs, not a confidence interval.

Complete-phase time includes the request sequence and its final durable flush.
The median paired reduction is 44.7% for durable singles and 20.1% for durable batches of 32.
It is 34.2% for queued singles and 37.8% for queued batches of 32.
A queued ack still has the existing memory contract; it does not establish disk durability.
Author-clock metadata can still delay that ack.

## Disk growth and process writes

These numbers describe one logical update with a 1 KiB body plus its title.
They include the engine's related metadata and internal writes.
Growth means new or expanded file bytes before compaction; net growth remains a separate value in the summary.
Darwin process counters measure process I/O. They do not measure physical SSD writes.
All size values below use KiB, or 1,024 bytes.

| Workload | Logical growth before → after | Allocated growth before → after | Process writes before → after |
|---|---:|---:|---:|
| Durable single | 826.6 → 665.8 KiB | 831.4 → 672.1 KiB | 900.9 → 716.8 KiB |
| Durable batch of 32, per record | 154.4 → 144.3 KiB | 154.6 → 144.5 KiB | 158.1 → 147.0 KiB |
| Queued single | 1,013.7 → 739.4 KiB | 1,014.8 → 742.0 KiB | 1,052.1 → 766.5 KiB |
| Queued batch of 32, per record | 195.8 → 166.0 KiB | 196.0 → 166.1 KiB | 199.6 → 168.8 KiB |

The median paired reduction in process write bytes is 20.4% for durable singles and 7.1% for durable batches of 32.
It is 27.4% for queued singles and 15.4% for queued batches of 32.
Directory, locator, and generation metadata still account for 96.4% of new update-phase file bytes after the change.
The remaining write cost is large relative to the value bytes.

Each write phase records two generations per frontier advance before, versus one after.
Accepted native requests per commit remain 1.0 in most phases and about 1.96 in queued singles for both versions.
The large gains therefore do not coincide with a large change in queue coalescence.
Some frontier counts differ slightly; the summary retains those counts and per-commit costs.

## Compaction and memory

| Observation | Before median | After median |
|---|---:|---:|
| Complete compaction request | 2.921 s | 2.219 s |
| Pack phase | 1.102 s | 1.002 s |
| Reclaim phase | 1.752 s | 1.186 s |
| Concurrent durable write probe | 1.099 s | 1.028 s |
| Concurrent warm read probe | 0.685 ms | 0.654 ms |
| Observed process memory peak | 59.7 MiB | 58.1 MiB |

The median paired compaction reduction is 24.8%, with a range of 21.1% to 28.0%.
Reclaim contributes most of that reduction; its paired median falls 33.3%.
The pack phase falls 9.4%.

The scheduled probe delay is 20 ms. Actual requests start 23.7–30.4 ms after compaction dispatch.
Their request intervals overlap the compaction request in all six runs.
That overlap does not prove the same internal lock phase.
One baseline write returns in 84 ms; the other five write probes take about one second.
The write-probe changes are mixed across pairs, so these receipts establish no consistent write-pause improvement.
All six warm-read probes remain below one millisecond.
Memory peaks include seed and compaction; this small workload does not exercise the warm budget.

## Correctness, evidence, and limits

Every measured phase verifies all 512 exact keys and complete ordered range results after its cost boundary.
Every run repeats that verification after a graceful reopen.
All requests succeed, all durable boundaries drain, and every owned node stops.
Each [run receipt](pair-runs.json) links its exact owned-home cleanup.
The temporary binaries and clean baseline worktree also leave disk after capture.
The primary, archive, shared services, and CI configuration stay unchanged.

The full generated payload captures remain local under the existing repository evidence policy.
The PR includes compact request timings, response summaries, exact payload hashes, and compressed file inventories.
The [evidence index](evidence-index.json) identifies both compact artifacts and full local captures.
The [compression receipt](compression.json) verifies the original bytes after decompression.
The [summarizer](summarize.py) reproduces all numerical results from the committed compact artifacts.

Three pairs do not establish significance or behavior at a larger database size.
This comparison establishes no cloud, power-loss, primary, or automatic-maintenance result.
Old journal-backed homes still require a separate explicit conversion; this PR includes no converter.

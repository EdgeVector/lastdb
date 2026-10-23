# Compaction benchmark: PR #275

The report compares three matched pairs on fresh generated homes with 100,000 records each.
The baseline is c6b4f7ed9. The candidate is c94ec7760.
Values below are medians of the three per-run measures. Brackets show the per-run range.

| Measure | Before | After | Change |
|---|---:|---:|---:|
| Durable seed of 100,000 records | 197.87 s [195.39, 284.93] | 196.72 s [189.03, 200.19] | -0.6% |
| Quiet full compaction | 69530.84 ms [67873.10, 112094.73] | 69184.85 ms [66988.74, 71171.26] | -0.5% |
| Normal write p95 without compaction | 106.25 ms [99.05, 119.50] | 106.87 ms [105.12, 121.66] | +0.6% |
| Successful copy beside normal writes | 25331.32 ms [24011.61, 33158.25] | 23930.90 ms [23735.94, 25493.21] | -5.5% |
| Cleanup response beside normal writes and a copy | 25289.64 ms [23961.70, 33103.64] | 120.35 ms [104.06, 166.30] | -99.5% |
| Normal write p95 during compaction | 105.65 ms [98.73, 132.88] | 114.05 ms [109.50, 118.22] | +8.0% |
| Normal write p99 during compaction | 154.22 ms [146.08, 159.01] | 162.94 ms [141.31, 169.23] | +5.7% |
| Normal client delay plus RPC p95 (all 400 writes) | 3693.60 ms [2527.46, 10184.00] | 2811.43 ms [2195.40, 3824.39] | -23.9% |
| Worst normal write during compaction | 168.19 ms [163.75, 174.79] | 171.54 ms [159.30, 185.28] | +2.0% |
| Successful copy beside durable writes | 18742.50 ms [14528.50, 18781.51] | 20556.06 ms [20458.51, 20965.38] | +9.7% |
| Durable write p95 without compaction | 121.66 ms [92.68, 131.64] | 121.24 ms [107.42, 133.45] | -0.3% |
| Cleanup response beside durable writes and a copy | 18671.35 ms [14548.69, 18805.82] | 104.73 ms [69.37, 118.75] | -99.4% |
| Durable write p95 during compaction | 120.44 ms [85.29, 124.54] | 126.93 ms [125.44, 130.27] | +5.4% |
| Durable client delay plus RPC p95 (all 200 writes) | 315.01 ms [147.33, 448.85] | 161.95 ms [142.01, 164.49] | -48.6% |
| Worst durable write during compaction | 559.28 ms [367.90, 853.97] | 170.51 ms [169.54, 186.50] | -69.5% |
| Two simultaneous compaction callers | 21273.47 ms [17512.15, 22234.86] | 12531.97 ms [11525.22, 14117.20] | -41.1% |
| Sampled memory size during two copies | 297.22 MiB [266.50, 299.12] | 294.78 MiB [280.34, 303.20] | -0.8% |
| Sampled file handles during two copies | 85.00 handles [85.00, 85.00] | 153.00 handles [153.00, 153.00] | +80.0% |

Capacity refusals: {'before': 0, 'after': 0}.

The new version completed 6 of six cleanup calls before its concurrent copy response.
Those six calls removed 706 files in total.
The largest reported successful publication hold across the new runs was 20 milliseconds.
That timer uses whole milliseconds and excludes unsuccessful publication attempts.

Every run checks all known record values and completes the atom-counter diagnostic with zero mismatches.
Each owned node stops, and the supported cleanup command removes its exact generated home.

The baseline and candidate publication timers have different scope. Their values are retained in per-run.csv without a speed ratio.
Memory and file-handle samples do not establish strict peaks between samples.
The workload has a fixed offered rate. It measures response latency, not maximum write throughput.
Cleanup figures measure response latency. Per-run.csv also retains removed-file counts and each deferral flag.
RPC percentiles exclude delay before dispatch. Separate percentiles retain the sum of client dispatch delay and RPC duration.
That sum excludes client payload construction and receipt bookkeeping; it is not an exact scheduled-arrival response timer.
The fixed write window can end before publication. It does not force write contention at every publication boundary.
Three local pairs on a shared host establish no general latency guarantee.
See METHODS.md, builds.json, comparison.json, and the archived request receipts for complete provenance.

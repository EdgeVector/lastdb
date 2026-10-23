# Exact-prefix read simplification: before/after benchmark

Recommendation: keep as a maintainability improvement. The change removes an
obsolete multi-source abstraction; these measurements do not establish a broad
performance improvement. Source review found no blocking correctness issue.

Implementation: **157 Rust lines removed, 82 added; net 75 removed across six files.**
The source-priority map, decoded-slot deduplication maps, and union loops are gone.
Both prefix and range preload collectors now operate on one cursor. Generation
overlays, Delete barriers, captured changes, and lease checks remain.

## Method

Baseline `c6b4f7ed9ac5a24169e3f41a66579fdc5ba80de7` (merged PR #269); candidate
`155d9192f9b1d682a979343138933671699c5265`. Both optimized binaries were built sequentially in
`/workspace/lastdb`, with the same Rust 1.96.1 toolchain, lockfile, target directory,
and flags. Binary hashes are in provenance.json and per-run inputs.json.
No compilation overlapped any timed run; the host Cargo/benchmark lock is held
for the entire matrix. The baseline was rebuilt because main had acquired changes
since the preceding exact-key benchmark. Do not compare absolute timings against
that older report.

Four adjacent pairs per configuration, alternating after/before then before/after:
**24 runs, 49,344 timed requests**, all successful. Fresh synthetic homes,
512 Hash and 512 HashRange records, four fields, deterministic 1 KiB body; one
synchronous HTTP-over-UDS client, four daemon workers. Configurations are plain,
blind_v1/ope_v1, and the same encrypted encoding with the range schema preloaded.
Automatic maintenance is off; persistence interval is 100 ms. Inputs retain exact
settings, platform, binary hashes, workload dimensions, and free space.

The previous benchmark driver is reused, adding 100-record prefix and 512-record
single-partition reads, plus an untimed prefix visibility check after Delete/Purge.
Integrity checks verify values, counts, ordering, duplicate/missing multi-get,
Delete/Purge visibility, flush, and representative keys after graceful reopen.
All owned daemons stopped; all synthetic homes were removed using ownership and
process checks. Cleanup receipts are retained. No user data was used.

Each table value is the median of four per-run p50s or p95s. Delta compares the
reported p50 medians; negative is faster. comparison.json also retains all paired
deltas and per-run medians. Request timing covers dispatch through full response
read, excluding JSON encode/decode and value checking. Write throughput includes
client work and the phase's final flush. A write32 sample is one 32-record request.

## Assessment

The useful signal is modest: encrypted range64 p50 improves 1.2% (all four pairs
improve); preloaded prefix100 improves 2.1% (all four pairs improve). Full 512-record
partition reads change -0.7% encrypted, +0.5% preloaded, and +0.9% plain. There is
no demonstrated large collection or write speedup. Default encrypted durable
write32 medians are 1.3% slower for Hash and 2.4% slower for HashRange; queued
single-write medians improve 5.5% and 1.1% respectively.

Submillisecond point reads are mixed: default encrypted warm Hash p50 is 10.3%
slower and HashRange is 7.3% slower, while plain and preloaded results improve.
Per-pair variation is large, including large changes in untouched point paths;
these results should not be attributed confidently to the prefix cleanup.
No statistical significance is claimed. The recommendation rests on clearer
single-source code with preserved semantics, not an across-the-board speed claim.

Limits: one host, one client, small dataset, uncontrolled filesystem caches and
host neighbors. Reopen-first-pass is not cold-disk latency. Batch p95 from 12
samples is coarse. This does not exercise concurrent writers, active generation
replacement, CAS conflicts, power-loss recovery, every fallback, or >4096-key
multi-chunk preload collection. Those relevant paths were source-reviewed.

## Reproduce

Build both revisions sequentially with the command in provenance.json and copy
each lastdbd into a separate directory as `before` and `after`. Then run:

```bash
python3 docs/proofs/exact-prefix-benchmark-20261023/run.py --binaries /absolute/binaries --output /new/results --scratch /new/scratch --rounds 4
python3 docs/proofs/exact-prefix-benchmark-20261023/summarize.py /new/results
```

Raw samples are losslessly archived as samples.json.gz, with raw and compressed
SHA256 receipts in sample_manifest.json. The driver reproduces uncompressed JSON.
Do not rerun into an existing output or scratch directory.

## Full latency results (ms/request)

### encrypted

| Workload | Before p50 | After p50 | Change | Before p95 | After p95 |
|---|---:|---:|---:|---:|---:|
| hash_durable_write1 | 16.263 | 16.712 | +2.8% | 20.965 | 19.068 |
| hash_durable_write32 | 79.478 | 80.542 | +1.3% | 88.568 | 87.298 |
| hash_missing_point | 0.353 | 0.404 | +14.3% | 0.556 | 0.575 |
| hash_queued_write1 | 6.050 | 5.719 | -5.5% | 18.972 | 19.079 |
| hash_queued_write32 | 36.221 | 36.106 | -0.3% | 256.478 | 169.015 |
| hash_reopen_first_pass | 1.804 | 1.747 | -3.2% | 2.158 | 2.076 |
| hash_warm_point | 0.311 | 0.343 | +10.3% | 0.446 | 0.497 |
| range_between64 | 9.105 | 8.992 | -1.2% | 10.422 | 10.316 |
| range_duplicates_and_miss | 0.674 | 0.684 | +1.5% | 0.888 | 0.928 |
| range_durable_write1 | 18.587 | 18.390 | -1.1% | 23.066 | 25.137 |
| range_durable_write32 | 71.932 | 73.669 | +2.4% | 82.991 | 84.166 |
| range_missing_point | 0.408 | 0.473 | +15.9% | 0.601 | 0.679 |
| range_multiget32 | 3.657 | 3.647 | -0.3% | 4.213 | 4.136 |
| range_partition512 | 64.177 | 63.740 | -0.7% | 67.343 | 66.885 |
| range_prefix10 | 2.001 | 1.994 | -0.3% | 2.564 | 2.307 |
| range_prefix100 | 13.681 | 13.603 | -0.6% | 14.765 | 15.209 |
| range_queued_write1 | 6.845 | 6.773 | -1.1% | 22.709 | 7.751 |
| range_queued_write32 | 69.498 | 71.538 | +2.9% | 135.501 | 133.118 |
| range_reopen_first_pass | 1.918 | 1.975 | +3.0% | 2.311 | 2.373 |
| range_warm_point | 0.352 | 0.378 | +7.3% | 0.502 | 0.516 |

### plain

| Workload | Before p50 | After p50 | Change | Before p95 | After p95 |
|---|---:|---:|---:|---:|---:|
| hash_durable_write1 | 16.683 | 16.387 | -1.8% | 19.046 | 21.078 |
| hash_durable_write32 | 83.940 | 82.097 | -2.2% | 95.552 | 90.976 |
| hash_missing_point | 0.499 | 0.350 | -29.9% | 0.656 | 0.545 |
| hash_queued_write1 | 5.885 | 5.749 | -2.3% | 18.153 | 15.303 |
| hash_queued_write32 | 35.765 | 36.212 | +1.3% | 191.746 | 173.083 |
| hash_reopen_first_pass | 1.790 | 1.664 | -7.0% | 2.005 | 2.089 |
| hash_warm_point | 0.430 | 0.298 | -30.7% | 0.531 | 0.443 |
| range_between64 | 7.620 | 7.635 | +0.2% | 8.525 | 8.526 |
| range_duplicates_and_miss | 0.660 | 0.544 | -17.6% | 0.801 | 0.686 |
| range_durable_write1 | 17.920 | 17.806 | -0.6% | 19.916 | 21.005 |
| range_durable_write32 | 71.652 | 71.597 | -0.1% | 80.804 | 83.775 |
| range_missing_point | 0.545 | 0.393 | -28.0% | 0.703 | 0.562 |
| range_multiget32 | 3.180 | 3.119 | -1.9% | 4.232 | 3.988 |
| range_partition512 | 54.810 | 55.286 | +0.9% | 60.678 | 60.058 |
| range_prefix10 | 1.806 | 1.696 | -6.1% | 2.007 | 2.031 |
| range_prefix100 | 11.604 | 11.638 | +0.3% | 12.518 | 12.851 |
| range_queued_write1 | 6.551 | 6.804 | +3.9% | 14.018 | 12.285 |
| range_queued_write32 | 51.839 | 46.585 | -10.1% | 133.437 | 133.910 |
| range_reopen_first_pass | 1.929 | 1.823 | -5.5% | 2.284 | 2.143 |
| range_warm_point | 0.461 | 0.311 | -32.5% | 0.562 | 0.425 |

### preload

| Workload | Before p50 | After p50 | Change | Before p95 | After p95 |
|---|---:|---:|---:|---:|---:|
| hash_durable_write1 | 16.356 | 16.567 | +1.3% | 22.078 | 18.121 |
| hash_durable_write32 | 79.299 | 79.998 | +0.9% | 88.030 | 90.364 |
| hash_missing_point | 0.427 | 0.375 | -12.3% | 0.631 | 0.513 |
| hash_queued_write1 | 6.066 | 5.304 | -12.6% | 10.941 | 17.023 |
| hash_queued_write32 | 36.900 | 33.441 | -9.4% | 229.056 | 226.657 |
| hash_reopen_first_pass | 1.834 | 1.676 | -8.6% | 2.194 | 2.004 |
| hash_warm_point | 0.368 | 0.320 | -13.0% | 0.552 | 0.430 |
| range_between64 | 8.508 | 8.347 | -1.9% | 9.804 | 9.365 |
| range_duplicates_and_miss | 0.611 | 0.604 | -1.2% | 0.871 | 0.733 |
| range_durable_write1 | 18.048 | 18.074 | +0.1% | 23.782 | 21.096 |
| range_durable_write32 | 74.319 | 72.407 | -2.6% | 87.140 | 79.390 |
| range_missing_point | 0.537 | 0.494 | -8.0% | 0.734 | 0.675 |
| range_multiget32 | 3.686 | 3.643 | -1.2% | 4.213 | 4.071 |
| range_partition512 | 64.082 | 64.385 | +0.5% | 71.023 | 67.271 |
| range_prefix10 | 1.684 | 1.601 | -4.9% | 1.937 | 1.809 |
| range_prefix100 | 12.878 | 12.612 | -2.1% | 14.218 | 13.786 |
| range_queued_write1 | 6.641 | 6.571 | -1.1% | 7.860 | 8.247 |
| range_queued_write32 | 71.138 | 68.211 | -4.1% | 132.710 | 125.832 |
| range_reopen_first_pass | 1.995 | 1.855 | -7.0% | 2.415 | 2.276 |
| range_warm_point | 0.420 | 0.374 | -11.0% | 0.559 | 0.485 |

## Through-flush write throughput (records/s)

| Configuration | Workload | Before | After |
|---|---|---:|---:|
| encrypted | hash_durable_write1 | 59.11 | 58.51 |
| encrypted | hash_durable_write32 | 393.46 | 392.57 |
| encrypted | hash_queued_write1 | 118.39 | 123.82 |
| encrypted | hash_queued_write32 | 482.78 | 452.90 |
| encrypted | range_durable_write1 | 51.25 | 51.36 |
| encrypted | range_durable_write32 | 434.83 | 424.16 |
| encrypted | range_queued_write1 | 116.09 | 118.65 |
| encrypted | range_queued_write32 | 479.09 | 472.34 |
| plain | hash_durable_write1 | 58.83 | 58.82 |
| plain | hash_durable_write32 | 365.69 | 382.08 |
| plain | hash_queued_write1 | 117.68 | 125.10 |
| plain | hash_queued_write32 | 463.19 | 456.61 |
| plain | range_durable_write1 | 54.72 | 55.11 |
| plain | range_durable_write32 | 436.81 | 432.63 |
| plain | range_queued_write1 | 119.44 | 114.95 |
| plain | range_queued_write32 | 494.41 | 518.06 |
| preload | hash_durable_write1 | 56.22 | 59.04 |
| preload | hash_durable_write32 | 395.81 | 390.39 |
| preload | hash_queued_write1 | 124.83 | 134.38 |
| preload | hash_queued_write32 | 510.77 | 534.13 |
| preload | range_durable_write1 | 53.69 | 53.29 |
| preload | range_durable_write32 | 417.57 | 434.63 |
| preload | range_queued_write1 | 118.67 | 120.19 |
| preload | range_queued_write32 | 471.87 | 484.68 |

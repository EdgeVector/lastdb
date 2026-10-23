# Exact native routing: assessment and before/after benchmarks

**Recommendation: hold; not recommended for merge yet.** This is a sound, small
maintainability simplification, but the measured performance is too inconsistent
to justify merging it for a net 41-line saving. There is no demonstrated general
speedup. Large point-read variation prevents a confident causal regression claim,
but plain Hash point medians were slower in both the matrix and longer follow-up.

The patch removes **85 Rust lines and adds 44: net 41 removed across 17 files**.
Exact keys use one canonical collection and one coordinate, removing singleton
candidate arrays/vectors, impossible absent-primary branches, and impossible
multi-destination length merging. Prefix/range routing and genuine multi-source
logical dependencies remain plural. See REVIEW.md for the equivalence argument.

## Revisions and builds

Baseline `bd517062ed3b86aef3e431d1e2cd64fd9dd08aa5` is current main with disabled native cloud backup.
Candidate `6965ecb93a4007e50130cc479b4a4040ad03c997` contains only this routing change. PR #277's separate
partition-reader simplification is not part of either binary. Both release binaries
were built sequentially in `/workspace/lastdb-before`, with Rust 1.96.1, identical
flags, locked dependencies, and `/workspace/bench-target`. The baseline build took
8m43s and corrected candidate 7m33s. An initial compiler check found a remaining
caller of the old optional helper; it was corrected before the candidate build
and before any measurement. Only the corrected binary is benchmarked.

Binary and driver SHA256 receipts, build command, run counts, and source revisions
are in provenance.json and per-run inputs. No compilation overlapped measurements;
heavy work and paired matrices held the host Cargo/benchmark lock. Full workspace
non-ONNX clippy passed with warnings denied, as did formatting, source size,
function size, no-new-allows, artifact, whitespace, and Python syntax checks.
No test suite was added or run, following repository instructions.

## Primary read/write matrix

**24 runs, 49,344 timed requests**: four balanced adjacent pairs
in each configuration, alternating before/after order. One synchronous HTTP-over-
owner-UDS client, four daemon workers, 4-vCPU quota, 16-GiB memory limit. Every run
starts with a fresh synthetic home, 512 Hash and 512 HashRange records, four fields,
and deterministic 1-KiB bodies. Configurations: plain key encoding; blind_v1/ope_v1;
and the same encrypted encoding with the range schema preloaded. Automatic
maintenance is off and persistence interval 100 ms; exact settings are in inputs.

The committed exact-prefix benchmark driver is reused unchanged. It measures
reopen-first-pass and warm point reads, misses, 32-key multi-get, ranges, prefixes,
512-record partitions, duplicate/missing multi-get, queued/durable single writes,
and queued/durable 32-record writes. Value/count/order checks, Delete/Purge
visibility, explicit flush, and representative reopen reads passed in all runs.
Schema installation and reopen also exercise metadata routing, but this is not
exhaustive coverage of every physical namespace or control path.

The encrypted matrix generally improved: range64 -2.4%, multi-get32 -1.6%, HashRange
queued single writes -2.5%, durable batch32 -2.6%. Other configurations raised
concerns. Plain warm Hash point p50 was +25.6%, warm HashRange +26.1%, queued
HashRange batch32 +30.9%, and prefix10 +8.3%. Preloaded Hash warm point p50 was
+74.3%, HashRange +19.1%, and prefix10 +10.3%. The broad variation and those
slower results prompted the separate longer follow-up, not selective omission.

## Fully-warmed plain/preload follow-up

**16 runs, 76,800 timed requests**, four balanced pairs
for each of plain and preload, same binaries. Fully warm both schemas, then time
2048 point reads per schema, 512 prefix10 reads, 128 range64 reads, and 64 queued
32-record write requests per run. Flush and reopen, then read back all 512 range
records to verify the final writes. This follow-up does not repeat Delete/Purge
coverage and is not pooled with the primary matrix.

The preload Hash point slowdown did not persist: 0.330 → 0.309 ms (-6.3%). Preload
HashRange point was +2.7%. Plain Hash point still measured slower, 0.310 → 0.380 ms
(+22.6%); plain HashRange point was +3.4%. Plain Hash per-pair changes were +8.2%,
+50.2%, +69.6%, and -40.0%, so variation remains large. Queued batch32 medians
improved in all eight follow-up pairs: -9.6% plain and -5.7% preload. Range64 medians
improved 3.5% plain and 3.0% preload, with mixed individual pair signs.

The follow-up therefore does not confirm the original broad preload or queued-
write regressions. It also does not establish a stable overall benefit. Holding
this small patch is a risk/benefit judgment under unresolved point-read timing,
not a correctness finding or proof that scalar routing is inherently slower.

## Method, retention, and limits

**40 successful runs and 126,144 timed requests total.** All
120 recorded daemon stops were graceful (zero exit, unforced), and all
owned synthetic homes were cleaned using uid/mode/device/inode, marker, and live-
process checks. No user data was used. Runs began with
18.85–18.86 GiB free; each run guards at least 8 GiB.

Each displayed latency is the median of four per-run p50s or p95s. Delta compares
the reported p50 medians. comparison.json retains every per-run median, all paired
deltas, and geometric ratios. Results are not pooled across workload/configuration
or primary/follow-up. HTTP dispatch through complete response read is timed;
JSON encode/decode and value validation are outside the timer. Write throughput
includes full client wall time and the phase's explicit final flush. A write32
sample is one request of 32 records. Primary batch p95 from 12 samples is coarse;
follow-up batches have 64 samples per run.

Raw samples are losslessly archived as samples.json.gz with both raw/compressed
SHA256 in sample_manifest.json. This is one host/client, uncontrolled OS caches
and host neighbors, without statistical significance established. Reopening is
not a cold filesystem benchmark. No runtime claim is made for concurrent generation
replacement, CAS conflicts, protein sibling sources, disabled cloud restore,
every namespace/control, or power-loss recovery. Relevant changed sites were
source-reviewed. This report retains favorable and unfavorable results.

## Reproduce

Build both pinned revisions sequentially with the command in provenance.json,
copying lastdbd into a separate binary directory as `before` and `after`. Use new
output/scratch paths; existing paths are refused:

```bash
python3 docs/proofs/native-routing-benchmark-20261023/run.py --binaries /absolute/binaries --output /new/matrix --scratch /new/matrix-homes --rounds 4
python3 docs/proofs/native-routing-benchmark-20261023/summarize.py /new/matrix
python3 docs/proofs/native-routing-benchmark-20261023/run_focused.py --binaries /absolute/binaries --output /new/focused --scratch /new/focused-homes --rounds 4
python3 docs/proofs/native-routing-benchmark-20261023/summarize.py /new/focused
```

The matrix uses the committed exact-prefix benchmark driver, which imports the
committed snapshot capture helper; all dependencies are in this repository.
The drivers reproduce uncompressed sample JSON. Per-run inputs contain the base
driver hash; provenance.json also retains the focused driver and runner hashes.

## Full latency results (ms/request)

### measurements

encrypted

| Workload | Before p50 | After p50 | Change | Before p95 | After p95 |
|---|---:|---:|---:|---:|---:|
| hash_durable_write1 | 16.954 | 16.969 | +0.1% | 19.157 | 18.863 |
| hash_durable_write32 | 80.151 | 80.177 | +0.0% | 88.294 | 88.537 |
| hash_missing_point | 0.450 | 0.370 | -17.7% | 0.570 | 0.479 |
| hash_queued_write1 | 6.016 | 5.658 | -6.0% | 18.792 | 18.851 |
| hash_queued_write32 | 35.587 | 33.861 | -4.8% | 195.613 | 190.840 |
| hash_reopen_first_pass | 1.738 | 1.669 | -4.0% | 1.975 | 1.923 |
| hash_warm_point | 0.404 | 0.295 | -26.9% | 0.528 | 0.385 |
| range_between64 | 8.750 | 8.540 | -2.4% | 9.492 | 9.073 |
| range_duplicates_and_miss | 0.688 | 0.704 | +2.3% | 0.793 | 0.810 |
| range_durable_write1 | 18.349 | 18.273 | -0.4% | 23.509 | 20.774 |
| range_durable_write32 | 73.282 | 71.413 | -2.6% | 85.957 | 78.398 |
| range_missing_point | 0.543 | 0.434 | -20.1% | 0.794 | 0.588 |
| range_multiget32 | 3.536 | 3.481 | -1.6% | 3.775 | 3.752 |
| range_partition512 | 61.186 | 61.039 | -0.2% | 65.139 | 65.768 |
| range_prefix10 | 1.965 | 1.890 | -3.8% | 2.117 | 2.137 |
| range_prefix100 | 13.160 | 13.015 | -1.1% | 14.115 | 13.621 |
| range_queued_write1 | 6.824 | 6.653 | -2.5% | 28.554 | 19.179 |
| range_queued_write32 | 70.031 | 69.871 | -0.2% | 138.462 | 124.382 |
| range_reopen_first_pass | 1.912 | 1.791 | -6.3% | 2.180 | 2.179 |
| range_warm_point | 0.440 | 0.336 | -23.8% | 0.539 | 0.430 |

plain

| Workload | Before p50 | After p50 | Change | Before p95 | After p95 |
|---|---:|---:|---:|---:|---:|
| hash_durable_write1 | 17.030 | 17.219 | +1.1% | 25.352 | 19.548 |
| hash_durable_write32 | 80.920 | 83.714 | +3.5% | 90.935 | 100.724 |
| hash_missing_point | 0.360 | 0.376 | +4.4% | 0.474 | 0.600 |
| hash_queued_write1 | 6.075 | 6.112 | +0.6% | 22.253 | 10.674 |
| hash_queued_write32 | 36.659 | 35.491 | -3.2% | 179.048 | 232.311 |
| hash_reopen_first_pass | 1.640 | 1.736 | +5.8% | 1.840 | 2.182 |
| hash_warm_point | 0.285 | 0.358 | +25.6% | 0.406 | 0.575 |
| range_between64 | 7.477 | 7.677 | +2.7% | 8.529 | 8.895 |
| range_duplicates_and_miss | 0.531 | 0.651 | +22.6% | 0.735 | 0.787 |
| range_durable_write1 | 17.990 | 19.675 | +9.4% | 20.866 | 25.426 |
| range_durable_write32 | 71.162 | 72.053 | +1.3% | 80.931 | 81.547 |
| range_missing_point | 0.383 | 0.425 | +10.9% | 0.544 | 0.754 |
| range_multiget32 | 3.028 | 3.141 | +3.7% | 3.486 | 3.560 |
| range_partition512 | 54.733 | 54.838 | +0.2% | 60.672 | 68.020 |
| range_prefix10 | 1.702 | 1.844 | +8.3% | 1.905 | 2.456 |
| range_prefix100 | 11.447 | 11.607 | +1.4% | 13.872 | 12.863 |
| range_queued_write1 | 6.727 | 6.869 | +2.1% | 8.077 | 8.242 |
| range_queued_write32 | 45.220 | 59.204 | +30.9% | 135.385 | 146.155 |
| range_reopen_first_pass | 1.828 | 1.942 | +6.2% | 2.219 | 2.416 |
| range_warm_point | 0.310 | 0.391 | +26.1% | 0.401 | 0.587 |

preload

| Workload | Before p50 | After p50 | Change | Before p95 | After p95 |
|---|---:|---:|---:|---:|---:|
| hash_durable_write1 | 18.070 | 17.038 | -5.7% | 25.291 | 22.426 |
| hash_durable_write32 | 83.497 | 79.076 | -5.3% | 93.221 | 89.355 |
| hash_missing_point | 0.365 | 0.594 | +62.7% | 0.518 | 0.741 |
| hash_queued_write1 | 6.213 | 6.268 | +0.9% | 11.031 | 24.576 |
| hash_queued_write32 | 38.161 | 36.681 | -3.9% | 215.125 | 195.346 |
| hash_reopen_first_pass | 1.712 | 1.916 | +11.9% | 2.213 | 2.203 |
| hash_warm_point | 0.302 | 0.526 | +74.3% | 0.440 | 0.630 |
| range_between64 | 8.132 | 8.283 | +1.9% | 10.740 | 9.169 |
| range_duplicates_and_miss | 0.597 | 0.747 | +25.2% | 0.723 | 1.002 |
| range_durable_write1 | 18.743 | 17.602 | -6.1% | 23.702 | 21.813 |
| range_durable_write32 | 74.260 | 72.077 | -2.9% | 92.035 | 84.559 |
| range_missing_point | 0.405 | 0.568 | +40.4% | 0.585 | 0.723 |
| range_multiget32 | 3.506 | 3.601 | +2.7% | 4.095 | 4.558 |
| range_partition512 | 62.567 | 64.549 | +3.2% | 75.203 | 68.806 |
| range_prefix10 | 1.570 | 1.732 | +10.3% | 1.760 | 1.960 |
| range_prefix100 | 12.348 | 12.686 | +2.7% | 14.785 | 15.422 |
| range_queued_write1 | 6.896 | 6.714 | -2.6% | 21.170 | 7.684 |
| range_queued_write32 | 72.010 | 68.303 | -5.1% | 134.211 | 131.373 |
| range_reopen_first_pass | 1.839 | 2.011 | +9.3% | 2.129 | 2.564 |
| range_warm_point | 0.345 | 0.411 | +19.1% | 0.461 | 0.575 |

### focused-measurements

plain

| Workload | Before p50 | After p50 | Change | Before p95 | After p95 |
|---|---:|---:|---:|---:|---:|
| hash_fully_warm_point | 0.310 | 0.380 | +22.6% | 0.484 | 0.702 |
| range_between64 | 7.724 | 7.456 | -3.5% | 8.994 | 10.697 |
| range_fully_warm_point | 0.342 | 0.353 | +3.4% | 0.523 | 0.597 |
| range_fully_warm_queued_write32 | 50.482 | 45.643 | -9.6% | 125.910 | 122.130 |
| range_prefix10 | 1.798 | 1.815 | +0.9% | 2.086 | 3.098 |

preload

| Workload | Before p50 | After p50 | Change | Before p95 | After p95 |
|---|---:|---:|---:|---:|---:|
| hash_fully_warm_point | 0.330 | 0.309 | -6.3% | 0.509 | 0.453 |
| range_between64 | 8.347 | 8.100 | -3.0% | 13.507 | 9.648 |
| range_fully_warm_point | 0.381 | 0.391 | +2.7% | 0.493 | 0.579 |
| range_fully_warm_queued_write32 | 78.736 | 74.276 | -5.7% | 148.605 | 123.916 |
| range_prefix10 | 1.570 | 1.544 | -1.6% | 2.016 | 1.894 |

## Through-flush write throughput (records/s)

| Measurement | Configuration | Workload | Before | After |
|---|---|---|---:|---:|
| measurements | encrypted | hash_durable_write1 | 57.59 | 57.53 |
| measurements | encrypted | hash_durable_write32 | 391.71 | 386.11 |
| measurements | encrypted | hash_queued_write1 | 119.23 | 124.51 |
| measurements | encrypted | hash_queued_write32 | 497.20 | 486.84 |
| measurements | encrypted | range_durable_write1 | 51.65 | 52.82 |
| measurements | encrypted | range_durable_write32 | 424.63 | 439.42 |
| measurements | encrypted | range_queued_write1 | 113.54 | 113.83 |
| measurements | encrypted | range_queued_write32 | 473.19 | 479.44 |
| measurements | plain | hash_durable_write1 | 55.06 | 55.12 |
| measurements | plain | hash_durable_write32 | 383.60 | 364.81 |
| measurements | plain | hash_queued_write1 | 114.78 | 120.33 |
| measurements | plain | hash_queued_write32 | 449.01 | 532.80 |
| measurements | plain | range_durable_write1 | 53.91 | 48.50 |
| measurements | plain | range_durable_write32 | 436.57 | 438.56 |
| measurements | plain | range_queued_write1 | 118.73 | 112.26 |
| measurements | plain | range_queued_write32 | 507.11 | 457.38 |
| measurements | preload | hash_durable_write1 | 50.99 | 56.27 |
| measurements | preload | hash_durable_write32 | 376.44 | 392.56 |
| measurements | preload | hash_queued_write1 | 117.06 | 108.00 |
| measurements | preload | hash_queued_write32 | 473.27 | 486.32 |
| measurements | preload | range_durable_write1 | 51.03 | 54.73 |
| measurements | preload | range_durable_write32 | 405.68 | 430.57 |
| measurements | preload | range_queued_write1 | 113.52 | 118.80 |
| measurements | preload | range_queued_write32 | 467.09 | 478.50 |
| focused-measurements | plain | range_fully_warm_queued_write32 | 468.08 | 498.72 |
| focused-measurements | preload | range_fully_warm_queued_write32 | 384.50 | 406.11 |

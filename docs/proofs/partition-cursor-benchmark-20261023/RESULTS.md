# Single-cursor partition reader: before/after assessment

Recommendation: **keep as a small maintainability improvement**. The obsolete
outer multi-stream coordinator is gone. The benchmark does not establish a general
speedup, and the initial small-cursor slowdown did not persist in a longer follow-up.

Implementation: **103 Rust lines deleted, 51 added; net 52 removed in one file**.
This removes TipLane, singleton lane construction, the outer BTreeMap union,
shared frontier selection, and empty-slot union helper. The real inner live /
generation merge remains. See REVIEW.md for the ordering and exhaustion argument.

## Revisions, builds, and environment

Baseline `a02d25ed0337f9fe6ce01d7b6d68a71d547f2e72` is merged PR #274. Implementation
`e5e3a413dbd39035cb615de2f0c6e2b5394660d5` changes only partition_page.rs. Both release binaries
were built sequentially in the same `/workspace/lastdb-before` worktree, using Rust
1.96.1, the same locked dependencies, flags, and `/workspace/bench-target` directory.
The baseline build took 8m17s and candidate 7m41s. Binary and driver SHA256 receipts
are in provenance.json and per-run inputs.json. New main changes required fresh
builds; absolute timings should not be compared against the previous report.

One managed Linux host, 4-vCPU quota, 16-GiB memory limit; one synchronous client
using HTTP over owner UDS, four daemon workers. Each fresh owned synthetic home
contains 512 Hash and 512 HashRange records, four fields, and a deterministic
1-KiB body. Plain, blind_v1/ope_v1, and encrypted with range-schema preload are
measured separately. Automatic maintenance is disabled; persistence interval is
100 ms. Exact settings and workload metadata are retained in inputs.json.

No compilation overlapped measurements. The host Cargo/benchmark lock serialized
all heavy work and the whole paired matrix. Runs began with
19.58–19.59 GiB free. Owned daemons stopped gracefully
(96 recorded zero-exit, unforced stops), and all synthetic homes were
removed with ownership and process checks. No user data was used.

## Primary read/write matrix

Four adjacent pairs per configuration, order alternates after/before and
before/after: **24 runs, 57,024 timed requests**. The existing
read/write matrix is reused through the previous committed driver, adding:

- A 16-row page at offset 144, crossing the internal 128-row batch boundary.
- Exclusive cursor pages after key127 and after an empty range cursor.
- A short final page after key499.
- An untimed complete 17-row-page cursor walk after Delete/Purge.
- An untimed independent partition containing empty, NUL, prefix-related, and
  Unicode range keys, checked for values, order, cursor continuity, and has_more.

All runs passed value/count/order checks, duplicate/miss multi-get, Delete/Purge
visibility, explicit flush, representative reopen reads, and all added paging
checks. The preload route returns before the changed disk path and is a control,
not evidence of executing that path.

The 512-record partition p50 improved 3.0% plain and 2.5% encrypted; all four pairs
improved in each configuration. Preloaded partition reads measured 2.8% slower.
Small encrypted pages initially measured slower: cursor127 +4.7%, after-empty
+8.3%, short-final +5.0%; offset144 was +0.7%. Plain small pages were near flat.
These directly relevant slower results motivated the separate follow-up below.

Writes are mixed despite unchanged write code. Default encrypted HashRange queued
single-write p50 changed -2.8%, durable single +1.8%, and durable batch32 +2.2%.
Preloaded write medians varied more widely. Do not interpret untouched point-read
or write-path changes as proof of a causal improvement from this refactor.

## Longer fully-warmed encrypted paging follow-up

**8 runs, 28,672 timed requests**, four balanced adjacent
pairs, same binaries and owned-home rules. Delete/Purge first, flush and reopen,
warm the entire partition and each page shape, then measure 512 offset requests
and 1024 requests for each of the three cursor shapes per run. This follow-up does
not repeat the write benchmark, and is not pooled with the primary matrix.

The initial cursor slowdown did not persist: cursor127 p50 changed -4.6%,
after-empty -1.6%, short-final -1.1%; offset144 was +0.6%. Pair deltas remain mixed,
so neither a universal speedup nor a stable small-page regression is established.
The recommendation rests on the simpler single-stream code and preserved behavior.

## Interpretation and limits

Tables report the median of four per-run p50s and p95s; delta is the ratio of the
displayed p50 medians. comparison.json retains every per-run median, paired delta,
and write throughput, with no pooling across configurations or workload shapes.
Request timing spans HTTP dispatch through complete response read, excluding JSON
encode/decode and value validation. Write throughput includes client work and a
final explicit flush. A write32 sample is a 32-record request; p95 from 12 batch
samples is coarse. Raw samples are losslessly archived in samples.json.gz with
both raw and compressed SHA256 hashes in sample_manifest.json.

This is one host and client, with uncontrolled OS caches and host neighbors;
reopen-first-pass is not cold-disk latency. No statistical significance or broad
capacity/concurrency claim is made. Concurrent generation replacement, CAS
conflicts, power-loss recovery, every fallback, and tombstone-including public
queries are not runtime benchmark coverage. Generation merging and its deletion
markers were source-reviewed and their implementation is unchanged.

## Validation and reproduction

Both optimized product builds, full workspace non-ONNX clippy with warnings denied,
formatting, changed-file/function size, no-new-allows, artifact/whitespace checks,
and Python syntax checks passed. No test suite was added or run, following the
repository instruction. All 32 benchmark runs passed their integrity checks.

Build both revisions sequentially with provenance.json's command, copying lastdbd
into a separate directory as `before` and `after`. Use new output/scratch paths:

```bash
python3 docs/proofs/partition-cursor-benchmark-20261023/run.py --binaries /absolute/binaries --output /new/matrix --scratch /new/matrix-homes --rounds 4
python3 docs/proofs/partition-cursor-benchmark-20261023/summarize.py /new/matrix
python3 docs/proofs/partition-cursor-benchmark-20261023/run_focused.py --binaries /absolute/binaries --output /new/focused --scratch /new/focused-homes --rounds 4
python3 docs/proofs/partition-cursor-benchmark-20261023/summarize.py /new/focused
```

The drivers reproduce uncompressed sample JSON. paging.py imports the committed
exact-prefix benchmark driver, which imports the committed snapshot capture helper.
These dependencies are in this repository; their hashes are in per-run inputs.
focused.py's hash is in provenance.json. Existing output/scratch paths are refused.

## Latency tables (ms/request)

### measurements

encrypted

| Workload | Before p50 | After p50 | Change | Before p95 | After p95 |
|---|---:|---:|---:|---:|---:|
| hash_durable_write1 | 16.571 | 17.329 | +4.6% | 21.503 | 22.303 |
| hash_durable_write32 | 82.963 | 81.848 | -1.3% | 95.423 | 89.924 |
| hash_missing_point | 0.465 | 0.351 | -24.5% | 0.604 | 0.523 |
| hash_queued_write1 | 5.956 | 5.970 | +0.2% | 14.609 | 22.954 |
| hash_queued_write32 | 34.763 | 35.321 | +1.6% | 211.113 | 252.094 |
| hash_reopen_first_pass | 1.810 | 1.676 | -7.4% | 2.220 | 1.911 |
| hash_warm_point | 0.416 | 0.299 | -28.1% | 0.530 | 0.424 |
| partition_cursor127_limit16 | 2.460 | 2.575 | +4.7% | 3.024 | 2.962 |
| partition_cursor_after_empty | 2.318 | 2.510 | +8.3% | 2.567 | 2.843 |
| partition_offset144_limit16 | 7.543 | 7.593 | +0.7% | 9.607 | 8.235 |
| partition_short_last_page | 1.789 | 1.878 | +5.0% | 1.993 | 2.087 |
| range_between64 | 8.817 | 8.602 | -2.4% | 9.713 | 9.355 |
| range_duplicates_and_miss | 0.716 | 0.588 | -17.9% | 0.837 | 0.717 |
| range_durable_write1 | 18.498 | 18.825 | +1.8% | 21.113 | 21.164 |
| range_durable_write32 | 72.882 | 74.471 | +2.2% | 82.309 | 88.901 |
| range_missing_point | 0.526 | 0.451 | -14.2% | 0.755 | 0.688 |
| range_multiget32 | 3.582 | 3.531 | -1.4% | 4.188 | 4.132 |
| range_partition512 | 62.381 | 60.845 | -2.5% | 65.843 | 66.217 |
| range_prefix10 | 1.948 | 1.885 | -3.2% | 2.305 | 2.162 |
| range_prefix100 | 13.335 | 13.044 | -2.2% | 14.509 | 14.331 |
| range_queued_write1 | 6.759 | 6.572 | -2.8% | 7.546 | 8.274 |
| range_queued_write32 | 69.453 | 71.681 | +3.2% | 132.824 | 132.209 |
| range_reopen_first_pass | 1.985 | 1.878 | -5.4% | 2.339 | 2.254 |
| range_warm_point | 0.456 | 0.360 | -21.0% | 0.570 | 0.470 |

plain

| Workload | Before p50 | After p50 | Change | Before p95 | After p95 |
|---|---:|---:|---:|---:|---:|
| hash_durable_write1 | 17.155 | 16.740 | -2.4% | 27.546 | 22.038 |
| hash_durable_write32 | 91.955 | 83.861 | -8.8% | 129.576 | 108.393 |
| hash_missing_point | 0.395 | 0.396 | +0.1% | 0.588 | 0.540 |
| hash_queued_write1 | 6.097 | 6.102 | +0.1% | 20.413 | 11.860 |
| hash_queued_write32 | 35.611 | 34.736 | -2.5% | 199.349 | 152.850 |
| hash_reopen_first_pass | 1.738 | 1.696 | -2.4% | 2.137 | 2.055 |
| hash_warm_point | 0.331 | 0.322 | -2.7% | 0.481 | 0.435 |
| partition_cursor127_limit16 | 2.253 | 2.241 | -0.5% | 2.531 | 2.466 |
| partition_cursor_after_empty | 2.163 | 2.174 | +0.5% | 2.363 | 2.561 |
| partition_offset144_limit16 | 6.568 | 6.575 | +0.1% | 7.304 | 8.767 |
| partition_short_last_page | 1.648 | 1.647 | -0.1% | 1.818 | 2.025 |
| range_between64 | 7.660 | 7.716 | +0.7% | 9.276 | 9.681 |
| range_duplicates_and_miss | 0.583 | 0.553 | -5.2% | 0.936 | 0.684 |
| range_durable_write1 | 20.016 | 18.335 | -8.4% | 28.423 | 26.766 |
| range_durable_write32 | 77.634 | 69.895 | -10.0% | 91.138 | 94.722 |
| range_missing_point | 0.439 | 0.434 | -1.1% | 0.681 | 0.606 |
| range_multiget32 | 3.156 | 3.127 | -0.9% | 3.723 | 4.408 |
| range_partition512 | 56.253 | 54.551 | -3.0% | 64.580 | 66.119 |
| range_prefix10 | 1.723 | 1.741 | +1.0% | 2.060 | 2.288 |
| range_prefix100 | 11.496 | 11.526 | +0.3% | 14.406 | 14.641 |
| range_queued_write1 | 6.741 | 7.199 | +6.8% | 25.127 | 31.478 |
| range_queued_write32 | 49.262 | 55.887 | +13.4% | 132.767 | 133.683 |
| range_reopen_first_pass | 1.907 | 1.921 | +0.7% | 2.442 | 2.458 |
| range_warm_point | 0.350 | 0.319 | -8.9% | 0.504 | 0.460 |

preload

| Workload | Before p50 | After p50 | Change | Before p95 | After p95 |
|---|---:|---:|---:|---:|---:|
| hash_durable_write1 | 17.507 | 19.946 | +13.9% | 26.168 | 33.194 |
| hash_durable_write32 | 83.111 | 89.096 | +7.2% | 96.724 | 127.284 |
| hash_missing_point | 0.444 | 0.443 | -0.0% | 0.688 | 0.643 |
| hash_queued_write1 | 5.537 | 5.636 | +1.8% | 18.032 | 13.244 |
| hash_queued_write32 | 34.752 | 37.036 | +6.6% | 220.310 | 213.588 |
| hash_reopen_first_pass | 1.770 | 1.806 | +2.0% | 2.128 | 2.125 |
| hash_warm_point | 0.419 | 0.370 | -11.8% | 0.615 | 0.497 |
| partition_cursor127_limit16 | 2.387 | 2.283 | -4.4% | 3.432 | 2.842 |
| partition_cursor_after_empty | 2.358 | 2.244 | -4.8% | 3.894 | 2.934 |
| partition_offset144_limit16 | 7.970 | 7.979 | +0.1% | 10.748 | 10.143 |
| partition_short_last_page | 1.762 | 1.697 | -3.7% | 2.686 | 2.416 |
| range_between64 | 8.096 | 8.284 | +2.3% | 9.423 | 10.129 |
| range_duplicates_and_miss | 0.606 | 0.636 | +5.0% | 0.800 | 0.855 |
| range_durable_write1 | 18.600 | 20.912 | +12.4% | 22.442 | 31.356 |
| range_durable_write32 | 73.830 | 79.971 | +8.3% | 85.361 | 128.924 |
| range_missing_point | 0.500 | 0.502 | +0.5% | 0.769 | 0.739 |
| range_multiget32 | 3.684 | 3.660 | -0.6% | 4.219 | 4.540 |
| range_partition512 | 62.805 | 64.557 | +2.8% | 72.742 | 75.952 |
| range_prefix10 | 1.541 | 1.590 | +3.2% | 1.870 | 2.216 |
| range_prefix100 | 12.467 | 12.609 | +1.1% | 16.790 | 14.864 |
| range_queued_write1 | 6.830 | 6.732 | -1.4% | 8.126 | 12.274 |
| range_queued_write32 | 74.627 | 73.040 | -2.1% | 141.589 | 124.495 |
| range_reopen_first_pass | 2.066 | 1.923 | -6.9% | 2.349 | 2.367 |
| range_warm_point | 0.506 | 0.399 | -21.1% | 0.660 | 0.613 |

### focused-measurements

encrypted

| Workload | Before p50 | After p50 | Change | Before p95 | After p95 |
|---|---:|---:|---:|---:|---:|
| partition_cursor127_limit16 | 2.643 | 2.521 | -4.6% | 4.649 | 3.070 |
| partition_cursor_after_empty | 2.476 | 2.437 | -1.6% | 4.974 | 3.861 |
| partition_offset144_limit16 | 7.701 | 7.748 | +0.6% | 11.282 | 11.836 |
| partition_short_last_page | 1.906 | 1.885 | -1.1% | 2.716 | 2.232 |

## Through-flush write throughput (records/s)

| Configuration | Workload | Before | After |
|---|---|---:|---:|
| encrypted | hash_durable_write1 | 57.45 | 55.41 |
| encrypted | hash_durable_write32 | 378.70 | 384.96 |
| encrypted | hash_queued_write1 | 120.35 | 119.42 |
| encrypted | hash_queued_write32 | 500.73 | 486.47 |
| encrypted | range_durable_write1 | 52.76 | 51.81 |
| encrypted | range_durable_write32 | 430.45 | 415.72 |
| encrypted | range_queued_write1 | 115.13 | 117.15 |
| encrypted | range_queued_write32 | 485.60 | 462.58 |
| plain | hash_durable_write1 | 53.63 | 56.07 |
| plain | hash_durable_write32 | 328.73 | 362.49 |
| plain | hash_queued_write1 | 118.12 | 116.61 |
| plain | hash_queued_write32 | 499.68 | 467.87 |
| plain | range_durable_write1 | 45.78 | 51.45 |
| plain | range_durable_write32 | 400.60 | 419.86 |
| plain | range_queued_write1 | 105.48 | 96.69 |
| plain | range_queued_write32 | 462.69 | 484.26 |
| preload | hash_durable_write1 | 53.38 | 46.55 |
| preload | hash_durable_write32 | 380.76 | 328.83 |
| preload | hash_queued_write1 | 126.66 | 128.72 |
| preload | hash_queued_write32 | 470.89 | 453.46 |
| preload | range_durable_write1 | 52.10 | 43.39 |
| preload | range_durable_write32 | 423.56 | 372.19 |
| preload | range_queued_write1 | 117.34 | 111.96 |
| preload | range_queued_write32 | 438.42 | 443.86 |

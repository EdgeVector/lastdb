# Exact-key refactor: review and before/after benchmark

Code review: no blocking correctness finding. See [REVIEW.md](REVIEW.md) for the source reasoning and unexercised paths.

## Interpretation

The benchmark does not show a general speedup. Under default encrypted keys, 32-key multi-get was 3.697 → 3.802 ms (+2.8%) and a 64-record bounded range was 8.981 → 9.106 ms (+1.4%). Warm HashRange point reads were 0.401 → 0.486 ms (+21.3%); individual paired changes ranged from -14.6% to +67.0%, so that sub-millisecond result is variable and should not be presented as a stable read improvement.

In the separate, fully warmed write follow-up, the four displayed request medians improved 4.6–7.2%. Through-flush throughput improved about 3–14%, depending on the phase. Hash durable-batch p95 was essentially unchanged; not every pair or tail latency improved. The initial exploratory +22% queued-HashRange write slowdown did not recur in the primary matrix (+0.4%) or longer follow-up (-4.7%).

The simplification removes 225 Rust lines. The source review and completed workload checks support that implementation, but these measurements do not justify a blanket read/write performance claim. The PR remains a draft for review.

## Scope and method

- Before: `4118c7baa12539c8c6bcf4391c497159f2a1529a`.
- After: `d7a5f441cc084a9624fac97d60d811c9b230702b` (PR #269 implementation).
- Both revisions contain the snapshot-commit change. The only implementation difference is the exact-key refactor.
- Rust 1.96.1, locked Cargo dependencies, optimized `release` builds, identical build command and checkout path. Binary hashes and commands: [provenance.json](provenance.json). No compilation overlapped measurements.
- Linux workspace: 4 vCPU quota, 16 GiB memory limit, 32 GiB overlay filesystem. One synchronous client over the owner Unix HTTP socket; four daemon workers.
- Each run creates a new owned synthetic home with 512 Hash records and 512 HashRange records. Four fields, including a deterministic 1,024-byte body. Schema descriptions keep the two schemas independent.
- Plain keys, product-default blind/OPE keys, and blind/OPE with HashRange schema preload. Automatic maintenance disabled; native flush delay fixed at 100 ms for both versions.
- Four adjacent pairs per configuration. Order alternates before/after then after/before. The longer encrypted-write follow-up adds four pairs with fully warmed records, 256 queued single writes and 32 durable batches of 32 per schema.
- Matrix phases use 64 single writes or 12 batches of 32 per acknowledgement mode and schema. Warm point phases have 384 requests; first-pass and missing-point phases 128; multi-get/range/prefix phases 96; duplicate/missing multi-get 64. A 12-sample batch p95 is the maximum, so batch tails are coarse.
- Request latency times HTTP request dispatch through reading the complete response. JSON encoding, response decoding, and value verification are outside that timer. Write throughput includes client work and the final explicit flush.
- Warm point reads reuse 128 known keys. Reopen-first-pass reads use 128 distinct keys after restart; they are not cold-filesystem reads. OS caches and host neighbors are uncontrolled.
- The primary matrix has 24 runs; the follow-up has 8 runs. All 32 completed, verified values and graceful reopening, and stopped their daemons. Matrix runs also verify duplicate/missing-key membership, range order, Delete/Purge visibility, and flush coverage. Matrix reopen checks sample 14 keys after full live readback; the focused follow-up verifies all 1,024 records again after reopen.
- 51,072 timed requests. Each run requires at least 8 GiB free, and removes only its verified owned synthetic home afterward. Actual free space before primary runs: 19.93–19.94 GiB. Cleanup receipts accompany every run.

## Reading the tables

All latencies are **milliseconds per request**, including a whole batch where applicable. Each displayed p50 or p95 is the median of four per-run p50s or p95s. The change column is `after / before - 1` for the displayed p50 values; negative means faster. Do not pool these rows into one overall percentage. Four pairs and variable sub-millisecond timings do not establish statistical significance.

Full run medians, paired deltas, p95s, acknowledgements, and throughput: [matrix comparison](measurements/comparison.json) and [longer-write comparison](focused-measurements/comparison.json). Every run retains raw samples, input hashes, shutdown receipts, and cleanup receipts. Raw `samples.json.gz` files are lossless gzip archives of the JSON emitted by the drivers; [sample_manifest.json](sample_manifest.json) records compressed and uncompressed hashes. The drivers reproduce uncompressed JSON.

## Full matrix

### Plain keys

| Workload | p50 before | p50 after | Change | p95 before | p95 after |
|---|---:|---:|---:|---:|---:|
| `hash_durable_write1` | 32.683 | 33.155 | +1.4% | 44.814 | 43.875 |
| `hash_durable_write32` | 126.886 | 132.075 | +4.1% | 172.563 | 159.148 |
| `hash_missing_point` | 0.352 | 0.394 | +11.8% | 0.578 | 1.551 |
| `hash_queued_write1` | 24.429 | 25.254 | +3.4% | 44.497 | 35.255 |
| `hash_queued_write32` | 114.979 | 113.631 | -1.2% | 157.747 | 126.787 |
| `hash_reopen_first_pass` | 1.678 | 1.688 | +0.6% | 2.060 | 2.119 |
| `hash_warm_point` | 0.289 | 0.305 | +5.7% | 0.448 | 0.458 |
| `range_between64` | 8.060 | 7.841 | -2.7% | 17.379 | 11.315 |
| `range_duplicates_and_miss` | 0.645 | 0.617 | -4.4% | 1.774 | 0.884 |
| `range_durable_write1` | 50.874 | 48.249 | -5.2% | 80.240 | 77.589 |
| `range_durable_write32` | 147.308 | 138.063 | -6.3% | 161.030 | 164.954 |
| `range_missing_point` | 0.483 | 0.421 | -12.8% | 0.754 | 0.669 |
| `range_multiget32` | 3.231 | 3.199 | -1.0% | 5.570 | 5.627 |
| `range_prefix10` | 1.956 | 1.781 | -8.9% | 4.676 | 2.151 |
| `range_queued_write1` | 26.313 | 26.408 | +0.4% | 32.069 | 34.756 |
| `range_queued_write32` | 102.892 | 106.232 | +3.2% | 178.484 | 195.801 |
| `range_reopen_first_pass` | 1.902 | 1.916 | +0.7% | 2.312 | 2.284 |
| `range_warm_point` | 0.373 | 0.333 | -10.8% | 0.534 | 0.524 |

### Default encrypted keys

| Workload | p50 before | p50 after | Change | p95 before | p95 after |
|---|---:|---:|---:|---:|---:|
| `hash_durable_write1` | 31.889 | 31.421 | -1.5% | 42.313 | 38.758 |
| `hash_durable_write32` | 126.368 | 127.215 | +0.7% | 137.154 | 139.499 |
| `hash_missing_point` | 0.407 | 0.377 | -7.2% | 0.589 | 0.558 |
| `hash_queued_write1` | 24.073 | 22.966 | -4.6% | 30.142 | 27.796 |
| `hash_queued_write32` | 110.009 | 108.997 | -0.9% | 129.990 | 128.919 |
| `hash_reopen_first_pass` | 1.831 | 1.750 | -4.4% | 2.116 | 2.082 |
| `hash_warm_point` | 0.358 | 0.335 | -6.4% | 0.520 | 0.459 |
| `range_between64` | 8.981 | 9.106 | +1.4% | 10.136 | 9.872 |
| `range_duplicates_and_miss` | 0.661 | 0.644 | -2.6% | 0.878 | 0.821 |
| `range_durable_write1` | 38.973 | 37.573 | -3.6% | 47.719 | 48.463 |
| `range_durable_write32` | 138.684 | 131.757 | -5.0% | 155.006 | 153.049 |
| `range_missing_point` | 0.458 | 0.514 | +12.1% | 0.672 | 0.775 |
| `range_multiget32` | 3.697 | 3.802 | +2.8% | 4.414 | 4.224 |
| `range_prefix10` | 1.973 | 1.962 | -0.6% | 2.241 | 2.383 |
| `range_queued_write1` | 25.677 | 25.783 | +0.4% | 33.674 | 32.661 |
| `range_queued_write32` | 109.339 | 105.000 | -4.0% | 176.699 | 156.943 |
| `range_reopen_first_pass` | 1.995 | 2.052 | +2.8% | 2.455 | 2.371 |
| `range_warm_point` | 0.401 | 0.486 | +21.3% | 0.563 | 0.618 |

### Default encrypted keys with schema preload

| Workload | p50 before | p50 after | Change | p95 before | p95 after |
|---|---:|---:|---:|---:|---:|
| `hash_durable_write1` | 30.904 | 30.780 | -0.4% | 37.475 | 37.032 |
| `hash_durable_write32` | 132.930 | 126.551 | -4.8% | 161.995 | 141.102 |
| `hash_missing_point` | 0.394 | 0.468 | +18.8% | 0.615 | 0.647 |
| `hash_queued_write1` | 23.142 | 22.580 | -2.4% | 27.795 | 27.280 |
| `hash_queued_write32` | 111.063 | 106.337 | -4.3% | 122.794 | 122.081 |
| `hash_reopen_first_pass` | 1.767 | 1.788 | +1.2% | 2.193 | 2.146 |
| `hash_warm_point` | 0.336 | 0.415 | +23.2% | 0.487 | 0.559 |
| `range_between64` | 8.559 | 8.641 | +1.0% | 9.363 | 9.488 |
| `range_duplicates_and_miss` | 0.670 | 0.669 | -0.2% | 0.919 | 0.903 |
| `range_durable_write1` | 39.418 | 40.073 | +1.7% | 49.440 | 50.434 |
| `range_durable_write32` | 140.847 | 139.136 | -1.2% | 164.182 | 156.316 |
| `range_missing_point` | 0.465 | 0.481 | +3.4% | 0.635 | 0.713 |
| `range_multiget32` | 3.644 | 3.698 | +1.5% | 4.255 | 4.240 |
| `range_prefix10` | 1.691 | 1.649 | -2.5% | 1.945 | 1.916 |
| `range_queued_write1` | 25.634 | 26.784 | +4.5% | 33.015 | 42.735 |
| `range_queued_write32` | 113.658 | 106.664 | -6.2% | 194.135 | 165.951 |
| `range_reopen_first_pass` | 2.006 | 2.042 | +1.8% | 2.415 | 2.446 |
| `range_warm_point` | 0.349 | 0.385 | +10.3% | 0.502 | 0.537 |

## Longer encrypted-write follow-up

These are separate, fully warmed runs, not additional samples pooled into the matrix.

| Workload | p50 before | p50 after | Change | p95 before | p95 after |
|---|---:|---:|---:|---:|---:|
| `hash_durable_write32` | 128.572 | 122.661 | -4.6% | 148.436 | 148.899 |
| `hash_queued_write1` | 24.126 | 22.398 | -7.2% | 36.269 | 25.881 |
| `range_durable_write32` | 128.123 | 121.846 | -4.9% | 153.569 | 143.438 |
| `range_queued_write1` | 34.364 | 32.733 | -4.7% | 48.881 | 43.852 |

| Longer-write workload | Records/s before, through flush | Records/s after, through flush |
|---|---:|---:|
| `hash_durable_write32` | 244.9 | 252.1 |
| `hash_queued_write1` | 38.7 | 44.1 |
| `range_durable_write32` | 245.1 | 256.8 |
| `range_queued_write1` | 27.6 | 29.0 |

## Excluded exploratory attempts

The first 24-run matrix retained every synthetic home, consuming about 17 GiB. A subsequent longer-write attempt ran out of disk space during the third run’s seed step and received HTTP 507. Those completed and failed attempts are retained under `exploratory-measurements/` and `failed-focused-measurements/`, with the error in [exploratory-followup-failure.txt](exploratory-followup-failure.txt). They are **excluded** from the primary comparison because disk occupancy changed throughout the attempt. All their daemons stopped; only ownership-verified generated homes were removed. The primary 32-run comparison was then rerun with per-run cleanup and free-space guards.

## Reproduction

Build both pinned revisions serially in an isolated checkout with the command in provenance.json, saving `lastdbd` as `BIN_DIR/before` and `BIN_DIR/after`. Use fresh output and scratch parent paths:

```bash
python3 docs/proofs/exact-key-benchmark-20261022/run.py --binaries BIN_DIR --output MATRIX_OUTPUT --scratch MATRIX_SCRATCH --rounds 4
python3 docs/proofs/exact-key-benchmark-20261022/run_focused.py --binaries BIN_DIR --output FOCUSED_OUTPUT --scratch FOCUSED_SCRATCH
python3 docs/proofs/exact-key-benchmark-20261022/summarize.py MATRIX_OUTPUT
python3 docs/proofs/exact-key-benchmark-20261022/summarize.py FOCUSED_OUTPUT
```

The runners share the repository’s host Cargo lock. They never touch primary data. They preserve receipts and remove each owned home only after shutdown and ownership validation. No test suite was added or run.

## Limits

End-to-end owner-socket latency on one host with one client and 1,024 generated records is not a production capacity or concurrency result. First-request latency is retained separately in per-run statistics; first-pass medians do not represent the one-time preload cost. No allocator profile, true cold-cache experiment, scaling study, process interruption, power-loss proof, CAS-conflict workload, protein move, active molecule-generation replacement, or cloud-sync measurement is included. Read assertions and final readback support the measured workload; they do not prove every storage path.

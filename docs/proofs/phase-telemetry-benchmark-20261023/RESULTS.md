# Phase telemetry simplification: before/after benchmark

**Assessment: merge for maintainability, without a general speedup claim.**
Point-read medians changed by +0.1% to +1.7%; most range/prefix medians were slightly
lower. The primary matrix's plain durable single-write result was +21.0%, with
mixed pair signs and large penalties only in candidate-first pairs. A longer
follow-up measured -2.9% for warmed plain single writes and -3.2% for warmed
preloaded durable batches. Plain single-write p50 improved in all four follow-up
pairs; preload batch pairs were mixed. Forensic status medians in the follow-up
were within 0.1%. See FOCUSED_RESULTS.md; the shorter post-reopen result remains
visible below and its cause is not established.

The source equivalence argument, passing product checks, and 179-line maintenance
reduction justify this small private-macro refactor. Residual performance risk is
low, but these warm single-client measurements cannot rule out every cold-write
or concurrent-workload regression. Forty measured runs across both experiments,
217,344 timed requests and 109 graceful stops passed their applicable checks.
The experiments differ in warmup and write duration; their timings are not pooled.

One ordered declaration now generates the 49 telemetry fields, display names,
phase count, and both array conversions. One Rust file shrinks from 611 to 432
lines: 449 deleted, 270 added, **net 179 removed**. No change to the arithmetic,
recording sites, wire fields/order, serde defaults/zero omission, or storage.
See REVIEW.md and schema-review.json for the source equivalence inspection.

## Method and validation

- Fresh optimized product builds passed for baseline `46e3024` and candidate
  `fffb4f2`, in the same worktree with identical compiler/flags and lockfile.
- Full workspace clippy (excluding the two ONNX packages) passed with warnings
  denied. Formatting, source/function size, no-new-allows, Python syntax,
  whitespace, and artifact checks passed. No test suite was added or run.
- 24 measured runs: four alternating adjacent pairs in each of plain, encrypted,
  and preloaded configurations. 211,200 timed requests in total. Three baseline
  fixtures were copied byte-identically within each configuration; fixture path/
  content digests were checked before startup and after the whole experiment.
- Client pinned to CPU 0; daemon and all four configured workers pinned to CPU 1.
  Host performance lock excludes heavy Cargo overlap. Full warming and eight
  interleaved 512-request point blocks per kind/run expose within-run variation.
- Each schema has 512 records, four fields, 1 KiB bodies. Validate every returned
  value/count/order, requested queued and durable acknowledgements, explicit flush,
  and reopen values. Forensic snapshots validate phase vocabulary/zero omission
  and sample-to-cumulative-aggregate phase inequalities. All checks passed.
- Both versions sample self metrics every one second for telemetry coverage.
  Forensic status uses the endpoint's cached snapshot, not a forced fresh build.
- 75 graceful unforced zero-exit daemon stops, including fixtures. All owned
  synthetic homes were ownership/process-checked and cleaned; no user data used.

## Results (ms/request)

Values are medians of four per-run p50/p95 values. Changes compare the reported
p50 medians; lower is better. Individual paired deltas and throughput through
final flush are retained in measurements/comparison.json. Requested queued
writes may acknowledge durable; actual acknowledgement counts are retained.

### encrypted

| Workload | Before p50 | After p50 | Change | Before p95 | After p95 |
|---|---:|---:|---:|---:|---:|
| hash_durable_write1 | 16.863 | 17.683 | +4.9% | 25.953 | 26.706 |
| hash_fully_warm_point | 0.526 | 0.526 | +0.1% | 0.660 | 0.652 |
| range_between64 | 8.791 | 8.737 | -0.6% | 11.168 | 11.443 |
| range_durable_write32 | 67.925 | 66.372 | -2.3% | 78.938 | 93.543 |
| range_fully_warm_point | 0.563 | 0.566 | +0.5% | 1.205 | 0.699 |
| range_fully_warm_queued_write32 | 64.987 | 64.703 | -0.4% | 83.478 | 88.148 |
| range_prefix10 | 2.082 | 2.080 | -0.1% | 2.458 | 2.419 |
| telemetry_forensics | 4.074 | 4.135 | +1.5% | 5.016 | 4.537 |

### plain

| Workload | Before p50 | After p50 | Change | Before p95 | After p95 |
|---|---:|---:|---:|---:|---:|
| hash_durable_write1 | 16.147 | 19.532 | +21.0% | 20.069 | 30.930 |
| hash_fully_warm_point | 0.518 | 0.519 | +0.3% | 0.648 | 0.617 |
| range_between64 | 7.699 | 7.658 | -0.5% | 8.987 | 12.070 |
| range_durable_write32 | 63.667 | 63.830 | +0.3% | 80.102 | 79.670 |
| range_fully_warm_point | 0.544 | 0.547 | +0.6% | 0.673 | 0.693 |
| range_fully_warm_queued_write32 | 60.116 | 60.439 | +0.5% | 65.661 | 72.886 |
| range_prefix10 | 1.931 | 1.934 | +0.2% | 2.220 | 2.187 |
| telemetry_forensics | 4.217 | 4.103 | -2.7% | 4.632 | 4.894 |

### preload

| Workload | Before p50 | After p50 | Change | Before p95 | After p95 |
|---|---:|---:|---:|---:|---:|
| hash_durable_write1 | 15.654 | 15.704 | +0.3% | 18.743 | 22.411 |
| hash_fully_warm_point | 0.525 | 0.532 | +1.3% | 0.651 | 1.208 |
| range_between64 | 8.289 | 8.227 | -0.7% | 10.907 | 9.592 |
| range_durable_write32 | 64.598 | 68.503 | +6.0% | 76.342 | 102.267 |
| range_fully_warm_point | 0.577 | 0.587 | +1.7% | 0.688 | 0.766 |
| range_fully_warm_queued_write32 | 66.720 | 62.906 | -5.7% | 86.018 | 73.468 |
| range_prefix10 | 1.721 | 1.698 | -1.3% | 2.652 | 1.850 |
| telemetry_forensics | 3.972 | 4.225 | +6.4% | 4.596 | 5.019 |

## Limits and reproduction

One host, one client, a small warm dataset, and one server CPU. CPU affinity
does not isolate these CPUs from neighboring load. OS caches are uncontrolled.
No statistical significance or production concurrency/scalability claim is made.
Naturally varying phase values are checked structurally, not for numerical equality
between binaries. The 49 mappings are source-reviewed; not every phase recording
site is exercised. This is not crash/power-loss recovery evidence.

Raw samples and telemetry snapshots are deterministic gzip files; sample_manifest.json
records raw/compressed SHA256s. Inputs, fixture hashes, point blocks, shutdown,
CPU-affinity, and cleanup receipts are retained. Fixtures and private identity
keys are not committed. provenance.json records binaries, compiler, and helpers.

```bash
python3 docs/proofs/phase-telemetry-benchmark-20261023/run.py --binaries /absolute/binaries --output /new/output --scratch /new/owned-homes --provenance docs/proofs/phase-telemetry-benchmark-20261023/provenance.json
python3 docs/proofs/native-routing-benchmark-20261023/summarize.py /new/output
```

Select two allowed CPUs with --client-cpu / --server-cpu if 0 and 1 are unavailable.
Existing output/scratch paths are refused. Build the two declared source revisions
with the recorded command; binary hashes must match the provenance.

# Owned query-result formatting: before/after benchmark

**Assessment: merge for fewer copies and an observed structured-read benefit.**
Structured JSON p50 medians improved 4.8–8.6% across the nine workload/configuration
cells; 34 of 36 individual paired p50 changes were lower. Flat point medians ranged
from -1.9% to +0.9%. The original flat multi-get32 and encrypted-write slowdowns
triggered the longer follow-up in FOCUSED_RESULTS.md. Multi-get32 then measured
-0.3% to -1.3%, including faster first blocks in all four preload pairs. Encrypted
durable single/batch medians were +1.6%/+2.0%, with mixed pair signs. No general
write-speedup, significance, or production-capacity claim.

Both experiments passed all applicable value/metadata, fingerprint, durability,
flush/reopen, shutdown and cleanup checks: **48 measured runs, 253,696 timed
requests, 480 complete-response fingerprint comparisons, 126 graceful stops**
including fixtures. Timings are not pooled across the different experiments.
The earlier adverse cells remain below; their cause is not established.

This is a six-line source reduction with no new abstraction. The value is removing
an unnecessary deep-copy stage and making ownership explicit. The internal Rust
helper now requires an owned map; its one workspace caller is updated. The HTTP
contract is unchanged. Residual mixed-workload/tail performance uncertainty remains
within the documented single-host evidence limits.

The formatter now consumes its owned field-result map and moves values into
records instead of deep-cloning and discarding the input. Seven per-field clone
sites and one conditional author-string clone are removed. Two Rust files:
20 lines deleted, 14 added, **net six removed**. No new abstraction. HTTP response
types, metadata members, projection, final sorting and conflict annotation stay
the same. See REVIEW.md for equivalence and the internal Rust signature tradeoff.

## Method and validation

- Optimized product builds passed for baseline `bcdcbba` and candidate `74355df`,
  in the same worktree/target directory with matching compiler/flags/lockfile.
- Full workspace clippy excluding the two ONNX packages passed with warnings
  denied. Formatting, source/function size, no-new-allows, Python syntax, archived
  hash, staged artifact and whitespace checks passed. No test suite added or run.
- 24 runs, four alternating adjacent pairs in each plain/encrypted/preloaded
  configuration, with configurations interleaved: **226,560 timed requests**.
- One baseline-created fixture/configuration, copied byte-identically before each
  run. Aggregate path/content digests checked before startup and after all runs.
- Each fixture contains 512 Hash and 512 HashRange flat records with four fields
  and 1 KiB bodies, plus 256 three-field HashRange records with structured JSON:
  16 child objects/document, nested maps/arrays, strings/numbers/booleans/nulls.
- Client CPU 0; all daemon threads/four configured workers on CPU 1. Host Cargo/
  performance lock excludes heavy build overlap. Fully warmed point reads use
  eight interleaved 512-request blocks per kind; point blocks were recorded
  during the run.
- Every query checks metadata field membership and atom identifier types. Every
  timed read checks returned values/count/order; ten complete pre-write response
  fingerprints per run compare fields, metadata and page payloads within each
  configuration. Includes misses/duplicates, projection and a hidden sort field.
- Requested queued/durable acknowledgements, explicit flush, Delete/Purge visibility,
  and readback after reopening all flat records and the nested partition passed.
- 75 graceful unforced zero-exit stops including fixtures. All owned synthetic
  homes were ownership/process-checked and cleaned; no user data used.
  An initial benchmark attempt expected a projection to omit its implicit hash
  field bucket. The baseline returned it, so that assertion aborted before any
  timed pair. The expectation was fixed and the entire experiment restarted.
  Its four stopped synthetic homes were cleaned; four additional graceful stops
  are excluded from the 75 above. No product change was needed and no
  initial-attempt samples enter the comparison. The excluded attempt artifacts were removed in the cleanup.

## Results (ms/request)

Values are medians of four per-run p50/p95 values. Change compares reported p50
medians; lower is better. Requested queued writes may acknowledge durable;
actual acknowledgement counts were recorded during the run.

### encrypted

| Workload | Before p50 | After p50 | Change | Before p95 | After p95 |
|---|---:|---:|---:|---:|---:|
| hash_durable_write1 | 18.355 | 19.051 | +3.8% | 25.232 | 44.047 |
| hash_fully_warm_point | 0.548 | 0.542 | -1.0% | 0.861 | 0.711 |
| nested_range16 | 4.102 | 3.879 | -5.4% | 5.666 | 4.644 |
| nested_range256 | 61.908 | 56.795 | -8.3% | 78.637 | 61.723 |
| nested_range64 | 15.508 | 14.533 | -6.3% | 19.116 | 17.125 |
| range_between64 | 8.802 | 8.671 | -1.5% | 10.706 | 11.669 |
| range_durable_write32 | 68.071 | 74.523 | +9.5% | 87.726 | 96.693 |
| range_fully_warm_point | 0.595 | 0.584 | -1.9% | 1.006 | 0.765 |
| range_fully_warm_queued_write32 | 66.476 | 68.577 | +3.2% | 87.737 | 89.865 |
| range_multiget32 | 3.763 | 3.714 | -1.3% | 4.405 | 4.930 |
| range_partition512 | 60.883 | 60.575 | -0.5% | 74.195 | 69.897 |
| range_prefix10 | 2.061 | 2.058 | -0.1% | 2.494 | 2.556 |
| range_prefix100 | 12.583 | 12.645 | +0.5% | 15.122 | 15.338 |

### plain

| Workload | Before p50 | After p50 | Change | Before p95 | After p95 |
|---|---:|---:|---:|---:|---:|
| hash_durable_write1 | 17.956 | 19.049 | +6.1% | 23.789 | 35.819 |
| hash_fully_warm_point | 0.537 | 0.538 | +0.3% | 0.725 | 0.742 |
| nested_range16 | 3.872 | 3.617 | -6.6% | 4.506 | 6.142 |
| nested_range256 | 57.265 | 54.521 | -4.8% | 68.568 | 97.515 |
| nested_range64 | 14.713 | 13.649 | -7.2% | 17.377 | 16.968 |
| range_between64 | 7.593 | 7.681 | +1.2% | 9.626 | 13.047 |
| range_durable_write32 | 72.514 | 68.190 | -6.0% | 137.283 | 100.618 |
| range_fully_warm_point | 0.564 | 0.569 | +0.9% | 0.770 | 1.139 |
| range_fully_warm_queued_write32 | 69.086 | 61.072 | -11.6% | 107.482 | 70.862 |
| range_multiget32 | 3.260 | 3.574 | +9.6% | 3.927 | 10.379 |
| range_partition512 | 54.962 | 55.132 | +0.3% | 86.929 | 85.766 |
| range_prefix10 | 1.894 | 1.894 | -0.0% | 2.213 | 2.533 |
| range_prefix100 | 11.255 | 11.001 | -2.3% | 18.620 | 17.011 |

### preload

| Workload | Before p50 | After p50 | Change | Before p95 | After p95 |
|---|---:|---:|---:|---:|---:|
| hash_durable_write1 | 18.751 | 18.747 | -0.0% | 27.825 | 33.654 |
| hash_fully_warm_point | 0.543 | 0.541 | -0.4% | 0.708 | 0.730 |
| nested_range16 | 4.108 | 3.861 | -6.0% | 5.110 | 4.397 |
| nested_range256 | 60.978 | 56.353 | -7.6% | 79.264 | 95.282 |
| nested_range64 | 15.673 | 14.318 | -8.6% | 19.898 | 17.033 |
| range_between64 | 8.083 | 8.164 | +1.0% | 9.588 | 11.744 |
| range_durable_write32 | 77.034 | 69.603 | -9.6% | 122.065 | 97.420 |
| range_fully_warm_point | 0.596 | 0.595 | -0.2% | 0.738 | 0.942 |
| range_fully_warm_queued_write32 | 70.995 | 64.188 | -9.6% | 103.907 | 81.184 |
| range_multiget32 | 3.742 | 4.762 | +27.3% | 4.441 | 9.652 |
| range_partition512 | 69.080 | 62.869 | -9.0% | 132.180 | 88.428 |
| range_prefix10 | 1.707 | 1.703 | -0.3% | 2.032 | 2.014 |
| range_prefix100 | 12.508 | 12.669 | +1.3% | 14.685 | 20.264 |

## Limits and reproduction

One host/client, small warm datasets, one server CPU shared by four workers,
uncontrolled OS caches and neighboring load. No statistical significance or
production concurrency/capacity claim. The benchmark includes flat and nested
documents but does not represent every schema/projection or conflict state.
Mixed-writer author choice is inspected in source; all fixture fields have one
writer population. This is not power-loss recovery evidence. No peak-memory
reduction was measured; avoided cloning is a structural claim from the source.

Bulk run artifacts, raw samples, response captures, receipts and their manifest
were removed from the current tree after PR #284. The original evidence remains
in that commit in Git history. This directory retains the reports, reusable
drivers and compact provenance.json with binary hashes, compiler/build command,
source revisions and shared helper hashes. Fixtures and private keys were never
committed. Benchmark outputs should be kept outside the source tree.

```bash
python3 docs/proofs/owned-query-format-benchmark-20261023/run.py --binaries /absolute/binaries --output /new/output --scratch /new/owned-homes --provenance docs/proofs/owned-query-format-benchmark-20261023/provenance.json
python3 docs/proofs/native-routing-benchmark-20261023/summarize.py /new/output
```

Select two allowed CPUs using --client-cpu / --server-cpu if necessary. Existing
output/scratch paths are refused. Binary hashes must match the provenance.

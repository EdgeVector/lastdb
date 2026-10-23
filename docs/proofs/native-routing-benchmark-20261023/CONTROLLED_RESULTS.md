# Controlled repeat: exact native routing

**Updated assessment: keep as a small maintainability cleanup.** The large earlier
point-read slowdown did not reproduce when both binaries used identical starting
files/identities and fixed CPU affinity. Read medians are effectively unchanged;
queued-write medians are modestly lower, with mixed individual pair signs. No
general speedup or statistical significance is claimed. The changed CPU setup is
a diagnostic, not proof of performance in the previous unpinned environment.

There are no new Rust changes. The same verified optimized binaries are used:
baseline `bd517062ed3b86aef3e431d1e2cd64fd9dd08aa5` and candidate `6965ecb93a4007e50130cc479b4a4040ad03c997`. Their SHA256s
match the earlier comparison and are retained in controlled_provenance.json.

## Controls and workload

- Create one owned synthetic fixture per plain/preload configuration using the
  baseline, seed 512 Hash and 512 HashRange records, flush, and stop gracefully.
- Copy its regular files to each fresh owned run home. Verify an aggregate digest
  of all paths/file contents before startup. Within each configuration every
  baseline and candidate starts with the same data, identity, OPE key, and file
  layout. Assert that fixtures remain unchanged after the experiment.
- Pin the client to CPU 0 and the daemon to CPU 1. All daemon threads inherit CPU 1;
  affinity is checked and recorded after each start. Four configured daemon workers
  therefore share one CPU. This differs from the unrestricted prior runs.
- Fully warm both schemas. Measure eight interleaved 512-request blocks for each
  point-read shape (4096 requests per kind/run), alternating which kind runs first
  in each block. Retain point_blocks.json to expose within-run variation.
- Also measure 256 prefix10 reads, 128 range64 reads, and 64 queued 32-record writes
  per run. Flush, stop/reopen, and read back all 512 range records after writes.
- Four adjacent pairs per configuration; alternate version order by pair and
  interleave plain/preload pairs. Hold the host Cargo/benchmark lock throughout;
  there is no compilation during measurements.

**16 complete measured runs, 138,240 timed requests.** All returned-value,
count/order, write acknowledgement, flush, and reopen checks passed. Both setup
fixtures and all measured homes were removed with ownership and process checks.
There are 34 verified graceful, unforced, zero-exit stops in this experiment,
including setup fixtures. No user data or primary homes were used.

An initial orchestration attempt imported a different cleanup module after an
import changed sys.path. It stopped after one unpaired baseline run whose product
checks passed. The helper import order was corrected, all three stopped synthetic
homes were ownership-verified and cleaned, and the experiment restarted completely.
That attempt is retained under controlled-initial-attempt with EXCLUDED.json, its
original driver snapshot, log, and raw samples; none enters the comparison below.

## Interpretation

Plain Hash point p50 is 0.516 → 0.515 ms (-0.1%), rather than the earlier +22.6%.
Its four paired changes are +1.6%, -0.1%, -2.0%, and +0.3%. The remaining point-read
medians are -0.2% to -0.3%. This is much more stable and does not reproduce the
large point penalty under these controls; it does not identify its original cause.

Queued batch32 p50 is 48.193 → 45.728 ms (-5.1%) plain and 76.243 → 73.161 ms
(-4.0%) preload. Individual pairs remain mixed: one plain pair and two preload
pairs are slower. Treat this as tentative, not a universal write improvement.
Range/prefix medians change by at most about 0.5%.

This clears the primary reason for the earlier hold in my assessment. The source
review's correctness/equivalence argument is unchanged, and the net 41 Rust-line
simplification is still a small cleanup. Recommendation: keep for clearer exact
routing; no claim of a general performance optimization. The PR remains draft
and unmerged for user review.

## Limits and reproduction

One host, one client, two fixed CPUs, four daemon workers sharing one server CPU,
small warm dataset, no CPU isolation from host neighbors, and OS caches not
controlled. Do not pool absolute timings with the earlier unpinned/new-identity
runs. Statistical significance and production concurrency/capacity are not
established. The prior correctness coverage limits still apply.

Table values are medians of four per-run p50/p95 statistics. Delta compares the
reported p50 medians. comparison.json retains individual pair deltas and raw run
medians; samples.json.gz and controlled_sample_manifest.json retain lossless data
and raw/compressed SHA256s. Fixtures themselves and their synthetic private keys
are not committed. Digests, metadata, shutdown, affinity, and cleanup receipts are.

```bash
python3 docs/proofs/native-routing-benchmark-20261023/controlled.py --binaries /absolute/binaries --output /new/control --scratch /new/owned-homes --client-cpu 0 --server-cpu 1 --rounds 4
python3 docs/proofs/native-routing-benchmark-20261023/summarize.py /new/control
```

Select two distinct CPUs allowed by the host. Existing output/scratch paths are
refused. Binary hashes must match the earlier provenance. The controlled driver
and shared benchmark/capture/cleanup helpers are in this repository. Python syntax,
artifact, size, and whitespace checks passed; the Rust source is unchanged.

## Results (ms/request)

### plain

| Workload | Before p50 | After p50 | Change | Before p95 | After p95 |
|---|---:|---:|---:|---:|---:|
| hash_fully_warm_point | 0.516 | 0.515 | -0.1% | 0.577 | 0.576 |
| range_between64 | 7.478 | 7.472 | -0.1% | 7.761 | 7.754 |
| range_fully_warm_point | 0.544 | 0.543 | -0.2% | 0.607 | 0.605 |
| range_fully_warm_queued_write32 | 48.193 | 45.728 | -5.1% | 108.848 | 109.345 |
| range_prefix10 | 1.912 | 1.911 | -0.0% | 2.018 | 2.027 |

### preload

| Workload | Before p50 | After p50 | Change | Before p95 | After p95 |
|---|---:|---:|---:|---:|---:|
| hash_fully_warm_point | 0.522 | 0.521 | -0.2% | 0.592 | 0.586 |
| range_between64 | 8.059 | 8.085 | +0.3% | 9.011 | 8.507 |
| range_fully_warm_point | 0.575 | 0.574 | -0.3% | 0.639 | 0.637 |
| range_fully_warm_queued_write32 | 76.243 | 73.161 | -4.0% | 133.338 | 99.201 |
| range_prefix10 | 1.686 | 1.694 | +0.5% | 1.842 | 1.800 |

## Through-flush queued batch throughput (records/s)

| Configuration | Before | After |
|---|---:|---:|
| plain | 516.45 | 525.27 |
| preload | 375.69 | 455.25 |

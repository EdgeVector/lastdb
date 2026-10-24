# Large-range continuation operations

This workflow builds and uses the actual LastDB daemon on GitHub Actions.
It uses new generated homes. It has no primary or archived data input.
It runs no test suite or fixture harness.
The driver refuses local product operations.

The workflow runs on relevant PR changes and manual dispatch.
It is independent of PR296 and the existing benchmark workflow.
Existing required CI gates stay unchanged.
Build or incomplete measurement failures fail this workflow.
A latency increase produces an advisory notice.
The Actions run supplies the build and benchmark result.

## Source and workload

The PR base and exact PR head use one Rust toolchain.
Each receives a fresh separate optimized `dev-release` target.
The cache holds dependency downloads only.
The manifest records source, executable, driver, and raw-file hashes.
The daemon version must contain its selected source revision.

Each variant receives 16,384 records with four fields and 1 KiB generated bodies.
Create mutations use batches of 128. Complete known-key reads use batches of 256.
Two matched pairs run for each plain and encrypted key configuration.
Pair order reverses in the second pair.
Automatic maintenance is off for both variants; the warm budget uses the product default.

The candidate walks the complete broad `HashRangeRange` and `HashRangePrefix` selectors with pages of 256.
Every page verifies actual fields, membership, order, and exclusive cursor advancement.
A complete `HashKey` walk also verifies the existing single-hash path on both variants.
The baseline may return its known nonretryable 503 `read_resource_limit` on broad range or prefix queries.
After Delete/Purge and reopen, the baseline may return the exact lower disk read-budget message for that same broad range.
The raw error remains visible. It never counts as a fast latency result.
Any other baseline error fails the operation.

Matched latency uses 32 identical 128-key intervals with 64-record pages.
Both variants first receive the same complete known-key warm prefill.
The report gives p50, p95, p99, maximum, and individual request times.
It compares medians of two per-run statistics. It does not pool requests across runs.
Elapsed time includes the socket and complete response bytes, with client decode and validation outside the timer.
The client, OS cache, and shared runner affect the result.

## Additional complete-value checks

The recipe includes late bounds near key 16000, half-open endpoints, prefixes, offsets, and exclusive cursors.
Cross-hash cursor ties use the public range-first/hash-second order.
Empty and inverted intervals, an empty range key, NUL bytes, and Unicode boundaries receive explicit selectors.
Delete, Purge, and an update precede a durable flush and graceful reopen.
Known-key reads verify every seeded coordinate, including absent deleted keys.
The candidate repeats its broad range after the reopen.

A further candidate reopen requests schema preload.
The report claims preload coverage only when actual ready-domain, key-count, and memory-selection counters confirm it.
Otherwise the result states that preload coverage remains unproved.
Linux process memory and native status remain separate observations.
Process `VmHWM` can include startup and seed work. No total-memory cap is claimed.
The recipe makes no power-loss, complete-merge, cross-device sync, cloud, or concurrent-mutation result.
This recipe does not measure received namespaces. The `received_ranges` recipe measures their bounded merge and current resource limits.
Offsets still walk and charge preceding selected keys. Cursors seek directly.
The complete-read key limit stays unchanged.

## Artifacts and limits

Full requests, responses, storage receipts, samples, and logs stay under `RUNNER_TEMP`, outside Git.
The verified artifact retains them for 14 days.
The job summary records the actual upload URL, archive SHA256, exact sources, and expiration.
The download must match the upload checksum. No separate report or `artifact.json` is created.
The artifact file manifest gives each retained file's hash and size.
Only useful source scripts and this method enter Git.
Generated evidence follows the shared 10-file/100,000-byte policy; this PR adds no raw evidence file.

Each owned run permits 1,200 seconds, 10,000 request rows, 1 GiB raw receipts, 512 MiB compressed receipts, and 4 MiB per response.
Exact-home cleanup permits 50,000 entries, depth 12, and 4 GiB of files after the owned daemon stops.
The whole measurement step permits 55 minutes; the workflow permits 90 minutes.
Raw evidence remains until upload verification; automatic runner cleanup occurs after the job.

Actions command:

```text
python3 candidate/scripts/benchmarks/range_continuation/run.py --baseline <base-binary> --candidate <head-binary> --baseline-revision <base-sha> --candidate-revision <head-sha> --output <RUNNER_TEMP/output>
```

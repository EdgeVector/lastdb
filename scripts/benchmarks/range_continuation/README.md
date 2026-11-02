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
Two matched pairs run on format 8 homes: blinded hash keys and typed range keys.
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
It also gives internal key requests, warm hits, file read calls, read bytes, and peak transient bytes per request.
Internal key requests count the logical storage requests, including repeats. They are not the native guard's examined-key count.
The report keeps process memory before/after the narrow phase and the process lifetime peak separate.
These observations do not establish an in-flight request peak or a total-memory cap.
It compares medians of two per-run statistics. It does not pool requests across runs.
Elapsed time includes the socket and complete response bytes, with client decode and validation outside the timer.
The client, OS cache, and shared runner affect the result.

## Additional complete-value checks

The recipe includes late bounds near key 16000, half-open endpoints, prefixes, offsets, and exclusive cursors.
Cross-hash cursor ties use the public range-first/hash-second order.
Empty and inverted intervals, an empty range key, NUL bytes, and Unicode boundaries receive explicit selectors.
Range-field-only and omitted-range-field projections verify the exact current response fields.
The current response also retains the declared primary hash field, `bucket`.
Both binaries must return that field and the requested fields, with no other field.
The selectors cover ranges, prefixes, and the single-hash path.
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

## Keyed field-condition comparison

Both existing range recipes also use at most 128 keys under the named hash `condition-reuse`.
The schema declares `score`, `optional`, and `never` in addition to the four original fields.
Only this hash receives the extra field values. The original timed range projections stay four fields.
Only this hash uses nested JSON bodies below 9 KiB. Each body contains eight 1,024-character payloads, arrays, and objects.
The body also contains signed and unsigned 64-bit boundaries, an integer above 2^53, a negative integer, and float values.
Every response preserves their exact values, integer or float types, and the sign of negative zero.
These bodies expose the cost of a full value copy. The original large-range body sizes stay unchanged.
The condition seed batches contain 64 records. A received seed line stays below its 1 MiB cap.
Scores have repeated numeric values. The optional field has missing, null, numeric, and string values.
The `never` field is always missing. Condition reads verify every live key under this hash.
Local Delete/Purge and an update precede these reads. The received recipe also removes values from selected sources.

Eight cases use the ordinary keyed query API. They cover a predicate with and without a projected condition field,
a primary-field condition, a captured empty field map, nullable absence, a numeric filter, and value order with a limit.
A separate value-order case omits its sort field from the projection.
An additional request verifies the API offset and limit after the condition stage.
Four additional requests use duplicate projection names for ordinary and conditional reads, with `include_tombstones` off and on.
These operational controls compare exact values and record metadata. They do not add timed cases or process restarts.
No condition request uses a cursor or an unkeyed selector.
Missing numeric values pass the numeric filter. Concrete null and string values fail it.
Equal numeric scores use ascending range keys as their tie order.

Each case has 32 warm requests per run after a complete condition reference read.
Each case also has one request after its own process restart, with schema preload off.
The eight fresh-process requests do not establish a latency distribution. The OS file cache stays uncontrolled.
Every returned value and the record metadata block must match the same-home reference.
The reference uses all fields and only `predicate_limit`. Its pass A has no secondary condition field to reuse.
The ordinary duplicate controls use a separate full-field reference. This reference preserves the ordinary record metadata contract.
The expected result includes the current primary hash helper. An omitted sort helper stays absent from fields.
Every record retains exactly `key`, `fields`, `metadata`, and `has_conflicts` (plus `conflicted_fields` on a conflicted record).
The key retains both hash and range coordinates. `metadata` is one block for the record, or `null`.
The report prints all condition latency and read-cost cells, plus process memory ranges, to the CI summary and logs.
After each timed cell, the recipe reads the current request ring through `/api/status?recent=1`.
It matches the exact request IDs and client name. Every warm cell requires 32 matches; every fresh-process cell requires one.
The existing samples retain the server duration and reported `hydrate_records_us` and `hydrate_format_us` phases.
The core query phase includes the value copy. The format phase measures the later response format work.
These status calls stay outside the timed and memory windows. Absent phase fields remain unreported; the report does not use zero.
The request counter does not measure the native guard's examined-key count.
Transient byte receipts omit captured field maps. Process snapshots do not measure an in-flight peak or a total-memory cap.
The public writer does not create legacy body tombstones. Empty-spine and legacy body-tombstone behavior remain source-review scope.

## Narrow-request tail diagnostics

All 64 narrow requests retain complete server phase records matched by request ID and client name.
The ring read follows the narrow memory-after observation and stays outside the latency and memory windows.
Each request also has bounded Linux process-counter observations before and after its HTTP timer.
These observations occur inside the narrow memory window and change the gaps between requests.
Compare the two product revisions with the same revised driver; old-driver absolute timings do not provide that control.
The summary prints each run separately, plus its five slowest requests with their own phases, read receipts, and counter deltas.
Missing counters and phases remain unreported. Host CPU and pressure values describe shared load and do not identify a cause alone.
Process counter precision, identity checks, scheduler-stat availability, and observation duration stay explicit.
No percentile is subtracted from another percentile. The current 256-entry ring must cover all 64 requests.

## Artifacts and limits

Full requests, responses, storage receipts, samples, and logs stay under `RUNNER_TEMP`, outside Git.
The verified artifact retains them for 14 days.
The job summary records the actual upload URL, archive SHA256, exact sources, and expiration.
The download must match the upload checksum. No separate report or `artifact.json` is created.
The artifact file manifest gives each retained file's hash and size.
Only useful source scripts and this method enter Git.
Generated evidence follows the shared 10-file/100,000-byte policy; this PR adds no raw evidence file.

Each owned run permits 1,200 seconds, 10,000 request entries, 1 GiB raw receipts, 512 MiB compressed receipts, and 4 MiB per response.
Exact-home cleanup permits 50,000 entries, depth 12, and 4 GiB of files after the owned daemon stops.
The whole measurement step permits 55 minutes; the workflow permits 90 minutes.
Raw evidence remains until upload verification; automatic runner cleanup occurs after the job.

Actions command:

```text
python3 candidate/scripts/benchmarks/range_continuation/run.py --baseline <base-binary> --candidate <head-binary> --baseline-revision <base-sha> --candidate-revision <head-sha> --output <RUNNER_TEMP/output>
```

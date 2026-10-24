# Received-data range operations

The workflow builds the exact PR base and head on GitHub Actions.
It uses the actual LastDB daemon and ordinary owner-socket reads.
Each run uses a new generated home under `RUNNER_TEMP`.
It has no primary, archive, cloud, or real sender input.
The driver and seed executable refuse local product operations.
No test suite or fixture harness runs.

## Data and source method

Two received sources each hold 16,384 generated keys under one hash.
The sources overlap at 8,192 keys. The total range has 24,576 keys.
Local records duplicate every sixteenth key, plus selected boundary keys.
The four fields identify the source and contain a generated 256-byte body.
Every returned field must match the expected source precedence.
Local values take precedence. Source zero precedes source one.
An alternate schema shares the hash field through an explicit field mapper.
It declares another range field and supplies local primary values without the first schema's range field.
Those local primary values take precedence before schema membership excludes them.
The recipe verifies 640 excluded keys before a valid refill and a 576-key all-hidden tail.
Received values at those same keys must remain hidden.

The public mutation API does not accept a received namespace override.
The CI-only `received_range_seed` executable therefore uses the ordinary mutation manager.
It writes batches of 128 to two exact generated `from:` namespaces.
It then writes the generated active receiver subscriptions and completes the durability barrier.
It does not edit snapshot bytes or contact a sender.
The generated subscriptions contain no cloud grant or encryption secret.
The homes have no cloud configuration or sync engine.

One candidate-built helper writes identical generated mutation input into both homes.
The manifest records the helper revision, executable SHA256, and input hashes.
Seed time is excluded from all query latency results.
The exact base and candidate daemons perform every measured read.
One toolchain builds both daemons into separate fresh optimized targets.
Only dependency downloads use the cache.

## Range operations and latency

Each candidate walks the complete range, prefix, and single-hash selectors.
Each page contains at most 256 records.
Every page verifies values, duplicate removal, order, count, and exclusive cursor continuation.
The baseline may return its known nonretryable 503 `read_resource_limit` for these broad selectors.
This includes its exact native 10,000-key work limit, the complete-read continuation limit, or the disk-read budget refusal.
That operation remains unavailable and never becomes a fast latency sample.
An unexpected error or an incomplete candidate operation fails the workflow.

Both variants receive a complete known-key prefill immediately before the latency sample, after the broad walks.
The latency comparison uses 32 identical 128-key intervals with 64-record pages.
Two matched pairs run for plain and encrypted key configurations.
The second pair reverses the run order.
Identical mutation, expected-value, and timed-request hashes are required for each pair.
The CI summary gives medians of per-run p50, p95, and p99 values.
It also gives internal key requests, warm hits, file read calls, read bytes, and peak transient bytes per request.
Internal key requests count the logical storage requests, including repeats. They are not the native guard's examined-key count.
The summary keeps process memory before/after the narrow phase and the process lifetime peak separate.
These observations do not establish an in-flight request peak or a total-memory cap.
A latency increase remains advisory. Incomplete measurements fail.
The existing `benchmark-complete` gate remains unchanged.

Candidate discovery retains at most four source pages within one query.
Each retained page contains at most 256 tips, with their keys and metadata.
A refill reuses future keys and reads only the missing part of a short page.
Covered candidate tips use the existing body reader without a second tip read.
The covered interval also proves absent keys. Unknown intervals use the exact reader.
Additional sources retain the ordinary bounded read path.
Four concurrent source reads and the merged page add a fixed amount of temporary memory.
The existing per-call key and byte work limits remain unchanged.

Additional selectors cover source overlap, source-only ranges, offsets after duplicate removal, and cross-hash cursor ties.
Range-field-only and omitted-range-field projections verify the exact current response fields and source values.
The current response also retains the declared primary hash field, `bucket`.
Both binaries must return that field and the requested fields, with no other field.
The selectors cover ranges, prefixes, and the single-hash path.
Empty keys, empty and inverted intervals, NUL bytes, and Unicode receive explicit selectors.
Source-specific Delete, Purge, and local/received updates precede a graceful reopen.
Complete known-key reads then verify every generated coordinate, including deleted coordinates.
The candidate repeats the broad selectors after the reopen.

A further candidate reopen requests schema preload.
Only actual ready-domain, key-count, and memory-selection counters support a preload result.
Otherwise the summary states that received preload selection remains unproved.
Linux process memory and native memory status remain separate observations.
No total-memory cap, power-loss recovery, cloud transport, or concurrent mutation result is claimed.
The public mutation path rejects reserved body tombstone values.
The recipe does not manufacture legacy tombstone bytes. Body-drop accounting remains source review only.

## Keyed field conditions

The recipe also seeds at most 128 keys under the named hash `condition-reuse` in each received source.
Local values replace every sixteenth key. Source-specific field values and file metadata identify the selected source.
A source-zero Delete exposes source one at key 2. A local Delete exposes source zero at key 0.
A source-one Purge leaves the source-zero value at key 3.
The condition comparison runs after those changes and a graceful reopen, before candidate-only preload.
It uses the [shared condition method](../range_continuation/README.md#keyed-field-condition-comparison):
eight cases, 32 warm requests per case, and one fresh-process request per case after separate restarts.
Only this fixed hash uses nested JSON bodies below 9 KiB and 64-record seed batches.
Four operational controls verify duplicate projection names with `include_tombstones` off and on.
The shared checks also preserve the exact response envelope, selected author, numeric boundaries, float types, and negative zero.
Each timed cell requires complete request ID coverage from the current request ring. The report includes core query and format phases.
The exact same-home metadata reference uses the existing secondary reader.
Both full CI summaries print to the job logs. The original four-field range timing selectors stay unchanged.

## Narrow-request tail diagnostics

The shared [narrow diagnostics](../range_continuation/README.md#narrow-request-tail-diagnostics) retain all 64 server records by exact request ID.
Each HTTP request has bounded process-counter observations outside its timer and inside the narrow memory window.
The summary shows each run and its five slowest calls, with complete phases, receipt work, counter availability, and timestamps.
Both product revisions use the same revised driver. The observations change gaps between requests, so old-driver absolute times are a separate experiment.
Shared host pressure indicates correlation; it does not establish a request's cause. Missing counters and phases stay unreported.

## Output and bounds

The raw generated inputs, requests, responses, native read receipts, logs, and samples stay outside Git.
The workflow retains the artifact for 14 days.
The CI summary records exact source revisions, the artifact URL, archive SHA256, and expiration.
The workflow downloads the artifact and checks its SHA256 before runner cleanup.
No separate routine report or `artifact.json` is created.
Only source scripts and this durable method enter Git.

Each run permits 1,200 seconds, 10,000 request rows, 1 GiB raw receipts, and 512 MiB compressed receipts.
A response permits 4 MiB. A seed helper invocation permits 600 seconds and 50,000 mutations.
The input permits 128 MiB and 1,024 batches, each at most 1 MiB and 128 mutations.
Exact-home cleanup permits 50,000 entries, depth 12, and 4 GiB after the owned process stops.
The measurement step permits 55 minutes. The workflow permits 90 minutes.

Actions command:

```text
python3 candidate/scripts/benchmarks/received_ranges/run.py --baseline <base-daemon> --candidate <head-daemon> --seed-helper <head-helper> --baseline-revision <base-sha> --candidate-revision <head-sha> --output <RUNNER_TEMP/output>
```

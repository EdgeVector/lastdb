# Receipt pruning and one-commit cache: follow-up method

The user authorized a longer default-maintenance comparison, a batch profile, and a bounded cache trial.
This comparison starts with the original journal-free writer.
It does not use the primary database or its archive.
All product requests use fresh owned homes and generated data.
No test suite or fixture harness forms part of this work.

## Sources and builds

| Variant | Product source | Purpose |
|---|---|---|
| Baseline | `08d8436ccce9e2493e187a6b0133dc93f47c410f` | Journal-free writer before receipt pruning |
| Draft | `a1941e3a8742d4da4743a02db99153119bdf034b` | Receipt pruning and periodic cleanup, with a new resolver per key |
| Cache | `e79b711517519c9ce27d0f6f1b405a364be3ba91` | The draft plus one bounded reference cache per publication |

The final baseline and cache builds use identical release commands, features, lockfile, and toolchain.
Each uses its own empty target directory under owned temporary storage.
The local dependency manifests must belong to the corresponding clean source worktree.
Source libraries and executables do not cross from one target directory to the other.
The accepted draft executable is an exact hash-verified copy of the earlier measured release executable.
Its original build receipts remain part of the provenance.
The draft is not a new build in this round.
The standard exclusive host Cargo lock covers each build, profile, and product run.
Host queue time and build time stay outside the product request-latency measurements.

An earlier shared-target attempt rebuilt only the node package.
It retained old storage libraries despite a new node version stamp.
The first product operation detected the old receipt format and stopped the sequence.
Those rejected build and profile receipts contribute no cache performance result.
Their original bytes and the rejection reason remain available separately.

Build acceptance checks the exact source commit and clean tracked state.
The controlled coordinator checks the accepted build receipts and executable hashes before its six-run sequence.
Each run records its source revision and executable hash.
Product format checks cover both the node writer and the physical writer before benchmark acceptance.
Build preparation, command, compiler, executable hash, and version remain in separate receipts.
Product source stays fixed during all accepted cache runs.
Later documentation commits do not change the measured product source.

## Profile

Each variant receives the same 256 durable requests with 32 updates per request.
The home starts with 512 keys, four fields, and a 1 KiB body.
Automatic maintenance stays off; the flush interval is 500 ms.
The native sampler observes a fixed interval within that work.
The diagnostic records the actual sampler interval and the requests that overlap it.
The fixed-work CPU interval covers all 256 requests and the final flush, before the driver waits for the profiler.
The counters cover only the node process; they exclude the sampler process and its symbol analysis.

The macOS process counters use Mach ticks on this host.
Each diagnostic captures the timebase and converts the raw user and system deltas to seconds.
Native sample counts describe wall-stack presence, not CPU percentages.
The stack analysis counts the first selected context on each branch to prevent duplicate attribution.
An inclusive subpath count must not be added to its parent count.
Profile latency is diagnostic and stays separate from normal request latency.

## Controlled comparison

The original controlled workload remains unchanged.
It uses 512 keys, four fields, 1 KiB bodies, four socket workers, a 16 MiB stack, and a 500 ms flush.
Automatic maintenance stays off to isolate foreground work.
The order is baseline 1, cache 1, cache 2, baseline 2, baseline 3, cache 3.
Each of the six runs uses a fresh home.

The six original phases cover durable single requests, durable 32-key batches, queued single requests, and queued 32-key batches.
They also cover concurrent requests during compaction and durable single requests after compaction.
Separate phases cover focused compaction delay and single and batch deletes.
Every phase checks exact values for all known keys and ordered ranges under one hash.
The run also checks a graceful reopen.
The report gives medians across independent runs, per-run ranges, and paired changes; it does not pool requests.

The earlier three-pair draft comparison remains historical evidence in its original proof folder.
It is not a repeated draft sample in this round.
The recommendation compares the cache directly with the original journal-free baseline.

## Default automatic maintenance

Each variant receives one descriptive run with 64 keys and the same deterministic input order.
The run sends 330 durable 32-key requests, nominally one request every two seconds.
The first request is due at two seconds; the last is due at 660 seconds.
The driver retains late requests and records their start delay.
It then observes a 60-second quiet tail from the actual final durable ack.
No manual pack or reclaim request occurs.

The default dependency high-water mark stays 4,096, the startup delay stays 30 seconds, and the grace period stays zero.
The orphan minimum age stays 600,000 ms.
The draft and cache runs span the first default repeat opportunity after startup.
The baseline has only the startup orphan sweep.
Timer deferral can delay completion.
Those settings define no exact deletion deadline.
This is one observed maintenance cycle per variant, not a steady-state or significance result.
Atom-key deletion records can still increase while the logical key count stays fixed.

Every 30 seconds, the driver captures native maintenance status, process memory, file counts, logical bytes, and allocated bytes.
The inventory records its start and end times and counts entries that disappear during concurrent cleanup.
It has finite depth, time, file, entry, and byte limits.
It does not claim an atomic inventory during writes.
Two final equal inventories require an idle worker after the fixed quiet tail and final flush.
Periodic exact reads, ordered ranges, and a final reopen check the known values.

All request intervals remain available for tail counts, maxima, cadence delay, and maintenance-overlap review.
The pack log follows pack and reclaim; its duration covers only the pack call.
Any reconstructed pack window is approximate, and status changes supply interval bounds.
Neither identifies the exact internal write-lock interval.
The report gives observed retained-size slopes and cleanup cycles, with the sample interval and races stated.

## Safety, bounds, and cleanup

The cache keeps checked metadata only within one snapshot publication.
It retains no value bodies and adds no global cache or disk file.
Every old work charge and integrity check remains in force.
The cache resets at 24 pages or part identities before the next logical resolver step.
Its source bound is 26 pages, 25 checked part identities, and 32 file handles.
The page bytes total about 416 KiB; this is not a complete process-memory cap.

The six prior product operations repeat on the cache executable.
They cover old-root holds, backup restore, a forced stop after flush, shared file parts, large records, and overflow key chains.
They also cover old-format count admission, old-program refusal, ordinary automatic reclaim, and repeated orphan cleanup.
A forced process stop does not prove power-loss behavior.

Exact ownership, executable identity, request receipts, and cleanup receipts remain available for review.
Each owned node stops before its temporary home leaves disk.
Proof packaging preserves the original summary bytes through a verified decompression round trip.
The primary, archive, cloud activation, shared services, and release settings stay unchanged.

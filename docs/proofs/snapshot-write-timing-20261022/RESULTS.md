# Durable write timing, October 22, 2026

This capture measures the journal-free durable-write branch.
The product changes add numeric diagnostics only.
The disk format, write order, memory publication, and disk barriers stay unchanged.
The diagnostics require both `LASTDB_SNAPSHOT_TIMING=1` and `LASTDB_ISOLATED_COPY=1`.
No schema, key, value, identity, or path enters a timing event.

## Results

The control repeats the earlier 64.1 ms result at 65.03 ms.
The profiled run averages 65.71 ms.
The largest individual cost is the shared-writer wait, at 24.24 ms.
Background disk commits overlap 23.66 ms of that wait.
The foreground disk commit itself averages 32.69 ms.
The results still miss the 20 ms average target.

| Whole request | Average | Median | P99 | Maximum | Write errors |
|---|---:|---:|---:|---:|---:|
| Capture off, automatic maintenance on | 65.03 ms | 59.88 ms | 136.48 ms | 296.64 ms | 0 |
| Capture on, automatic maintenance on | 65.71 ms | 62.20 ms | 138.43 ms | 175.36 ms | 0 |
| Capture on, automatic maintenance off | 56.41 ms | 48.05 ms | 147.93 ms | 293.73 ms | 0 |

Each mode contains 1,000 complete requests across 64 schemas.
The same binary supplies all three rows.
The first two averages differ by 0.69 ms, but one observation per mode does not isolate diagnostic overhead.
The final row disables automatic compaction and cleanup only for its owned generated home.
It keeps the product's change-feed writes active.

### Exclusive request budget

These phase averages add to the full 65.71 ms request average.
The right column describes one actual request, number 400, at the full-request P99.
It does not add separate phase percentiles.

| Step | Average | Actual P99 request |
|---|---:|---:|
| Wait for the shared writer | 24.24 ms | 86.01 ms |
| Prepare records, capacity, and acceptance | 6.42 ms | 5.73 ms |
| Write the payload files | 2.06 ms | 1.10 ms |
| Update lookup files and prepare dependencies | 10.20 ms | 12.07 ms |
| Read and write the recovery file lists | 8.74 ms | 3.63 ms |
| Publish the selected disk root | 5.24 ms | 10.26 ms |
| Complete the final disk drain | 6.30 ms | 10.77 ms |
| Publish memory and release its disk reservation | 0.09 ms | 0.11 ms |
| Other request, queue, delivery, and response work | 2.42 ms | 8.75 ms |
| **Complete request** | **65.71 ms** | **138.43 ms** |

The raw [summary](summary.json) retains every finer phase and every exact value.
The [per-request budget](profile/request-breakdown.json) retains all 1,000 joined client intervals.
The analyzer matches all 1,000 requests in each profiled mode without a missing or shared foreground commit.
Every exclusive sum matches its client duration within one microsecond.

### Background writes still delay the next foreground write

The route sends each successful write's change event to the existing change-feed queue.
It submits that job after the product write and before it returns the response.
The queue worker writes the notification and its tip through `background_scope`.
That scope now requires a complete snapshot commit, as the product write does.
The queue's native disk worker holds the same global writer gate during its commit.
The next foreground request therefore waits while that background commit completes.

Both profiled runs contain 999 background commits during the 1,000-request measurement interval.
Every such background commit contains exactly two storage operations.
That matches the notification and tip pair in the change-feed writer.
Their synchronous disk intervals overlap 23.66 ms of the 24.24 ms average writer-admission wait.
For the actual P99 request, that overlap is 85.78 ms of its 86.01 ms admission wait.
The interval calculation excludes background work from the foreground commit's own disk totals.

Relevant source:

- [Route submits the existing notification job](../../../lastdb_node/src/exec/batch_mutation_routes.rs).
- [Existing queue and worker](../../../lastdb_node/src/change_feed_queue.rs).
- [Notification and tip share the background scope](../../../fold_db/crates/core/src/db_operations/change_feed.rs).
- [Complete native disk commits hold the shared writer](../../../fold_db/crates/core/src/storage/disk_pack/queue.rs).

The background label does not make the disk work independent of foreground writes.
This is a remaining source of shared-writer contention beyond compaction.
The capture adds no storage journal and no new notification queue.

### Actual durability calls within the disk phases

| Child disk work | Average per foreground commit |
|---|---:|
| Full device flush | 6.29 ms |
| Bounded file-set flushes, including worker setup | 1.30 ms |
| Ordered publication barriers | 2.91 ms |
| **All measured child durability calls** | **10.50 ms** |

These values are already inside the exclusive disk phases above.
They must not be added to the 65.71 ms total.
Each foreground commit performs one full device flush in this run.
The 32.69 ms foreground commit also includes file reads, creation, writes, metadata preparation, checks, and memory publication.
The wall-time capture does not separate CPU execution from waits inside those other operations.

### Compaction and size limits

Five automatic compaction attempts defer during the profiled measurement.
The automatic-maintenance-off mode records no such deferral.
Its average falls to 56.41 ms, while its P99 is 147.93 ms.
The remaining average admission wait is 21.71 ms; background commits overlap 21.23 ms of it.
This diagnostic mode does not remove the main shared-writer wait or meet 20 ms.
The single mode pair does not establish the exact causal cost of compaction.

The profiled home grows from 720 to 13,996 required file parts during the capture.
A foreground commit writes an average 20.45 lookup metadata pages and names 7,224 required file parts in its recovery list.
The first 100 update calls, after the 64 initial creates, average 48.60 ms.
The final 100 update calls average 82.80 ms.
This observation does not isolate file-count growth from concurrent maintenance or host variation.
The file-list and lookup phases remain concrete targets for further work.

### Recovery and disposition

All 3,000 measured requests return durable acks without a write error.
All 195 final known records match after the three forced stops.
Every next durable write advances the author clock.
Every final shutdown completes its flush and returns exit code zero.
Every stopped home contains no storage journal directory or journal tail.
The [checks](checks.json) record the optimized build, Clippy, format, size, and redaction results.

This work measures the current path and adds opt-in diagnostics.
It changes no write order, disk format, durability barrier, or ack guarantee.
Nothing is pushed or merged.

## Method

The same optimized daemon runs three modes on fresh generated homes.
The control disables timing capture and keeps automatic maintenance active.
The profile enables timing capture and keeps automatic maintenance active.
The final mode enables timing capture and disables automatic maintenance for that isolated node.
The primary and its archive stay unchanged.

Each mode sends 1,000 single-record requests from one client across 64 distinct schemas.
The schemas have distinct field identities and 64-byte values.
The node uses eight socket workers and the 4,096-part maintenance trigger.
Every request omits the durability option and must receive a durable ack.
All samples remain in the result, including the first calls that create records.
The other calls update the same known keys.

Each mode checks final values by key, writes a durable sentinel, and stops its owned daemon with SIGKILL.
After reopen, all final values must match and the next durable write must advance the author clock.
The final shutdown must complete its flush and exit with code zero.
The stopped home must contain no storage journal directory or journal tail.
These checks establish no physical power-loss, cloud, or primary result.

The host exclusion prevents cooperative compilers and other cooperative captures from overlap with this matrix.
Other host work and OS cache state remain uncontrolled.
One run per mode describes this capture and does not establish a universal latency bound.
The capture-on and capture-off runs also include host variation; their difference does not isolate instrumentation overhead.

## Phase accounting

The analyzer joins each client interval to one foreground native scope and one successful native commit.
It checks the numeric sequence, complete time containment, and one request per commit.
It refuses a missing event, a reused commit, or an exclusive sum that differs from the client duration.
The capture includes the request path, queue admission, private preparation, disk publication, and memory publication.

The disk phases separate payload files, the prior recovery file list, lookup files, the new recovery file list, and root publication.
Lookup work includes the dependency map that prepares the complete file list.
The final drain covers the remaining file flushes and the full device flush after root publication.
Small residuals retain setup, queue dispatch, delivery, socket, response, and diagnostic output costs.

The file-set, ordered-barrier, and full-device timings describe child work inside the disk phases.
Those child values must not be added to the exclusive phase totals.
Each child total belongs to the foreground commit thread; concurrent compaction cannot enter those totals.
File-set time includes the bounded flush workers and their setup.

The average phase values add to the average full request duration.
The P99 breakdown uses the actual request at the nearest-rank P99 of the full request distribution.
Separate phase percentiles cannot be added to obtain a request percentile.

## Evidence

The [capture](capture.py) retains the real daemon method.
The [analyzer](analyze.py) derives the phase totals from client receipts and numeric events.
Each mode retains every request receipt, exact-key recovery results, and a compressed daemon log.
The plan records the source, executable hash, mode order, scratch owner, and host load.
The measured product source is `0021d5371e97844922c72f53df8cbecdf0811175`.
The executable SHA-256 is `b9506269d266b4b51c688a817c0dca293c83c696cf4e6b180cd2e38e7937408c`.
The build uses the same `dev-release` profile as the earlier 64.1 ms measurement.
The cleanup receipts preserve the owned paths and confirm removal of the generated homes and pinned binary.

# Storage parts and automatic maintenance

A generation receipt names at most 65,536 parts. A write that exceeds that limit is refused.
The node copies recent parts before that limit and keeps a separate reclaim queue.
The [concurrent compaction protocol](lastdb-concurrent-compaction.md) describes the copy and handoff.

## Parts and receipts

A part is a file named `part-<8 hex digits>` under `directory`, `locator`, or `packs`.
A part holds at most 8 MiB. The snapshot format has no native recovery journal.
Each receipt entry uses 80 bytes. Each publication writes one complete receipt and one `CURRENT` root.
Earlier generations stay until reclaim no longer needs them.

A batch with payloads normally creates four parts: data, its map, directory, and locator.
A batch with only tombstones needs no payload output.
A large batch can need additional data or tree parts.
New hash groups share an existing locator object. They do not each create an object.
One API request can still require more than one stored batch.

## Compaction and reclaim

The first automatic copy after a reopen covers the full home.
Later copies move recent tree pages and payloads. They retain older immutable subtrees.
Eight recent outputs merge above the base boundary.
Substantial byte growth or record changes start another full copy.
The copy runs outside the write gate. A bounded catch-up precedes the final root publication.
Reads and writes can continue during the copy and reclaim.

`POST /api/db/compact` defaults to a preview.
`{"dry_run":false}` runs a full copy and reclaim.
`{"dry_run":false,"full_copy":false}` permits a recent tier.
`{"dry_run":false,"reclaim_only":true}` runs reclaim alone.
A named collection is refused because native parts share one root.
`lastdb db compact --execute` uses the whole native home.

## The worker and admission

One maintenance owner excludes another copy or cleanup.
A pack has priority over old generations that wait for reclaim.
The normal trigger starts at 4,096 parts and stays below half of the hard limit.
It accounts for the last output size and copy cost.
The observed part rate and previous copy time can move the trigger earlier.
A new automatic run also needs fresh growth after the previous run.

The first step waits 30 seconds unless the home already exceeds the soft limit.
After a copy, reclaim retires a bounded set of old generations.
The next calls adjust their slice from 8 to 1,024 generations according to their duration.
Local read holds retain their complete generations. They do not defer reclaim.
An external shared lifetime lock can defer it.
A failed reclaim retains its back-off and moves the trigger to 75 percent of the hard limit.
The status keeps that error until reclaim succeeds.

At 85 percent of the hard limit, a new automatic-maintenance write receives HTTP 503.
The error is `compaction_behind`, with `retryable: true` and `retry_after_secs`.
The refusal wakes the worker. The caller can retry after the part count falls.
The copy does not hold that request for one minute.
At the hard limit, native admission still returns `write_resource_limit`.
With automatic maintenance off, the original hard-cap admission remains in force.

A supervisor retains the existing restart and back-off behavior after a worker panic.
A failed copy leaves the writer healthy when the old `CURRENT` still names the selected state.
An uncertain root publication requires recovery before further writes.

## Status

`lastdb status` shows the part count, limit, trigger, peak, failures, and last run.
It also shows the active compaction phase and last swap time.
`GET /api/db/native-status` exposes the fields under `native_disk.parts`.
The fields include `compaction_phase`, copy time, swap time, maximum swap time,
catch-up rounds, changed locators, and the tier boundary.
The part and phase fields come from current atomics rather than the sampler cache.

## Unreferenced files

An interrupted copy can leave private files that no receipt names.
The orphan sweep defers while the compaction lease protects private output.
After that lease ends, the existing age and receipt checks govern removal.
An active local reader retains its generation and does not defer the sweep.
An unreadable receipt ends the sweep without a removal.

Reclaim removes only provably unfinished receipts and exact obsolete paths.
An unknown file, torn first page, or inconsistent complete receipt produces a named error.
The snapshot format has no `TAIL` or archived native journal to repair.

## Settings

| Name | Default | Meaning |
|---|---|---|
| `LASTDB_DEPENDENCY_HIGH_WATER` | 4,096 | Initial trigger, subject to the live-size and rate bounds. |
| `LASTDB_AUTO_MAINTENANCE` | on | `0` or `false` disables the worker. |
| `LASTDB_AUTO_MAINTENANCE_GRACE_MS` | 0 | Grace for automatic reclaim. Active references always remain. |
| `LASTDB_ORPHAN_MIN_AGE_MS` | 600,000 | Minimum age for an unreferenced part. |
| `LASTDB_AUTO_MAINTENANCE_START_DELAY_SECS` | 30 | Wait before the first maintenance step. |

## Limits

A full copy still reads every retained record. Its duration grows with the home.
The copy holds no write gate during that work.
The final handoff permits at most 4,096 changed locators or new hash groups.
A larger final delta is refused without publication.
A configured disk cap still adds a bounded file inventory to space reservations.
The source limit is 67,108,864 records and 65,536 hash groups.
The receipt remains limited to 65,536 parts.
A named low-space refusal preserves the current root.

Current product receipts reside in `docs/proofs/atom-lifecycle-completion-20261022`.
The measurements below describe earlier journal-backed builds. They do not measure this snapshot implementation.

## Historical measurements before concurrent snapshot compaction

Synthetic homes, one node each, a shared development host, so latencies carry the load of the host. The part
counts do not depend on the load.

| Workload | Before automatic maintenance | After |
|---|---|---|
| durable, 1 operation per request, 2 clients | stopped after 4,205 requests at 65,520 parts | no stop in the runs below |
| durable, 10 operations per request, 4 clients | stopped after 20,100 mutations at 65,452 parts | no stop |
| default acknowledgement, 10 operations per request, 8 clients | stopped after 26,090 mutations at 65,436 parts | no stop |
| 206,000 acknowledged mutations, 20 kills and 7 restarts | not run | parts never above 4,840, no capacity error |
| 104,200 purge operations | not run | parts never above 4,107 |
| a home stuck at 65,441 parts | refused every write | healthy in 62 s, pack 65,441 to 21 parts in 38 s |
| a home stuck at 65,533 parts with 20,390 old generations (52 GB), a cold clone on a shared host | node ready after 304 s, no write accepted in 30 minutes, no pack, about 300 generations retired each minute | node ready and first write accepted after 66 s (pack 65,533 to 12 parts in 53 s), all 20,390 generations retired in 15 minutes (about 1,300 each minute), writes accepted all along |
| 3 tight read loops, 4 durable writers, trigger 200, 150 s | parts up to 2,914, 1 run, 1,239 old generations left | parts up to 235, 12 runs, 62 old generations left; reads 0.4 ms median in both; no error |
| a home at the limit with `LASTDB_AUTO_MAINTENANCE=0` | every write waits 90 s, then "handler deadline exceeded" | the named 503 body after 5.3 s |
| an unknown file in `generations/`, 4 durable writers, trigger 200 | 5 packs and 5 failures in 80 s | 1 pack, then the trigger rises to 49,152, 2 retries in 90 s, and the error is in the status; after the file left, 835 generations were retired in 20 s |

Write latency with the worker on or off differs by less than the round-to-round spread of about 10 percent
(`durable`, 1 operation, 2 clients: median 2,436 ms before, 2,510 ms after; default acknowledgement, 10 operations,
4 clients: 5,418 ms before, 5,394 ms after).

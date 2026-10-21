# Storage parts and automatic maintenance

A native home stops taking writes when the part count of its newest generation
reaches 65,536. Before automatic maintenance, a home reached that limit after
about 20,000 durable mutations, and a restart did not clear it. This page says
what a part is, what adds and removes parts, how the node keeps the count low,
what a writer sees near the limit, and which settings exist.
The file protocol of the storage engine is in `docs/lastdb-disk-file-lifetime.md`.

## Parts, dependencies and the receipt

| Term | Meaning |
|---|---|
| Part | One file of the storage engine: `part-<8 hex digits>` under `directory/`, `locator/`, `packs/` or `journal/`. A part holds at most 8 MiB. |
| Generation | One published state of the home. `CURRENT` names the newest generation. |
| Receipt | The file that a generation owns. It lists every part that the generation needs. |
| Dependency | One entry of a receipt: the kind, the object, the batch and the number of one part. Each entry takes 80 bytes. |

A receipt holds at most 65,536 dependencies (`MAX_DEPENDENCY_PARTS`). A write that would pass the
limit is refused.

## What adds parts

Each publication copies every earlier entry to the new receipt and adds its own:

- two journal parts for each batch (the data part and the map part),
- one new directory part,
- one new locator part for each locator tree that the batch edits.

A small durable request makes two or three batches and about 13 parts. A write of ten keys makes about
three parts for each key. A purge adds parts like any write. A restart adds a few parts for its own
writes. An idle wait and a kill do not merge parts.

Every publication writes the whole receipt again, and the earlier receipts stay on disk until reclaim. The
part count is therefore a hard limit and also a cost of each write.

## What removes parts

Only compaction (the pack step) removes dependencies from a later receipt. The pack step copies every live
record again into new parts, so its cost grows with the number of records and with the number of hash groups
that hold data. It holds the write gate. Reclaim then removes the files of old generations that no live
generation needs.

- `POST /api/db/compact` with `{"dry_run":false}` runs pack and reclaim. `{"dry_run":false,"reclaim_only":true}`
  runs reclaim only. `grace_ms` sets an explicit grace. A named collection is refused, because native parts
  share one committed root (so `lastdb db compact --collection <name>` does not work on a native home).
- `lastdb-pack-maintain` works on a stopped home (`compact`, `reclaim`, `inventory`, and more).
- The node also runs this work by itself (next section).

The pack step takes the number of live records into account. The compaction quote allows at most 67,108,864
records across the home, 65,536 hash groups, and 1,000,000 records in one group. A home above those numbers
cannot pack.

## The ledger and the worker

The runtime keeps a ledger of the part count after each publication (an atomic read, no lock). One worker
task runs maintenance when the count passes the trigger.

- The trigger starts at the high-water mark, 4,096 parts (`LASTDB_DEPENDENCY_HIGH_WATER`), at most 75 percent
  of the limit.
- The trigger rises to twice the parts that the last run left, so a home with a large live set does not loop.
- It also rises with the square root of twenty times the records that the last run copied (about 14,000 parts
  for ten million records), because a larger home costs more to pack and so packs less often.
- A new run needs at least 1,024 parts of growth after the last run (at most half the high-water mark).
- The first pack of a process waits 30 seconds, so a restart is not blocked behind it. A home that is above
  90 percent of the limit does not wait.
- A run packs, then reclaims in slices of 32 old generations, oldest first. The worker releases the write gate
  between slices, so writers and readers run while old receipts retire.
- When the worker starts it first reclaims the old generations that an earlier run left, and then it sweeps
  unreferenced part files.
- A reclaim that active reads defer is tried again after 1, 2, 4, 8, 16 and then every 30 seconds, for about
  20 minutes.
- A failed run waits 2 to 60 seconds before the next try and keeps the writer healthy when `CURRENT` still names
  the old root.

A home that already sits at the limit starts, waits for the pack step, and accepts writes when it ends. In one
test the first start took 62 seconds.

## What a writer sees

| Part count | Behavior |
|---|---|
| below the trigger | normal |
| at the trigger | the worker runs; writers wait on the write gate while a step holds it |
| above 90 percent of the limit (58,982 parts) | a new write waits for the worker, for at most one minute, before its own admission; it does not wait when the last run failed |
| at the limit | the write is refused with HTTP 503 `write_resource_limit`, `retryable: true`, `retry_after_secs: 2`, and a message that names the parts, for example "complete mutation exceeds dependency capacity: the receipt names N parts, this write needs up to M more, and the limit is 65536"; the refusal wakes the worker |

A pack of a large home can hold the gate for longer than the 90 second handler deadline (17 to 186 seconds under
load on a synthetic home). A request that waits behind it gets a 503 and can retry. A cheaper incremental pack is
not built. With `LASTDB_AUTO_MAINTENANCE=0` a home at the limit still hangs write requests.

## What to read

- `lastdb status` has a `Storage parts` line: the count, the limit and the percent, whether maintenance runs or
  waits (and its trigger), the peak since start, the number of runs and failures, and the last run
  (`before -> after parts in N ms`, or the last error).
- `GET /api/db/native-status` returns the same fields under `native_disk.parts`. The status JSON carries
  `storage_parts`. The status read never waits for the view lock that maintenance holds.
- The log line `native storage maintenance reached the dependency high-water mark` reports each run with the
  counts and the pack and reclaim times.

## The sweep of unreferenced files, and the fail-closed rule

A kill between the first output file of a publication or a pack and the rename of `CURRENT` leaves part files
that no receipt names. Reclaim removes only files that an old receipt lists, so these files stay. When its drain
ends, the worker sweeps them.

- The sweep runs under the writer lease with a complete journal checkpoint. A part file is an orphan when no
  receipt on disk names it. A minimum age guards against an unforeseen writer: the sweep removes only files older
  than `LASTDB_ORPHAN_MIN_AGE_MS` (600,000), or the grace when that is longer.
- An unreadable receipt could name parts, so it ends the sweep with no removal.

Reclaim and its scan follow one rule: **only a provably unfinished receipt is removed. Anything else that cannot
be read stops with an error that names the directory.** Reclaim removes a generation directory that is empty, and
a partial receipt whose first page declares more pages than are present. It also removes stale `CURRENT.pending-*`
and `TAIL.pending-*` files. A complete receipt under another number, an unknown file, a gap in the part numbers, a
subdirectory in a generation directory, and a torn first page stop reclaim with the named error, and the directory
stays for an operator to read.

Earlier builds could leave a zero-length archived tail file after a kill, and the next start refused
it. The journal archive step now writes a `.pending` file, syncs it, renames it, and syncs the directory, so a kill
leaves the old tail or the new one (32 kill points, a 24-round chain and 70 random kills: every start was clean
and no acknowledged key was lost).

## Settings

| Name | Default | Meaning |
|---|---|---|
| `LASTDB_DEPENDENCY_HIGH_WATER` | 4,096 | Parts at which maintenance starts. At most 75 percent of the limit. |
| `LASTDB_AUTO_MAINTENANCE` | on | `0` or `false` turns the worker off. |
| `LASTDB_AUTO_MAINTENANCE_GRACE_MS` | 0 | Grace for the reclaim of an automatic run. With 0, a run keeps only `CURRENT` and the named references. |
| `LASTDB_ORPHAN_MIN_AGE_MS` | 600,000 | Youngest unreferenced part file that the sweep removes. |
| `LASTDB_AUTO_MAINTENANCE_START_DELAY_SECS` | 30 | Wait before the first pack of a process. |

## Measured

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

Write latency with the worker on or off differs by less than the round-to-round spread of about 10 percent
(`durable`, 1 operation, 2 clients: median 2,436 ms before, 2,510 ms after; default acknowledgement, 10 operations,
4 clients: 5,418 ms before, 5,394 ms after).

## Limits

- The pack copies every live record. Its hold of the gate grows with the home (184 s for 1.1 million records on a
  loaded host).
- A home with more than 67,108,864 records, 65,536 hash groups, or 1,000,000 records in one group cannot pack, and
  can still reach the limit.
- With no grace (the default of an automatic run), reclaim keeps no old generation except `CURRENT` and named
  references.
- Cloud sync is off in these runs.

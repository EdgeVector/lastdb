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
- one new locator part for each locator object that the publication edits.

All locator trees of a home live in one locator object. A group that is new takes a fresh part in that
object, as an edit of its empty tree at page zero, and does not make a new object. Only the first group of an
empty directory makes the object. A batch therefore adds 4 parts: a journal data part, a journal map part,
one directory part and one locator part. (A publication that edits very many trees can fill the 512 pages of
its locator part and add a second one. A home that earlier builds wrote keeps its old locator objects until
its next pack.)

A small durable request makes two or three batches and about 9 parts. A write of ten keys adds the same
4 parts to the batch that holds it. A purge adds parts like any write. A restart adds a few parts for its
own writes. An idle wait and a kill do not merge parts.

All the part files of one object share one directory, so the directory of the locator object holds as many
files as the receipt names locator parts (at most 65,536). A listing of 65,536 files takes 31 ms and of
100,000 files takes 47 ms on APFS, the same as the same files in 32 directories.

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
task runs maintenance when the count passes the trigger. The worker is a loop of bounded steps. Each step holds
the write gate for a bounded time, so waiting writers and readers run between two steps.

- A pack has priority. It runs whenever the count passes the trigger, also while old generations wait for
  reclaim and also while a reclaim waits for its next try. A home above 90 percent of the limit packs first
  and drains afterward.
- The trigger starts at the high-water mark, 4,096 parts (`LASTDB_DEPENDENCY_HIGH_WATER`), at most 75 percent
  of the limit.
- The trigger rises to twice the parts that the last run left, so a home with a large live set does not loop.
- It also rises with the square root of twenty times the records that the last run copied (about 14,000 parts
  for ten million records), because a larger home costs more to pack and so packs less often.
- A new run needs at least 1,024 parts of growth after the last run (at most half the high-water mark).
- The first step of a process waits 30 seconds, so a restart is not blocked behind it. A home that is above
  90 percent of the limit does not wait.
- A run packs, then retires a first slice of 32 old generations, oldest first. The next steps retire the rest
  (the drain). A drain call starts at 32 generations. Its size doubles, up to 1,024, while a call takes less
  than one second, and it halves, down to 8, when a call takes more than four seconds.
- Active reads in this process never defer reclaim. A read holds one generation, and reclaim keeps the
  whole closure of every generation that a read holds. Another process that holds the home (a read or a backup)
  still defers reclaim. The worker then tries again after 1, 2, 4, 8, 16 and then every 30 seconds, for about 20
  minutes, and a pack runs in that time when the count passes the trigger.
- A reclaim that fails (for example an unknown file in `generations/`) repeats after 30 seconds, then 60, and so
  on up to 15 minutes. The status line shows its error. While reclaim fails, the trigger stays at 75 percent of
  the limit, because a pack that reclaim cannot follow only adds one more full copy of the live records on disk.
  A reclaim that ends without an error clears the failure and the trigger returns to its normal value.
- A reclaim call that removes nothing while old generations remain waits like a failed call. It does not repeat.
- When the worker starts, it retires the old generations that an earlier run left (after the pack, if the home
  needs one), and then it sweeps unreferenced part files.
- A failed pack waits 2 to 60 seconds before the next try and keeps the writer healthy when `CURRENT` still names
  the old root.
- A supervisor restarts the worker after a panic: after 2 seconds, then 4, up to 60. The status shows
  `maintenance worker DOWN` while the worker waits for its restart, and counts the restarts. Writers above the
  soft limit do not wait for a dead worker.

A home that already sits at the limit starts, packs, and accepts writes when the pack ends.

## What a writer sees

| Part count | Behavior |
|---|---|
| below the trigger | normal |
| at the trigger | the worker runs; writers wait on the write gate while a step holds it |
| above 90 percent of the limit (58,982 parts) | a new write waits for the worker, for at most one minute, before its own admission; it does not wait when the last run failed |
| at the limit | the write is refused with HTTP 503 `write_resource_limit`, `retryable: true`, `retry_after_secs: 2`, and a message that names the parts, for example "complete mutation exceeds dependency capacity: the receipt names N parts, this write needs up to M more, and the limit is 65536"; the refusal wakes the worker |

The author clock of a mutation is also stored in the home, so it needs room too. A refusal for part capacity is
tried again for five seconds, which gives a running worker time to make room. After that time the request gets the
same named 503 body. With `LASTDB_AUTO_MAINTENANCE=0` a home at the limit therefore answers in about five seconds
with the named error and not with the generic "handler deadline exceeded".

A pack of a large home holds the gate for its whole run. Before the pack kept its facts for the whole pack, the run
on a synthetic home of 1.4 million records took 149 seconds in a node, and 4 of the waiting requests passed the
90 second handler deadline and got a 503. Now it takes 49 seconds, and no request failed. The cost still follows the
number of records. An incremental pack is not built.

## What to read

- `lastdb status` has a `Storage parts` line: the count, the limit and the percent, whether maintenance runs or
  waits (and its trigger), the peak since start, the number of runs and failures, and the last run
  (`before -> after parts in N ms`, or the last error). It also says when the worker is down or was restarted,
  when old generations wait for reclaim, and when reclaim fails (`reclaim failing (N failures), disk grows with
  each pack: <error>`).
- The part count and the worker fields are exact: the status route reads them from atomics at each call. The
  rest of the status document comes from the sampler, which runs about once a minute and can be that old.
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
- An active read in this process does not defer the sweep. A read holds a generation whose receipt stays, and the
  sweep removes only files that no receipt names.

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
| a home stuck at 65,533 parts with 20,390 old generations (52 GB), a cold clone on a shared host | node ready after 304 s, no write accepted in 30 minutes, no pack, about 300 generations retired each minute | node ready and first write accepted after 66 s (pack 65,533 to 12 parts in 53 s), all 20,390 generations retired in 15 minutes (about 1,300 each minute), writes accepted all along |
| 3 tight read loops, 4 durable writers, trigger 200, 150 s | parts up to 2,914, 1 run, 1,239 old generations left | parts up to 235, 12 runs, 62 old generations left; reads 0.4 ms median in both; no error |
| a home at the limit with `LASTDB_AUTO_MAINTENANCE=0` | every write waits 90 s, then "handler deadline exceeded" | the named 503 body after 5.3 s |
| an unknown file in `generations/`, 4 durable writers, trigger 200 | 5 packs and 5 failures in 80 s | 1 pack, then the trigger rises to 49,152, 2 retries in 90 s, and the error is in the status; after the file left, 835 generations were retired in 20 s |

Write latency with the worker on or off differs by less than the round-to-round spread of about 10 percent
(`durable`, 1 operation, 2 clients: median 2,436 ms before, 2,510 ms after; default acknowledgement, 10 operations,
4 clients: 5,418 ms before, 5,394 ms after).

## Limits

- The pack copies every live record. Its hold of the gate grows with the home. Measured on a synthetic home with
  `lastdb-pack-maintain compact` (a cold clone, so the file cache holds none of it): 98,553 records in 2.6 s,
  980,553 records in 40 s, 1,400,553 records in 59 s (171 s before the pack kept its facts for the whole pack).
  A second pack of a home that is already packed takes 10 s for 1.4 million records (107 s before).
- During the pack, the cache holds at most 64 MiB of block-map pages (it reaches that only for a home with more than
  4,000 batches), 1 MiB of decoded blocks, 6 MiB of part records and 64 open part files. The measured peak of
  resident memory was 31 MB at 1.4 million records.
- A home with more than 67,108,864 records, 65,536 hash groups, or 1,000,000 records in one group cannot pack, and
  can still reach the limit.
- With no grace (the default of an automatic run), reclaim keeps no old generation except `CURRENT`, named
  references, and the generations that active reads hold.
- Reclaim plans from the receipts of the generations that must stay. Those receipts can name at most 262,144
  parts together. A home with more stale named references than that stops reclaim with an error that says the
  retained generations name too many parts and that nothing was deleted. The status shows the error, and the
  trigger stays at 75 percent of the limit. Writes stay healthy, because a pack still bounds the receipt, but the
  disk grows by one full copy of the live records for each pack. Release the stale references
  (`lastdb-pack-maintain references`, then `release --reference <id>`) to resume reclaim.
- Cloud sync is off in these runs.

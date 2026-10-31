# The molecule drain

The drain removes a dead molecule: the keys of a dropped schema first, then the
rows that the molecule owns. The counters decide. Design: `docs/lastdb-atom-counter.md`
section 12 (step S7). The counters themselves: `docs/lastdb-molecule-counters.md`.

Code: `fold_db/crates/core/src/db_operations/atom_store/molecule_drain/` (the engine),
`fold_db/crates/core/src/fold_db_core/molecule_drain.rs` (the paced task).

## Dead

A molecule is **dead** in a storage scope when nothing names it:

- the `bindings` counter is zero or absent, and
- no binding record `mref:v1:e:{target}\0...` exists, and
- no counter is `suspect`, and the header (if it exists) carries counters.

A binding record with no counter is a fault, not a dead molecule. The molecule
view says `mismatch`, and the drain refuses it. A dead molecule can still hold tip
records (`slots` above zero: its schema was dropped) and kept conflict records
(`records` above zero). The drain handles both before it removes the rows.

## What starts a drain

The batch that lowers `bindings` to zero writes a candidate row in the same batch
(`flip_molecule_bindings`, `molecule_bindings.rs`):

```
mref:v1:d\0{scope}\0{molecule}      value: {"v":1,"since_ms":...}
```

All candidate rows are one range under one hash (`mref:v1:d\0`). The scope is part
of the key, because one list serves every scope of the node. The personal scope is
the empty string.

The paths that unbind:

| Path | What it unbinds |
|---|---|
| `POST /api/schemas/drop`, `FoldDB::drop_schema_and_bindings` | first the database catalog rows of the schema, each with its bindings and with no instance reclaim (`delete_catalog_rows_for_drop`: up to 64 rows in one batch); then, in one batch with the schema record, the schema-field bindings |
| `store_schema` with a changed molecule set | the stale schema-field bindings |
| `DELETE /api/db/catalog` | the catalog bindings of the row, in the scope of the row |

A **crash between the drop and the drain** loses nothing. The candidate row is part
of the batch that unbinds, and that batch also holds the schema record, so the drop,
the unbind and the candidate rows are all durable or all absent (40 SIGKILL rounds
across the commit of a drop under write load found only these two states). After a restart the task
reads the candidate list from its start. That is one bounded range read under one hash. No scan runs.

The drop is two steps: the catalog rows, then the schema record. A crash or a failure
between them leaves the catalog rows gone and the schema installed. Run the drop again:
it removes the rows that are left and then the schema. The first step does not reclaim
the instance of a catalog row (`DELETE /api/db/catalog` does, in one native request,
and a large instance is over the budget of one request). The drain erases the keys of
the instance in small batches once its last binding is gone.

A molecule that lost its last binding before this step existed has no candidate.
`lastdb db inventory` counts such molecules (`unbound with tips`), and
`lastdb db molecule-drain --molecule M [--scope S]` queues one and prints the verdict.

## Steps

One step is one batch in one native write scope. A page is read outside the gate.
The batch then reads the header, the `bindings` counter and the binding records
again inside the gate, and decides from those reads (`judge.rs`):

| Phase | Batch | Needs inside the gate |
|---|---|---|
| Tips | a page of `mk:{M}:` keys through the atom counter planner: tip deletes, atom counters lowered, bodies deleted at zero, `slots` lowered, up to 100 kept conflict records of the slots released (a janitor marker for the slots with more) | dead (`slots` may be above zero) |
| Records | a page (up to 100) of the slot directory of the molecule (`crs:v1:{M}\0...`) as release requests: the history row of each kept conflict record, its conflict row, directory and queue entries, the atom references it holds (an atom at zero goes by delete-on-zero), and `records` | dead |
| Rows | a page of `mgd:v1:{M}:`, then `mgr:v1:{M}:`, then `rdel:v2:` (delete barriers of the tip keys of `M`) | dead, `slots` = 0, `records` = 0 |
| Finish | `mgp:v1:{M}`, `mcc:{M}`, the header `mh:{M}`, and the candidate row | the same |

Every write goes through the planner (`commit_tip_changes`). The module holds no
write verb of its own and the writer lint allow list gets no new line.

What the gate decides:

| Reads | Result |
|---|---|
| `bindings` above zero | **bound**: the batch deletes only the candidate row. The molecule lives again. |
| no header, no counter, no record | **absent**: the batch deletes only the candidate row. |
| a `suspect` header or counter, a header without counters, a binding record with no counter | **refused**: nothing is deleted. The candidate stays and the status names it. |
| tip records and no header | **refused** (`counters_unset`): the molecule has no counters at all. |
| a pass of the tip phase found no key, and the gate still counts `slots` | **refused** (`slots_without_tips`): the counter is above the rows. No walk can lower it, so the step stops instead of looping. |
| dead, `slots` = 0, `records` above zero in a row or finish batch | **waiting**: kept conflict records still name the molecule, but the slot directory names none (the records phase released all it names). The records end with their own window. The drain looks again in a minute. |
| dead, `slots` above zero in a row or finish batch | a tip appeared: back to the tip phase |
| dead, all zero | the batch runs |

A writer that binds the molecule or writes a tip between the page read and the
batch is seen by the batch, because the batch reads inside the gate.

### Resume

The state of the drain is in the store: the candidate row, the counters and the
rows. The cursor of the task (`RangeWalk`) only saves reads. A crash keeps whole
batches. The next wake reads the candidate list, walks the tip keys again (the
deleted keys cost reads, not writes) and goes on. The header is deleted in the last
batch, with the candidate row. A molecule that a crash leaves half removed still has
its header and zero counters, so it reads as dead and the candidate row is still
there. The next wake finishes it.

## What is not removed

- `mkb:{M}` and `mkw:{M}:...`, the key bundle of the molecule. A bundle is
  node-global and serves the molecule in every scope. The counters of one scope cannot
  show that no other scope uses it, so the drain leaves the bundle. The catalog
  reclaim of an instance does the same.
- The search index entries of a dropped schema, the keep-small meters and the
  hard-delete audit ledger. The drain writes no ledger row.

## The task

`BackgroundMoleculeDrainTask` wakes every `LASTDB_MOLECULE_DRAIN_MS` (default 30000, 0 =
off) and at once when a batch lowers `bindings` to zero. A wake reads the candidate
list from its start and runs batches until the list is done or the batch budget is
spent. The pacing is the one of the retention janitor and the blob dwell pass
(`docs/lastdb-time-queue.md`, "Pacing"). After each batch the task rests for
`blocked * (100 - duty) / duty`, where `blocked` is the time the batch kept writers waiting:
the hold of the write gate, and for a batch that seals the write queue the wait until it is
durable. The page reads before the gate are not part of it. The rest is at least 25 ms and at
most 30 s. The rest protects the writers, so the task needs it only when a writer is there. The
writer watch (`WriterWatch`) remembers the last write scope that another task opened. For a
quiet period after it (`LASTDB_MAINTENANCE_QUIET_SECS`, 60 s by default) every rest is the full
one. When no other task opened a write scope for the whole period, the rest is 25 ms and the
drain runs at the speed of its flush. A long rest ends early when the quiet period has passed.
The drop that makes a candidate is a write scope of another task, so a drain starts in the
paced mode and goes on at full speed 60 s after the drop if no writer comes. A write scope
that stages nothing is not counted as a writer (see "What the watch sees" in the same
section).

| Variable | Meaning | Default |
|---|---|---|
| `LASTDB_MOLECULE_DRAIN_MS` | wake interval, 0 turns the task off | 30000 |
| `LASTDB_MOLECULE_DRAIN_DUTY_PCT` | share of the time that batches may keep writers waiting (while a writer is there) | 10 |
| `LASTDB_MAINTENANCE_QUIET_SECS` | seconds without a write scope of another task after which the rest is 25 ms (the knob of the janitor and the blob pass too) | 60 |
| `LASTDB_MOLECULE_DRAIN_TIPS` | tip records in one batch, 1 to 512; a batch that hits the read limit halves the size | 256 |
| `LASTDB_MOLECULE_DRAIN_BATCHES` | batches in one wake | 1024 |

A tip costs at most three operations in a batch (the tip, the counter, the body), so
256 tips are at most about 800 operations. The row phase deletes up to four times
the tip count of keys in a batch (at most 2048). A step that fails does not hold back
the other candidates: the molecule goes to the back of the line and the wake ends.
A wake that ends with an error doubles the wait before the next one, up to ten minutes.

### Kept conflict records

A dropped schema can have kept conflict records on its keys (the losers of a conflict
stay for the retention window, 30 days by default). The drain releases them as a client
purge or delete would: the tip batch releases up to 100 records of the slots it erases and
leaves a janitor marker for the slots with more, and the records phase then releases all that
the slot directory of the molecule still names, 100 in a batch. So the molecules reach
`records` = 0 and drain within minutes, and the janitor is not needed (it can be off:
`LASTDB_CONFLICT_JANITOR_MS=0`). A loser atom goes by delete-on-zero when its last reference
goes. A record that the slot directory does not name (a count that differs from the
directory) is not released by any drain step. It ends with its own window, and the molecule
waits.

### Cost to writers

Historical measurements below used batches that sealed the write queue. A writer then waited
for the gate and then for the flush of the batch (0.3 to 1 s on a busy disk). The drain rests for
a share of the time its batches kept writers waiting (a duty of 10 percent), and only while a
writer is there. So about one write in ten meets a batch while a writer is there, and a node with
no other writer for the quiet period has nobody to protect and the drain runs at the speed of its
flush. The same knobs and the same watch as the janitor and the blob pass apply; the measurements
are in `docs/lastdb-time-queue.md` ("The molecule drain"). With the default knobs, against the
pacing by the wall time of a step, in runs of both builds at the same time on one host (3 pairs of
a schema of 2,500 keys, 10,000 tips):

| Tips a second | wall-time pacing (before) | this pacing |
|---|---|---|
| no other writer, RAM disk | 261 to 283 | 330 to 354 |
| no other writer, system disk | 14 to 20 | 81 to 102 |
| 4 continuous writers, RAM disk | 40 to 46 | 216 to 219 |
| 4 continuous writers, busy system disk | 8 to 13 | 14 to 21 |
| one sparse writer (a write every 3 s), RAM disk, 40,000 tips | 298 to 303 | 364 to 378 |
| one sparse writer, system disk | 48 to 84 | 76 to 100 |

The latency of a sparse writer with the drain on, against the same writer before the drop and
after the drain (p50, 3 pairs): RAM disk 42 to 48 ms against 38 to 47 and 42 to 52 ms; system disk
472 to 636 ms against 450 to 462 and 478 to 602 ms. The 95th percentile with the drain on is
higher (RAM disk 86 to 130 ms against 46 to 157; system disk 937 to 1,380 ms against 498 to 1,296),
because about one write in ten meets a sealed batch. The previous build gave the same figures. The
first 60 s after a drop go at the paced speed (the drop is a write scope of another task): 38 to 50
tips a second on the system disk.

## Show verbs

- `lastdb status` has a `Molecule drain` line: pending, waiting, refused, molecules drained,
  tips erased, bodies deleted, batches, and the refused molecules with the reason.
- `lastdb db inventory` has, in `Molecule counters`: `droppable` (all zero and no binding
  record), `binding record, no counter` (a fault), `unbound with tips`, `drain candidates`,
  and the molecules drained since the daemon started. The key classes now list
  `mgd:v1:`, `mgr:v1:`, `mgp:v1:`, `rdel:v2:`, `mcc:` and the candidate rows, so a leak of
  these rows shows.
- `lastdb liveness explain molecule <uuid> [--scope S]` shows `binding_records_present` and
  the state `mismatch`.
- `lastdb db molecule-drain [--molecule M] [--scope S]` queues a named molecule and prints
  its verdict (`dead`, `bound`, `refused`, `absent`) and the report. It exits non-zero for a
  refused molecule. `POST /api/db/molecule-drain` is the same. A call with no molecule only
  reports: it does not wake the task, so a poller does not end the back-off of a failed wake.
  Queueing a molecule wakes the task after the batch commits.
- `lastdb db verify-refs` stays clean after a drain: a molecule with no header and no tip
  has nothing to compare.

## Why the candidate list is not the time queue

A candidate has no due time, and the list holds only the molecules that lost their
last binding and are not done. The list is one hash, read with a bounded range walk
(`RangeWalk`) that passes the deleted keys of finished candidates with reads that
cannot fail on them (`docs/lastdb-time-queue.md` explains the failure of a plain page
read). The task reads the list from its start at each wake, so the cost of a wake grows
with the number of molecules ever drained, not with the pending ones. A home that
drained millions of molecules would pay reads at each wake. A bucketed layout can
replace the single hash without a change to the drain steps.

## Known limits

- **An install of a schema whose molecules are still draining is refused.** `store_schema`
  fails with `SchemaBeingDropped`, and the HTTP layer answers `409` with
  `schema_being_dropped` and a `Retry-After` header when a molecule that the install would
  bind has a candidate row and reads dead (checked on `POST /api/schemas/seed-system`).
  Nothing changes. The body says what the install waits for in `reason`, with the number of
  molecules in `molecules`: `draining` (the drain erases keys, retry after 30 s),
  `waiting_for_conflict_records` (no tip is left and the slot directory still names kept
  records that the drain releases, retry after 60 s), or `drain_task_off` (the task is off,
  `LASTDB_MOLECULE_DRAIN_MS=0`: nothing erases the keys until an operator sets the knob above
  0 and restarts the node; `Retry-After` is 3600 and `retryable` is false). A molecule whose
  only remains are kept records that no directory entry names does not block an install.
  The install works after the drain has finished. A molecule that a counter fault keeps from
  the drain (refused) does not block an install: the install binds it again and the
  drain drops its candidate row. A catalog row put for a schema in a database scope has
  no such check: the put binds the molecules of that scope again, and the drain stops
  at its next batch.
- **A changed molecule set in `store_schema` erases the data of the old field.** The
  install unbinds the molecules the schema no longer names. When no schema field or
  database catalog names such a molecule, its last binding goes and the drain erases its keys.
  Before this step the old molecule and its keys stayed for ever (the counters did not
  exist), so a repair that points a field at another molecule kept the old keys. Now it
  loses them. This is the rule of the counters (the count of bindings is the only
  authority), and it is a change of behavior.
- **A lost candidate list is a silent leak.** The drain finds work in the candidate rows
  only. Nothing rescans the molecule headers. If the rows are lost (for example a raw
  delete of the rows, or a molecule that lost its last binding before this step existed),
  the molecules stay with their keys. `lastdb db inventory` shows them as `unbound with
  tips` (the first 500 are listed in `unbound_sample` of the JSON report, 20 on the
  screen), and each needs `lastdb db molecule-drain --molecule=<uuid>`. A molecule recount
  that rediscovers candidates is not built.
- **A refused molecule has no repair path.** The drain never repairs a counter. A `suspect`
  header or `bindings` record, a header without counters, and a binding record with no
  counter stay refused and stay in the status line. A tip that something wrote outside the
  planner into a dead molecule is erased by the drain, `slots` goes below zero, and the
  header stays `suspect` for ever. A molecule recount would repair it; it is not built.
- **The drain leaves some rows.** The node-global key bundle (`mkb:`, `mkw:`), the
  delete-ledger rows, the search index entries and the keep-small meters of the
  erased keys.
- **The drop reads the whole catalog.** `drop_schema_and_bindings` uses `list_entries`,
  the hygiene enumeration of the catalog (a scan of one row for each database and
  schema). `drop_schemas_owned_by_with_bindings` reads it once for each schema. The call
  is not on a data path. A catalog read by schema name is a later step.
- **A scope that a verb names is the personal scope (leave the value out) or exactly 64
  lowercase hex characters.** `db verify-refs`, `db recount`, `liveness explain` and
  `db molecule-drain` refuse any other value, an empty value and a blank value too, with a
  400 (a catalog instance id of another shape cannot be named, also with `--all-scopes`).
- **The failed-wake back-off ends on new work.** A wake that fails doubles the wait up to
  ten minutes. A new drop, or a molecule named by hand, ends the wait. A batch that hit the
  read limit is halved, and it doubles back after 5 wakes that did work and had no read limit.
- **A writer that meets a batch waits for its flush.** The drain paces like the janitor and the
  blob dwell pass, so a batch that seals the write queue makes every writer that arrives wait
  until it is durable. At a duty of 10 percent about one write in ten meets a batch while a writer
  is there. A smaller `LASTDB_MOLECULE_DRAIN_DUTY_PCT` lowers that share and slows the drain (see
  "Cost to writers").
- **The watch counts write scopes of the process.** A caller that takes the write gate without a
  scope is not seen. A write scope that stages nothing is not counted (`lastdb db verify-refs` opens
  one at the start of each run). `lastdb db inventory` writes rows and counts as a writer; `lastdb
  status`, `lastdb liveness explain` and a `lastdb db molecule-drain` call that names no molecule open
  no write scope.
- A write that the node accepted before the drop and that commits after the drain
  finished creates a tip in a molecule with no binding and no candidate. `lastdb db inventory`
  counts it under `unbound with tips`; queue it with `lastdb db molecule-drain`.
- The drain does not repair a counter (see above).
- A catalog row put for a schema that is not installed yet binds nothing
  (`catalog_membership.rs`). Such a molecule has no binding counter. A candidate arises only from
  an unbind that finds a binding, so this gap cannot start a drain by itself.


## Concurrent snapshot maintenance

The current background path shares the bounded collection window without early publication.
Only write preparation reads its private prefix. Ordinary reads retain the committed state.
The drain returns after durability and paces from its measured writer cost.
The earlier latency measurements above describe the former sealed-batch path.
See `docs/lastdb-eager-durable-queue.md` for the current queue and merge.

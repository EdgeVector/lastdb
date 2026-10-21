# The molecule drain

The drain removes a dead molecule: the keys of a dropped schema first, then the
rows that the molecule owns. The counters decide. Design: `docs/design-lastdb-counter-lifecycle.md`
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
| `POST /api/schemas/drop`, `FoldDB::drop_schema_and_bindings` | first the database catalog rows of the schema, each with its bindings and with no instance reclaim (`delete_catalog_rows_for_drop`: up to 64 rows in one batch); then, in one batch with the schema record, the schema-field bindings and the protein members |
| `store_schema` with a changed molecule set | the stale schema-field bindings, and the protein members of the molecules they leave |
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
| Tips | a page of `mk:{M}:` keys through the atom counter planner: tip deletes, atom counters lowered, bodies deleted at zero, `slots` lowered, kept conflict records of the slots released | dead (`slots` may be above zero) |
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
| dead, `slots` = 0, `records` above zero | **waiting**: kept conflict records still name the molecule. The janitor ends them. The drain looks again in a minute. |
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
- `protein:{uuid}` records that lost all their members. `lastdb db gc-proteins`
  removes them.
- The search index entries of a dropped schema, the keep-small meters and the
  hard-delete audit ledger. The drain writes no ledger row.

## The task

`BackgroundMoleculeDrainTask` wakes every `LASTDB_MOLECULE_DRAIN_MS` (default 30000, 0 =
off) and at once when a batch lowers `bindings` to zero. A wake reads the candidate
list from its start and runs batches until the list is done or the batch budget is
spent. After each batch it rests for `took * (100 - duty) / duty`.

| Variable | Meaning | Default |
|---|---|---|
| `LASTDB_MOLECULE_DRAIN_MS` | wake interval, 0 turns the task off | 30000 |
| `LASTDB_MOLECULE_DRAIN_DUTY_PCT` | share of wall time that batches may take | 10 |
| `LASTDB_MOLECULE_DRAIN_TIPS` | tip records in one batch, 1 to 512; a batch that hits the read limit halves the size | 256 |
| `LASTDB_MOLECULE_DRAIN_BATCHES` | batches in one wake | 1024 |

A tip costs at most three operations in a batch (the tip, the counter, the body), so
256 tips are at most about 800 operations. The row phase deletes up to four times
the tip count of keys in a batch (at most 2048). A step that fails does not hold back
the other candidates: the molecule goes to the back of the line and the wake ends.
A wake that ends with an error doubles the wait before the next one, up to ten minutes.

### Cost to writers

A write that shares time with a drain batch waits longer than a write that does not.
Measured on a loaded host, with 4 writers that wrote as fast as the node answered
(the median latency was about 110 ms): the median of a write that overlapped a batch was
1.3 to 1.8 times the median of the writes in the same window that did not. The hold of
the write gate is not the cause: a batch of 256 tips holds it for 30 ms at the median
and 70 ms at most. The duty share sets how many writes meet a batch (36 to 38 percent of
the writes at a duty of 25, 13 to 19 percent at a duty of 10). The penalty for a write that
meets a batch did not change in a clear way with the batch size (64 to 256 tips). The default
duty is 10 percent for this reason: the schema is already gone when the drain runs, and only
disk space waits for it. At this duty the drain erased 55 to 100 tips a second in the same runs.

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
  refused molecule. `POST /api/db/molecule-drain` is the same.
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
  (and every route that installs a schema) answers `409` with
  `schema_being_dropped` ("schema is still being dropped; retry") when a molecule that
  the install would bind has a candidate row and reads dead. Nothing changes. The
  install works after the drain has finished. A molecule that a counter fault keeps from
  the drain (refused) does not block an install: the install binds it again and the
  drain drops its candidate row. A catalog row put for a schema in a database scope has
  no such check: the put binds the molecules of that scope again, and the drain stops
  at its next batch.
- **A changed molecule set in `store_schema` erases the data of the old field.** The
  install unbinds the molecules the schema no longer names. When no other schema or
  protein names such a molecule, its last binding goes and the drain erases its keys.
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
  that rediscovers candidates is a later step (S8).
- **A refused molecule has no repair path.** The drain never repairs a counter. A `suspect`
  header or `bindings` record, a header without counters, and a binding record with no
  counter stay refused and stay in the status line. A tip that something wrote outside the
  planner into a dead molecule is erased by the drain, `slots` goes below zero, and the
  header stays `suspect` for ever. A molecule recount (S8) is the repair.
- **The drain leaves some rows.** The node-global key bundle (`mkb:`, `mkw:`), the
  `protein:` records that lost all their members (`lastdb db gc-proteins`), the `fldprot:`
  rows, the delete-ledger rows, the search index entries and the keep-small meters of the
  erased keys.
- **The drop reads the whole catalog.** `drop_schema_and_bindings` uses `list_entries`,
  the hygiene enumeration of the catalog (a scan of one row for each database and
  schema). `drop_schemas_owned_by_with_bindings` reads it once for each schema. The call
  is not on a data path. A catalog read by schema name is a later step.
- **A scope that a verb names is the personal scope or exactly 64 lowercase hex
  characters.** `db verify-refs`, `db recount`, `liveness explain` and `db molecule-drain`
  refuse any other value with a 400 (a catalog instance id of another shape cannot be
  named, also with `--all-scopes`).
- **The failed-wake back-off ends on new work.** A wake that fails doubles the wait up to
  ten minutes. A new drop, or a molecule named by hand, ends the wait. A batch that hit the
  read limit is halved, and it doubles back after 5 wakes that did work and had no read limit.
- **Pacing follows the wall time of a step**, not the gate hold. The janitor and the blob
  pass pace by the gate hold in a later change (S8b), and the drain follows after that change merges.
- A write that was acknowledged from memory before the drop and flushes after the drain
  finished creates a tip in a molecule with no binding and no candidate. `lastdb db inventory`
  counts it under `unbound with tips`; queue it with `lastdb db molecule-drain`.
- The drain does not repair a counter (see above; a molecule recount is left for step S8).
- A catalog row put for a schema that is not installed yet binds nothing
  (`catalog_membership.rs`). Such a molecule has no binding counter. A candidate arises only from
  an unbind that finds a binding, so this gap cannot start a drain by itself.

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
| `POST /api/schemas/drop`, `FoldDB::drop_schema_and_bindings` | first the database catalog rows of the schema (each row with its bindings, one batch for each row, `delete_catalog_membership`); then, in one batch with the schema record, the schema-field bindings and the protein members |
| `store_schema` with a changed molecule set | the stale schema-field bindings, and the protein members of the molecules they leave |
| `DELETE /api/db/catalog` | the catalog bindings of the row, in the scope of the row |

A **crash between the drop and the drain** loses nothing. The candidate row is part
of the batch that unbinds, and that batch also holds the schema record, so the drop,
the unbind and the candidate rows are all durable or all absent (24 SIGKILL rounds
across the commit of a drop under write load found only these two states). After a restart the task reads the candidate list from its start. That is
one bounded range read under one hash. No scan runs.

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
| `LASTDB_MOLECULE_DRAIN_DUTY_PCT` | share of wall time that batches may take | 25 |
| `LASTDB_MOLECULE_DRAIN_TIPS` | tip records in one batch, 1 to 512; a batch that hits the read limit halves the size | 256 |
| `LASTDB_MOLECULE_DRAIN_BATCHES` | batches in one wake | 1024 |

A tip costs at most three operations in a batch (the tip, the counter, the body), so
256 tips are at most about 800 operations. The row phase deletes up to four times
the tip count of keys in a batch (at most 2048). A step that fails does not hold back
the other candidates: the molecule goes to the back of the line and the wake ends.
A wake that ends with an error doubles the wait before the next one, up to ten minutes.

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

- A schema that is installed again before its drain finishes binds its molecules again. The
  drain stops at the next batch and drops its candidate rows. The tips that the drain already
  erased are gone, and the rest stay. The schema then shows part of its old keys.
- A write that was acknowledged from memory before the drop and flushes after the drain
  finished creates a tip in a molecule with no binding and no candidate. `lastdb db inventory`
  counts it under `unbound with tips`; queue it with `lastdb db molecule-drain`.
- The drain does not repair a counter. A `suspect` molecule stays refused until an operator
  repairs it (a molecule recount is not built; it is left for step S8).
- A catalog row put for a schema that is not installed yet binds nothing
  (`catalog_membership.rs`). Such a molecule has no binding counter. A candidate arises only from
  an unbind that finds a binding, so this gap cannot start a drain by itself.

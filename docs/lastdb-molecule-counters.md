# Molecule counters

## Contract

Every molecule in a storage scope has three counters. A molecule may go only
when all three are zero, none is `suspect`, and no binding record exists (a
binding record with a zero or absent counter is a fault, not a dead molecule).
No other structure allows a drop and no other structure blocks one.

| Counter | Counts | Where it lives |
|---|---|---|
| `slots` | tip records (`mk:{M}:...`) of the molecule in the scope | the molecule header `mh:{M}` |
| `records` | conflict records (history rows tagged `conflict` or `suppressed`) that name the molecule | the molecule header `mh:{M}` |
| `bindings` | binding records of the molecule: schema field, protein member, database catalog | `mref:v1:n:{target}` next to the binding records `mref:v1:e:{target}\0{source}` |

`{target}` is the hex SHA-256 of the molecule uuid. A scope adds its prefix before
the key, as for every other record.

The drain that removes a dead molecule is step S7: `docs/lastdb-molecule-drain.md`.

## Why `bindings` is not in the header

The presence of a header means "the molecule lives at this prefix". The write path
(`load_molecule_for_write`), the filtered load and the share fan-out read it that
way. A bind comes before the first tip, so a bind that created the header would
change what a first write does. `slots` and `records` change only with tips, and the
header exists whenever tips exist, so they sit in the header and cost no extra
record or hash group on a write. The binding counter changes only on a bind or an
unbind.

## Invariants

For every scope and every molecule `M`:

- M1: `slots(M)` is the number of tip records of `M` in the scope. A tip record with
  a tombstone flag counts. A deleted tip does not.
- M2: `records(M)` is the number of counted conflict records of `M` in the scope.
- M3: `bindings(M)` is the number of binding records of `M` in the scope. A counter
  record never holds zero, except a `suspect` one: the batch that lowers it to zero
  deletes it.
- M4: a counter never changes outside the batch that changes what it counts.

## How the counters change

`slots` and `records`. The atom counter planner (`counter/`) changes them in the
batch of the tip change. `counter/molecule.rs` reads the headers of the molecules
that the run touches by key, inside the write gate. It adds the net change of tips
(a new tip adds one, an old tip that goes takes one away, a tip that replaces a tip
changes nothing) and the number of new conflict records. It merges the counters into
the header row of the run, or makes a header row when the run has none.
`counter/molecule_plan.rs` is the pure arithmetic.

`bindings`. `molecule_bindings.rs` reads which binding records exist, flips only the
records that need it, and writes the flipped records and the new counter in one batch
inside the write gate. A bind that already exists changes nothing. The cost follows
the number of fields of the schema. It does not follow the number of keys of a
molecule, and it reads no tip and no atom.

Writers of a binding record:

| Writer | When | Scope |
|---|---|---|
| `store_schema`, `drop_schema` (`db_operations/schema_operations.rs`) | a schema is stored or dropped | personal |
| `protein_add_member_in_scope` (`protein/ops.rs`) | a protein gets a member | personal |
| `protein_release_members` (`protein/ops/release.rs`) | a schema drop or a changed schema leaves a molecule that only a protein names; the protein lets it go | personal |
| `bind_catalog_molecules`, `unbind_catalog_molecules` (`catalog_bindings.rs`) | a catalog row is written or deleted | the instance of the row |

The catalog row and its binding records are written in one batch by the share route,
the put route, the declare of a schema under a named database, and the delete of a
catalog row (`fold_db_core/fold_db/catalog_*.rs`).

## The recorder and the release of records (step S3)

A conflict record offered to the planner is a `RecordRefs` with the molecule it
belongs to. When the record is new, the planner raises `records` of that molecule in
the same batch. A release of a record (`ReleaseRequest`, step S3: the end of its
window, a purge, a TTL sweep or a delete of its key, or the drain of an erased
slot) lowers `records` in the same batch: the planner reads the history row inside
the write gate and takes the molecule from the row (`counter/records.rs`,
`lower_molecules`, merged into `molecule_changes`). A record whose row is already
gone lowers nothing. An erase reads the header of each molecule it touches and
looks for kept records only in a molecule whose `records` is above zero (or whose
header cannot be trusted: `suspect`, or no counters).

The molecule drain releases the kept records of a dead molecule itself, from the slot
directory of the molecule, and does not wait for the janitor (see
`docs/lastdb-molecule-drain.md`, "Kept conflict records"). A schema that was dropped with
kept conflict records can be installed again within minutes, not after the retention
window. An install that meets a molecule the drain still works on answers `409`; the body
names the reason (`draining`, `waiting_for_conflict_records`, `drain_task_off`) and the
response has a `Retry-After`.

## Underflow

A removal can ask for more slots or records than the counter holds, and a header can
carry no counters. The planner clamps the counter to zero, sets `suspect` and reports
the case (`lastdb status`, `molecule_faults`). A `suspect` molecule never counts as
droppable. A header written by a build or a path that has no counters (for example
cloud replay, which is off) reads as `suspect` at its next planner change.

## Reads

- `lastdb liveness explain molecule <uuid> [--scope PREFIX]` (and
  `POST /api/storage/liveness/explain` with class `molecule`, `id` or `ids`, and
  `scope`) returns the three counters, the state (`live`, `droppable`, `suspect`, `mismatch`,
  `absent`) and, for one id, the binding records.
- `lastdb db inventory` has a `Molecule counters` block: headers, slots, records,
  bindings, binding records, live, droppable, suspect, and the first droppable
  molecules.
- `lastdb db verify-refs` checks the counters (see below).

## Verifier

Three phases follow the atoms. `headers` compares `slots` and `records` with the tips
and conflict records that the first two phases counted for each molecule (a molecule
that a writer changed during the pass is a candidate, as for an atom). `bindings`
compares each counter with its binding records, read at one root: a bind writes both
in one batch, so a difference is always a fault. `binding_records` finds a binding
record without a counter. The verifier writes nothing.

Faults of these phases:

| Kind | Meaning |
|---|---|
| `molecule_slots_above_tips`, `molecule_slots_below_tips` | a header `slots` differs from the tips of the molecule that the tip phase counted |
| `molecule_records_above_records`, `molecule_records_below_records` | a header `records` differs from the counted conflict records of the molecule |
| `tips_without_header` | a molecule has tips and no header |
| `molecule_counters_unset` | a header carries no counters |
| `suspect_molecule` | a header has the `suspect` flag |
| `unreadable_header`, `unreadable_bindings` | a header or a `bindings` record does not decode |
| `bindings_counter_mismatch` | a `bindings` counter differs from the number of binding records |
| `binding_record_without_counter` | a binding record has no `bindings` counter |
| `zero_bindings_counter` | a `bindings` counter holds zero and is not `suspect` |
| `suspect_bindings` | a `bindings` counter has the `suspect` flag |

## Known limits

- A protein member binding goes only when a schema drop (or a changed schema) leaves a
  molecule that only a protein names (`protein_release_members`). A protein member of a
  molecule that a schema or a catalog still names stays.
- Cloud replay and restore write headers and binding records that bypass the planner.
  Cloud sync is off in this build.
- A crash between the share route's domain grant and its batch leaves a domain grant
  with no binding. The grant is not a reference.
- Nothing finds a dead molecule except the candidate rows of the molecule drain
  (`docs/lastdb-molecule-drain.md`). If the rows are lost, the molecules stay with their
  keys and no scan finds them again. `lastdb db inventory` lists the first 500 of them as
  `unbound_sample`, and `lastdb db molecule-drain --molecule=<uuid>` queues one. A molecule
  recount that rediscovers candidates is not built.
- The drain never repairs a `suspect` counter. Stop the node and run
  `lastdb db recount --molecules` to report exact header and binding counts.
  Add `--execute` to write the repairs under the stopped-home lock.
  Each scope and pass validates tips, retained conflicts, headers, and binding records before its first molecule write.
  The repair preserves header metadata and queues an unbound molecule for the normal bounded drain.
  It refuses a missing live header, unreadable holder, or discarded storage record.
  It creates no atom body or missing schema definition.
- After a drain the node-global key bundle (`mkb:`, `mkw:`), the `protein:` records without
  members, the delete-ledger rows and the search entries of the erased keys stay.
- The schema drop reads the whole database catalog with `list_entries` (a scan of a small
  table that has one row for each database and schema).
- A molecule whose `records` counter names kept conflict records that no slot directory entry
  names (the counter and the directory differ) is not released by any drain step. The records
  end with their own retention window, and the drain waits for them. Such a molecule does not
  block an install. The offline molecule recount repairs the counter from the actual retained records.


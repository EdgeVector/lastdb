# Atom counter

## Contract

Every atom body in a storage scope has one counter record. The counter holds
the number of tip records and conflict records that name the atom. The counter
is the only authority for the life of a body. No other structure allows a
delete. No other structure blocks one.

A batch that lowers the count to exactly zero deletes the body, the counter,
and the blob references of the body in the same batch as the last tip change.
A body exists if and only if its counter exists. A counter never holds zero,
except a counter with the `suspect` flag.

## Records

| Record | Key | Collection |
|---|---|---|
| Body | `atom:{uuid}\0b` | `atoms` |
| Counter | `atom:{uuid}\0c` | `atoms` |

The first NUL ends the hash. The body and the counter of one atom share one
hash group. A storage scope adds its prefix before the key.

The counter is JSON: `{"v":1,"refs":N,"blobs":[...],"suspect":false}`.
`blobs` lists the blob references of the body, so a delete needs no body read.

## Invariants

For every scope and every atom `A`:

- I1: `refs(A)` equals the number of tip records plus conflict records that name `A`.
- I2: the body exists if and only if the counter exists.
- I3: a batch lowers `refs(A)` by `k` only if the same batch removes `k` references to `A`.
- I4: the stored count is never lower than the true number of references.

## The planner

One module writes tips, conflict references, bodies and counters:
`fold_db/crates/core/src/db_operations/atom_store/counter/`.

1. The caller holds the native write gate through a request scope.
2. The planner reads the old tip records, the counters and the bodies by key.
   Every read sees the committed root, the accepted batches and the pending batch.
3. `plan.rs` computes each counter change from the old tips, the new tips and the
   new conflict records. It is pure arithmetic.
4. `build.rs` makes the writes. `commit.rs` joins them to the pending batch of
   the request with one `batch_mutate`.

A crash keeps whole batches only. Each batch moves the store from one equal
state to another.

Tip writes, user Delete, Purge, the TTL sweep and the protein sibling write all
go through `commit_tip_changes`. `scripts/lints/lint-atom-counter-writers.sh`
guards the boundary. A file that writes a raw tip, conflict, body or counter
record must be in `scripts/lints/atom-counter-writers.allow` with a reason.

## Underflow

A removal can ask for more references than the counter holds, or name an atom
with a body and no counter. The planner never clamps the count and deletes the
body. It erases the tip, keeps the body, sets `suspect`, and reports the case.
`lastdb status` shows the totals and the recent atoms. `lastdb db inventory`
shows `bodies_without_counter`, `counters_without_body`, `zero_counters` and
`suspect` for the whole home.

## Delete and Purge

Both verbs read the tips of their keys by key, in one batch, inside the gate.
They give every tip change to the planner and stage one batch. They take no
schema barrier, no molecule lock and no atom lock. The cost follows the number
of keys of the request, and never the size of the schema.

- Purge writes no delete barrier. A Purge with a cutoff spares a key that has a
  tip written at or after the cutoff.
- Delete writes the delete barrier of each key, so a later replayed write cannot
  bring the key back.
- Both verbs reach the protein siblings of the key.

## Reads

- `GET /api/atom/{id}` returns 404 after the last holder is purged.
- `POST /api/storage/liveness/explain` with `{"class":"atom","ids":[...]}` returns
  the counter, the `suspect` flag and the body state of each atom. It cannot
  list the referrers: the counter holds a number, not a list.
- `lastdb status` has an `Atom counters` line. `lastdb db inventory` has an
  `Atom counters` block.

## Format fence

A home with this record layout sets the required-feature bit `FEATURE_ATOM_RECORDS`
(value 128) in the LastStore `CURRENT` file. A build that does not know the bit
refuses the home. This build refuses a home without the bit.

## Not in this step

Steps S2 to S8 of the design (`docs/design-lastdb-counter-lifecycle.md`) remain:

- S3: the recorder releases conflict references (a janitor), and a purge reads the slot directory.
- S4: molecule counters replace the molecule edges and the catalog binding records.
- S5: blob counters replace the blob edges and the file blob sweep.
- S6: `verify-refs` and offline `recount`.
- S7: a resumable drain of a dead molecule.
- Cloud sync replay and restore write rows that bypass the planner. Cloud sync is off in this build.

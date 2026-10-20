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
go through `commit_tip_changes`. `scripts/lints/lint-atom-counter-writers.py`
guards the boundary, function by function. A function that writes through a
store and names a tip, conflict, body or counter key (or holds a raw store
handle or scans and deletes) must be in `scripts/lints/atom-counter-writers.allow`
with the families it touches and a reason. A new family in a listed function
fails the lint, and so does a line that no function matches. The lint is a
source check: it cannot see a key that a caller builds elsewhere and passes to
a generic helper.

## Underflow

A removal can ask for more references than the counter holds, or name an atom
with a body and no counter. The planner never clamps the count and deletes the
body. It erases the tip, keeps the body, sets `suspect`, and reports the case.
A new write that brings an atom whose body exists and whose counter is gone
takes the same path: the planner keeps the body, writes a counter with only the
new references and sets `suspect`. It does not treat the write as a fresh
create, because the holders that the lost counter counted are not known. A
`suspect` counter never reaches zero for a delete.
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

## Memory acknowledgement

A write that asks for a memory acknowledgement (`durability: queued`) returns
when its batch is accepted, before the flush. The batch holds the tips, the
counters and the bodies of the request, and the typed model of the warm set
changes at the same acceptance. The check that allows this
(`logical_paths/completeness.rs`, `covers`) accepts the counter key and the body
key of an atom without a typed cover of their own. Why this is safe:

1. A body is immutable. One atom id holds one content, so a body create cannot
   change a value that a cached path holds. The path record of a tip write
   lists its new body as a source.
2. A body delete happens only in the batch that takes the counter to exactly
   zero (I1). After that batch no tip and no conflict record names the atom, so
   no live path resolves through the body.
3. A cached path that lists the deleted body as a source is the old value of a
   slot that the same batch rewrites or erases. The typed model requires that
   (`core_set/model/coverage.rs`, `covered`): every cached slot with a changed
   source must be one of the typed changes of the publication. If one is not,
   the dirty publication is refused and the write takes the disk-first path,
   which waits for the flush. A stale value cannot be published.
4. A reader that captured its view before the acceptance keeps the old root,
   the older accepted batches and the older typed model. It sees the old tip with
   the old body. A reader after the acceptance sees the new tip and no old body.
   No reader sees half of a batch.
5. The typed publication happens at the acceptance, under the queue lock, with
   no await between it and the hand-over to the queue owner. The flush writes
   the accepted batches later, as one durable commit.
6. A crash before the flush loses the whole batch. The disk keeps the old tip,
   the old body and the old counter, which agree. The loss is the one that a
   `queued` acknowledgement already allows (up to the flush interval).

Without the body key in `covers`, every write whose old atom reached zero took the
disk-first path and waited about 250 ms for the flush, and the per-schema lane
waited with it.

## Size of one request

A native batch holds at most 16,384 operations (4,096 on a home from an older
build). The reads of one write request, inside the write gate, share a budget of
40,000 examined keys. A purge or delete stages all its chunks of 64 keys in one
batch. The request is counted before it takes the write gate
(`fold_db_core/purge/budget.rs`), with these upper bounds for K keys of F fields:

- purge: `4 * K * F` operations and `12 * K * F` examined keys,
- delete: `5 * K * F` operations and `13 * K * F` examined keys,
- plus `F + 8` of each for every chunk of 64 keys.

A tip costs the tip delete, the `mgd` marker, and the body delete and counter
delete when it holds the last reference. A delete adds one barrier row. The reads
are the target tips, the peer expansion, the counters and the body records
(measured 9 to 12 examined keys for each tip). A request over either limit is
refused with the number of keys that fits ("send at most M keys of F fields in
one request"). For a schema of 24 fields, a purge fits 138 keys and a delete
fits 128 on a fresh home (42 and 33 on an older home). For 300 fields the limits
are 11 and 10 keys. The TTL sweep cuts its batch to fit, and it halves a batch
that still hits a size limit and retries, so the default of 16 keys fits a
schema of up to about 200 fields on a fresh home and about 60 on an older home.

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

## Known limits

- `lastdb db inventory` reads the counter and the body record of each atom, two
  records for each atom, inside one read call. The read limit of a call (10,000
  keys) stops the walk at about 4,600 atoms. Step S6 replaces the walk with a
  paged verifier.
- A count that is too low but still above every removal is not found until step S6.

## Not in this step

Steps S2 to S8 of the design (`docs/design-lastdb-counter-lifecycle.md`) remain:

- S3: the recorder releases conflict references (a janitor), and a purge reads the slot directory.
- S4: molecule counters replace the molecule edges and the catalog binding records.
- S5: blob counters replace the blob edges and the file blob sweep.
- S6: `verify-refs` and offline `recount`.
- S7: a resumable drain of a dead molecule.
- Cloud sync replay and restore write rows that bypass the planner. Cloud sync is off in this build.

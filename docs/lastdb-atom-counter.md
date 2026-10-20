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

Blob roots (`bref:v2`) have two sources. A body names the blobs in its counter.
A tip record that carries file metadata names its own blob. The planner changes
the roots of both in the batch of the change: the root of an old tip record goes
and the root of a new tip record comes in the same batch as the tip.

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
- Molecule counters (`slots`, `records`, `bindings`) are in
  `docs/lastdb-molecule-counters.md`. The verifier checks them in three more phases.
- `lastdb status` has an `Atom counters` line and a `Verify refs` line.
  `lastdb db inventory` has an `Atom counters` block.
  - `Atom counters` counts the planner events since the daemon started:
    `bodies_created`, `bodies_deleted`, `underflows`, `counters_absent`,
    `dangling_removals` and `suspect_marked` (the counters this process marked
    `suspect`; `underflows` plus `counters_absent`).
  - `Verify refs` shows the last complete run of the verifier: the time, the
    pages, the faults, `suspect_atoms` (the `suspect` counters that run read),
    and the candidates. It shows a run in progress. The verifier keeps this in
    memory, so a restart clears it. The status JSON has the same data in
    `atom_counters.verify_refs`.
- `POST /api/db/verify-refs` runs one step of the verifier. `GET /api/db/verify-refs`
  returns the last result and the run in progress. Owner socket only.

## Verifier: `lastdb db verify-refs`

The verifier finds a counter that disagrees with the tips and the conflict
records. It runs on request only. It **writes nothing**: no counter, no body,
no tip, and no record of its own. It takes **no write gate**. It keeps its
result in memory, so a restart clears it. The source check
`scripts/lints/lint-atom-counter-writers.py` lists no writer in
`counter/verify/`, so the verifier has no write call to a store.

It reads in three phases, each in pages with a cursor:

1. Tip records (`mk:`): the atom of each tip has a body, and its counter is not
   below the tips of the page.
2. Counted conflict records (`history`, tagged `conflict` or `suppressed`): the
   same checks for the atoms a record holds. All history rows share one hash,
   and a row does not say by its key alone whether it holds references, so this
   phase reads every `history\0` row and skips the uncounted ones. Its cost
   follows the total size of the history, not the number of counted records.
3. Body and counter records of atoms (`atom:`): body and counter exist
   together, no counter holds zero, no counter is `suspect`, and the counter
   equals the totals of phases 1 and 2.

A step reads pages until it has used `max_pages` (default 64) or `max_ms`
(default 3,000), pauses `pause_ms` (default 2) after each page, and returns a
`next_cursor`. The command follows the cursor until the run is complete and
exits non-zero when it finds a fault. `--steps N` stops after N steps and
prints the cursor. `--last` shows the last result without a run.

Faults, by name:

| Kind | Meaning |
|---|---|
| `counter_below_tips` | the counter holds fewer references than the tips and records that name the atom |
| `counter_above_tips` | the counter holds more references than the tips and records that name the atom (a leak) |
| `body_without_counter` | a body record has no counter record |
| `counter_without_body` | a counter record has no body record |
| `zero_counter` | a counter holds zero and is not `suspect` |
| `suspect_counter` | a counter has the `suspect` flag |
| `tip_without_body` | a tip names an atom that has no body (a lost body) |
| `record_without_body` | a conflict record names an atom that has no body |
| `unreadable_tip`, `unreadable_record`, `unreadable_counter` | a record does not decode |
| `unconfirmed_count_mismatch` | not a fault: see below |

A kind is a fact about one atom, and an atom can show more than one. For
example, a counter at zero for an atom that a tip names shows `zero_counter` and
`counter_below_tips`.

### How it reads a consistent view

One page is one captured read scope. The tip rows, the counters and the bodies
of the page are read at one committed root. A committed root holds whole
batches, so I1 to I4 hold at it. Therefore:

- A fault found inside a page (a tip without a body, a counter below the tips of
  the page, an unpaired body or counter, a zero or `suspect` counter) has **no
  false positive** under concurrent writers. The other holders of an atom only
  raise its true count.
- A count that differs from the totals of the **whole pass** needs the pages
  together, and they are read at different roots. The planner tells the
  verifier which atoms it changed while a run reads (one atomic load per batch
  when no run reads). A difference for an atom that no batch touched is a fault,
  because the holders did not change. A difference for a touched atom is
  counted as `unconfirmed_count_mismatch`: a candidate, not a fault. **Run the
  verifier again when the writers are quiet**: a candidate that was a write
  between two pages is gone, and a real fault stays.
- A new run takes the write gate once, for an empty scope, to order itself after
  the writes in flight. It holds the gate for no read. The empty scope is a
  durable fence: it seals the write queue and waits for one flush. A writer
  that arrives in that time waits for the same flush (up to the flush
  interval, 500 ms). The run pays this once, at its start, and not at each step.

### Rows without an at-rest envelope

A row in an encrypted collection that carries no at-rest envelope reads as
absent (the product treats it as not written). A scan drops such a row **after**
it applied the row limit, so a page can come back short although the range
holds more rows. A short page is therefore not the end of a range. The range
walk (`atom_store/bounded_range.rs`) counts the dropped rows for each read
(`crypto::count_unsealed_discards`, a count that belongs to that read only) and
adds them to the rows it got: a page is full when the sum reaches the rows asked
for. When a page holds only dropped rows, it names no key to continue from, so
the walk asks for more rows from the same start (up to 10,000 rows). A read that
then hits the read limit, and a page of 10,000 dropped rows, **cut the key range
in two** and read the lower half first: a smaller page would repeat the same
reads and never move. Each cut makes the span smaller, so the walk always moves,
also for a run of dropped rows that is longer than one read and is followed by
many deleted keys. Cutting a tight run of keys takes about 150 reads. If 1,024
reads in a row find no row to continue from, or the range cannot be cut, the walk
stops with an error that names the rows without an envelope (it does not loop).
The verifier, the recount and the inventory all use this walk.

- The verifier reports the dropped rows as the fault `unsealed_rows` (a count of
  reads that dropped a row, with a sample that names the fix). The rest of the
  run reads on, so the other faults are not false.
- The recount **refuses to write** while any read dropped such a row: it cannot
  count a row that reads as absent, and a body or counter that is only absent
  for that reason must not decide a delete. A dry run plans no delete of a body.
  Run `lastdb db reap-unsealed --collection <name>` (dry run first), then
  recount again.
- The inventory counts the reads in `scan.unsealed_rows` and adds a note.

### Limits

- The totals hold about 32 bytes for each referenced atom, at most `--max-atoms`
  (default 1,000,000). Beyond it the verifier does not compare counters with
  totals and says so in the report (`tally_complete: false`). The checks inside
  a page still run.
- A range read across hash groups seeks all 1,024 hash groups of the collection,
  and one read call may read 4,096 pages. A very large home can exceed that
  page budget before the first row. The verifier then fails with a read
  resource error instead of a wrong answer.
- A run checks **one storage scope**: the personal scope, or `--scope PREFIX`
  (an org or share scope is a 64-hex prefix). A tip under an org prefix is not
  seen by the default run. `--all-scopes` checks the personal scope and each
  scope that the database catalog names (the catalog is small; listing it is not
  a scan of the data). A scope that holds data and has no catalog entry needs
  `--scope`. The report, the `Verify refs` status line and the command print the
  scope. A run that reads 0 tips in its scope prints a warning on stderr, a note
  in the report and `WARNING` in the status line: a mistyped `--scope` reads
  nothing and finds no fault.
- A run that cannot compare the counters with the totals (`tally_complete:
  false`, more atoms than `--max-atoms`) skips the check for a counter above its
  holders. The command prints `INCOMPLETE` and **exits non-zero** with a message
  that names the cap, also when it found no other fault.
- A record whose encryption does not verify ends the step with an error
  (HTTP 500): the batch read of the page fails as a whole. A record that
  decrypts and does not decode as JSON is counted as `unreadable_counter`,
  `unreadable_tip` or `unreadable_record`.

## Recount: `lastdb db recount`

The recount rebuilds every counter from the tips and the counted conflict
records. It is the one scan the counter design keeps. No gate and no write path
calls it.

It works on a **stopped home**. It refuses when a daemon is alive for the home,
when a daemon socket accepts a connection, when the last session did not end
cleanly, or when another offline tool holds `data/maintenance.lock`. It refuses
the primary home unless `--i-know-this-is-primary` is set.

For each atom of a scope it counts the tips and the counted conflict records
that name it, and compares the count with the counter record and the body:

| Found | The recount does |
|---|---|
| counter equals the count | nothing |
| counter below, above, or missing; counter does not decode | writes the exact count (`raise`, `lower`, `create`, `rewrite`) |
| counter equals the count and is `suspect` | clears the flag |
| counter without a body, and nothing names the atom | deletes the counter |
| body, and nothing names the atom | deletes the body, the counter and the blob references |
| a tip or record names the atom and no body exists | reports `lost_bodies`; nothing can repair a lost body |

The default is a dry run that reports every change. `--execute` writes the
changes in whole batches of at most 400 operations. It is safe to run again: a
stopped run leaves whole batches, and the next run changes only what is still
wrong. A second run after a complete run changes nothing.

It writes nothing while a tip or conflict row does not decode, because a count
that is too low loses data. It writes nothing while a read drops a row with no
at-rest envelope (see above). It recounts the personal scope; `--scope PREFIX`
(repeat for more) and `--all-scopes` add other scopes. `--shards N` makes N passes over the tips and
records and holds one Nth of the atoms in memory in each pass.
`scripts/lints/atom-counter-writers.allow` lists the two functions that write
(`recount/apply.rs`) with the reason.

After a recount, start the daemon and run `lastdb db verify-refs` (with
`--all-scopes` when you recounted other scopes). It must find
no fault except the lost bodies. Purge the keys of a lost body to remove them.

## Inventory: `lastdb db inventory`

The inventory walks the key classes of the home. A read call examines at most
10,000 keys, and a range read counts deleted keys. After many purges, the keys
between two live rows can be more than the budget, and an inventory then failed
with HTTP 500 `read resource limit exceeded`. Every walk of the inventory now
uses a bounded range walk (`atom_store/bounded_range.rs`): a read that hits the
limit is retried with fewer rows, and then the key range is cut in two and the
lower half is read first. Each read is its own call with its own budget. The
report shows the calls (`scan.calls`, `scan.shrinks`, `scan.splits`).

The inventory reports the counter records as their own key class
(`atom_counter`) and not as atom content. The `atom_counters` block shows the
counters, the sum and the largest count, the number of counters with a count of
1, 2, 3 to 9, 10 to 99 and 100 or more, the five most referenced atoms, and the
pairing faults (`bodies_without_counter`, `counters_without_body`,
`zero_counters`, `suspect`). The key classes list the colon plane of each key
family. History rows and conflict rows are in the NUL plane and are not in the
class list.

## Format fence

A home with this record layout sets the required-feature bit `FEATURE_ATOM_RECORDS`
(value 128) in the LastStore `CURRENT` file. A build that does not know the bit
refuses the home. This build refuses a home without the bit.

## Known limits

- Cloud sync replay and restore write rows that bypass the planner. Cloud sync
  is off in this build. `lastdb db verify-refs` finds the damage, and
  `lastdb db recount` repairs it.
- A lost body (a tip names an atom that has no body) is found, never repaired.

## Not in this step

Steps S2 to S8 of the design (`docs/design-lastdb-counter-lifecycle.md`) remain:

- S3: the recorder releases conflict references (a janitor), and a purge reads the slot directory.
- S5: blob counters replace the blob edges and the file blob sweep.
- S7: a resumable drain of a dead molecule.
- Cloud sync replay and restore write rows that bypass the planner. Cloud sync is off in this build.

# Atom counter and the life of stored data

This page is the operator and reader reference for how LastDB decides that an
atom body, a molecule, or a file blob may leave the store. The plan and the
reasons are in `docs/design-lastdb-counter-lifecycle.md`. The detail pages are
linked from each section.

## Terms

| Term | Meaning |
|---|---|
| Atom | One immutable value of one field. Its body is a record. Many tips can name the same atom. |
| Tip | The record that says which atom is current for one key of one molecule (`mk:{molecule}:...`). |
| Molecule | The index of one field. It maps keys to tips. |
| Hash group | All records whose key shares the part before the first NUL. A hash group is one unit of storage placement. |
| Counter | A small record that holds a count of references. |
| Scope | A storage prefix. The personal scope has none. An org or share scope has a 64-hex prefix. A counter exists in the scope of its atom. |
| Batch | The set of writes of one request. A crash keeps a whole batch or none of it. |
| Write gate | The one lock that orders the writers of a node. A request holds it from prepare to the acceptance of its batch. |

## The rule

The counter is the only authority for the life of an atom body, of a molecule,
and of a file blob.

- An object may leave the store only when its counter reads exactly zero. The
  read happens inside the same write-gate hold as the batch that removes the object.
- No other structure allows a delete. No other structure blocks one. A blob
  has one extra rule, the dwell (see Blobs and the dwell).
- A counter that is wrong in the low direction loses data. A counter that is
  wrong in the high direction leaks space. A crash cannot cause either, because
  a batch is atomic. Only a defect in a writer can.

## The records

| Record | Key | Collection | Holds |
|---|---|---|---|
| Atom body | `atom:{uuid}\0b` | `atoms` | the value |
| Atom counter | `atom:{uuid}\0c` | `atoms` | `{"v":1,"refs":N,"blobs":[...],"sz":[a,b],"src":"source-schema","suspect":true}`; optional values appear only when set |
| Molecule header | `mh:{M}` | `tips` | `slots` and `records` counters |
| Binding counter | `mref:v1:n:{target}` | `molecule_ref_edges` | `bindings` |
| Binding record | `mref:v1:e:{target}\0{source}` | `molecule_ref_edges` | one per schema field, protein member, or catalog entry |
| Drain candidate | `mref:v1:d\0{scope}\0{molecule}` | `molecule_ref_edges` | a molecule that lost its last binding |
| Conflict record | `history\0{molecule}:{ts}:conflict:{digest}` or `...:suppressed:{digest}` | `tips` | one counted reference to each distinct atom it names |
| Retention queue entry | `crq:v1:{bucket:020}\0{expires:020}:{record_id}` | `tips` | the expiry of a conflict record |
| Queue bucket directory row | `crb:v1\0{bucket:020}` | `tips` | a token |
| Slot directory entry | `{scope:}crs:v1:{M}\0{esc(hash)}\0{range}\0{record_id}` | `tips` | links a slot to its conflict records |
| Blob counter | `bref:v3:c:{sha256(blob_ref)}\0c` | `atoms` | `refs`, `zero_since`, `suspect` |
| Blob dwell queue entry, directory row | `bref:v3:q:...`, `bref:v3:d\0...` | `atoms` | the blobs that wait out their dwell |

`{M}` is the molecule uuid. `{target}` is the hex SHA-256 of it. A scope adds
its prefix before the key. The first NUL ends the hash, so the body and the
counter of one atom share one hash group, and a purge touches one group for
each atom. A counter holds `suspect` only after a planner found a fault.
The optional `src` value retains the atom's source schema for a usage debit after a protein peer removes its last reference.
An older counter without `src` makes schema usage state incomplete; the code does not guess its owner.

Details per family: `docs/lastdb-molecule-counters.md`,
`docs/lastdb-blob-counter.md`, `docs/lastdb-time-queue.md`.

### Invariants

For every scope and every atom `A`:

- I1: `refs(A)` equals the number of tip records plus counted conflict records that name `A`.
- I2: the body exists if and only if the counter exists. A counter never holds zero, except a counter with `suspect`.
- I3: a batch lowers `refs(A)` by `k` only if the same batch removes `k` references to `A`. The batch reads each of them from the store inside the gate hold.
- I4: the stored count is never lower than the true number of references.

## The planner and the write gate

One module writes tips, conflict references, bodies and counters:
`fold_db/crates/core/src/db_operations/atom_store/counter/`.

1. The caller holds the write gate through a request scope. A call without a
   scope on a native store fails: the one atomic batch is the safety argument.
2. The planner reads the old tip records, the counters and the bodies by key.
   A read inside the gate sees the committed root, the accepted batches and the
   pending batch of the request.
3. `plan.rs` computes each counter change from the old tips, the new tips and the
   changed conflict records. It is pure arithmetic and it aggregates per atom
   first. Sixteen purged keys that share one atom give one change of minus sixteen.
4. `build.rs` makes the writes and `commit.rs` joins them to the pending batch
   with one `batch_mutate`. The same run changes the molecule counters `slots`
   and `records` and the blob counters.

The callers: tip writes (create, update, rewrite), the protein sibling write,
user Delete, Purge, the TTL sweep, the conflict recorder, the retention janitor,
the molecule drain, the retirement of queue buckets, and the repair of queue
rows.

`scripts/lints/lint-atom-counter-writers.py` guards the boundary, function by
function. A function that writes through a store and names a tip, conflict,
body, counter, molecule or blob-counter key must be in
`scripts/lints/atom-counter-writers.allow` with the families it touches and a
reason. The lint is a source check. It cannot see a key that a caller builds
elsewhere and passes to a generic helper.

### What a request may spend

| Limit | Value |
|---|---|
| Operations in one native batch | 16,384 (4,096 on a home from an older build) |
| Examined keys in the reads of one write request | 40,000 |
| Examined keys in one ordinary read call | 10,000 |
| Metadata pages in one read call | 4,096 |
| Decoded blocks, bytes in one read call | 16,384 blocks, 256 MiB |

A read call ends with the first limit it reaches. The error is
`read_resource_limit` (HTTP 503). The binding limit is often the 4,096
metadata pages and not the key count: a request that reads one slot directory
for each of hundreds of slots, or that rewrites about 2,000 fields, spends the
pages first. A deleted key counts as an examined key until compaction removes it.

A purge or delete stages all its chunks of 64 keys in one batch. The node counts
the request before it takes the gate (`fold_db_core/purge/budget.rs`). For K keys
of F fields the upper bounds are:

- purge: `4*K*F` operations and `12*K*F` examined keys,
- delete: `5*K*F` operations and `13*K*F` examined keys,
- plus `F + 8` of each for every chunk of 64 keys.

A tip costs the tip delete, the `mgd` marker, and the body delete and the
counter delete when it holds the last reference. A delete adds one barrier row.
A request over a limit is refused with HTTP 400 before it takes the gate, and
the message gives the number of keys that fits. A schema of 24 fields fits 138
keys for a purge and 128 for a delete on a fresh home (42 and 33 on a home from an
older build). The TTL sweep cuts its batch to fit and halves a batch that still
hits a limit.

## Delete on zero, delete and purge

A batch that lowers the count to exactly zero deletes the body and the counter,
and lowers the blob counters that the counter lists, in the same batch as the
last tip change. There is no grace window, no audit and no queue for an atom body.

Delete and Purge read the tips of their keys by key, in one batch, inside the
gate. They give every tip change to the planner and stage one batch. They take no
schema barrier, no molecule lock and no atom lock. A Delete also takes the tip-commit
locks of its keys, to order itself against a memory delete that registered its
barrier before the persist lane ran. A Purge takes none. The cost follows the number
of keys of the request and never the size of the schema.

| | Delete | Purge |
|---|---|---|
| Removes the tips of the key | yes | yes |
| Frees each body whose last reference goes | yes, same batch | yes, same batch |
| Writes a delete barrier (`rdel:v2:`) | yes, so a replayed older write cannot bring the key back | no |
| Reaches the protein siblings of the key | HashRange siblings only | HashRange siblings only |
| Releases the kept conflict records of the key | yes | yes |
| Leaves `mgd:v1:` markers | yes | yes |

A sibling of another layout keeps its tip, so its atom stays. The counter keeps the data
safe, but the record is not erased everywhere. A Purge with a cutoff spares a key that
has a tip written at or after the cutoff.
`GET /api/atom/{id}` returns 404 after the last holder is gone (see Reads and
acknowledgement for the exception after a `queued` write).

## Underflow and `suspect`

A removal can ask for more references than the counter holds, or name an atom
with a body and no counter. The planner never clamps the count to zero and
deletes the body. It erases the tip, keeps the body, sets `suspect` on the counter,
and reports the case. A write that brings an atom whose body exists and whose
counter is gone takes the same path: the planner keeps the body, writes a counter
with only the new references, and sets `suspect`. It does not treat the write as a
fresh create, because the holders that the lost counter counted are not known.
A `suspect` counter never reaches zero for a delete.

**Limit.** The planner catches a counter that is below the removal it gets. It
cannot catch a counter that is too low by less than that. Example: two tips name
an atom and the counter holds 1. A purge of one key asks for 1 reference, the
counter holds 1, so the planner takes it to zero and deletes the body. The other
key still names the atom, and a query of that key then returns a bare HTTP 500.
`lastdb db verify-refs` reports such a counter as `counter_below_tips` before the
purge. `lastdb db recount --execute` (offline) repairs it. Repair a counter before
a purge or a TTL sweep reaches its atom.

A planner fault that raises `suspect` for a molecule counter works the same way:
`docs/lastdb-molecule-counters.md`, section Underflow.

## Format fence

Two markers keep one build from opening a home with another counter model.

- The LastStore `CURRENT` file carries the required-feature bit
  `FEATURE_ATOM_RECORDS` (value 128): every atom body is one record at
  `atom:{uuid}\0b` in `atoms`. A build that does not know the bit refuses the
  home. This build refuses a home without the bit.
- The logical key format marker is `layout:v3:keys` with the value `5`
  (`atom_store/key_format.rs`). Value `4` uses the same counter model.
  The new reader accepts it and advances the encrypted marker before new writes.
  Value `5` permits atom and CAS records without a duplicate outer envelope.
  An older reader refuses value `5` before it can discard those records.

Earlier counter layouts remain refused. The marker advance rewrites no existing
atom, CAS record, or pack file. See [the element encryption policy](designs/native-element-encryption-v1.md).

## Conflict records and their retention

The database keeps every conflicting write. A conflict record is a history row
from the tip-conflict recorder (`...:conflict:...`) or from a Put that lost to a
Delete (`...:suppressed:...`). It holds one counted reference to each distinct atom
it names (I1). A Replaced record names the loser and the winner. A Dropped record
names the loser only. A kept loser keeps its body while its record exists, also
after its last tip is purged.

When a record is new, the same batch writes three more rows (a repeat of one
decision writes none):

| Row | Use |
|---|---|
| Retention queue entry | one hash for each time slice of the expiry; a range read of a bucket returns the oldest expiry first |
| Bucket directory row | one row for each bucket that holds, or held, an entry; the value is a token that changes with each entry |
| Slot directory entry | one hash for each scope and molecule; a purge reads the entries of the slots it removes |

`record_id` is the SHA-256 of the full key of the history row. The queue and the
directories hold no atom reference. The history row is the only holder. The queue
is the time-bucketed queue of `docs/lastdb-time-queue.md`. The bucket is the start
of a slice of the expiry. The slice is a 64th of the retention window, between one
second and one hour.

A record ends in two ways, and both use the same release.

**The key goes.** A delete, a purge and a TTL sweep read the slot directory of the
slots they remove and release the records in the same batch as the tip deletes.

- The erase first reads the header of each molecule it touches, in one batch read.
  It skips a molecule whose `records` is zero. A molecule that is `suspect`, or has
  no counters, is read as if it held records.
- A removed slot of a molecule that holds records costs one range read. An entry
  counts only when its `mk:` key matches the slot exactly. One erase request reads
  the directories of at most 256 slots and releases at most 100 records in its
  own batch (the cap is for the whole request).
- For each slot that still holds records, and for each slot the erase did not read,
  the same batch writes one **drain marker**: a queue entry that is due at once.
  It costs one operation for each slot, however many records the slot holds.
  `purge_capped_slots` in the status counts these slots.
- The janitor drains the markers. It keeps a cursor over the directory of the slot,
  releases the records 32 for each batch, and deletes the marker with the last
  batch. A drain releases only the records written before the erase.
  The marker stays until that exact slot directory is complete.
  A shorter retention window cannot discard unfinished erase work.
  Each record also retains its own expiry queue entry.
  The marker's older `window_nanos` field remains readable but supplies no completion proof.
- A protein key move is not an erase and releases nothing.
- The reads of an erase run inside the gate and share the 40,000 key budget. A slot
  whose earlier releases left more than that many deleted directory keys can make
  the read of a later erase of the same slot fail with a read error. The erase then
  fails as a whole and changes nothing. Compaction removes the deleted keys.

**The window ends.** The janitor (`fold_db_core/conflict_janitor.rs`) wakes every
`LASTDB_CONFLICT_JANITOR_MS` (60,000). It lists the bucket directory and walks the
buckets that have started, oldest first, in pages of 32 keys, without the write
gate. It runs one release batch for each page that has due entries. The window is
`LASTDB_CONFLICT_RETENTION_SECS` (30 days). The clock starts when this node records
the pair.

- Pacing. A background release batch shares the bounded collection window.
  It returns after durability. Its measured cost includes the gate and shared flush holds. The janitor rests
  after each batch for `blocked * (100 - duty) / duty`, where `blocked` is that
  time and `duty` is `LASTDB_CONFLICT_JANITOR_DUTY_PCT` (10). The rest is at least
  25 ms and at most 30 s. When no other task opened a write scope for a quiet period
  (`LASTDB_MAINTENANCE_QUIET_SECS`, 60 s), the rest protects nobody and it is the
  minimum rest. See `docs/lastdb-time-queue.md`, section Pacing.
- Alternation. A wake runs up to 4 batches of slot drains, then up to 4 batches of
  due records, and again. A long drain delays due records by at most 4 batches.
- Failure. A wake that ends with an error doubles the wait before the next wake,
  from the interval (at least one second) up to ten minutes. A good wake clears it.
- Clock. The queue reads one clock for the expiry it writes and the expiry it tests:
  the wall clock, never more than a day ahead of the time that the monotonic clock
  gives since the store was made. A forward jump longer than a day gives no record a
  far-future expiry. A smaller forward jump releases records early by its size. A
  backward jump delays the release by its size.
- Restart. The cursors are in memory. After a restart, and once in 60 wakes, the
  janitor lists the directory once and walks each live bucket from its start.
- Safety net. A bucket that holds live entries and has no directory row is never
  listed. At its start, and once in `LASTDB_CONFLICT_JANITOR_NET_SECS` (3,600; zero
  turns it off), the janitor probes the slices that can hold a live entry by
  arithmetic and writes the missing row again.

A release names the history row to the planner (`counter::ReleaseRequest`). The
planner reads the row inside the same gate hold, lowers one reference for each
distinct atom the row names, lowers `records` in the header of the molecule that the
row names, and deletes the row, the conflict row, the directory entry and the queue
entry in one batch. A caller never names an atom, and the janitor never deletes an
atom: an atom whose count reaches zero goes by the rule above. A release of a row
that is gone lowers nothing. A row that does not decode is kept with its references
and counted (`undecodable_rows`). Only a row that `is_counted_record_key` accepts can
be released.

`lastdb db clear-history` (`POST /api/db/clear-history`) never selects a conflict row.

Not covered: the conflict-state caches `mcc:` and `hcu:mols` (the conflict reader
repairs them when it finds a conflict row gone), and the keep-small meters, which a
release does not debit.

## Molecule counters and the drain

A molecule has three counters: `slots` (tip records) and `records` (conflict records)
in its header, and `bindings` (schema field, protein member and catalog binding
records) in `mref:v1:n:{target}`. `bindings` is not in the header because the
presence of a header means that the molecule lives at that prefix, and a bind comes
before the first tip. A molecule may go only when all three are zero, none is
`suspect`, and no binding record exists.

The batch that lowers `bindings` to zero writes a drain candidate row. The drain
(`BackgroundMoleculeDrainTask`) then erases the keys of the dead molecule through the
planner, releases its kept conflict records, and removes its rows, in bounded
batches. It re-reads the counters inside the gate for every batch. A schema whose
molecules still drain cannot be installed again: the install answers HTTP 409
`schema_being_dropped` with a reason and a `Retry-After`.

Details, the batch phases and the limits: `docs/lastdb-molecule-counters.md`,
`docs/lastdb-molecule-drain.md`.

## Blobs and the dwell

Every local file blob has one counter. A reference is an atom counter that lists the
blob, or a live tip whose metadata names it. The batch of the change raises or lowers
the blob counter. A blob at zero is not deleted at once, because an upload comes
before the atom that names it. The blob waits for the dwell (`LASTDB_BLOB_DWELL_SECS`,
3,600; the floor is 120). A reference that arrives during the dwell cancels the
delete. The dwell pass reads the counter again inside the gate, then deletes the blob
row, the counter and the queue entry in one batch.

A write that names a blob that is already gone is accepted, and it is made loud:
`late_references` in the status, `referenced_row_missing` in `liveness explain`, and
`blob_counter_without_blob` in `verify-refs`.

Details: `docs/lastdb-blob-counter.md`.

## The time-bucketed queue

LastDB has no scan, and a read call has a budget. A queue under one hash fails after
about 10,000 deletions, because a deleted key still counts as examined and the oldest
entries sit at the head. The time-bucketed queue (`atom_store/time_queue/`) gives each
slice of time its own hash and keeps a directory of buckets. A reader lists the
directory, walks one bucket with a cursor that moves over deleted keys, and retires a
bucket with a guarded delete of its directory row. A generic safety net re-registers a
bucket that has entries and no directory row. The retention queue and the blob dwell
queue use it. Its contract is in `docs/lastdb-time-queue.md`.

## TTL sweep

The node-local sweep erases the expired keys of every schema that has a retention
policy. The default verb is `purge`. `delete` is opt-in and leaves barrier rows.
The owner can remove older barriers through an explicit stopped-home replay cutoff.
No timer advances that cutoff. See [delete-barrier cleanup](delete-replay-cutoff.md).
Both verbs go through the planner, free the bodies at once, and
release the kept conflict records of the slots they remove. See
`docs/lastdb-ttl-sweep.md`.

## Reads and acknowledgement

- Every native write returns `durability: durable` after the complete disk commit and clean core publication.
  Create, Update, Delete, Purge, and replay share that boundary. There is no early memory ack or 500 ms collection timer.
  Each native commit keeps the affected tips, counters, and bodies consistent.
- A query by key sees the new committed value after the ack.
  `GET /api/atom/{id}` reads a body by identity, without a tip.
  It returns 404 after the commit removes that body's last counted reference.
  A body with another reference remains present.
- `POST /api/storage/liveness/explain` (`lastdb liveness explain atom|molecule|blob`)
  returns the counters. For an atom it returns `refs`, `suspect`, `blobs`,
  `body_present` and a state: `live`, `suspect`, `inconsistent` or `absent`. One
  call takes up to 10,000 ids. It cannot list the referrers: the counter holds a
  number, not a list.
- Private background requests can share the eager FIFO commit without early publication.
  Write preparation can read that owned prefix; ordinary reads retain the committed state.
  The janitor, dwell pass, and molecule drain still return after durability.
  A batch that requires a sealed fence includes that flush wait in its measured writer cost.

## Checking and repair

| Tool | Route (owner socket) | Where it runs | What it does |
|---|---|---|---|
| `lastdb db verify-refs` | `POST` and `GET /api/db/verify-refs`, `GET /api/db/verify-refs/scopes` | live node | reads, writes nothing, holds no write gate for a read, reports faults |
| `lastdb db recount [--execute]` | none (offline) | stopped home | rebuilds the counters from the tips and the counted conflict records |
| `lastdb db inventory` | `GET /api/db/inventory` | live node | walks the key classes of the home; it also writes idempotent attribution rows, so it is not read-only |
| `lastdb db molecule-drain` | `POST /api/db/molecule-drain` | live node | names a molecule to the drain and shows the drain report |
| `lastdb liveness explain atom\|molecule\|blob` | `POST /api/storage/liveness/explain` | live node | shows the counters of named atoms, molecules or blobs |
| `lastdb db delete-ledger` | `GET /api/db/delete-ledger` | live node | reads the durable audit trail of purges and deletes, oldest first; a row names the caller and holds counts, never content |
| `lastdb db clear-history` | `POST /api/db/clear-history` | live node | trims history rows; it never selects a conflict row |
| `lastdb db gc-proteins` | `POST /api/db/gc-proteins` | live node | deletes empty, unbound `protein:` rows (the drain leaves them) |
| `lastdb db reap-unsealed` | `POST /api/db/reap-unsealed` | live node | removes rows that have no at-rest envelope (see below) |
| `lastdb db compact` | `POST /api/db/compact` | live node | packs the native home and reclaims old files (`docs/lastdb-storage-parts.md`) |
| `lastdb status` | `GET /api/status` | live node | counters, last verifier result, queue depth, parts |

### `lastdb db verify-refs`

The verifier finds a counter that disagrees with the tips and the conflict records.
It runs on request only. It writes no counter, no body, no tip and no record of its
own, and it holds no write gate for any read (see A consistent view for the one empty
write scope at the start of a run). It keeps its result in memory, so a restart clears
it. The writer lint lists no writer in `counter/verify/`.

A run reads in phases, each in pages with a cursor:

1. Tip records (`mk:`): the atom of each tip has a body and a counter that is not
   below the tips of the page.
2. Counted conflict records: the same checks for the atoms a record holds. All
   history rows share one hash, so this phase reads every `history\0` row and skips
   the uncounted ones. Its cost follows the total size of the history.
3. Atom bodies and counters (`atom:`): body and counter exist together, no counter
   holds zero, none is `suspect`, and the counter equals the totals of phases 1 and 2.
4. Molecule headers, `bindings` counters and binding records (three phases).
5. The retention queue (`queue` and `suspects`): the entries against the bucket
   directory, and the conflict records against the entries.
6. Blob counters and the blob dwell queue (two phases, personal run only).

Phases 1 and 3 also read the blob counter of each blob that a page names. A counter
that is absent or below the holders of the page is a fault. A counter above its
holders is not found by a run, because the blob counters are global and a run checks
one scope. The recount finds it.

A step reads pages until it has used `max_pages` (default 64) or `max_ms` (default
3,000), pauses `pause_ms` (default 2) after each page, and returns a `next_cursor`.
The command follows the cursor until the run is complete. It exits non-zero when it
finds a fault. Flags: `--json`, `--last` (show the last result, run nothing),
`--steps N`, `--cursor`, `--max-pages`, `--max-ms`, `--pause-ms`, `--page-rows`
(default 1,000), `--scope PREFIX`, `--all-scopes`, `--max-atoms` (default 1,000,000).
Routes: `POST /api/db/verify-refs` runs one step, `GET /api/db/verify-refs` returns
the last result and the run in progress, `GET /api/db/verify-refs/scopes` lists the
scopes of the catalog. All three are on the owner socket only.

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
| `record_without_queue_entry` | no queue entry can be reached from a conflict record (the janitor never releases it) |
| `queue_entry_without_directory` | a queue entry sits in a bucket that the directory does not list (the janitor, or the dwell pass, never finds it) |
| `unreadable_tip`, `unreadable_record`, `unreadable_counter` | a record does not decode |
| `unsealed_rows` | a read dropped a row that has no at-rest envelope |
| `blob_counter_missing` | an atom counter or a tip names a blob that has no counter |
| `blob_counter_below_refs` | a blob counter holds fewer references than the atom counters or tips of one page that name the blob |
| `blob_zero_without_dwell` | a blob counter holds zero, is not `suspect`, and has no `zero_since` |
| `blob_dwell_with_refs` | a blob counter holds references and has a `zero_since` |
| `zero_blob_without_queue_entry` | a blob counter has a `zero_since` and the queue has no entry for it |
| `blob_stale_queue_entry` | a queue entry names a counter that is absent, has references, is `suspect`, or has another `zero_since` |
| `blob_suspect` | a blob counter has the `suspect` flag |
| `unreadable_blob_counter` | a blob counter does not decode, or its blob does not match its key |
| `blob_counter_without_blob` | a blob counter holds references and the store has no blob row for it |
| `unconfirmed_count_mismatch` | not a fault: a candidate that a concurrent write can explain |

The molecule phases report their own faults, such as `molecule_slots_below_tips` and
`bindings_counter_mismatch` (`docs/lastdb-molecule-counters.md`, section Verifier).
A kind is a fact about one atom, and an atom can show more than one.

**A consistent view.** One page is one captured read scope. The tip rows, the
counters and the bodies of the page are read at one committed root, and a committed
root holds whole batches, so I1 to I4 hold at it. A fault found inside a page has no
false positive under concurrent writers. A count that differs from the totals of the
whole pass needs pages read at different roots. The planner tells the verifier which
atoms it changed while a run reads. A difference for an atom that no batch touched is
a fault. A difference for a touched atom is an `unconfirmed_count_mismatch`: run the
verifier again when the writers are quiet. A new run takes the write gate once, for an
empty scope, to order itself after the writes in flight. This is a durable fence: it
seals the write queue and waits for one flush, once at the start of the run. This
empty write scope also counts as a writer for the pacing of the background tasks.

**Limits.**

- The totals hold about 32 bytes for each referenced atom, at most `--max-atoms`.
  Beyond it the verifier does not compare counters with totals (`tally_complete:
  false`). The command prints `INCOMPLETE` and exits non-zero.
- A range read across hash groups seeks all 1,024 hash groups of the collection, and
  one call may read 4,096 pages. A very large home can exceed that budget before the
  first row. The verifier then fails with a read resource error.
- A run checks one storage scope: the personal scope, `--scope PREFIX` (a 64-hex
  prefix), or `--all-scopes` (the personal scope and each scope the database catalog
  names). A scope that holds data and has no catalog entry needs `--scope`. A scope
  name that no key can carry is refused with HTTP 400. A run that reads 0 tips prints a
  warning: a mistyped `--scope` reads nothing and finds no fault.
- A record whose encryption does not verify ends the step with HTTP 500.

**Rows without an at-rest envelope.** A row in an encrypted collection that carries no
envelope reads as absent. The encrypting store drops it after the inner store applied
the row limit, so a page of `limit` raw rows can open to fewer rows. The paged scans
of the encrypting store read on from the key after the last raw row until the page is
full or the range ends (`storage/encrypting_store/fill.rs`), so a short page from them
is the end of the range. The bounded range walk (`atom_store/bounded_range.rs`) names
the key where the next read goes on. The verifier reports dropped rows as
`unsealed_rows`. The recount refuses to write while any read dropped such a row: run
`lastdb db reap-unsealed --collection <name>` (dry run first), then recount again. A
key listing (`GET /api/list`) reads one page in one call with a budget of 10,000
keys, and a row without an envelope costs a key. A page that needs more fails with the
named 503 `read_resource_limit`. Ask for a smaller page.

### `lastdb db recount`

The recount rebuilds every counter from the tips and the counted conflict records. It
is the one scan the design keeps. No gate and no write path calls it. It works on a
stopped home. It refuses when a daemon is alive for the home, when a daemon socket
accepts a connection, when the last session did not end cleanly, or when another offline
tool holds `data/maintenance.lock`. It refuses the primary home unless
`--i-know-this-is-primary` is set.

| Found | The recount does |
|---|---|
| counter equals the count | nothing |
| counter below, above, or missing; counter does not decode | writes the exact count |
| counter equals the count and is `suspect` | clears the flag |
| counter without a body, and nothing names the atom | deletes the counter |
| body, and nothing names the atom | deletes the body, the counter and the blob references |
| a tip or record names the atom and no body exists | reports `lost_bodies`; nothing can repair a lost body |

The default is a dry run. `--execute` writes the changes in whole batches of at most
400 operations. A stopped run leaves whole batches, and a second run changes only
what is still wrong. It writes nothing while a tip or conflict row does not decode,
and nothing while a read drops a row with no envelope. It recounts the personal scope;
`--scope PREFIX` (repeat) and `--all-scopes` add others. `--shards N` makes N passes and
holds one Nth of the atoms in memory in each pass.

The recount also repairs the retention rows: a conflict record that no queue entry
reaches gets its queue entry back (at its own key, with its slot directory entry, when
the slot directory still holds the whole entry; otherwise it expires one window from
now), and a bucket gets its directory row. It uses the window that the janitor recorded
in `crb:v1\0params`, not its own environment.

For the blob counters, the default only raises counters, makes missing ones, writes a
missing queue entry or directory row, and deletes a queue entry that no counter backs.
`--blobs-exact` (needs `--all-scopes`, and every scope of the home must be listed) also
lowers a counter that is too high, clears `suspect`, and starts the dwell of a blob
that nothing references. A blob row with references and no row is listed as
`rows_missing`.

After a recount, start the daemon and run `lastdb db verify-refs`. It must find no
fault except the lost bodies. Purge the keys of a lost body to remove them.

### `lastdb db inventory`

The inventory walks the key classes of the home and reports the atom counters as their
own class (`atom_counter`): the counters, the sum, the largest count, the buckets by
count, the five most referenced atoms, and the pairing faults (`bodies_without_counter`,
`counters_without_body`, `zero_counters`, `suspect`). It also has a `Molecule counters`
block and classes for the retention rows. It writes idempotent attribution rows before
it reports (it is a GET, and it is not side-effect free). It takes `--out PATH`,
`--timeout SECS` and `--json`.

Every walk of the inventory, the verifier and the recount uses the bounded range walk
with `scan_range_yielding`. A read that runs short of key budget is not a failure: it
returns the live rows it found and names the last key it examined, and the next read
goes on above it. A read that fails with the read limit is retried with fewer rows, and
then the key range is cut in two. The report shows `scan.calls`, `scan.shrinks`,
`scan.splits` and `scan.skips`. On a synthetic home with a live row before each of three
clusters of 12,000, 5,000 and 40,000 deleted keys, `verify-refs` took 164 s and `inventory`
183 s before the walk existed, and about 3 s and 7 s after.

### Status lines

`lastdb status` is a snapshot that the sampler publishes every 60 s
(`LASTDB_SELF_METRICS_SAMPLE_SECS`).

| Line | Shows |
|---|---|
| `Atom counters` | planner events since this process started (a restart sets them to zero): `bodies_created`, `bodies_deleted`, `underflows`, `counters_absent`, `dangling_removals`, `suspect_marked`, `molecule_faults`, and recent faults. The count of `suspect` counters on disk comes from `verify-refs` or `inventory`. |
| `Verify refs` | the last complete run (time, scope, pages, faults, `suspect_atoms`, candidates) and a run in progress. A restart clears it. The status JSON has it in `atom_counters.verify_refs`. |
| `Molecule drain` | pending, waiting, refused, drained molecules, tips erased, bodies deleted, batches |
| `Blob counters` | the blob counter totals, dwell queue depth and buckets, next due time, passes, net sweeps |
| `Conflict retention` | window, janitor interval, last run, queue depth and buckets, released by janitor and by purge, slot drains pending, failed wakes, net sweeps |
| `Purge stats` | for each schema: purges, records purged, target slots and atoms deleted, since the process started |
| `Storage parts` | dependency parts of the storage receipt: `docs/lastdb-storage-parts.md` |
| `Local retention` | the TTL sweep: `docs/lastdb-ttl-sweep.md` |

## Crash states

A crash keeps whole batches only, in order. Each batch moves the store from one state
that holds I1 to another. The table lists the states a crash can leave and what recovers
each one.

| Crash point | State on disk | What recovers it |
|---|---|---|
| before the batch is accepted | no change | nothing to recover |
| after a `queued` acknowledgement, before the flush | the whole batch is lost | the client was told `queued`; a retry applies it |
| during the journal append, or after the commit and before `CURRENT` | no batch, or a complete batch | recovery returns only complete batches |
| after batch N of a job of many batches (TTL pass, janitor, drain, dwell pass, mixed request) | batches 1 to N applied | each batch is valid alone; the next pass continues |
| a schema drop between the catalog rows and the schema record | rows gone, schema still installed | run the drop again |
| a molecule drain between batches | header with zero counters, candidate row present | the next wake reads the candidate list and finishes |
| after a blob upload, before the atom | a blob with no reference | the dwell holds the blob; the atom comes in time or the dwell pass deletes the blob |
| an erase marked a slot and stopped | drain marker in the queue | the janitor drains it, or the records expire with their own window |
| a queue bucket without directory row (outside the queue) | entries that no reader lists | the safety net, `verify-refs`, `recount --execute` |
| a conflict record with no queue entry | a record that the janitor never releases | `verify-refs` finds it; `recount --execute` writes the entry |
| a writer that bypassed the planner | a count that is wrong | `verify-refs`, then `recount --execute` |
| a kill during pack or reclaim | files that no receipt names, an unfinished generation directory | `lastdb-pack-maintain reclaim`, the automatic sweep (`docs/lastdb-storage-parts.md`) |
| a row without an at-rest envelope | the row reads as absent | `lastdb db reap-unsealed`, then `recount` |

A leak and a loss do not come from a crash. They come from a defect in the planner or in a
writer that skips it.

## Settings

Only the node environment sets them. A change needs a restart of the daemon. A value
that does not parse gives the default.

| Name | Default | Meaning |
|---|---|---|
| `LASTDB_MAX_ATOM_CONTENT_BYTES` | 262,144 | Ceiling of one new atom (256 KiB). Range 1 KiB to 1 MiB. |
| `LASTDB_CONFLICT_RETENTION_SECS` | 2,592,000 | Retention window of a kept conflict record (30 days). |
| `LASTDB_CONFLICT_JANITOR_MS` | 60,000 | Interval of the janitor wake. `0` turns the janitor off. |
| `LASTDB_CONFLICT_JANITOR_DUTY_PCT` | 10 | Share of time (1 to 100) that the janitor may keep writers waiting. |
| `LASTDB_CONFLICT_JANITOR_NET_SECS` | 3,600 | Interval of the safety-net sweep. `0` turns it off. |
| `LASTDB_MAINTENANCE_QUIET_SECS` | 60 | Seconds without another writer after which the janitor and the dwell pass rest the minimum. |
| `LASTDB_BLOB_DWELL_SECS` | 3,600 | Dwell of a blob at zero. A value below 120 is refused. |
| `LASTDB_BLOB_DWELL_UNSAFE_SHORT_FOR_DEV` | off | `1` allows a dwell below 120 s, for a synthetic home only. |
| `LASTDB_BLOB_DWELL_PASS` | on | `0`, `false`, `no` or `off` turns the dwell pass off. |
| `LASTDB_BLOB_DWELL_CHUNK` | 32 | Blobs deleted in one batch (1 to 64). |
| `LASTDB_BLOB_DWELL_DUTY_PCT` | 10 | Share of time (1 to 100) that the pass may keep writers waiting. |
| `LASTDB_BLOB_DWELL_MIN_PAUSE_MS` | 25 | Shortest rest between two chunks. |
| `LASTDB_BLOB_DWELL_NET_SECS` | 3,600 | Interval of the safety-net sweep. `0` turns it off. |
| `LASTDB_MOLECULE_DRAIN_MS` | 30,000 | Interval of the drain wake. `0` turns the drain off. |
| `LASTDB_MOLECULE_DRAIN_DUTY_PCT` | 10 | Share of wall time (1 to 100) that drain batches may take. |
| `LASTDB_MOLECULE_DRAIN_TIPS` | 256 | Tip records in one drain batch (1 to 512). |
| `LASTDB_MOLECULE_DRAIN_BATCHES` | 1,024 | Batches in one drain wake. |
| `LASTDB_TTL_SWEEP` | on | `0`, `false`, `no` or `off` stops the TTL sweep. |
| `LASTDB_TTL_SWEEP_VERB` | `purge` | `purge` or `delete`. |
| `LASTDB_TTL_SWEEP_BATCH` | 16 | Keys erased in one batch (1 to 64). |
| `LASTDB_TTL_SWEEP_DUTY_PCT` | 25 | Share of wall time (1 to 100) that the sweep spends in batches. |
| `LASTDB_TTL_SWEEP_MIN_PAUSE_MS` | 25 | Shortest rest between two batches. |
| `LASTDB_DEPENDENCY_HIGH_WATER` | 4,096 | Storage parts at which maintenance starts (at most 75 percent of the limit). |
| `LASTDB_AUTO_MAINTENANCE` | on | `0` or `false` turns the storage maintenance worker off. |
| `LASTDB_AUTO_MAINTENANCE_GRACE_MS` | 0 | Grace for the reclaim of an automatic run. |
| `LASTDB_ORPHAN_MIN_AGE_MS` | 600,000 | Youngest unreferenced part file that the sweep removes. |
| `LASTDB_AUTO_MAINTENANCE_START_DELAY_SECS` | 30 | Wait before the first pack of a process. |

## Known limits

- Logical replay uses the counter writer. Raw lifecycle-row replay refuses before its cursor advances.
  Native restore keeps its target blocked until the restored records and counts validate.
  Cloud activation remains disabled; these guards do not establish cloud readiness.
- A lost body (a tip names an atom that has no body) is found and never repaired.
- The drain does not repair counter faults. The offline `db recount --molecules` option
  repairs molecule headers and binding counters from complete, readable source records.
  A missing live header needs metadata recovery before that repair.
- The planner cannot catch a counter that is too low by less than the removal.
- A blob row with no counter is invisible to the verifier and the recount, and nothing
  deletes it (cloud paths only).
- A request that mixes creates or updates with deletes or purges runs the writes first
  and the erasures after, as two batches. A delete and a write of the same key in one
  request leave the key deleted.
- Maintenance batches require a durable commit. A foreground writer can wait for that commit.
  Unreserved background batches can share the
  current commit window. Maintenance pacing uses its measured writer wait.
  See [TTL pacing and its limits](lastdb-ttl-sweep.md#pacing).
- Delete markers can remain in a long-lived molecule. The owner can use
  [bounded cleanup](lastdb-ttl-sweep.md#bounded-cleanup-of-redundant-delete-markers)
  when that molecule has no active or prepared immutable generation.

Limits of each part are on its page: `docs/lastdb-molecule-counters.md`,
`docs/lastdb-molecule-drain.md`, `docs/lastdb-blob-counter.md`,
`docs/lastdb-time-queue.md`, `docs/lastdb-ttl-sweep.md`.

## History

Before this design, an atom was deleted only after a live count, a candidate queue,
a 24-hour grace window, an audit lap and a peer-apply interlock all agreed. Reverse edge
records held the count outside the batch of the tip. The design page lists what was
removed and what replaced it, and the section "What was built and where it differs"
lists the steps and the limits: `docs/design-lastdb-counter-lifecycle.md`.

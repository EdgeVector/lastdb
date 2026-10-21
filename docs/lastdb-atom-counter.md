# Atom counter

## Contract

Every atom body in a storage scope has one counter record. The counter holds
the number of tip records and conflict records that name the atom. The counter
is the only authority for the life of a body. No other structure allows a
delete. No other structure blocks one.

A batch that lowers the count to exactly zero deletes the body and the counter,
and lowers the blob counters that the counter lists, in the same batch as the
last tip change. A body exists if and only if its counter exists. A counter never
holds zero, except a counter with the `suspect` flag.

## Records

| Record | Key | Collection |
|---|---|---|
| Body | `atom:{uuid}\0b` | `atoms` |
| Counter | `atom:{uuid}\0c` | `atoms` |

The first NUL ends the hash. The body and the counter of one atom share one
hash group. A storage scope adds its prefix before the key.

Blob references have two sources. A body names the blobs in its counter. A tip
record that carries file metadata names its own blob. The planner changes the
blob counters of both in the batch of the change: a created atom counter raises
the counters of its blobs, a deleted one lowers them, and an old tip record lowers
and a new tip record raises the counters of the blobs that their metadata names.
The contract of the blob counters is in `docs/lastdb-blob-counter.md`.

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

**Limit.** The planner catches a counter that is below the removal it gets. It
cannot catch a counter that is too low by less than that. Example: two tips name
an atom and the counter holds 1. A purge of one key asks for 1 reference and the
counter holds 1, so the planner takes it to zero and deletes the body. The other
key still names the atom, and a query of that key then returns a bare HTTP 500.
`lastdb db verify-refs` reports such a counter as `counter_below_tips` before the
purge, and `lastdb db recount --execute` (offline) repairs it. Repair a counter
before any purge or TTL sweep reaches its atom.

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

## Conflict records and their retention

The database keeps every conflicting write. A conflict record is a history row:
`history\0{molecule}:{ts}:conflict:{digest}` from the tip-conflict recorder, or
`history\0{molecule}:{ts}:suppressed:{digest}` from a Put that lost to a Delete.
It holds one counted reference to each distinct atom it names (I1). A Replaced
record names the loser and the winner. A Dropped record names the loser only.
So a kept loser keeps its body while its record exists, also when its last tip
is purged.

Three more rows go with a new record, in the same batch as its history row and
only when the record is new (a repeat of one decision writes none of them):

| Row | Key | Use |
|---|---|---|
| Queue entry | `crq:v1:{bucket:020}\0{expires_at_nanos:020}:{record_id}` | one hash for each time slice of the expiry; the range is the expiry, so a range read of a bucket returns the oldest expiry first |
| Bucket directory row | `crb:v1\0{bucket:020}` | one row for each bucket that holds, or held, an entry; the value is a token that changes with each entry |
| Slot directory entry | `{scope:}crs:v1:{M}\0{esc(hash)}\0{range}\0{record_id}` | one hash for each scope and molecule; a purge reads the entries of the slots it removes |

The queue is a time-bucketed queue. Its contract (layout, writers, readers,
retirement of a bucket, count of its size) is in `docs/lastdb-time-queue.md`.
The bucket is the start of a slice of the expiry. The slice is a 64th of the
retention window, between one second and one hour (one hour for the default
window).

`record_id` is the SHA-256 of the full key of the history row. Both entries hold
the same JSON: the history key, the conflict row key, both entry keys and the
`mk:` key of the slot. The queue and the directories hold no atom reference. The
history row is the only holder.

A record ends in two ways, and both use the same release:

1. **The key goes.** A delete, a purge and a TTL sweep read the directory of the
   slots they remove (inside `remove_molecules_keys_with_rows`) and release the
   records in the same batch as the tip deletes. The erase first reads the
   header of each molecule it touches, in one batch read, and skips a molecule
   whose `records` is zero (a molecule that is `suspect` or has no counters is
   read as if it held records). For a molecule that holds records, a removed
   slot costs one range read, and an entry counts only when its `mk:` key
   matches the slot exactly. The reads run in the write gate and a call may
   spend a fixed budget (4,096 metadata pages, 40,000 keys, deleted keys
   included), so one erase request reads the directories of at most 256 slots.
   One erase request releases at most 100 records in its own batch
   (the cap is for the whole request, not for each slot). For each slot that
   still holds records, and for each slot that the erase did not read (the cap
   was full, or 256 slots were read), the same batch writes one **drain marker**, a queue
   entry that is due at once, in the bucket of now (the key holds the hash of
   the slot prefix). It costs one operation for each slot, however many
   records the slot holds, and it needs no read of the records. The janitor
   keeps a cursor for each marker: a range walk over the directory of the slot
   that moves over the keys of the records it released. It releases the records,
   32 for each batch, and deletes the marker with the last batch. So no record
   of a removed slot waits for its window: the janitor ends it in the wake that
   follows the erase, and a slot of any size drains in batches of one size. The
   drain releases only the records written before the erase (`created` in the
   entry). A drain does not hold back the due records of the queue: the janitor
   alternates a few drain batches with a few batches of due records. A protein
   key move is not an erase and releases nothing. `purge_capped_slots` in the
   status counts the slots that got a marker.

   The reads of the erase run in the write gate. A read passes the deleted
   directory keys of its slot, and the gate allows 40,000 keys for the request. A
   slot that had more than that many records released before can make the read of
   a later erase of the same slot fail with a read error. The erase then fails as
   a whole and changes nothing. Compaction of the files removes the deleted keys.
   The drain of the janitor has no such limit: it reads outside the gate.
2. **The window ends.** The janitor (`fold_db_core/conflict_janitor.rs`) wakes
   every `LASTDB_CONFLICT_JANITOR_MS` (60,000; zero turns it off). It lists the
   bucket directory (the buckets that have started), and walks those buckets
   oldest first, in pages of 32 keys, with no write gate. It runs one
   release batch for each page that has due entries (`background_scope`: one
   gate hold, then durable). Its cursors are in memory, one range walk for each
   bucket, and they move over deleted keys: no read starts again at the head of a
   hash that holds many deleted keys. A bucket that is read to its end, with
   no marker in it that waits for a drain, leaves the directory. The directory
   row is deleted in the write gate only while its token is the one the janitor
   read before the walk, so an entry that a writer added after the walk keeps the
   bucket. The deleted keys of a retired bucket are never read again. The gate is
   free between pages, and the janitor
   rests after each batch for `took * (100 - duty) / duty`
   (`LASTDB_CONFLICT_JANITOR_DUTY_PCT`, default 10), because the flush of a
   release batch shares the disk with the writers. The window is
   `LASTDB_CONFLICT_RETENTION_SECS` (30 days). The clock starts when this node
   records the pair.

   The queue reads one clock (`RetentionState::clock_nanos`) both when it writes
   an expiry and when the janitor tests it: the wall clock, but never more than
   a day ahead of the time that the monotonic clock gives since the store was
   made. A forward excursion of the wall clock longer than a day therefore gives
   no record a far-future expiry and makes the janitor release nothing early. A
   smaller forward jump moves the clock, and the janitor then releases records
   early by at most the size of the jump (acceptable for a conflict record). A
   backward jump delays the release by its size. A wall clock that was wrong
   when the node started and was corrected by more than a day later is read as
   the monotonic time; at the next start the records written in the wrong period
   are released early.

   A restart loses the cursors. The janitor then lists the directory once and
   walks each live bucket once from its start, with bounded reads. Once in 60
   wakes the cursors of the live buckets start again the same way, in case the
   clock stepped back. Neither reads a bucket that the directory dropped.

   The queue has a safety net. A bucket that holds live entries and has no
   directory row is never listed, so its records would stay for ever. At its
   start, and then once in `LASTDB_CONFLICT_JANITOR_NET_SECS` seconds (3,600;
   zero turns it off), the janitor probes the slices that can hold a live entry
   (by arithmetic, no scan) and writes the missing row again. The status shows
   the sweeps and `buckets repaired`. See `docs/lastdb-time-queue.md`
   ("The safety net").

   A page of drain markers needs no write gate and costs no batch: the janitor
   keeps the markers it reads as pending drains (at most 1,024).

   The wake alternates two kinds of work: up to 4 batches of slot drains, then
   up to 4 batches of due records, and again. So a long drain delays the due
   records by at most 4 batches, and a long backlog delays a drain by at most 4
   batches. A wake that ends with an error (for example, the storage refuses a
   write because a file holds too many parts) makes the janitor wait longer: the
   wait doubles with each wake in a row that fails, from the interval (at least
   one second) up to ten minutes. The status shows the error, the number of failed
   wakes and the time of the next try. A good wake sets the number to zero.

A release names the history row to the planner (`counter::ReleaseRequest`). The
planner reads the row inside the same gate hold, lowers one reference for each
distinct atom the row names, lowers `records` in the header of the molecule that
the row names (a record raises it when it is new), and deletes the row, the
conflict row, the directory entry and the queue entry in one batch. A caller never names an atom,
and the janitor never deletes an atom: an atom whose count reaches zero goes by
the delete-on-zero rule above. A release of a row that is gone lowers nothing,
and a row that does not decode is kept with its references and counted
(`undecodable_rows`). Only a row that `is_counted_record_key` accepts can be
released.

`lastdb db verify-refs` surveys the queue (phase `queue`) and reports
`queue_entry_without_directory` (entries in a bucket that the directory does not
list) and `record_without_queue_entry` (a conflict record that no queue entry
names, read again before it is reported). `lastdb db recount --execute` (a
stopped home) writes the missing rows again through the planner: the row of a
bucket, or a queue entry that expires one window from now.

`lastdb status` shows the window, the janitor's last run, the queue depth (up to
1,000 entries) and the number of buckets, the slot drains that wait, the failed
wakes, and the records released by the janitor and by purges. The buckets come
from the directory. The entries are counted with bounded range walks, which pass
deleted keys, so a queue that holds many deleted keys still shows its depth. A
bucket that cannot be read makes the depth a lower bound (shown as `N+`). `lastdb
db inventory` has the classes `retention_queue`, `retention_bucket_directory` and
`retention_directory`.

Not done in this step: the conflict-state caches `mcc:` and `hcu:mols`, which the conflict reader repairs
when it finds a conflict row gone. The keep-small meters are not debited when a
release deletes rows or atoms (the old reaper did not debit them either).

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
   the old body. A query after the acceptance reads the new tip and the new
   body, and it never reads the old body. No query sees half of a batch. One
   read does not go through a tip: `GET /api/atom/{id}` reads a body by its id.
   After a `queued` acknowledgement it still returns 200 with the old body of a
   replaced atom until the flush commits the batch, and then it returns 404
   (see Reads for the measured delay). After a `durable` acknowledgement it
   returns 404 at once.
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

- `GET /api/atom/{id}` returns 404 after the last holder is purged. With
  `durability: queued` it returns 200 with the old body of a replaced atom until
  the flush commits the batch (measured 0.14 to 0.43 s after the acknowledgement,
  median 0.26 s, in 20 rounds on a synthetic home; the flush interval is
  500 ms). A query by key shows the new value at once. With `durability: durable`
  the route returns 404 as soon as the acknowledgement returns.
- `POST /api/storage/liveness/explain` with `{"class":"atom","ids":[...]}` returns
  the counter, the `suspect` flag and the body state of each atom. It cannot
  list the referrers: the counter holds a number, not a list.
- Molecule counters (`slots`, `records`, `bindings`) are in
  `docs/lastdb-molecule-counters.md`. The verifier checks them in three more phases.
- `lastdb status` has an `Atom counters` line, a `Verify refs` line and a
  `Blob counters` line (`docs/lastdb-blob-counter.md`).
  `lastdb db inventory` has an `Atom counters` block.
  - `Atom counters` counts the planner events since this process started:
    `bodies_created`, `bodies_deleted`, `underflows`, `counters_absent`,
    `dangling_removals` and `suspect_marked` (the counters this process marked
    `suspect`; `underflows` plus `counters_absent`). A restart sets these
    numbers to zero, so a home with `suspect` counters shows zeros after a
    restart. The line says so. The count of `suspect` counters on disk comes
    from one run of `lastdb db verify-refs` (`suspect_atoms`) or from
    `lastdb db inventory`; the status does not scan for it.
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

It reads in three phases for the atoms, three more for the molecule counters
(`docs/lastdb-molecule-counters.md`), and two more for the blob counters on the
personal run (`docs/lastdb-blob-counter.md`), each in pages with a cursor:

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
4. to 6. The molecule headers, the `bindings` counters and the binding records.
7. Blob counters (personal run only): each counter alone. A counter is
   `suspect`, holds zero with no `zero_since`, holds references and has a
   `zero_since`, or has a `zero_since` and no queue entry.
8. The blob queue (personal run only): each entry has a counter at zero with the
   same `zero_since`. The phase walks the entries of all buckets with one bounded
   range walk (an admin walk; the dwell pass never does that, it walks one bucket
   at a time from the directory). It does not check the directory against the
   entries; the generic check of the time queue does (follow-up).

Phases 1 and 3 also read the blob counter of every blob that the tips or the atom
counters of a page name. A counter that is absent, or below the number of holders
in the page, is a fault. A counter above its holders is not found by a run,
because the blob counters are global and a run checks one scope. The recount finds
it.

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
| `record_without_queue_entry` | no queue entry can be reached from a conflict record: its slot directory entry is missing, does not decode, or names a queue entry that is missing (the janitor never releases the record) |
| `queue_entry_without_directory` | a queue entry sits in a bucket that the directory of the queue does not list (the janitor never finds it; for the blob queue the dwell pass never finds it, and the blob phase reads every bucket of that queue) |
| `unreadable_tip`, `unreadable_record`, `unreadable_counter` | a record does not decode |
| `blob_counter_missing` | an atom counter or a tip names a blob that has no counter |
| `blob_counter_below_refs` | a blob counter holds fewer references than the atom counters or tips of one page that name the blob |
| `blob_zero_without_dwell` | a blob counter holds zero, is not `suspect`, and has no `zero_since` |
| `blob_dwell_with_refs` | a blob counter holds references and has a `zero_since` |
| `zero_blob_without_queue_entry` | a blob counter has a `zero_since` and the queue has no entry for it |
| `blob_stale_queue_entry` | a queue entry names a counter that is absent, has references, is `suspect`, or has another `zero_since` |
| `blob_suspect` | a blob counter has the `suspect` flag |
| `unreadable_blob_counter` | a blob counter does not decode, or its blob does not match its key |
| `blob_counter_without_blob` | a blob counter holds references and the store has no blob row for it |
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
absent (the product treats it as not written). The encrypting store drops such a
row **after** the inner store applied the row limit. A page of `limit` raw rows
can therefore open to fewer rows, and a loop that takes a short page for the end
of a range stops early with no error. Two parts of the code close this:

- The paged scans of the encrypting store (`scan_range_paged`,
  `scan_prefix_paged`) read on from the key after the last raw row until the
  page is full or the range ends (`storage/encrypting_store/fill.rs`). A short
  page from them is the end of the range. This covers every page loop (the key
  listings, the keyed pages, the journal replay, the pin-log walks).
- The range walk (`atom_store/bounded_range.rs`) reads with
  `scan_range_yielding`. Each read names the key where the next read goes on,
  taken from the raw rows, so a dropped row does not change it. The walk counts
  the dropped rows of each read (`crypto::count_unsealed_discards`, a count that
  belongs to that read only) to report them. The verifier, the recount and the
  inventory all use this walk.

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

The recount also checks the blob counters (`blobs` in the report). It counts, for
each blob, the atom counters that list it and the live tips that name it in the
listed scopes, and compares the count with the blob counter:

| Found | The default (raise only) | With `--blobs-exact` (needs `--all-scopes`) |
|---|---|---|
| counter below the count | raises it | raises it |
| counter missing, blob named | makes it | makes it |
| counter above the count | nothing | lowers it |
| counter `suspect` | nothing | clears the flag when the count is exact |
| blob with a counter and no reference | nothing | starts its dwell (the dwell pass reclaims it) |
| counter with a `zero_since` and no queue entry | puts the entry | puts the entry |
| queue entry that no counter at zero backs | deletes it | deletes it |
| blob with references and no blob row | lists it (`rows_missing`) | lists it |

The blob counters are global, and a recount lists scopes. A scope that is missing
makes the count too low, and a too low counter loses a blob. So only
`--blobs-exact` lowers a counter, and it asks for `--all-scopes`. List every scope
of the home: a scope that holds data and has no catalog entry needs `--scope`.

After a recount, start the daemon and run `lastdb db verify-refs` (with
`--all-scopes` when you recounted other scopes). It must find
no fault except the lost bodies. Purge the keys of a lost body to remove them.

## Inventory: `lastdb db inventory`

The inventory walks the key classes of the home. A read call examines at most
10,000 keys, and a range read counts deleted keys. After many purges, the keys
between two live rows can be more than the budget. Every walk of the inventory,
and every walk of the verifier and the recount, uses the bounded range walk
(`atom_store/bounded_range.rs`) with `scan_range_yielding`. Each read is its own
call with its own budget. A read that uses its whole key budget in a run of
deleted keys is not a failure: it returns no row and names the last key it
examined, and the next read goes on above that key. A range of D deleted keys
costs about D / 10,000 reads, with no failed read and no cut of the key range.
A read that fails with the read limit (the page or byte budget, or a live row
that needs more than a call) is retried with fewer rows, and then the key range
is cut in two and the lower half is read first. The report shows the calls
(`scan.calls`, `scan.shrinks`, `scan.splits`) and the reads that went on above a
run of deleted keys (`scan.skips`).

Measured before the change (a home of 10,000 keys of six fields, created and
purged in five rounds): `verify-refs` took 118 s and `inventory` 130 s, with 3,213
reads and 1,587 cuts. The cut halves the key space, and the deleted keys sit in a
few clusters that are very narrow in it, so a cluster took about 250 cuts to find.
Each read of an administrative range also seeks the trees of all 1,024 hash groups
(400 to 2,000 pages, 20 to 450 ms), so the cost followed the number of reads.

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

- S7: a resumable drain of a dead molecule.
- Cloud sync replay and restore write rows that bypass the planner. Cloud sync is off in this build.

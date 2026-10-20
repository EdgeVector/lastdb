# Blob counter

## Contract

Every local file blob has one counter record. The counter holds the number of
references to the blob. The counter is the only authority for the life of the
blob row. No other structure allows a delete of the row. No other structure
blocks one, with one exception for a cloud feature (see "Retired-intent roots").

A reference is one of:

- an atom counter that lists the blob in its `blobs` (the atom body names the blob
  in a `$lastdb_file` pointer, or in its metadata), in any storage scope;
- a live tip record whose metadata names the blob (`file_blob_ref` or `file_hash`).

A blob at zero is not deleted at once. An upload comes before the atom that names
it, so a blob with no reference yet is normal. The blob has a dwell that starts
when its count reaches zero, or when it is uploaded at a count of zero. The dwell
is `LASTDB_BLOB_DWELL_SECS` seconds, one hour by default. A reference that arrives
during the dwell cancels the deletion.

## Records

All records are in the `atoms` collection of the main store. The blob row itself
stays in the `cas_blobs` plane, keyed by its `blob_ref`.

| Record | Key | Value |
|---|---|---|
| Counter | `bref:v3:c:{sha256(blob_ref)}` | `{"v":1,"blob":"sha256:..","refs":N,"zero_since":ms,"suspect":false}` |
| Queue entry | `bref:v3:q\0{zero_since_ms:020}\0{sha256(blob_ref)}` | `{"blob":"sha256:.."}` |

`zero_since` is in unix milliseconds. It is present only while `refs` is zero.
`suspect` is present only when true. The queue key hashes on its first NUL, so the
whole queue is one hash and the oldest entries are one range read.

A counter has a queue entry if and only if it has a `zero_since`. The batch that
sets or clears `zero_since` puts or deletes the entry.

## Who changes a counter

1. **The atom counter planner** (`atom_store/counter/build.rs`) in the batch of the
   change. A created atom counter that lists a blob raises it. A deleted one lowers
   it. A tip record that names a blob raises it. A replaced or removed tip lowers it.
   The blob changes are summed for the whole run first. `blob_counter::BlobDeltas`
   does that, and `AtomStore::blob_counter_mutations` reads the counters by key and
   builds the writes.
2. **An upload** of a blob row (`blob_cas::put_raw_record` and the dedup put)
   calls `AtomStore::register_blob_upload` in the same native write request as the
   row. A blob at zero starts its dwell again. A blob with references is left alone.
3. **The drop of a storage scope** (`delete_catalog_membership`) lowers the blobs
   that the dropped atom counters and tips held, in the same batch as the rows.
4. **The dwell pass** deletes a blob (see below).

No other code writes these records. `scripts/lints/lint-atom-counter-writers.py`
checks that, function by function (family `R`).

## Arithmetic

`blob_counter/plan.rs` is pure. For one blob, the stored counter and the net change:

- no counter, change above zero: make the counter with that count;
- no counter, change below zero: nothing is written, the planner reports a dangling
  removal, and the blob row stays;
- counter, change above zero: add. If `zero_since` was set, clear it and delete the
  queue entry (the dwell is cancelled);
- counter, change below zero: if the counter holds fewer references than the change
  removes, set `suspect`, set the count to zero and clear `zero_since` (never queue).
  If the result is exactly zero and the counter is not `suspect`, set `zero_since`
  to now and put the queue entry. Otherwise subtract.

A `suspect` counter never starts a dwell and never authorizes a delete.

## The dwell pass

`lastdbd` starts at most one pass at a time from the sampler tick
(`LASTDB_SELF_METRICS_SAMPLE_SECS`, 60 s by default). A pass:

1. reads the oldest page of the queue (256 entries, one range read, no gate);
2. takes the entries whose `zero_since` plus the dwell has passed;
3. for each chunk of 64 of them, takes the local mutation admission and the native
   write gate, takes the blob gates, reads each counter again, and reads the
   retired-intent roots of the blobs;
4. in one batch, deletes the blob row (and any resident `cas_blob:` copy), the
   counter and the queue entry of every blob that is still at exactly zero with the
   same `zero_since`. A stale queue entry (the counter is gone, has references, is
   `suspect`, or has another `zero_since`) is deleted alone;
5. releases the gate. The next chunk takes it again.

The gate is never held across chunks, and the read of the queue takes no gate. A
pass handles at most 16 pages. `LASTDB_BLOB_DWELL_PASS=0` turns the pass off.

## Retired-intent roots

A cloud retired intent keeps permanent roots on the blobs it needs
(`retired_intent_retention`). No code path of a node with cloud sync off writes
one. The dwell pass reads them with the chunk and does not delete a blob that one
roots. It starts a new dwell for that blob. This is the one structure besides the
counter that blocks a delete. A later cloud design should make a retained root a
counted reference, and this read can go.

## Format fence

The key format marker `layout:v2:keys` is `3`. A home from a build that kept blob
reference edges (`bref:v2`) has the marker `2`. It has no blob counters, so this
build refuses it ("create a new empty home"). The design has no migration.

## Reads

- `POST /api/storage/liveness/explain` with `{"class":"blob","id":"sha256:.."}` (or
  `"ids": [...]`) returns the counter, `zero_since_unix_ms`, `suspect`, whether the
  queue holds the entry, whether the `cas_blobs` plane holds the row, and a state:
  `referenced`, `in_dwell`, `suspect`, `absent`, `row_without_counter` or
  `inconsistent`. `lastdb liveness explain blob <blob_ref>` prints it.
- `lastdb status` has a `Blob counters` line: process-lifetime totals, the queue
  depth that the last pass read (`N+` when the page was full), and the time to the
  next due entry.

## Known limits

- A blob is counted in each scope that lists it. Cloud snapshot restore and cloud
  replay write atom rows that bypass the planner (see `docs/lastdb-atom-counter.md`).
  A restored atom that lists a blob does not raise the blob counter. The blob can
  then reach zero while the restored atom names it. Cloud sync is off in this build.
  A recount belongs to step S6.
- A tip record names a blob only when a cloud file upload wrote its metadata
  (`put_personal_file_blob`). A synthetic home cannot write that.
- An erase of many atoms that list blobs costs up to three more operations for each
  blob. The size check of an erase request counts four operations for each tip and
  does not know the blobs. A request that exceeds the batch limit fails with the
  limit error and changes nothing. The TTL sweep halves its batch and retries.
- The dwell uses the wall clock. A clock that jumps forward by more than the dwell
  can end a dwell early.

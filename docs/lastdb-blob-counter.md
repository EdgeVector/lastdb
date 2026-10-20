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

### The floor of the dwell

The dwell must be longer than the delay between the ack of a `queued` write and the
batch of that write. A `queued` write is acknowledged from memory (flush interval
500 ms) and its batch, which raises the blob counters, runs after the ack. A backlog
on one schema lane stretches that delay. A `durable` write has its batch accepted
before the ack, so the count rises before the client sees the write.

So the node **refuses a dwell below 120 seconds**: the floor applies, and the node
logs the refusal once. 120 s is the handler budget of a request (90 s) plus 30 s,
and the persist lane that runs a queued batch has a bulk queue budget of about 60 s.
The floor is not a proof. A lane that stalls for longer than the floor can still lose
the race, and the one hour default is the real margin. The race is real: with a dwell
of 1 s, a queued write was acked after 3.6 s, and the pass had deleted the blob row
first. `LASTDB_BLOB_DWELL_UNSAFE_SHORT_FOR_DEV=1` allows a shorter dwell. It is for a
synthetic home in a short proof, with `durable` writes. Never set it on a home that
holds user data.

### The clock of the dwell

A writer stamps `zero_since` and the dwell pass reads it back, both on one clock:
the wall clock, but never more than a tolerance ahead of the time that the monotonic
clock gives since the store was made. The tolerance is a quarter of the dwell (15
minutes for the default dwell), at least 30 s and at most one day. A forward
excursion of the wall clock beyond it (a host that wakes with a wrong clock, a jump
of 7 hours) cannot end a dwell early: the clock then advances with the monotonic
clock only. The conflict janitor uses the same rule with a fixed tolerance of one
day. For a dwell of one hour a tolerance of one day would protect nothing, so here
the tolerance follows the dwell.

What stays:

- A forward jump below the tolerance moves the clock, and the dwells end early by at
  most the size of the jump.
- A backward jump delays every dwell by its size.
- The monotonic clock of a host may stop while the host sleeps. The clock then lags
  the wall clock. A restart takes the wall clock again, and the dwells that were
  stamped during the lag end early by the lag.
- A wall clock that was wrong when the node started, and was corrected by a large
  step later, reads as the monotonic time plus the tolerance until the next start.
  The dwells that were stamped during that time end early at the next start.

### A write that names a blob that is gone

An atom can name a blob after the dwell deleted it: the client uploaded, waited
longer than the dwell, and then wrote. The counter of the blob went with the row, so
the write finds no counter and makes one with a count of one. **The write succeeds.**
The reasons:

- The planner is shared with the paths that bring atoms from a peer. A peer's atom
  can arrive before the bytes of its blob (on-demand file sync), and a refusal there
  would stop the sync.
- A refusal needs a policy for every path that makes an atom counter. Only the local
  write route can tell the client what to do, and that is a decision for the API.

So the write is accepted and the fault is made loud. The write path reads the row of
a blob that has no counter (one keyed read, only for that case), and for a blob with
no row it
- counts a `late_references` event, shown in `lastdb status` and in `recent_faults`,
  and logs the blob at error level;
- makes `lastdb liveness explain blob` say `referenced_row_missing`;
- makes `lastdb db verify-refs` report `blob_counter_without_blob`.

`lastdb db recount` lists such blobs as `blob rows missing`. It cannot restore the
bytes. The fix is to upload the file again and write the atom again.

## Records

All records are in the `atoms` collection of the main store. The blob row itself
stays in the `cas_blobs` plane, keyed by its `blob_ref`.

| Record | Key | Value |
|---|---|---|
| Counter | `bref:v3:c:{sha256(blob_ref)}\0c` | `{"v":1,"blob":"sha256:..","refs":N,"zero_since":ms,"suspect":false}` |
| Queue entry | `bref:v3:q\0{zero_since_ms:020}\0{sha256(blob_ref)}` | `{"blob":"sha256:.."}` |

`zero_since` is in unix milliseconds. It is present only while `refs` is zero.
`suspect` is present only when true. A key hashes on its first NUL. The counter of
a blob is one hash, as the counter of an atom is. The queue key hashes with its tag
only, so the whole queue is one hash and the oldest entries are one range read.

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
5. **The offline recount** (see "Verify and recount").

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

A counter record that does not decode has an unknown state. The planner replaces it
with a `suspect` counter that holds the references of its own batch, and counts an
`unreadable_counters` event. The blob stays. An upload leaves such a record alone.

## The dwell pass

`lastdbd` starts at most one pass at a time from the sampler tick
(`LASTDB_SELF_METRICS_SAMPLE_SECS`, 60 s by default). A pass:

1. reads the oldest page of the queue (256 entries, no gate). A delete leaves a
   tombstone, one read call may examine at most 10,000 keys, and the oldest entries
   are the ones that go, so the head of the queue range fills with tombstones. The
   scan is a bounded range walk: a read that hits the limit is cut in two, and the
   lower half is read first;
2. takes the entries whose `zero_since` plus the dwell has passed;
3. for each chunk of 32 of them (`LASTDB_BLOB_DWELL_CHUNK`, 1 to 64), takes the
   local mutation admission and the native write gate, takes the blob gates, reads
   each counter again, and reads the retired-intent roots of the blobs;
4. in one batch, deletes the blob row (and any resident `cas_blob:` copy), the
   counter and the queue entry of every blob that is still at exactly zero with the
   same `zero_since`. A stale queue entry (the counter is gone, has references, is
   `suspect`, or has another `zero_since`) is deleted alone. An entry whose counter
   does not decode is deleted alone too, and the blob and the counter stay: nothing
   can say that the count is zero (the verifier reports the counter);
5. releases the gate, and **rests**. The rest after a chunk is
   `took * (100 - duty) / duty`, with a duty share of 10 percent by default
   (`LASTDB_BLOB_DWELL_DUTY_PCT`, 1 to 100), at least 25 ms
   (`LASTDB_BLOB_DWELL_MIN_PAUSE_MS`) and at most 30 s. A chunk that took 30 ms is
   followed by a rest of 270 ms. A writer then waits for one small chunk and runs
   free in the rest. The next chunk takes the gate again. The time of a chunk is the
   time of the whole call, which includes the wait for the local mutation admission
   and for the batch to be accepted. On a busy host a chunk takes 0.3 s to 2 s, so the
   pass then deletes about 30 to 100 blobs a minute (three to ten seconds for a chunk
   of 32 with its rest). That is the price of leaving the writers alone. Set
   `LASTDB_BLOB_DWELL_DUTY_PCT` higher to drain a large backlog faster. The node logs
   the time and the rest of each chunk at debug level
   (`lastdbd::blob_dwell`).

The gate is never held across chunks, and the read of the queue takes no gate. A
pass handles at most 16 pages. `LASTDB_BLOB_DWELL_PASS=0` turns the pass off. The
knobs come from the node environment and are read at the start of each pass.

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

## Verify and recount

`lastdb db verify-refs` (`docs/lastdb-atom-counter.md`) reads the blob counters
without a write gate. For the page of every atom counter and tip it reads the
counters of the blobs they name at the same root, and reports `blob_counter_missing`
and `blob_counter_below_refs`. The personal run then walks the counters
(`blob_suspect`, `blob_zero_without_dwell`, `blob_dwell_with_refs`, `blob_unqueued`,
`unreadable_blob_counter`) and the queue (`blob_stale_queue_entry`). For each counter
that holds references it also reads the blob row at the same root and reports
`blob_counter_without_blob` when the row is missing (a counter at zero is not asked:
the dwell pass deletes the row and the counter together). Every check has
no false positive under concurrent writers, because one page is one committed root.
A counter above its references is not found: the verifier checks one scope and the
counters are global.

`lastdb db recount` (offline) counts the references of each blob from the atom
counters and the tips of the listed scopes. The default only raises counters, makes
missing ones, restores a missing queue entry and deletes a queue entry that no
counter backs. `--blobs-exact --all-scopes` also lowers a counter that is too high,
clears `suspect`, and starts the dwell of a blob that nothing references. It also
checks that each blob with references has its row, and lists the ones that do not
(`rows_missing`, with the first ten). It cannot list a row that has no counter: that
needs a read of the whole `cas_blobs` plane, which holds the blob bytes.

## Reads

- `POST /api/storage/liveness/explain` with `{"class":"blob","id":"sha256:.."}` (or
  `"ids": [...]`) returns the counter, `zero_since_unix_ms`, `suspect`, whether the
  queue holds the entry, whether the `cas_blobs` plane holds the row, and a state:
  `referenced`, `referenced_row_missing`, `in_dwell`, `suspect`, `unreadable`,
  `absent`, `row_without_counter` or `inconsistent`. `lastdb liveness explain blob <blob_ref>` prints it.
- `lastdb status` has a `Blob counters` line: process-lifetime totals, the queue
  depth that the last pass read (`N+` when the page was full), and the time to the
  next due entry.

## Known limits

- A blob is counted in each scope that lists it. Cloud snapshot restore and cloud
  replay write atom rows that bypass the planner (see `docs/lastdb-atom-counter.md`).
  A restored atom that lists a blob does not raise the blob counter. The blob can
  then reach zero while the restored atom names it. Cloud sync is off in this build.
  `lastdb db verify-refs` reports it (`blob_counter_missing` or
  `blob_counter_below_refs`) and `lastdb db recount` raises the counter.
- A tip record names a blob only when a cloud file upload wrote its metadata
  (`put_personal_file_blob`). A synthetic home cannot write that.
- An erase of many atoms that list blobs costs up to three more operations for each
  blob. The size check of an erase request counts four operations for each tip and
  does not know the blobs. A request that exceeds the batch limit fails with the
  limit error and changes nothing. The TTL sweep halves its batch and retries.
- A row that a dropped scope held and that does not decode lowers no blob counter.
  The drop goes on, the counters stay too high, and `lastdb db recount` repairs them.
- Cloud purge copies blob rows (`sync/cloud_purge/blob_copy.rs`, `org_files.rs`) and
  the cloud file closure writes rows without a counter. A row with no counter is never
  queued, so nothing deletes it. Cloud sync is off in this build.
- A delete leaves a tombstone in the queue hash. `lastdb db compact --collection atoms`
  returns them. The scan stays correct without it and costs one more read call for
  every 10,000 tombstones.
- The clock of the dwell ignores a forward excursion beyond a quarter of the dwell
  only (see "The clock of the dwell"). A smaller forward jump ends dwells early by its
  size.
- A blob row with no counter is invisible to the verifier and to the recount, and
  nothing deletes it (see the cloud rows above).
- With on-demand cloud file sync, a blob that has not downloaded yet has references
  and no row. The verifier cannot tell it from a late write and reports
  `blob_counter_without_blob`. Cloud sync is off in this build.

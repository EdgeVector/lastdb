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
| Queue entry | `bref:v3:q:{bucket:020}\0{zero_since_ns:020}:{sha256(blob_ref)}` | `{"blob":"sha256:.."}` |
| Queue directory row | `bref:v3:d\0{bucket:020}` | a token (`{"v":1,"t":"{zero_since_ns:020}:{id}"}`) |

`zero_since` in the counter is in unix milliseconds. It is present only while
`refs` is zero. `suspect` is present only when true. A key hashes on its first
NUL. The counter of a blob is one hash, as the counter of an atom is.

### The queue

The queue is the time-bucketed queue of `docs/lastdb-time-queue.md`
(`atom_store/time_queue`). Read that contract first. Why: a queue under one hash
loses speed after about 10,000 deleted entries, because a delete leaves a deleted
key, the oldest entries go first, and every read has to pass the deleted keys at
the head. The review measured 16 blobs a minute after 9,800 deletions.

- **The time of an entry is the `zero_since` of its counter**, in nanoseconds
  (`zero_since_ms * 1,000,000`). The dwell is not in the key: the reader asks for
  the entries older than `now - dwell`. A change of `LASTDB_BLOB_DWELL_SECS`
  between two starts then needs no rewrite of the queue, and the cancel of a dwell
  can always name the key from the counter.
- **A bucket is a slice of time**: the dwell, between one second and one hour
  (`TimeQueue::for_window` with a window of 64 dwells). With the default dwell a
  bucket is one hour, so the directory gets 24 rows a day.
- **Two key families**, `bref:v3:q` for the entries and `bref:v3:d` for the
  directory. Neither holds a NUL, and neither is a prefix of the other or of the
  counter family `bref:v3:c:`. The entries have no scope: a blob is global.
- **A new entry brings the directory row of its bucket** in the same batch, with a
  new token that names the entry. A planner run that zeroes several blobs of one
  slice writes the directory row once (the last token stays).
- **An entry ends with a delete** in the batch of the work that names it: the dwell
  chunk (the blob row, the counter and the entry), a cancel (a reference arrives:
  the counter loses `zero_since`, and the entry goes), or a stale entry.
- **A bucket retires** when the reader has walked every key of it, with a guarded
  delete of its directory row that holds only while the row has the token that the
  reader read before the walk. A writer that adds an entry meanwhile changes the
  token, the row stays, and the reader walks the bucket again. The deleted keys of
  a retired bucket go out of reach with it.

A counter has a queue entry if and only if it has a `zero_since`. The batch that
sets or clears `zero_since` puts or deletes the entry. What can go wrong with the
queue, and what catches it, is in "What can go wrong with the queue" below.

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

1. keeps its reader of the queue from one pass to the next (`DwellCursor`): the
   buckets it knows, and one bounded range walk for each of them. The walk is the
   cursor. It moves over deleted keys and never goes back to the head of a hash. A
   restart of the node loses the reader, and the next pass then walks each live
   bucket once from its start; every 60th pass does the same, for a clock that
   stepped back;
2. lists the directory of the buckets that have started (`start <= now - dwell`) at
   the start of the pass and every 16 steps, with a bounded range walk. A bucket
   that is not in the directory is not read;
3. a step reads one page of due entries of the oldest bucket that has some, with no
   gate, and stops at the first key that is not due (the bucket then waits for its
   time). Then it takes the entries of the page, up to a chunk of 32
   (`LASTDB_BLOB_DWELL_CHUNK`, 1 to 64), takes the local mutation admission and the
   native write gate, takes the blob gates, reads each counter again, and reads the
   retired-intent roots of the blobs. The cursor moves only after the chunk's batch
   is accepted. A step that fails leaves it where it was;
4. in one batch, deletes the blob row (and any resident `cas_blob:` copy), the
   counter and the queue entry of every blob that is still at exactly zero with the
   same `zero_since`. A stale queue entry (the counter is gone, has references, is
   `suspect`, or has another `zero_since`) is deleted alone. An entry whose counter
   does not decode is deleted alone too, and the blob and the counter stay: nothing
   can say that the count is zero (the verifier reports the counter);
5. releases the gate, and **rests**. A step that finds a bucket done retires it
   (a batch of one guarded delete) and rests the same way. The rest after a chunk is
   `took * (100 - duty) / duty`, with a duty share of 10 percent by default
   (`LASTDB_BLOB_DWELL_DUTY_PCT`, 1 to 100), at least 25 ms
   (`LASTDB_BLOB_DWELL_MIN_PAUSE_MS`) and at most 30 s. A chunk that took 30 ms is
   followed by a rest of 270 ms. A writer then waits for one small chunk and runs
   free in the rest. The next chunk takes the gate again. The time of a chunk is the
   time of the whole call, which includes the wait for the local mutation admission
   and for the batch to be accepted. The gate is held for 1 to 15 ms of it, but a
   chunk took 0.2 s to 1 s on a busy host, so a chunk of 29 blobs and its rest took
   about 9 s: the pass then deletes about 200 blobs a minute. That is the price of
   leaving the writers alone. Set
   `LASTDB_BLOB_DWELL_DUTY_PCT` higher to drain a large backlog faster. The node logs
   the time and the rest of each chunk at debug level
   (`lastdbd::blob_dwell`).

The gate is never held across chunks, and the read of the queue takes no gate. A
pass runs at most 2,048 steps. `LASTDB_BLOB_DWELL_PASS=0` turns the pass off. The
knobs come from the node environment and are read at the start of each pass.

A pass that ends with an error does not make the next pass start at once. The wait
is 30 s after the first pass that fails and doubles with each pass in a row that
fails, up to 10 minutes. A good pass clears it. `lastdb status` shows the error, the
count and the time to the next try.

A write that names many blobs adds one keyed read of the blob counters to the read
budget of its preparation, and one keyed read of the blob rows for the blobs that
have no counter (a late reference). The budget of a write preparation is one
aggregate for the whole request, so a request with thousands of writes that name
blobs can reach it earlier than a request that names none. The request then fails
with the read limit error and changes nothing.

The same holds for the 16,384 storage operations of one native mutation. A write
costs about 8 operations in the atom planner, and a write that names a new blob adds
one more (the blob counter; an upload adds the queue entry and the directory row
too). Measured on a synthetic home: a request of 1,980 plain writes fits, a request
of 1,800 writes that each name a distinct blob fits, and 1,900 do not (`complete
native mutation exceeds 16384 storage operations`, retryable, nothing changes). A
client splits such a request.

The reads and the batches stay small on purpose. A read call of the walk asks for
a page of one chunk (32 keys by default) and is cut in two when it meets the
read budget. The real budget
of one native call is 4,096 metadata pages, and one native mutation may publish at
most 4,096 metadata pages, so a chunk (about 4 rows for each blob) stays far below
it.

## What can go wrong with the queue

The writers keep two promises: an entry and the directory row of its bucket are
written in one batch, and a retirement deletes a row only while its token is
unchanged. So the queue itself makes none of the faults below. Something else can: a
delete that bypassed the queue, a partial restore, a bug in the verdict "the bucket
is empty", a disk fault. Each fault hides a blob from the dwell pass, so the blob is
never deleted (a leak, never a loss).

| What goes wrong | What the dwell pass does | What catches it | What repairs it |
|---|---|---|---|
| A bucket holds entries and has no directory row | Never lists the bucket, so never reads its entries | The safety net of the pass (below); `verify-refs`: `queue_entry_without_directory`; `lastdb status`: `buckets_repaired` | The net registers the bucket again, with a warn log for each bucket; `recount --execute` writes the row |
| A counter is in its dwell (`zero_since`) and has no queue entry | Never reads the blob | `verify-refs`: `zero_blob_without_queue_entry`; `liveness explain blob`: `queued` is false | `recount --execute` writes the entry (and its directory row) at the `zero_since` of the counter. The net cannot see it: it looks for buckets that hold entries |
| An entry names a counter that is gone, has references, is `suspect`, or has another `zero_since` (a stale entry) | Deletes the entry alone when it is due | `verify-refs`: `blob_stale_queue_entry` | The pass; or `recount --execute` |
| A directory row has no entries | Walks the bucket, finds nothing, retires the row | Nothing needs to: it is harmless | The pass |

**The safety net.** It is the generic one of the time queue (`TimeQueue::net_slices`
and `mend_slices`, `docs/lastdb-time-queue.md`). It needs no scan: it lists, by
arithmetic, the slices that can hold a live entry, reads the directory row of each
by key, and for a slice with no row reads the hash once to see whether it holds a
live key. A bucket that does is registered again with a new directory row, through
the planner.

The slices that can hold a live entry of the blob queue are few. An entry has the
time of its `zero_since` (never later than now) and stays until its dwell is over, so
a live entry lies at most one dwell and one slice before now. The generic list
covers one dwell and one slice before now and the same after it. The slice of the
blob queue is one dwell (one hour by default), so a sweep probes **about five slices**
with the default dwell: five reads of directory rows, and one bounded range read for
each slice without a row. An entry that is older than one dwell and one slice is out
of the net's reach (the pass was off for more than a dwell, or the node was down).
`verify-refs` reads every bucket of the queue, whatever its age, and finds those.

The pass sweeps once at its start and then once in `LASTDB_BLOB_DWELL_NET_SECS`
seconds (3,600 by default; 0 turns the net off). A sweep is paced like a step. After a
sweep that registered a bucket, the pass reads the directory again at once, so the
blobs of the bucket are deleted at their due time. `lastdb status` shows
`net_sweeps` and `buckets_repaired`; a `buckets_repaired` above zero means that
something wrote or deleted queue rows outside the queue.

A race can count a repair that was not needed: a writer adds an entry and its row
after the net read the row and before it read the hash. The net then registers the
bucket with its own token, which is harmless: the row names a live bucket, and the
next retirement of that bucket checks the token it reads.

## Retired-intent roots

A cloud retired intent keeps permanent roots on the blobs it needs
(`retired_intent_retention`). No code path of a node with cloud sync off writes
one. The dwell pass reads them with the chunk and does not delete a blob that one
roots. It starts a new dwell for that blob. This is the one structure besides the
counter that blocks a delete. A later cloud design should make a retained root a
counted reference, and this read can go.

## Format fence

The key format marker `layout:v3:keys` is `4`. A home from a build that kept blob
reference edges (`bref:v2`) has the value `3` (an older one has the marker
`layout:v2:keys`). It has no blob counters, so this build refuses it ("create a new
empty home"). A home that this build made is refused the same way by a build that
expects `3`. The design has no migration.

## Verify and recount

`lastdb db verify-refs` (`docs/lastdb-atom-counter.md`) reads the blob counters
without a write gate. For the page of every atom counter and tip it reads the
counters of the blobs they name at the same root, and reports `blob_counter_missing`
and `blob_counter_below_refs`. The personal run then walks the counters
(`blob_suspect`, `blob_zero_without_dwell`, `blob_dwell_with_refs`, `zero_blob_without_queue_entry`,
`unreadable_blob_counter`) and the queue (`blob_stale_queue_entry`, and
`queue_entry_without_directory` for an entry whose bucket has no directory row, read
at the same root as the entry). For each counter
that holds references it also reads the blob row at the same root and reports
`blob_counter_without_blob` when the row is missing (a counter at zero is not asked:
the dwell pass deletes the row and the counter together). Every check has
no false positive under concurrent writers, because one page is one committed root.
A counter above its references is not found: the verifier checks one scope and the
counters are global.

`lastdb db recount` (offline) counts the references of each blob from the atom
counters and the tips of the listed scopes. The default only raises counters, makes
missing ones, writes a missing queue entry and a missing directory row again (both
through the planner, so a second run changes nothing) and deletes a queue entry that
no counter backs. `--blobs-exact --all-scopes` also lowers a counter that is too high,
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
- `lastdb status` has a `Blob counters` line (with `net_sweeps` and `buckets_repaired`): process-lifetime totals, the size of
  the dwell queue (`queue_depth`, up to 1,000 and `N+` when it stopped at the cap,
  and `queue_buckets`), both counted by the bucket directory with bounded reads, and
  `next_due_in`, the earliest time at which the oldest bucket can hold a due entry.

## Known limits

- A blob is counted in each scope that lists it. Cloud snapshot restore and cloud
  replay write atom rows that bypass the planner (see `docs/lastdb-atom-counter.md`).
  A restored atom that lists a blob does not raise the blob counter. The blob can
  then reach zero while the restored atom names it. Cloud sync is off in this build.
  `lastdb db verify-refs` reports it (`blob_counter_missing` or
  `blob_counter_below_refs`) and `lastdb db recount` raises the counter.
- A tip record names a blob only when a cloud file upload wrote its metadata
  (`put_personal_file_blob`). A synthetic home cannot write that.
- An erase of many atoms that list blobs costs up to four more operations for each
  blob (the counter, the queue entry, the entry that goes, and the directory row of
  the bucket, once for each bucket). The size check of an erase request counts four operations for each tip and
  does not know the blobs. A request that exceeds the batch limit fails with the
  limit error and changes nothing. The TTL sweep halves its batch and retries.
- A row that a dropped scope held and that does not decode lowers no blob counter.
  The drop goes on, the counters stay too high, and `lastdb db recount` repairs them.
- Cloud purge copies blob rows (`sync/cloud_purge/blob_copy.rs`, `org_files.rs`) and
  the cloud file closure writes rows without a counter. A row with no counter is never
  queued, so nothing deletes it. Cloud sync is off in this build.
- A delete leaves a deleted key in its bucket, and a retired bucket leaves one in
  the directory (24 each day with the default dwell). No read passes them again, but
  a restart of the node walks each live bucket and the directory from their start
  with bounded reads, which cost one read call for every 10,000 deleted keys.
  `lastdb db compact --collection atoms` returns them.
- The clock of the dwell ignores a forward excursion beyond a quarter of the dwell
  only (see "The clock of the dwell"). A smaller forward jump ends dwells early by its
  size.
- A blob row with no counter is invisible to the verifier and to the recount, and
  nothing deletes it (see the cloud rows above).
- With on-demand cloud file sync, a blob that has not downloaded yet has references
  and no row. The verifier cannot tell it from a late write and reports
  `blob_counter_without_blob`. Cloud sync is off in this build.

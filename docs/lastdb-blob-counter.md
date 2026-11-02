# Blob counter

## Contract

Every local file blob has one counter entry. The blob itself is one entry of the
`cas_blobs` collection, keyed by its `blob_ref` (the blob entry). The counter
holds the number of references to the blob. The counter is the only authority
for the life of the blob entry. No other structure allows a delete of the
entry. No other structure blocks one.

A reference is one of:

- a record (`rk:`) that lists the blob in its `blob_refs` (a field value names the
  blob in a `$lastdb_file` pointer, or the record metadata names it), in any
  storage scope;
- a field history entry (`rh:`) whose copied value names the blob.

A blob at zero is not deleted at once. An upload comes before the record that names
it, so a blob with no reference yet is normal. The blob has a dwell that starts
when its count reaches zero, or when it is uploaded at a count of zero. The dwell
is `LASTDB_BLOB_DWELL_SECS` seconds, one hour by default. A reference that arrives
during the dwell cancels the deletion.

### The floor of the dwell

Every native write raises its blob counters in the complete disk commit before the ack.
The [eager queue](lastdb-eager-durable-queue.md) has no early memory ack and no collection timer.
The dwell protects the gap between a file upload and the later mutation that references that file.
It does not permit an unlimited delay before that mutation.

The node **refuses a dwell below 120 seconds** and logs the refusal once.
The one-hour default stays in force.
`LASTDB_BLOB_DWELL_UNSAFE_SHORT_FOR_DEV=1` permits a shorter dwell for an isolated synthetic home.
Never set it on a home that holds user data.

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

A record can name a blob after the dwell deleted it: the client uploaded, waited
longer than the dwell, and then wrote. The counter of the blob went with the blob
entry, so the write finds no counter and makes one with a count of one.
**The write succeeds.**
The reasons:

- The planner is shared with every path that writes a record. A refusal needs a
  policy for each of them.
- Only the local write route can tell the client what to do. That is a decision
  for the API.

So the write is accepted and the fault is made loud. The write path reads the blob
entry of a blob that has no counter (one keyed read, only for that case). For a
blob with no blob entry, it
- counts a `late_references` event, shown in `lastdb status` and in `recent_faults`,
  and logs the blob at error level;
- makes `lastdb liveness explain blob` say `referenced_row_missing`;
- makes `lastdb db verify-refs` report `blob_counter_without_blob`.

`lastdb db recount` lists such blobs as `blob rows missing`. The JSON key is
`blobs.rows_missing`, with the first ten blobs in `blobs.missing_row_samples`. The
same object is also `recount.blobs` in the full report under the key `recount`. It
cannot restore the bytes. The fix is to upload the file again and write
the record again.

`late_references` in `lastdb status`, like every figure of the `Blob counters` line,
comes from the status snapshot that the sampler publishes (every
`LASTDB_SELF_METRICS_SAMPLE_SECS`, 60 s by default). A late reference shows there at
the next sampler tick, up to one tick after the write. The explain route and
`verify-refs` read the store at the time of the call and are exact.

## Entries

All entries are in the `blob_counts` collection of the main store. The node
routes every `bref:` key to that collection. The blob entry itself stays in the
`cas_blobs` collection, keyed by its `blob_ref`.

| Entry | Key | Value |
|---|---|---|
| Counter | `bref:c:{sha256(blob_ref)}` | `{"v":1,"blob":"sha256:..","refs":N,"zero_since":ms,"suspect":false}` |
| Queue entry | `bref:q:{bucket:020}\0{zero_since_ns:020}:{sha256(blob_ref)}` | `{"blob":"sha256:.."}` |
| Queue directory entry | `bref:d\0{bucket:020}` | a token (`{"v":1,"t":"{zero_since_ns:020}:{id}"}`) |

`zero_since` in the counter is in unix milliseconds. It is present only while
`refs` is zero. `suspect` is present only when true. A key hashes on its first
NUL. The counter of a blob is one hash.

### The queue

The queue is the time-bucketed queue of `docs/lastdb-time-queue.md`
(`record_store/time_queue`). Read that contract first. Why: a queue under one hash
loses speed after about 10,000 deleted entries, because a delete leaves a deleted
key, the oldest entries go first, and every read has to pass the deleted keys at
the head. The review measured 16 blobs a minute after 9,800 deletions.

- **The time of an entry is the `zero_since` of its counter**, in nanoseconds
  (`zero_since_ms * 1,000,000`). The dwell is not in the key: the reader asks for
  the entries older than `now - dwell`. A change of `LASTDB_BLOB_DWELL_SECS`
  between two starts then needs no rewrite of the queue, and the cancel of a dwell
  and the check of `queued` can always name the key from the counter.
- **A bucket is a slice of time of one hour** for every dwell the node accepts (at
  least 120 s), so the directory gets 24 entries a day and the key of an entry
  does not depend on the dwell. Only a dwell below the floor (the dev knob of a
  synthetic home) gives a shorter slice, one dwell, between one second and one
  hour. A synthetic home that changed such a dwell between two starts shows
  `queued=false` for an entry that an earlier start wrote.
- **Two key families**, `bref:q` for the entries and `bref:d` for the
  directory. Neither holds a NUL, and neither is a prefix of the other or of the
  counter family `bref:c:`. The entries have no scope: a blob is global.
- **A new entry brings the directory entry of its bucket** in the same batch, with
  a new token that names the entry. A planner run that zeroes several blobs of one
  slice writes the directory entry once (the last token stays).
- **An entry ends with a delete** in the batch of the work that names it: the dwell
  chunk (the blob entry, the counter and the queue entry), a cancel (a reference
  arrives: the counter loses `zero_since`, and the queue entry goes), or a stale
  entry.
- **A bucket retires** when the reader has walked every key of it. A guarded
  delete removes its directory entry, but only while the directory entry has the
  token that the reader read before the walk. A writer that adds an entry
  meanwhile changes the token. The directory entry then stays, and the reader
  walks the bucket again. The deleted keys of a retired bucket go out of reach
  with it.

A counter has a queue entry if and only if it has a `zero_since`. The batch that
sets or clears `zero_since` puts or deletes the entry. What can go wrong with the
queue, and what catches it, is in "What can go wrong with the queue" below.

## Who changes a counter

1. **The record write and the history release** (`record_store/records.rs`,
   `record_store/field_history/records.rs`) in the batch of the change. A record or a
   history entry that names a blob raises it. A replaced or removed record, and a
   released history entry, lowers it. The blob changes are summed for the whole
   batch first. `blob_counter::BlobDeltas` does that, and
   `RecordStore::blob_counter_mutations` reads the counters by key and builds the
   writes.
2. **An upload** of a blob entry (`blob_cas::put_blob_sealed_with_access_dedup_in_ops`)
   calls `RecordStore::register_blob_upload` in the same native write request as the
   blob entry. A blob at zero starts its dwell again. A blob with references is left alone.
3. **The drop of a storage scope** (`delete_catalog_membership`) and **the drop of
   a schema** (`record_store/schema_release.rs`) lower the blobs that the dropped
   records and history entries held, in the same batch as the deletes.
4. **The dwell pass** deletes a blob (see below).
5. **The offline recount** (see "Verify and recount").

No other code writes these entries. `scripts/lints/lint-store-writers.py`
checks that, function by function (family `R`).

## Arithmetic

`blob_counter/plan.rs` is pure. For one blob, the stored counter and the net change:

- no counter, change above zero: make the counter with that count;
- no counter, change below zero: nothing is written, the planner reports a dangling
  removal, and the blob entry stays;
- counter, change above zero: add. If `zero_since` was set, clear it and delete the
  queue entry (the dwell is cancelled);
- counter, change below zero: if the counter holds fewer references than the change
  removes, set `suspect`, set the count to zero and clear `zero_since` (never queue).
  If the result is exactly zero and the counter is not `suspect`, set `zero_since`
  to now and put the queue entry. Otherwise subtract.

A `suspect` counter never starts a dwell and never authorizes a delete.

A counter entry that does not decode has an unknown state. The planner replaces it
with a `suspect` counter that holds the references of its own batch, and counts an
`unreadable_counters` event. The blob stays. An upload leaves such an entry alone.

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
   native write gate, takes the blob gates, and reads each counter again. The cursor
   moves only after the chunk's batch is accepted. A step that fails leaves it where
   it was;
4. in one batch, deletes the blob entry, the
   counter and the queue entry of every blob that is still at exactly zero with the
   same `zero_since`. A stale queue entry (the counter is gone, has references, is
   `suspect`, or has another `zero_since`) is deleted alone. An entry whose counter
   does not decode is deleted alone too, and the blob and the counter stay: nothing
   can say that the count is zero (the verifier reports the counter);
5. releases the gate, and **rests**. A step that finds a bucket done retires it
   (a batch of one guarded delete) and rests the same way. A private background chunk
   shares the bounded collection window and still returns after durability.
   Its measured writer cost includes the gate and shared flush holds. A sealed fence also counts its flush wait.
   The rest after a chunk is
   `blocked * (100 - duty) / duty`, where `blocked` is that time and the duty share
   is 10 percent by default (`LASTDB_BLOB_DWELL_DUTY_PCT`, 1 to 100): writers wait for
   the pass about a tenth of the time. The rest is at least 25 ms
   (`LASTDB_BLOB_DWELL_MIN_PAUSE_MS`) and at most 30 s. The rest is the minimum rest
   only when no other task opened a write scope for a quiet period
   (`LASTDB_MAINTENANCE_QUIET_SECS`, 60 s by default; `WriterWatch`). The rest then
   protects nobody, and a node that does nothing else deletes about 30 to 45 blobs
   a second. A write scope of another task ends that, and for the whole quiet period
   every rest is the full one. With four writers that write all the time the pass
   deleted 20 to 28 blobs a second on a RAM disk (main: 13 to 17). A long rest ends
   early only when the quiet period has passed. The watch sees write scopes only (see the section Pacing in
   `docs/lastdb-time-queue.md`). Set `LASTDB_BLOB_DWELL_DUTY_PCT` higher to drain a
   large backlog faster when writers are present, and accept that they wait longer.
   The node logs the time, the blocked time and the rest of each chunk at debug
   level (`lastdbd::blob_dwell`).

The gate is never held across chunks, and the read of the queue takes no gate. A
pass runs at most 2,048 steps. `LASTDB_BLOB_DWELL_PASS=0` turns the pass off. The
knobs come from the node environment and are read at the start of each pass.

A pass that ends with an error does not make the next pass start at once. The wait
is 30 s after the first pass that fails. It doubles with each next pass that
fails, up to 10 minutes. A good pass clears it. `lastdb status` shows the error, the
count and the time to the next try.

A write that names many blobs adds one keyed read of the blob counters to the read
budget of its preparation, and one keyed read of the blob entries for the blobs
that have no counter (a late reference). The budget of a write preparation is one
aggregate for the whole request, so a request with thousands of writes that name
blobs can reach it earlier than a request that names none. The request then fails
with the read limit error and changes nothing.

The same holds for the 16,384 storage operations of one native mutation. A write
costs a few operations in the record batch, and a write that names a new blob adds
one more (the blob counter; an upload adds the queue entry and the directory entry
too). Measured on a synthetic home: a request of 1,980 plain writes fits, a request
of 1,800 writes that each name a distinct blob fits, and 1,900 do not (`complete
native mutation exceeds 16384 storage operations`, retryable, nothing changes). A
client splits such a request.

The reads and the batches stay small on purpose. A read call of the walk asks for
a page of one chunk (32 keys by default) and is cut in two when it meets the
read budget. The real budget
of one native call is 4,096 metadata pages, and one native mutation may publish at
most 4,096 metadata pages, so a chunk (about 4 entries for each blob) stays far
below it.

## What can go wrong with the queue

The writers keep two promises: an entry and the directory entry of its bucket are
written in one batch, and a retirement deletes a directory entry only while its
token is unchanged. So the queue itself makes none of the faults below. Something else can: a
delete that bypassed the queue, a partial restore, a bug in the verdict "the bucket
is empty", a disk fault. Each fault hides a blob from the dwell pass, so the blob is
never deleted (a leak, never a loss).

| What goes wrong | What the dwell pass does | What catches it | What repairs it |
|---|---|---|---|
| A bucket holds entries and has no directory entry | Never lists the bucket, so never reads its entries | The safety net of the pass (below); `verify-refs`: `queue_entry_without_directory`; `lastdb status`: `buckets_repaired` | The net registers the bucket again, with a warn log for each bucket; `recount --execute` writes the directory entry |
| A counter is in its dwell (`zero_since`) and has no queue entry | Never reads the blob | `verify-refs`: `zero_blob_without_queue_entry`; `liveness explain blob`: `queued` is false | `recount --execute` writes the queue entry (and its directory entry) at the `zero_since` of the counter. The net cannot see it: it looks for buckets that hold entries |
| An entry names a counter that is gone, has references, is `suspect`, or has another `zero_since` (a stale entry) | Deletes the entry alone when it is due | `verify-refs`: `blob_stale_queue_entry` | The pass; or `recount --execute` |
| A directory entry has no queue entries | Walks the bucket, finds nothing, retires the directory entry | Nothing needs to: it is harmless | The pass |

**The safety net.** It is the generic one of the time queue (`TimeQueue::net_slices`
and `mend_slices`, `docs/lastdb-time-queue.md`). It needs no scan: it lists, by
arithmetic, the slices that can hold a live entry, reads the directory entry of
each by key, and for a slice with no directory entry reads the hash once to see
whether it holds a live key. A bucket that does is registered again with a new
directory entry, through the planner.

The slices that can hold a live entry of the blob queue are few. An entry has the
time of its `zero_since` (never later than now) and stays until its dwell is over, so
a live entry lies at most one dwell and one slice before now. The generic list
covers one dwell and one slice before now and the same after it. The slice of the
blob queue is one hour, so a sweep probes **about five slices** with the default
dwell: five reads of directory entries, and one bounded range read for each slice
without a directory entry. An entry that is older than one dwell and one slice is out
of the net's reach (the pass was off for more than a dwell, or the node was down).
`verify-refs` reads every bucket of the queue, whatever its age, and finds those.

The pass sweeps once at its start and then once in `LASTDB_BLOB_DWELL_NET_SECS`
seconds (3,600 by default; 0 turns the net off). A sweep is paced like a step. After a
sweep that registered a bucket, the pass reads the directory again at once, so the
blobs of the bucket are deleted at their due time. `lastdb status` shows
`net_sweeps` and `buckets_repaired`. A `buckets_repaired` above zero means that
something wrote or deleted queue entries outside the queue.

A race can count a repair that was not needed: a writer adds an entry and its
directory entry after the net read the directory entry and before it read the
hash. The net then registers the bucket with its own token. That is harmless: the
directory entry names a live bucket, and the next retirement of that bucket checks
the token it reads.

## Format fence

The node marker (`format=8`) and the key format entry `layout:keys`
(`typed-ranges`) fence the home. This build opens no other home. The design has
no migration.

## Verify and recount

`lastdb db verify-refs` reads the blob counters without a write gate. The
personal run walks the counters
(`blob_suspect`, `blob_zero_without_dwell`, `blob_dwell_with_refs`, `zero_blob_without_queue_entry`,
`unreadable_blob_counter`) and the queue (`blob_stale_queue_entry`, and
`queue_entry_without_directory` for an entry whose bucket has no directory entry,
read at the same root as the entry). For each counter that holds references, it
also reads the blob entry at the same root. It reports `blob_counter_without_blob`
when the blob entry is missing. A counter at zero is not asked: the dwell pass
deletes the blob entry and the counter together. Every check has
no false positive under concurrent writers, because one page is one committed root.
A counter above its references is not found: the verifier checks one scope and the
counters are global.

`lastdb db recount` (offline) counts the references of each blob from the records
and the field history entries of the listed scopes. The default only raises counters, makes
missing ones, writes a missing queue entry and a missing directory entry again (both
through the planner, so a second run changes nothing) and deletes a queue entry that
no counter backs. `--blobs-exact --all-scopes` also lowers a counter that is too high,
clears `suspect`, and starts the dwell of a blob that nothing references. It also
checks that each blob with references has its blob entry, and lists the ones that
do not (`rows_missing`, with the first ten). It cannot list a blob entry that has
no counter: that needs a read of the whole `cas_blobs` collection, which holds the
blob bytes.

## Reads

- `POST /api/storage/liveness/explain` with `{"class":"blob","id":"sha256:.."}` (or
  `"ids": [...]`) returns the counter, `zero_since_unix_ms`, `suspect`, whether the
  queue holds the entry, whether the `cas_blobs` collection holds the blob entry,
  and a state:
  `referenced`, `referenced_row_missing`, `in_dwell`, `suspect`, `unreadable`,
  `absent`, `row_without_counter` or `inconsistent`. `lastdb liveness explain blob <blob_ref>` prints it.
- `lastdb status` has a `Blob counters` line (with `net_sweeps` and `buckets_repaired`): process-lifetime totals, the size of
  the dwell queue (`queue_depth`, up to 1,000 and `N+` when it stopped at the cap,
  and `queue_buckets`), both counted by the bucket directory with bounded reads, and
  `next_due_in`, the earliest time at which the oldest bucket can hold a due entry.

## Known limits

- A blob is counted in each scope that lists it.
- A V2 restore installs the files of the home. The blob counters come back with
  the records that they count. The restore proof checks each key of the
  `blob_counts` collection; it does not recount the references.
- An erase of many records that list blobs costs up to four more operations for each
  blob (the counter, the queue entry, the entry that goes, and the directory entry
  of the bucket, once for each bucket). The size check of an erase request does not
  know the blobs. A request that exceeds the batch limit fails with the
  limit error and changes nothing. The TTL sweep halves its batch and retries.
- A record that a dropped scope held and that does not decode lowers no blob
  counter. The drop goes on, the counters stay too high, and `lastdb db recount`
  repairs them.
- A delete leaves a deleted key in its bucket, and a retired bucket leaves one in
  the directory (24 each day with the default dwell). No read passes them again, but
  a restart of the node walks each live bucket and the directory from their start
  with bounded reads, which cost one read call for every 10,000 deleted keys.
- The clock of the dwell ignores a forward excursion beyond a quarter of the dwell
  only (see "The clock of the dwell"). A smaller forward jump ends dwells early by its
  size.
- A blob entry with no counter is invisible to the verifier and to the recount,
  and nothing deletes it. An upload always writes the counter in the same request,
  so only a fault outside the planner can make such a blob entry.

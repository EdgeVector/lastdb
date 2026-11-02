# The time-bucketed queue

A queue of entries that become due at a time. A reader releases the entries
that are due, oldest first, and each release deletes its entry. Two queues use it:
the retention queue of kept conflict records
(`docs/operations/selected-key-conflict-metadata.md`) and the dwell queue of file
blobs at zero references (`docs/lastdb-blob-counter.md`).
Another queue with the same needs takes the same contract.

Code: `fold_db/crates/core/src/db_operations/record_store/time_queue/`.

## The problem

LastDB has no scan. A range read works under one hash, and one read call has a
budget. The budget has more than one limit, and the first one that is reached
ends the call:

- **10,000 examined keys** for an ordinary read, and 40,000 for the reads of a
  write preparation. A deleted key counts as an examined key.
- **4,096 metadata pages** for one call. A key read or a range read touches a few
  pages of the index of the files. This is the limit that a request with many
  keys reaches first: a request that rewrites about 2,000 fields spends the pages
  before it spends the keys.

A reader must not rely on the key count alone. The error is `read_resource_limit`
(HTTP 503), and a read-limit error ends a write scope.

A queue that keeps all its entries under one hash fails in this way:

1. A release deletes the entry. The key stays as a deleted key in the hash.
2. The deleted keys sit at the head of the hash, because the oldest entries
   go first.
3. A page read has to pass all of them to reach the first live key.
4. After about 10,000 releases, the page read exceeds its budget and fails.
   The reader does not move, so it fails again. A restart does not help.

A page of live entries does not bound a read call. The key space bounds it.

## The layout

| Kind | Key | Value |
|---|---|---|
| Queue entry | `{entries}:{bucket:020}\0{expires:020}:{id}` | the entry |
| Directory entry | `{directory}\0{bucket:020}` | a token: `{"v":1,"t":"{expires:020}:{id}"}` |

The retention queue uses `crq:{bucket:020}\0{expires_at_nanos:020}:{record_id}`
and `crb\0{bucket:020}`. The blob queue uses `bref:q:` and `bref:d\0`.

- `bucket` is the start of a slice of time in nanoseconds: `expires - expires %
  slice`. It is not an index, so a change of the slice length cannot make an old
  bucket name another slice.
- The hash of an entry is `{entries}:{bucket:020}\0`: one slice of time. The
  range is the expiry and then the id. A range read of one bucket returns the
  oldest expiry first.
- The directory is one hash. It has one entry for each bucket that holds, or held,
  a queue entry. A directory entry is small.
- The slice is a choice of the user of the queue. `TimeQueue::for_window` takes
  a 64th of the entry lifetime, between one second and one hour. The blob queue
  uses one hour for every dwell at or above its floor (`TimeQueue::with_slice`),
  so the key of an entry does not depend on a knob.

## Writers

- A writer adds an entry and the directory entry of its bucket in one batch
  (`TimeQueue::rows`). The batch goes through the planner: the writer hands both
  key/value pairs to the planner as part of the batch. No writer writes a queue
  key on its own, and the writer lint (`scripts/lints/lint-store-writers.py`)
  lists none.
- Every entry rewrites the directory entry with a new token. The token names the
  entry, so two entries never leave the same directory value. This is the only
  way to give a reader that retires a bucket a check that sees every later
  write.
- **Entries are not scoped.** The key of an entry has no storage scope prefix: one
  queue serves every scope of the node. The scope of the work is in the value of
  the entry (for the retention queue: the key of the kept conflict record). A
  queue that scoped its keys would need one directory for each scope.
- **Each queue has its own families.** A queue is two families: the entries
  (`crq`) and the directory (`crb`). A queue must not share a directory
  with another queue, because a bucket start does not say which queue it
  belongs to. A family name holds no NUL (the NUL ends the hash), and no name is
  a prefix of another name (`crq` and `crq:a` would put the keys of one
  family inside the key range of the other). Register the names in the key
  validation (`storage/current_keys.rs`), the main key classes and the inventory
  classes (`admin_db/inventory_key_classes.rs`).
- **The writer and the reader read one clock.** The expiry that the writer puts in
  the key and the due test of the reader come from the same clock
  (`RetentionState::clock_nanos`). A reader on a faster clock releases early. A
  reader on a slower clock leaves entries below its cursor, which only the hourly
  restart finds.
- One batch can add several entries to one bucket. The batch must write the
  directory entry once. The planner keeps the last value of a key. A caller that
  builds its batch by itself does the same.
- An entry is added only with the work that it names. It is never a promise of
  work that is not in the batch.

## Release

An entry ends with a delete in the batch of the work that it names (for the
retention queue: the batch that releases the record). A deleted entry leaves a
deleted key in its bucket. Nothing reads that key again after the bucket
retires.

## Readers

A reader keeps its state in memory (`QueueReader`) and follows these rules.

1. **List the directory first.** Read the directory entries of the buckets that
   started (`start <= now`) with a bounded range walk. A bucket that is not in
   the directory is not read. A bucket that the reader knows and the directory
   no longer lists leaves the state.
2. **Walk a bucket with a cursor.** Each bucket has one `RangeWalk`
   (`record_store/bounded_range.rs`). The walk is the cursor: it holds the least
   key not read yet. It moves over deleted keys, and it is never reset to the head
   of the hash by a successful page.
3. **Bound each read by key space.** `RecordStore::walk_step` asks for a page. If
   the call exceeds the read budget, the walk asks for fewer keys. If that is
   still too much, it cuts the key range in two and reads the lower half first.
   Each cut makes the span smaller, so the walk always moves. A walk that finds
   no key to continue from for too long stops with an error that names the cause.
   A reader never loops on a failed read. One `QueueReader::read_due` call makes
   at most 16 such reads. A call that passed only released keys returns an empty
   page that does not wait. The dwell step stops after 64 empty pages and reports
   `DwellStep::passed`; the next step continues from the cursor.
4. **Move the cursor after the work.** A page read changes nothing in the state.
   The caller releases the keys, then calls `QueueReader::finish`. A failure
   leaves the cursor where it was, and the next try reads the same keys.
   `QueueReader::finish_before(page, key, retry_at)` is the other way to end a
   page: the caller handled the keys of the page that sort before `key` and could
   not handle `key` (for the dwell queue: a blob that the pass could not delete
   in this wake). The cursor goes to
   `key`, and the bucket waits until `retry_at`, a time on the clock of the queue.
   Without it the reader would pass the key and never see it again.
5. **Stop at the first key that is not due.** The cursor stays at that key, and
   the bucket waits until its time. A bucket is waiting, not retired.
6. **Start again now and then.** `QueueReader::restart` starts every live bucket
   from its start and reads the directory from its start. The janitor calls it
   once in 60 wakes, which is once in an hour with the default wake of 60 s, and
   a restart of the node does the same by losing the state. The restart covers a
   clock that stepped back, so an entry sorts below a cursor. It never reads a
   bucket that the directory dropped. The cost is one bounded walk for each live
   bucket.

## Retirement

A bucket retires when its walk has read every key of it. Retirement deletes the
directory entry. The deleted keys of the bucket then go out of reach with it. A caller that
keeps other work of a bucket beside the reader must finish it before it asks for
the token (`QueueReader::retire_token`).

A writer can add an entry to the bucket after the walk passed that place. The
reader must not delete the directory entry in that case, or the queue entry is
lost.

- The reader reads the directory entry (the token) **before** the walk of the
  bucket. A token that changes at a refresh starts the walk again.
- The retirement is a guarded delete in the batch commit. The signature is
  `GuardedDelete { key: Vec<u8>, expected: Vec<u8> }`, and a caller puts a list of
  them in `RowChanges.guarded_deletes`. `RecordStore::retire_bucket(key, token)`
  is the call of the queue. The commit reads each key by key in the write gate
  (`get_many`) and deletes it only if the stored bytes equal `expected`.
  `RowCommit.guarded_deleted` counts the keys it deleted. The gate is held for
  one batch of one key, and nothing else is read.
- **The token is the raw value of the directory entry, as the range read of the
  directory returned it.** The reader keeps the bytes; it does not decode them,
  and it does not build them. A decoded and encoded value could differ in the
  bytes and the guard would never match (or, worse, always match).
- Any batch that adds an entry after the reader read the directory entry changes
  the token, so the delete leaves the directory entry. The reader then leaves the
  bucket in its state, and the next refresh adds it again with a new walk.

The retirement never needs a range read in the write gate.

## Fairness

A reader lists the directory again at the start of each wake, so a bucket that
starts during a long backlog gets its turn. Many due entries share a batch.

## Pacing

A user of the queue deletes in batches. A background batch goes through the
[eager durable queue](lastdb-eager-durable-queue.md) and returns after
durability. Ordinary reads keep the committed state until publication.

The cost of a batch to the writers is its `blocked` time: the hold of the write
gate and the wait for the shared flush. It excludes the open collection period,
when new preparation can continue. A job that needs a sealed fence also counts
the full wait of that fence. `storage::disk_pack::transaction::blocked::measure`
runs a step and adds up the time of every write scope that the step opens in its
own task. The steps of the retention janitor (`release_step`, `net_step`) and of
the dwell pass (`blob_dwell_step`, `blob_dwell_net_step`) return it in `blocked`.

The user rests after each batch for `blocked * (100 - duty) / duty`, where `duty`
is a share of the time in percent (10 by default). The rest is at least 25 ms and
at most 30 s. With 10 percent, writers wait for the user about a tenth of the
time. The page reads before the gate block nobody and are not part of it. The two
users share the meter, the watch, the quiet period and the rest rule
(`jobs/paced_rest.rs`).

The rest protects the writers, so the user needs it only when a writer is there. A
`WriterWatch` counts the write scopes that the process opens and takes off the ones
that the user opened itself. It remembers the time of the last write scope of another
task. For a quiet period after that time, every rest is the full rest. The quiet period
is `LASTDB_MAINTENANCE_QUIET_SECS` (60 s by default; 0 means that only the step that
saw the write counts). The rest is the minimum rest only when no other task opened a
write scope for the whole period. A long rest ends early only when the period has
passed, so a backlog does not wait out a rest of 20 s after the writers went away.

The period is long on purpose. A writer that writes once every few seconds is not a
load, but each of its writes that meets a background batch waits for that flush. If
the user went on at once between two writes of such a writer, nearly every write of
it would meet a batch. So one write scope of another task keeps the user in the
paced mode for the whole period.

What the watch sees: only write scopes (`transaction/request.rs`). A caller that takes
the write gate without a scope is not seen, for example `PackRuntime::mutate` outside
a scope and the creation of the current key format at start. A write scope that
stages a write counts, also when it writes little (the seed of a schema, for
example). A write scope that stages nothing does not count. `lastdb db verify-refs`
opens one such scope at the start of each run, to order the run after the writes in
flight. A scope counts when it ends, and only if it staged something
(`transaction/request.rs`, `blocked::opened`). A read, `lastdb status` and
`lastdb liveness explain` open no write scope at all. A task of the node itself that
opens a write scope counts as another writer: the blob dwell pass and the janitor see
each other.

The meter counts the gate hold and the flush wait, not the gate hold alone. The flush
is the larger part of the time that the writers wait, so a rest that follows only the
gate hold is too short and raises writer latency.

## Failure

A wake that ends with an error does not retry at once. The wait doubles with each
next wake that fails, from the wake interval (at least one second) up to ten
minutes. A good wake sets the count to zero. The error, the count and the time
of the next try are in the status. A lane that fails ends for the wake, and the
other lane goes on.

## Size

`RecordStore::time_queue_stats` gives the number of buckets (from the directory)
and the number of entries up to a cap. Every read is a bounded range walk, so
deleted keys cannot make it fail. The count stops after 1,024 bucket reads, and
a bucket that cannot be read ends it. The entry count is then a lower bound
(`capped`). Only a failure to read the directory is an error.

## The safety net

A bucket whose entries are live and whose directory entry is missing is never
listed. Its entries stay for ever. For the retention queue, each of them is a kept
conflict record that the janitor never releases. This is a leak, not a loss, and it
is exactly what the queue exists to prevent. No code of the queue makes it, but
nothing else guards the directory, so the queue carries a net that finds it.

`TimeQueue::mend_slices(store, slices, stamp)` is the net, and the reader of a
queue calls it. It needs no scan:

1. `TimeQueue::net_slices(now, lifetime)` lists the starts of the slices from one
   lifetime and one slice before now to one lifetime and one slice after it. An
   entry lives in the slice of its expiry, and an expiry lies between a little
   before now (entries that are due and wait) and one lifetime after now (entries
   just written). That is about twice the slices of a lifetime, 130 for the
   retention queue.
2. One batch read, by key, gives the directory entries of the slices.
3. A slice without a directory entry gets one bounded range read of its hash (a
   `RangeWalk` with one key). The read stops at the first live key. Deleted keys
   do not make it fail.
4. A slice that holds a live key and has no directory entry gets a new one with a
   new token (`net:{stamp}`) in one batch of the planner (`register_buckets`).

The retention janitor sweeps once at its start and then once in
`LASTDB_CONFLICT_JANITOR_NET_SECS` seconds (default 3,600; 0 turns the net off).
A sweep is chunks of 16 slices; each chunk is paced like a batch. The status
counts the sweeps and `buckets_repaired`, and the janitor logs one warning for
each repaired bucket. A count above zero means that something wrote or deleted
entries of the queue outside the queue, or that a retirement was wrong.

What the net does not cover:

- The directory covers every slice length, because a directory entry names the
  start of its bucket. The net covers the slicing of the current lifetime only.
  After a change of the lifetime the old slices do not line up with the new ones,
  and the net does not probe them. The directory lists their entries as before.
- A process with another lifetime than the node that wrote the entries. The slice
  follows `LASTDB_CONFLICT_RETENTION_SECS`. `verify-refs` asks the daemon, so it
  uses the daemon's slice. `recount` is a process of its own, and the janitor
  records the window of the node in one entry (`crb\0params`, once at its start
  and when the window changes). The recount reads that entry and uses the recorded
  window and slice for its survey and its repair, and it prints a note when its own
  `LASTDB_CONFLICT_RETENTION_SECS` differs. A home whose janitor never ran has no
  such entry, and the recount then uses its own environment.
- A bucket older than one lifetime and one slice. The net looks back one lifetime
  (a node that was down for longer than that, with a lost directory entry on top,
  is rare), and the survey of `verify-refs` and `recount` uses the same range. A
  listed bucket stays visible to the reader after that range. A bucket outside
  that range with no directory entry is not found.
- A bucket that holds only deleted keys is not a fault, and the net leaves it.

## What can go wrong and what catches it

The directory entry of a bucket can be missing in four ways. Each has a
prevention, a detection and a repair. The dwell queue of the file blobs inherits
all of them.

| Way | Prevented by | Detected by | Healed by |
|---|---|---|---|
| A writer adds a queue entry without its directory entry (code outside `TimeQueue::rows`). | The queue entry and the directory entry are one `TimeQueue::rows` call, in the batch of the work, through the planner. The writer lint finds a writer that bypasses the planner. | `verify-refs`: `queue_entry_without_directory`. The net counts `buckets_repaired`. | The net registers the bucket within one sweep. `recount --execute` writes the directory entry. |
| A crash between the write of the queue entry and the write of the directory entry. | Excluded: one batch holds both, and a crash keeps whole batches. | (not possible) | |
| A retirement races a writer: the reader decided that the bucket was empty, and a writer added an entry. | Every queue entry rewrites the directory entry with a new token, and the retirement is a guarded delete that compares the raw bytes in the write gate. A writer that came after the reader read the directory entry changes the token, so the delete leaves the directory entry. | As above, if the guard were wrong. | As above. |
| An external delete of the directory entry. | Nothing in the queue can prevent it. | `verify-refs` and the net. | The net, and `recount --execute`. |

A kept conflict record whose queue entry is missing is never released by the
janitor, because the janitor finds records through the queue. The net cannot find
it: there is no entry to probe. The verifier does not compare kept conflict
records with queue entries.

### `verify-refs` and `recount`

The `queue` phase of `lastdb db verify-refs` surveys the retention queue
(`time_queue/survey.rs`, `QueueSurvey`). It reads the directory, walks the entries
of every listed bucket, and also walks the slices that the arithmetic lists, have
no directory entry, and hold a live key. A bucket that holds entries and still has
no directory entry when the survey reads it again is a fault: each of its entries
is a `queue_entry_without_directory`. The status line `Verify refs` shows
`queue_rows`.

`lastdb db recount --execute` (a stopped home) runs the same survey. It writes the
missing directory entries again through the planner (`TimeQueue::register_buckets`,
`retention/repair.rs`). The window is the one that the node recorded. A second run
finds nothing to change.

## Limits

- The reads of a request are bounded by 4,096 metadata pages as well as by keys
  (see "The problem"). A request that rewrites about 2,000 conflicting fields is
  near that budget, and it can get `read_resource_limit` (503). The queue adds
  entries to such a request but no read.
- Disk format v2 merges its run files in the background. Each commit writes one
  run file, so a queue should keep its release batch large and its duty share
  low.
- The directory is one hash. A retired bucket leaves one deleted key there. With
  one-hour slices that is 24 each day. A restart walks the directory from its
  start with bounded reads, so the deleted keys cost reads, not a failure.
  Compaction of the files removes them.
- Entries of one bucket are ordered by the expiry. A reader that reads a bucket
  while a writer adds entries at the same expiry needs the token rule above.
- The retention clock is the wall clock with a clamp (`RetentionState::
  clock_nanos`). An entry written while the clock was far behind sorts below a
  cursor. The periodic restart finds it.

## What the next user provides

A new queue takes the layout, the reader, the retirement, the safety net and the size
count as they are (the blob dwell queue already does). It provides:

1. A `TimeQueue` value: the two family names and the slice. The families are its
   own (see "Writers").
2. The entry value, and the queue entry and directory entry of a new entry
   (`TimeQueue::rows`) in the batch of the work that creates the entry.
3. The release of one due entry, through the planner, with the delete of the
   entry in the same batch.
4. A cursor holder like `RetentionCursor` that alternates its kinds of work, and
   that checks its unfinished long work before it asks for a retirement token.
5. A call of the net (`TimeQueue::net_slices` and `TimeQueue::mend_slices`) from
   its janitor, once at the start and then now and then.
6. A survey of its queue in the verifier and the recount (`QueueSurvey`, used
   from `counter/verify/queue.rs` and `counter/recount/retention.rs`), and a
   repair that writes the missing directory entries again
   (`retention/repair.rs`).
7. The key family names in the key validation (`storage/current_keys.rs`), the
   main key classes and the inventory classes
   (`admin_db/inventory_key_classes.rs`).

It must not read an entry hash with a plain range read and a limit.

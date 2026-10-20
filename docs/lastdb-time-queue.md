# The time-bucketed queue

A queue of entries that become due at a time. A reader releases the entries
that are due, oldest first, and each release deletes its entry. The first user is
the retention queue of kept conflict records (`docs/lastdb-atom-counter.md`).
Another queue with the same needs takes the same contract.

Code: `fold_db/crates/core/src/db_operations/atom_store/time_queue/`.

## The problem

LastDB has no scan. A range read works under one hash, and one read call has a
budget. The budget has more than one limit, and the first one that is reached
ends the call:

- **10,000 examined keys** for an ordinary read, and 40,000 for the reads of a
  write preparation. A deleted key counts as an examined key.
- **4,096 metadata pages** for one call. A key read or a range read touches a few
  pages of the index of the files. This is the limit that a request with many
  keys reaches first: a request that reads one slot directory for each of
  hundreds of slots, or that rewrites about 2,000 fields, spends the pages
  before it spends the keys. The 256-slot cap of an erase and the width of the
  requests of the proofs both come from this limit.

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

| Row | Key | Value |
|---|---|---|
| Entry | `{entries}:{bucket:020}\0{expires:020}:{id}` | the entry |
| Directory row | `{directory}\0{bucket:020}` | a token: `{"v":1,"t":"{expires:020}:{id}"}` |

- `bucket` is the start of a slice of time in nanoseconds: `expires - expires %
  slice`. It is not an index, so a change of the slice length cannot make an old
  bucket name another slice.
- The hash of an entry is `{entries}:{bucket:020}\0`: one slice of time. The
  range is the expiry and then the id. A range read of one bucket returns the
  oldest expiry first.
- The directory is one hash. It has one row for each bucket that holds, or held,
  an entry. A row is small.
- The slice is a choice of the user of the queue. `TimeQueue::for_window` takes
  a 64th of the entry lifetime, between one second and one hour.

## Writers

- A writer adds an entry and the directory row of its bucket in one batch
  (`TimeQueue::rows`). The batch goes through the planner: the writer hands the
  rows to the planner as rows of the batch. No writer writes a queue key on its
  own, and the writer lint (`scripts/lints/lint-atom-counter-writers.py`) lists
  none.
- Every entry rewrites the directory row with a new token. The token names the
  entry, so two entries never leave the same row. This is the only way to give a
  reader that retires a bucket a check that sees every later write.
- **Entries are not scoped.** The key of an entry has no storage scope prefix: one
  queue serves every scope of the node. The scope of the work is in the value of
  the entry (for the retention queue: the key of the history row). A queue that
  scoped its keys would need one directory for each scope.
- **Each queue has its own families.** A queue is two families: the entries
  (`crq:v1`) and the directory (`crb:v1`). A queue must not share a directory
  with another queue, because a bucket start does not say which queue it
  belongs to. A family name holds no NUL (the NUL ends the hash), and no name is
  a prefix of another name (`crq:v1` and `crq:v1:a` would put the keys of one
  family inside the key range of the other). Register the names in the key
  validation, the cutover classes, the capture skip list and the inventory.
- **The writer and the reader read one clock.** The expiry that the writer puts in
  the key and the due test of the reader come from the same clock
  (`RetentionState::clock_nanos`). A reader on a faster clock releases early. A
  reader on a slower clock leaves entries below its cursor, which only the hourly
  restart finds.
- One batch can add several entries to one bucket. The batch must write the
  directory row once. The planner keeps the last row of a key
  (`counter/records.rs`). A caller that builds `rows` by itself does the same.
- An entry is added only with the work that it names. It is never a promise of
  work that is not in the batch.

## Release

An entry ends with a delete in the batch of the work that it names (for the
retention queue: the batch that releases the record). A deleted entry leaves a
deleted key in its bucket. Nothing reads that key again after the bucket
retires.

## Readers

A reader keeps its state in memory (`QueueReader`) and follows these rules.

1. **List the directory first.** Read the directory rows of the buckets that
   started (`start <= now`) with a bounded range walk. A bucket that is not in
   the directory is not read. A bucket that the reader knows and the directory
   no longer lists leaves the state.
2. **Walk a bucket with a cursor.** Each bucket has one `RangeWalk`
   (`atom_store/bounded_range.rs`). The walk is the cursor: it holds the least
   key not read yet. It moves over deleted keys, and it is never reset to the head
   of the hash by a successful page.
3. **Bound each read by key space.** `AtomStore::walk_step` asks for a page. If
   the call exceeds the read budget, the walk asks for fewer keys. If that is
   still too much, it cuts the key range in two and reads the lower half first.
   Each cut makes the span smaller, so the walk always moves. A walk that finds
   no key to continue from for too long stops with an error that names the cause.
   A reader never loops on a failed read.
4. **Move the cursor after the work.** A page read changes nothing in the state.
   The caller releases the keys, then calls `QueueReader::finish`. A failure
   leaves the cursor where it was, and the next try reads the same keys.
   `QueueReader::finish_before(page, key, retry_at)` is the other way to end a
   page: the caller handled the keys of the page that sort before `key` and could
   not handle `key` (for the retention queue: a drain marker, when the janitor
   already holds the most pending drains it keeps in memory). The cursor goes to
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
   bucket. Long work that the caller keeps beside the reader (the pending drains)
   keeps its own cursors across a restart.

## Retirement

A bucket retires when its walk has read every key of it and no entry in it waits
for other work (for the retention queue: no drain marker that is not finished).
Retirement deletes the directory row. The deleted keys of the bucket then die
with it.

The reader does not know the other work. **The caller must check it before it
asks for the token** (`QueueReader::retire_token`): the retention janitor calls
`holds_marker(bucket)` first. A reader that retired a bucket with an unfinished
drain marker in it would drop the only pointer to the drain.

A writer can add an entry to the bucket after the walk passed that place. The
reader must not delete the row in that case, or the entry is lost.

- The reader reads the directory row (the token) **before** the walk of the
  bucket. A token that changes at a refresh starts the walk again.
- The retirement is a guarded delete in the planner. The signature is
  `GuardedDelete { key: Vec<u8>, expected: Vec<u8> }`, and a caller puts a list of
  them in `TipChangeSet.guarded_deletes`. `AtomStore::retire_bucket(key, token)`
  is the call of the queue. The planner reads each key by key in the write gate
  (`get_many`) and deletes it only if the stored bytes equal `expected`.
  `TipCommit.guarded_deleted` counts the keys it deleted. The gate is held for
  one batch of one key, and nothing else is read.
- **The token is the raw value of the directory row, as the range read of the
  directory returned it.** The reader keeps the bytes; it does not decode them,
  and it does not build them. A decoded and encoded value could differ in the
  bytes and the guard would never match (or, worse, always match).
- Any batch that adds an entry after the reader read the row changes the token,
  so the delete leaves the row. The reader then leaves the bucket in its state,
  and the next refresh adds it again with a new walk.

The retirement never needs a range read in the write gate.

## Fairness

A queue can hold two kinds of work: due entries, and work that takes many
batches (a long drain). Run them in turns: at most N batches of the long work,
then at most N batches of the due entries, and again. The retention janitor uses
N = 4. A long drain then delays the due entries by at most 4 batches, and a
long backlog delays a drain by at most 4 batches.

A long piece of work keeps its own cursor in memory. It does not stay at the
head of the queue. The reader lists the directory again at the start of each
turn, so a bucket that starts while a long drain runs gets its turn. Many small
pieces of work share a batch (the drains of many slots with one record each take
one batch of 32 records), and a batch touches at most 64 drains.

A page of drain markers needs no write gate: the reader decodes the markers from
the page it read, and keeps them as pending drains. Such a page costs no batch and
no rest, so a bucket of many markers is passed in a few reads. A marker is ordered
by its due time like any entry: due records that expired before the erase are
released first.

The janitor holds at most 1,024 pending drains. A marker that finds no room stays
in its bucket. The cursor of the bucket stays at it, and the bucket waits for a
second before it is read again. Memory does not grow with the number of markers.

## Failure

A wake that ends with an error does not retry at once. The wait doubles with each
wake in a row that fails, from the wake interval (at least one second) up to ten
minutes, and a good wake sets the count to zero. The error, the count and the time
of the next try are in the status. A lane that fails ends for the wake, and the
other lane goes on.

## Size

`AtomStore::time_queue_stats` gives the number of buckets (from the directory)
and the number of entries up to a cap. Every read is a bounded range walk, so
deleted keys cannot make it fail. The count stops after 1,024 bucket reads, and
a bucket that cannot be read ends it. The entry count is then a lower bound
(`capped`). Only a failure to read the directory is an error.

## The safety net

A bucket whose entries are live and whose directory row is missing is never
listed. Its entries stay for ever. For the retention queue that is a conflict
record, its references and its loser atom. This is a leak, not a loss, and it is
exactly what the queue exists to prevent. No code of the queue makes it, but
nothing else guards the directory, so the queue carries a net that finds it.

`TimeQueue::mend_slices(store, slices, stamp)` is the net, and the reader of a
queue calls it. It needs no scan:

1. `TimeQueue::net_slices(now, lifetime)` lists the starts of the slices from one
   lifetime and one slice before now to one lifetime and one slice after it. An
   entry lives in the slice of its expiry, and an expiry lies between a little
   before now (entries that are due and wait) and one lifetime after now (entries
   just written). That is about twice the slices of a lifetime, 130 for the
   retention queue.
2. One batch read, by key, gives the directory rows of the slices.
3. A slice without a row gets one bounded range read of its hash (a `RangeWalk`
   with one key). The read stops at the first live key. Deleted keys do not make
   it fail.
4. A slice that holds a live key and has no row gets a new row with a new token
   (`net:{stamp}`) in one batch of the planner (`register_buckets`).

The retention janitor sweeps once at its start and then once in
`LASTDB_CONFLICT_JANITOR_NET_SECS` seconds (default 3,600; 0 turns the net off).
A sweep is chunks of 16 slices; each chunk is paced like a batch. The status
counts the sweeps and `buckets_repaired`, and the janitor logs one warning for
each repaired bucket. A count above zero means that something wrote or deleted
rows of the queue outside the queue, or that a retirement was wrong.

What the net does not cover:

- The directory covers every slice length, because a row names the start of its
  bucket. The net covers the slicing of the current lifetime only. After a change
  of the lifetime the old slices do not line up with the new ones, and the net
  does not probe them. Their rows are listed by the directory as before.
- A bucket older than one lifetime. Entries that wait for longer than a lifetime
  and lost their row are found by `verify-refs` (for the records that name them)
  and `recount`, not by the net.
- A bucket that holds only deleted keys is not a fault, and the net leaves it.

## What can go wrong and what catches it

The directory row of a bucket can be missing in four ways. Each has a
prevention, a detection and a repair. S5 inherits all of them when it adopts the
queue.

| Way | Prevented by | Detected by | Healed by |
|---|---|---|---|
| A writer adds an entry without the row (code outside `TimeQueue::rows`). | The entry and the row are one `TimeQueue::rows` call, in the batch of the work, through the planner. The writer lint finds a writer that bypasses the planner. | `verify-refs`: `queue_entry_without_directory` for a record entry. The net counts `buckets_repaired`. | The net registers the bucket within one sweep. `recount --execute` writes the row. |
| A crash between the write of the entry and the write of the row. | Excluded: one batch holds both, and a crash keeps whole batches. | (not possible) | |
| A retirement races a writer: the reader decided that the bucket was empty, and a writer added an entry. | Every entry rewrites the row with a new token, and the retirement is a guarded delete that compares the raw bytes in the write gate. A writer that came after the reader read the row changes the token, so the delete leaves the row. | As above, if the guard were wrong. | As above. |
| An external delete of the row, or a partial restore that brought the entries and not the row. | Nothing in the queue can prevent it. | `verify-refs` (a record whose queue entry has no row) and the net (any entry, also a drain marker). | The net, and `recount --execute`. |

A fifth, quieter fault belongs to the same chain. A conflict record whose queue
entry is missing is never released by the janitor, because the janitor finds
records through the queue. `verify-refs` reports it as
`record_without_queue_entry`, and `recount --execute` writes the queue entry
again (see below). The net cannot find it: there is no entry to probe.

### `verify-refs` and `recount`

For each counted conflict record that the records phase reads, the verifier
checks the chain by key, at the root of its page:

1. the slot directory entry at the key that the molecule, the slot and the id of
   the record give (the expiry is not in the record, so the queue entry cannot be
   read without it);
2. the queue entry that the slot directory entry names;
3. the directory row of the bucket in the key of that queue entry.

A record whose slot directory entry is missing, does not decode, or names a
queue entry that is missing is a `record_without_queue_entry`. A queue entry
whose bucket has no row is a `queue_entry_without_directory`. The reads are in
the read scope of the page, so a committed root shows them whole: a record is
written with its rows, a release deletes them together, and a retirement keeps
the row of a bucket that holds an entry. No writer can explain a finding. The
status line `Verify refs` shows `queue_rows` (the sum of the two).

`lastdb db recount --execute` (a stopped home) walks the records once for each
scope and writes the missing rows again through the planner. A record gets a new
queue entry that expires one window from now (the original expiry is not known),
its slot directory entry and the row of the new bucket. A queue entry whose
bucket has no row gets the row. A second run finds nothing to change.

## Limits

- The reads of a request are bounded by 4,096 metadata pages as well as by keys
  (see "The problem"). A request that rewrites about 2,000 conflicting fields
  (a 1,980-field rewrite, 10 keys of 198 fields) is near that budget: on a home
  that grew, it is refused with `read_resource_limit` (503) after about 11 to 13
  such requests, also with no release at all. The queue adds rows to such a
  request but no read. Replay batches (a mutation log that brings many records
  at once) were not run, so their margin is not shown.
- After many SIGKILL rounds a home can reach the storage limit of 65,536
  dependency parts for one publication (`dependency capacity`), and writes
  block. This is a limit of the storage, not of the queue. The janitor adds to
  the number of publications: every release batch and every retirement is a
  durable batch. [[PARTS]]
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

The blob queue (or another queue) takes the layout, the reader, the retirement,
the safety net and the size count as they are. It provides:

1. A `TimeQueue` value: the two family names and the slice. The families are its
   own (see "Writers").
2. The entry value, and the rows of a new entry (`TimeQueue::rows`) in the batch
   of the work that creates the entry.
3. The release of one due entry, through the planner, with the delete of the
   entry in the same batch.
4. A cursor holder like `RetentionCursor` that alternates its kinds of work, and
   that checks its unfinished long work before it asks for a retirement token.
5. A call of the net (`TimeQueue::net_slices` and `TimeQueue::mend_slices`) from
   its janitor, once at the start and then now and then.
6. A check in the verifier and a repair in the recount for the chain from its
   work item to its queue entry and the row of the bucket
   (`retention/check.rs`, `retention/repair.rs`).
7. The key family names in the key validation (`storage/current_keys.rs`),
   the cutover classes (`mini_cutover/main_keys.rs`), the capture skip list
   (`sync/policy.rs`) and the inventory classes.

It must not read an entry hash with a plain range read and a limit.

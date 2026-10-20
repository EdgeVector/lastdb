# The time-bucketed queue

A queue of entries that become due at a time. A reader releases the entries
that are due, oldest first, and each release deletes its entry. The first user is
the retention queue of kept conflict records (`docs/lastdb-atom-counter.md`).
Another queue with the same needs takes the same contract.

Code: `fold_db/crates/core/src/db_operations/atom_store/time_queue/`.

## The problem

LastDB has no scan. A range read works under one hash, and one read call may
examine at most 10,000 keys. A deleted key counts as an examined key. A queue
that keeps all its entries under one hash fails in this way:

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
5. **Stop at the first key that is not due.** The cursor stays at that key, and
   the bucket waits until its time. A bucket is waiting, not retired.
6. **Start again now and then.** A restart, and one pass in a while, start each
   live bucket from its start (`QueueReader::restart`). They never read a bucket
   that the directory dropped. The restart covers a clock that stepped back, so an
   entry sorts below a cursor. The cost is one bounded walk for each live bucket.

## Retirement

A bucket retires when its walk has read every key of it and no entry in it waits
for other work (for the retention queue: no drain marker that is not finished).
Retirement deletes the directory row. The deleted keys of the bucket then die
with it.

A writer can add an entry to the bucket after the walk passed that place. The
reader must not delete the row in that case, or the entry is lost.

- The reader reads the directory row (the token) **before** the walk of the
  bucket. A token that changes at a refresh starts the walk again.
- The retirement is a guarded delete in the planner (`GuardedDelete`,
  `AtomStore::retire_bucket`). It reads the row in the write gate and deletes it
  only while the row holds the token. It holds the gate for one batch of one key
  and reads nothing else.
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
head of the queue.

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

## Limits

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

The blob queue (or another queue) takes the layout, the reader, the retirement
and the size count as they are. It provides:

1. A `TimeQueue` value: the two family names and the slice.
2. The entry value, and the rows of a new entry (`TimeQueue::rows`) in the batch
   of the work that creates the entry.
3. The release of one due entry, through the planner, with the delete of the
   entry in the same batch.
4. A cursor holder like `RetentionCursor` that alternates its kinds of work.
5. The key family names in the key validation (`storage/current_keys.rs`),
   the cutover classes (`mini_cutover/main_keys.rs`), the capture skip list
   (`sync/policy.rs`) and the inventory classes.

It must not read an entry hash with a plain range read and a limit.

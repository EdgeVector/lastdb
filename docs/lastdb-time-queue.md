# The time-bucketed queue

A queue of entries that become due at a time. A reader releases the entries
that are due, oldest first, and each release deletes its entry. Two queues use it:
the retention queue of kept conflict records (`docs/lastdb-atom-counter.md`) and the
dwell queue of file blobs at zero references (`docs/lastdb-blob-counter.md`).
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
  a 64th of the entry lifetime, between one second and one hour. The blob queue
  uses one hour for every dwell at or above its floor (`TimeQueue::with_slice`),
  so the key of an entry does not depend on a knob.

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

## Pacing

The current private background path shares the bounded collection window.
Only write preparation reads its owned prefix. Ordinary reads retain the committed state.
The job still returns after durability. Its cost normally measures the gate hold.
A sealed fence also counts its flush wait.
The measurements below describe the earlier sealed-batch path.
See `docs/lastdb-concurrent-compaction.md` for the current protocol.


A user of the queue deletes in batches, and each batch is durable work that seals
the write queue. A writer that arrives while a batch is open waits for the write
gate. And when the batch has the gate no more, the writer still waits in
`PackRuntime::enter_prepare` until the batch is durable. So the time that a batch
keeps writers waiting is its hold of the gate (8 to 30 ms) plus its flush. The
flush costs about 15 to 20 ms for each released record: 0.5 to 1 s for a batch of
32, and it grows with the batch, so a larger batch does not release more in the same
time and freezes the writers for longer.

The user rests after each batch for `blocked * (100 - duty) / duty`, where `blocked`
is that time and `duty` is a share of the time in percent (10 by default). The rest
is at least 25 ms and at most 30 s. With 10 percent, writers wait for the user about
a tenth of the time. The page reads before the gate block nobody and are not part of
it. `storage::disk_pack::transaction::blocked::measure` runs a step and adds up the
time of every write scope the step opens in its own task. The steps of the retention
janitor (`release_step`, `drain_step`, `net_step`), of the dwell pass
(`blob_dwell_step`, `blob_dwell_net_step`) and of the molecule drain
(`molecule_drain_step`, `docs/lastdb-molecule-drain.md`) return it in `blocked`. The three
users share the meter, the watch, the quiet period and the rest (`conflict_janitor_pace.rs`).
The drain keeps its own duty knob (`LASTDB_MOLECULE_DRAIN_DUTY_PCT`, 10 percent).

The rest protects the writers, so the user needs it only when a writer is there. A
`WriterWatch` counts the write scopes that the process opens and takes off the ones
that the user opened itself. It remembers the time of the last write scope of another
task. For a quiet period after that time, every rest is the full rest. The quiet period
is `LASTDB_MAINTENANCE_QUIET_SECS` (60 s by default; 0 means that only the step that
saw the write counts). The rest is the minimum rest only when no other task opened a
write scope for the whole period. A long rest ends early only when the period has
passed, so a backlog does not wait out a rest of 20 s after the writers went away.

The period is long on purpose. A writer that writes once every few seconds is not a
load, but each of its writes that meets a sealed batch waits for a flush (0.3 to
1 s). If the user went on at once between two writes of such a writer, nearly every
write of it would meet a batch. So one write scope of another task keeps the user
in the paced mode for the whole period.

What the watch sees: only write scopes (`transaction/request.rs`). A caller that takes
the write gate without a scope is not seen: `PackRuntime::mutate` outside a scope,
native maintenance (`maintain_native`), and the creation of the current key format at
start. A write scope that stages a write counts, also when it writes little: the seed
of a schema and `lastdb db inventory` (it writes its attribution rows) do. A write scope
that stages nothing does not count. `lastdb db verify-refs` opens one such scope at the
start of each run, to order the run after the writes in flight. It counted as a writer
before, and a poll of it every 30 s kept the user in the paced mode for ever (the molecule
drain on a system disk, a poll every 30 s: 24 and 36 tips a second, 418 and 279 s for 10,000
tips, against 87 and 111 tips a second, 115 and 90 s, with the empty scope left out; the same
drain with no poll: 81 to 102). A scope counts when it ends, and only if it staged something
(`transaction/request.rs`, `blocked::opened`). A read, `lastdb status`, `lastdb liveness
explain` and a `lastdb db molecule-drain` call that names no molecule open no write scope at
all (a poll of each of them every 30 s left the drain at its idle speed). A task of the node
itself that opens a write scope (the blob dwell pass, the janitor and the drain see each
other) counts as another writer.

Measured on synthetic homes with the default knobs. In each test the nodes ran at the
same time on one host: main before this change, this change with the drain on, and
this change with the drain off. A test writer sent queued writes and timed each one.
The p50 and p95 are of the pooled samples of 3 sets.

**No other writer.** After the quiet period the user rests the minimum rest, and its
speed is the speed of its flush. The janitor drained 21 to 26 records a second (a
200-field schema, 32 records in each batch) against 4.5 for the rest by wall time.
The dwell pass deleted 27 to 44 blobs a second against 4.7 to 5.2. The flush of a
batch took 0.5 to 2.2 s, depending on what else the host did.

**A writer that comes and goes** (one write every 3 s for 40 s, quiet period 60 s).
The janitor drained 26 records a second before the writer, 1.6 a second while the
writer wrote (rests of 14 s), 2.7 a second for the rest of the quiet period, and
24 a second after it. With a quiet period of 20 s the figures were 26, 1.6, 3.2 and 21.

**One sparse writer** (one queued write every 3 s, a backlog that lasts the whole
window of 120 s for the janitor and 90 s for the pass). The time of one write in ms,
p50 / p95:

| | main, drain on | this change, drain on | this change, drain off |
|---|---|---|---|
| Janitor, RAM disk | 32 / 44 | 32 / 42 | 29 / 40 |
| Janitor, system disk | 319 / 908 | 370 / 1,421 | 336 / 586 |
| Dwell pass, RAM disk | 31 / 42 | 33 / 43 | 32 / 43 |
| Dwell pass, system disk | 323 / 649 | 294 / 926 | 347 / 485 |

On the RAM disk the drain costs a sparse writer nothing that the test can see (p50
+10 percent and p95 +7 percent for the janitor, +5 and 0 for the pass). On the system
disk the p50 is within the noise of the host (the same build gave 211 to 400 ms from
one set to the next). The p95 is about twice as high as with the drain off, for main
and for this change alike: the drain keeps writers waiting a tenth of the time, so about
one write in ten meets a sealed batch and waits for its flush (0.5 to 2 s on that disk).
The drain speed in the same runs was the same as main (janitor: 3.3 to 3.9 records a
second for main and 3.6 to 3.9 for this change on the system disk, 7.0 to 7.7 and 6.5
to 7.4 on the RAM disk). A smaller `..._DUTY_PCT` lowers that share and slows the
drain. Without the quiet period, a writer of this kind found the user at full speed
between two writes, and nearly every write of it met a sealed batch.

**Four continuous writers.** With the drain on, this change drains faster than main
and costs the writers about the same:

| | main, drain on | this change, drain on |
|---|---|---|
| Janitor, records a second, RAM disk | 2.7 to 6.4 | 8.5 to 11.9 (1.9 to 3.1 times) |
| Dwell pass, blobs a second, RAM disk | 12.8 to 16.6 | 20.5 to 28.2 (1.6 to 1.7 times) |
| Janitor, records a second, busy system disk | 2.1 to 3.2 | 2.4 to 3.2 (equal) |
| Writer p50 against drain off, janitor / pass, RAM disk | 0 / +2 percent | +2 / -2 percent |
| Writer p95 against drain off, janitor / pass, RAM disk | +4 / +1 percent | +3 / +9 percent |
| Writer p95 against drain off, janitor, busy system disk | +24 percent | +30 percent |

The factor is above 1 because the
meter counts the time that a batch holds the gate and waits for its flush, and it leaves
out the wait for the gate itself. With writers that keep the gate busy, the rest is then
shorter than the cost to the writers. Another run on a quiet disk, with fewer nodes at the
same time, gave 2.1 to 2.5 times for both users (janitor 24 to 27 against 10.6 to 15.1
records a second, pass 29 to 30 against 12 to 14.5 blobs a second). On a busy disk the
factor is 1.

Why not the hold of the gate alone: that was tried first (a rest of 0.1 to 0.3 s).
It drained 9 to 11 times as fast, and it raised the median latency of a test writer
by 33 to 60 percent while the janitor released records (3 pairs of nodes, one with
the janitor and one without, on the same host; the same test gave a median change of
+5 percent, from -2 to +18 percent, for the wall-time rest). The flush is the larger
part of the time that the writers wait, and the gate hold leaves it out.

**The molecule drain** (`docs/lastdb-molecule-drain.md`) uses the same meter, watch, quiet
period and rest as the janitor and the pass. It rested for a share of the wall time of each step
before. Measured on synthetic homes with the default knobs: a clone of one home with a schema of
2,500 keys (10,000 tips) dropped, the build before and the build after at the same time on one host,
3 pairs for each case (a schema of 10,000 keys, 40,000 tips, for the sparse writer on the RAM disk).

| Tips a second | before | after |
|---|---|---|
| No other writer, RAM disk (28 to 38 s) | 261 to 283 | 330 to 354 |
| No other writer, system disk | 14 to 20 (487 to 717 s) | 81 to 102 (98 to 124 s) |
| 4 continuous writers, RAM disk | 40 to 46 (217 to 252 s) | 216 to 219 (46 s) |
| 4 continuous writers, busy system disk | 8 to 13 (one of 3 not done in 1,200 s) | 14 to 21 (478 to 726 s) |
| One sparse writer, RAM disk | 298 to 303 | 364 to 378 |
| One sparse writer, system disk | 48 to 84 | 76 to 100 |

The drop that makes the candidates is a write scope of another task, so the first 60 s of a drain
with no other writer go at the paced speed (38 to 50 tips a second on the system disk) and the rest
at full speed (120 to 240). With four continuous writers the drain is 5 times as fast on the RAM
disk and the writers wait the same: the p50 of a test writer with the drain on was 106 to 116 ms
against 101 to 124 ms before the drop (3 pairs), and the p95 672 to 1,123 ms against 705 to 1,337 ms.
On the busy system disk (a write took 3 to 5 s there, whatever the drain did) the p50 was 3,898 to
5,324 ms against 3,237 to 4,483 ms.

One sparse writer, one write every 3 s, from the start; the drain began after 150 s and the
writer went on for 150 s after it ended. The time of one write in ms, p50 / p95 (the range of 3
pairs):

| | before the drop | drain on | after the drain |
|---|---|---|---|
| RAM disk, before build | 38 to 46 / 52 to 74 | 42 to 47 / 53 to 136 | 42 to 52 / 45 to 72 |
| RAM disk, after build | 38 to 47 / 53 to 157 | 42 to 48 / 86 to 130 | 42 to 52 / 46 to 69 |
| System disk, before build | 458 to 482 / 495 to 757 | 482 to 729 / 758 to 1,561 | 436 to 592 / 514 to 839 |
| System disk, after build | 450 to 462 / 498 to 719 | 472 to 636 / 937 to 1,380 | 478 to 602 / 737 to 1,296 |

On the RAM disk the p50 with the drain on is within the noise of the writer without it (the two
windows without the drain differ by as much). The p95 is higher with the drain on, for both builds:
about one write in ten meets a sealed batch. On the system disk the p50 moved by +2 to +41 percent
(before build: +1 to +59) while the windows without the drain differed by up to 30 percent from
each other, and the p95 was about twice as high, for both builds. The drain does not change what a
sparse writer sees compared with the build before.

A poll of a read-only verb every 30 s (drain of 10,000 tips on the system disk, no other writer, 1
pair for each verb, the build with the pacing change but with empty scopes counted against the
final build): `lastdb db verify-refs` kept the first build at 24 and 36 tips a second (418 and 279
s) and the final build at 87 and 111 (115 and 90 s). `lastdb status`, `lastdb liveness explain` and
`lastdb db molecule-drain` (no molecule) left both builds at full speed after the first 60 s (318 to
544 tips a second), so they open no write scope at all. `lastdb db inventory` kept both builds at 58
to 62 tips a second, because it writes rows and counts as a writer.

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
- A process with another lifetime than the node that wrote the entries. The slice
  follows `LASTDB_CONFLICT_RETENTION_SECS`. `verify-refs` asks the daemon, so it
  uses the daemon's slice. `recount` is a process of its own, and the janitor
  records the window of the node in one row (`crb:v1\0params`, once at its start
  and when the window changes). The recount reads that row and uses the recorded
  window and slice for its survey and its repair, and it prints a note when its own
  `LASTDB_CONFLICT_RETENTION_SECS` differs. A home whose janitor never ran has no
  row, and the recount then uses its own environment.
- A bucket older than one lifetime and one slice. The net looks back one lifetime
  (a node that was down for longer than that, with a row lost on top, is rare),
  and the survey of `verify-refs` and `recount` uses the same range. Such a
  bucket is not found as a bucket. A conflict record in it shows as
  `record_without_queue_entry`, and `recount --execute` gives it a new queue
  entry. The old entry stays as a row that no directory lists: it holds no
  reference, so it is garbage and not a leak of atoms.
- A drain marker is not older than one lifetime, so the range above covers it for
  its whole life. Every record of an erased slot has its own queue entry, due one
  window after the record was written (the window of the node at that time), so each
  record ends at most that window after its own write, and the ordinary lane releases
  it. The marker only makes the janitor release the records of the slot sooner. A
  marker older than its lifetime is deleted without a drain (the log says `dropped
  drain markers older than the retention window`). The marker records the window of
  the node that wrote it (`window_nanos`), and the janitor uses the longer of that
  window and its own. A shorter `LASTDB_CONFLICT_RETENTION_SECS` at a later start does
  not drop a marker that an earlier start wrote. **The order that is not covered:**
  the window is cut first (from 120 s to 30 s, for example) and the purge comes
  after it. The marker then records the new, short window, and the janitor deletes
  it when it is older than that, also when it has not drained it yet (a backlog of
  markers that the janitor reaches under its duty pacing). The records of the slot
  were written under the old window. They end with their own queue entries, up to the
  old window after their write, and not at once. A review measured it with a window cut
  from 120 s to 30 s and 1,480 markers: the janitor dropped the markers and the
  records ended about 92 s after the purge (before S8b the markers drained at once).
  Nothing is kept longer than the old window. This is a known limit and it is not fixed
  here: the marker would need the latest expiry of the records of its slot, and an erase
  reads only a part of them (at most 100 records and 256 slots). A marker from an
  older build has no recorded window. The janitor does not delete it for its age; it
  drains it. Two faults remain out of reach, and both
  hold no reference: a marker bucket that lost its row while the janitor was
  stopped for longer than a window, and a marker the janitor could not see.
- A bucket that holds only deleted keys is not a fault, and the net leaves it.

## What can go wrong and what catches it

The directory row of a bucket can be missing in four ways. Each has a
prevention, a detection and a repair. The dwell queue of the file blobs inherits
all of them.

| Way | Prevented by | Detected by | Healed by |
|---|---|---|---|
| A writer adds an entry without the row (code outside `TimeQueue::rows`). | The entry and the row are one `TimeQueue::rows` call, in the batch of the work, through the planner. The writer lint finds a writer that bypasses the planner. | `verify-refs`: `queue_entry_without_directory` for a record entry. The net counts `buckets_repaired`. | The net registers the bucket within one sweep. `recount --execute` writes the row. |
| A crash between the write of the entry and the write of the row. | Excluded: one batch holds both, and a crash keeps whole batches. | (not possible) | |
| A retirement races a writer: the reader decided that the bucket was empty, and a writer added an entry. | Every entry rewrites the row with a new token, and the retirement is a guarded delete that compares the raw bytes in the write gate. A writer that came after the reader read the row changes the token, so the delete leaves the row. | As above, if the guard were wrong. | As above. |
| An external delete of the row, or a partial restore that brought the entries and not the row. | Nothing in the queue can prevent it. | `verify-refs` (a record whose queue entry has no row) and the net (any entry, also a drain marker, which lives at most one window). | The net, and `recount --execute`. |

A fifth, quieter fault belongs to the same chain. A conflict record whose queue
entry is missing is never released by the janitor, because the janitor finds
records through the queue. `verify-refs` reports it as
`record_without_queue_entry`, and `recount --execute` writes the queue entry
again (see below). The net cannot find it: there is no entry to probe.

### `verify-refs` and `recount`

The verifier cannot read the queue entry of a record by key. The key holds the
expiry, and the record does not. The slot directory entry names the queue entry,
but the key of the slot directory entry needs the storage form of the key of the
record, and that needs the key codec of its molecule, which a node loads with
its schema. A verifier that derived the key from the record gave wrong answers
on a node that had just started. So the verifier and the recount compare sets.

- The records phase keeps the id of each counted conflict record (the first 128
  bits of the id that the queue keys name; 16 bytes each, at most `--max-atoms`).
- A `queue` phase surveys the queue (`time_queue/survey.rs`). It reads the
  directory, walks the entries of every listed bucket, and also walks the slices
  that the arithmetic lists, have no row, and hold a live key. It keeps the id of
  each entry. A bucket that holds entries and has no row after the survey read
  its row again is a fault: each of its entries is a
  `queue_entry_without_directory`.
- A record whose id no entry has is a suspect. A last phase (`suspects`) reads
  the suspects again. A record that is still there is a
  `record_without_queue_entry`. A record that a release deleted in the meantime
  is gone, and is not reported. A record that was written after the records
  phase is not a suspect. So a writer cannot cause a finding.

The status line `Verify refs` shows `queue_rows` (the sum of the two kinds).

`lastdb db recount --execute` (a stopped home) surveys the queue, walks the
records once for each scope, and writes the missing rows again through the
planner: a bucket gets its row; a record gets its queue entry back and the row of
its bucket. The slot directory entry of the record, when it still exists, holds
the whole original entry (the queue key, the slot key, the creation time), so one
walk of the slot directory finds it and the entry goes back at its own key, with
the slot directory entry named: the release of the record then deletes that slot
directory entry too, and a record past its window is released at once. A record
with no slot directory entry gets an entry that expires one window from now (the
original expiry is not known), with no slot directory entry of its own, for the
reason above. The window is the one that the node recorded. A second run finds
nothing to change.

## Limits

- The reads of a request are bounded by 4,096 metadata pages as well as by keys
  (see "The problem"). A request that rewrites about 2,000 conflicting fields
  (a 1,980-field rewrite, 10 keys of 198 fields) is near that budget: on a home
  that grew, it is refused with `read_resource_limit` (503) after about 11 to 13
  such requests, also with no release at all. The queue adds rows to such a
  request but no read. Replay batches (a mutation log that brings many records
  at once) were not run, so their margin is not shown.
- The storage limits one publication to 65,536 dependency parts (`dependency
  capacity`; see `docs/lastdb-storage-parts.md`). A durable batch adds
  about 5 to 13 parts, and nothing merged them: a home refused writes after
  about 20,000 to 22,000 mutations in a row, with or without a queue. This is a
  limit of the storage, not of the queue. The node now runs maintenance when
  the count passes 4,096 parts, so the janitor and the retirements no longer
  feed the limit. Every publication still writes the whole receipt, so a queue
  should keep its release batch large and its duty share low.
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
2. The entry value, and the rows of a new entry (`TimeQueue::rows`) in the batch
   of the work that creates the entry.
3. The release of one due entry, through the planner, with the delete of the
   entry in the same batch.
4. A cursor holder like `RetentionCursor` that alternates its kinds of work, and
   that checks its unfinished long work before it asks for a retirement token.
5. A call of the net (`TimeQueue::net_slices` and `TimeQueue::mend_slices`) from
   its janitor, once at the start and then now and then.
6. A survey of its queue in the verifier and the recount (`QueueSurvey`, used
   from `counter/verify/run.rs` and `counter/recount/retention.rs`), the ids of
   its work items to compare with the entries, and a repair that writes the
   missing rows again (`retention/repair.rs`).
7. The key family names in the key validation (`storage/current_keys.rs`),
   the cutover classes (`mini_cutover/main_keys.rs`), the capture skip list
   (`sync/policy.rs`) and the inventory classes.

It must not read an entry hash with a plain range read and a limit.

# TTL sweep: verb, pacing and knobs

The node-local TTL sweep (`lastdb_node/src/ops/ttl_sweep.rs`) erases the expired keys of every
schema that has a retention policy. The age rule is in
`fold_db/crates/core/src/schema/schema_types.rs` (`SchemaRetentionPolicy`) and in
`lastdb schema-retention set --help`. This page covers what the sweep does with the keys it
selected, the two verbs, what each verb leaves on disk, how the sweep paces itself, the guard
for rewritten keys, the measured cost, and the knobs.

The default verb is `purge`. It uses the atom counter.
The same batch frees each body whose last reference goes.
`delete` is opt-in because it leaves barrier rows.
The owner can remove older barriers through an [explicit stopped-home replay cutoff](delete-replay-cutoff.md).

## Schema shape that a TTL needs

A TTL deletes records for good. The sweep finds the expired keys without a scan only when the schema
has the right shape (`docs/app-manifest-retention.md`).

- A Range or HashRange schema needs a range key that starts with 20 digits of unix seconds, zero-padded.
  A key that does not start with 20 ASCII digits has no entry time, and the sweep never expires it.
- Use a day-slice hash for a HashRange log (for example the day of the entry). The sweep lists the
  expired keys hash by hash, in the hashes that it has seen written since the policy exists. A
  day-slice hash keeps each hash small, so one listing reads few keys.
- Set the policy before the first write, or explicitly backfill existing personal Hash/HashRange records
  with `lastdb schema-retention backfill --schema <name> --confirm-expire-existing`.
  The backfill keeps the original Hash tip ages and registers untouched HashRange hashes.
  Retired Single/Range layouts do not support this backfill.

CAUTION: Backfill can make an existing record expire at once.
The owner must use `--confirm-expire-existing`.
For Hash records, each page reads the current tips and writes their original clocks through the normal age writer.
This also corrects an existing age that an older program recorded from replay arrival time.
The page can move that incorrect age backwards. A newer current tip still keeps its actual newer age.
The native write scope covers the tip reads and age changes. A repeat page uses the current tips again.
The same batch removes the old forward age entry and replaces the reverse age entry.
HashRange backfill registers hashes; the range key still defines the age.

## What a pass does

1. The sampler tick starts at most one pass. A pass skips while a backup cut is held or local
   writes are paused.
2. The pass keeps schema metadata and exclusive cursors. It fetches one key page for the schema
   that it serves, then erases that page before it fetches another. Hash and Single use a bounded
   age-entry read. Range and HashRange use the normal keyed page reader. A page holds at most the
   configured batch size: 16 keys by default, 64 at most, across all schemas combined.
   Schema and observed-hash metadata still scale with the configured schemas and hashes.
   An exclusive cursor advances across stale age entries and keys without a valid range age.
   Only an empty physical page ends an interval. A short page does not end it.
   The next pass starts from the oldest remaining key; no offset survives an erase.
3. Each round serves one page from each schema, in schema name order. A size refusal splits the
   current page into smaller batches. A failed schema releases its page and retains only its
   outstanding count. Other schemas continue. The status does not call a failed or cut pass complete.
4. Before each batch the sweep re-checks its keys, inside the batch admission:
   - Hash and Single keys take their age from the write-time index. One batched read drops every
     key that a client rewrote after the pass listed it. A key with no index row was erased by
     someone else and is dropped too.
   - Range and HashRange keys take their age from the key, so a rewrite does not change it.
   - A policy that is gone ends the schema for this pass. A policy with a longer TTL protects the
     keys that it protects now.
   The log line `ttl sweep re-check dropped keys that are no longer expired` names the count.
5. The erase spares a Hash or Single key whose tip is newer than the cutoff (see The write-time
   guard).
6. The sweep holds the local mutation admission for one batch at a time, and rests after each
   batch (below). The pass stops between batches when the daemon stops, when a backup cut starts,
   or when local writes pause. The next tick lists the rest again.
7. A `purge` refuses a batch that names a key which is already gone, for example a key that a
   client, a peer or another purger removed after the re-check. After such a refusal the sweep
   asks which keys of the batch are absent, counts them as done, drops them, and sends the rest.
   It repeats this until the batch is sent or empty. Each refusal that names an absent key makes
   the batch smaller, so the loop ends. A key that another purger removed is never a failure of the
   sweep. The count is `missing_dropped` in the status. The log line is `ttl sweep drops keys that
   are already gone and sends the rest of the batch`. A refusal that names no absent key is a real
   error and ends the schema for this pass (step 8).
8. A listing error or a batch error ends that schema for this pass. The sweep logs it once with
   the schema name, counts it, and goes on with the other schemas. A pass in which no schema
   succeeded counts as failed. After three failed passes in a row the sweep waits before each next
   pass: 5 minutes, then 10, 20 and 40 minutes, and 1 hour at most for every pass after that. It
   writes one `ttl sweep waits before the next pass` line at the start of each wait. The `Local
   retention:` line shows `failures`, and a `sweep_waits:` line shows `next_attempt_unix_secs`
   while the wait lasts. A pass in which one schema succeeded ends the wait and resets the count.
   The sweep never stops for the life of the process, so a restart is not needed to resume it.

## Verbs

| `LASTDB_TTL_SWEEP_VERB` | Meaning |
|---|---|
| `purge` (default) | The hard-erasure verb. It deletes the tips of the key. The atom counter deletes each body whose last tip goes, in the same batch. It writes no delete barrier. |
| `delete` | The ordinary delete a client uses. It removes the tips and writes the delete barrier. The atom counter deletes each body whose last tip goes, in the same batch. |

Both verbs release the kept conflict records of the slots they remove, in the same batch as the
tip deletes (the slot directory, `docs/lastdb-atom-counter.md`, section Conflict records and their
retention). A kept loser whose last reference goes is deleted in that batch. Both verbs leave one
`mgd:v1:` record for each field of each key (see Records that stay).

Any other value logs one warning at the first sampler tick and uses `purge`. The delete ledger
(`lastdb db delete-ledger`) names the caller of each erase: `ttl-sweep-purge` or `ttl-sweep-delete`.

Both verbs free the atom bodies at once. The counter record of each atom decides
(`docs/lastdb-atom-counter.md`). No audit and no grace window stand between the tip delete and the
body delete.

## Why `purge` is the default

`delete` leaves `rdel:v2:` barrier rows for each removed molecule key.
They remain until the owner selects an explicit replay cutoff and executes stopped-home cleanup.
The older build below wrote two barriers for each key and field, about 3,170 B in total.
A log that expires 100,000 records per day on a 3-field schema retained about
28.6 GB of barriers in 30 days (measured on a build from before the counter work: 2,000 keys of 3
fields gave 12,000 rows and about 19 MB, and a run of 400 keys gave the same bytes per key, so the
growth is linear). Compaction removed none of them. `purge` writes no barrier. Both verbs also leave about 700 B for each key and field of `mgd:v1:` rows
(measured on the same earlier build).

What `delete` buys: the barrier stops an older write from bringing the key back. A write that
a peer made before the sweep and did not yet sync, and the replay of an older log, are the cases.
With `purge`, an older write can restore the key. A Range or HashRange key takes its age from the key.
A Hash or Single key takes its age from the newest current tip's original write time.
The writer reads the final tips by key inside the native write scope and records their clocks in the same commit.
A replay that loses to a current tip cannot refresh that clock.
A replay that restores an expired key keeps its old age, so the next pass can erase it again.

A TTL `delete` barrier covers only writes before its signed age cutoff.
Replay uses that cutoff with the original author order, as the normal tip reader does.
A write at or after the cutoff remains eligible even when it predates the sweep's author clock.
A Delete that spares any current field reports that key as spared.
The key keeps its TTL entry and its LastSeek record; cleanup removes those entries only for erased keys.

The default remains `purge`. No timer infers a maximum offline time for a device.
The owner selects the replay cutoff explicitly.
After cleanup, an old or unclocked replay refuses and the peer needs a current snapshot.
The [cleanup guide](delete-replay-cutoff.md) gives the peer upgrade order and the older-program refusal rule.

## Pacing

A sweep batch is one erase request. It retains its durability fence.
The sweep measures the time its write scopes keep other writers waiting.
That measure includes a sealed flush wait and excludes reads before the write gate.
The rest is `blocked * (100 - duty) / duty`, subject to the configured minimum and maximum.

The TTL sweep uses the same task-local writer-cost measure as the other maintenance jobs.
A sticky writer watch retains the duty share for sparse writers.
`LASTDB_MAINTENANCE_QUIET_SECS` defaults to 60 seconds.
When no other writer remains, the rest returns to its configured minimum.
The watch persists between passes. A long rest ends early after the quiet period.

Private janitor, dwell, and drain batches can share the bounded collection window.
They still return after durability. Their ordinary measured cost includes the gate and shared flush holds.
See `docs/lastdb-concurrent-compaction.md` and `docs/lastdb-time-queue.md`.

## Historical measured cost

These measurements precede writer-cost TTL pacing and private background collection.

Synthetic fresh home, development-release build, `EXEMEM_ENV=dev`, default knobs (batch 16,
duty 25, minimum rest 25 ms), verb `purge`. One HashRange schema with 6 fields and 2,400 rows
(2,100 expired) and 150-byte bodies. Two writers: one on the swept schema and one on a second
schema (one small durable write every 100 ms each).

All runs used one shared development host (macOS, local SSD) that other builds and runs also used,
so the load is part of each result. "Load" is the one-minute load average that the run recorded.
Three runs are in the table.

- `main`: the build before the counter (`main` at `8ed44f56`), host load 4 to 5.
- `reference`: the build at `a9eb17526`, which is the counter lifecycle from before the pacing change of
  the janitor and the dwell pass, on a quieter host (load 3 to 4).
- `2026-10-21`: this page's build (`main` with the pacing change of the janitor and the dwell
  pass), on a busy host: load 2.5 to 9, with other builds and runs on the disk. A second node on
  the same host ran the same two writers on the same 2,400 rows with `LASTDB_TTL_SWEEP=0`
  (`no sweep`). It shows what the host does to the writers without a sweep: their median write
  took 2.4 s, against 0.4 s in the reference run. The run was cut after 20 minutes.

| | `main` | reference | 2026-10-21 | no sweep (2026-10-21) |
|---|---|---|---|---|
| Keys erased | 0 in 782 s | 2,100 in 497 s (4.22 keys per second, 132 batches) | 1,152 in 1,203 s (0.96 keys per second, 72 batches) | - |
| Sweep result | each pass fails: `read resource limit exceeded`, because the old purge read the whole schema | every pass succeeds | every pass succeeds | - |
| Time of one batch of 16 keys | no batch finished | median 0.67 s, shortest 0.24 s, longest 3.13 s (131 full batches) | median 4.15 s, shortest 0.76 s, longest 7.79 s (72 full batches) | - |
| Hold of the native write gate for one erase request | not measured | median 45 ms, longest 123 ms | median 48 ms, longest 109 ms | - |
| Writes to the swept schema | p50 470 ms, p95 1,124 ms, longest 16.6 s | p50 423 ms, p95 994 ms, longest 2.4 s | p50 2,615 ms, p95 5,308 ms, longest 9.1 s (399 writes) | p50 2,483 ms, p95 4,418 ms, longest 6.9 s (143 writes in 407 s) |
| Writes to the second schema | p50 446 ms, p95 1,146 ms, longest 2.9 s | p50 404 ms, p95 1,019 ms, longest 2.1 s | p50 2,538 ms, p95 5,289 ms, longest 7.3 s (417 writes) | p50 2,379 ms, p95 4,096 ms, longest 6.1 s (148 writes in 407 s) |
| A client `purge` of 16 keys of the swept schema | HTTP 503 after 73.9 s | HTTP 200 after 0.61 s | HTTP 200 after 6.7 s, during the sweep | HTTP 200 after 3.8 s |

A client `purge` of 16 keys, on a fresh home with no retention policy (no sweep), 3 requests
in a row on the same busy host: 1,000 rows 1.8, 2.6 and 2.2 s; 2,400 rows 1.6, 2.2 and 2.4 s. All
answered HTTP 200. The gate hold of the erase request was 47 ms (median) on 1,000 rows and 37 ms
on 2,400. The cost follows the 16 keys, not the rows of the schema.

The time of a batch follows the flush of the host: the gate hold of an erase request was the same
in both runs (about 48 ms), and the batch took six times as long on the busy host. The sweep is
paced by the time of its batches (duty 25), so it erased a quarter as many keys a second.

The write latencies are mostly the durable wait of each writer (the flush interval is 500 ms).
A sweep batch holds the write gate for about 50 ms, and it also seals the write queue: a writer
that arrives while a batch is open waits for the batch to be durable, though the gate is free.
In the 2026-10-21 run the writers were 5 to 7 percent slower at the median and 20 to 30 percent
slower at p95 than the same writers on the node with no sweep. The two nodes ran in different
windows of a host that changed, so read these as a bound and not as a measurement. The duty of
25 percent means that writers wait for the sweep about a quarter of the time.

A `purge` batch reads 9 to 12 keys for each tip it removes (the target tips, the protein peers, the
counters and the body records), so its cost follows the number of keys and fields of the batch. It
never follows the size of the schema. A purge of 64 keys of a 24-field schema (1,536 tips,
6,046 operations) held the gate for 215 to 255 ms and took 0.66 to 1.6 s.

## Batch size and the request limit

One erase request stages all its chunks of 64 keys in one native batch. A batch holds at most
16,384 operations (4,096 on a home from an older build), and the reads of the request share a
budget of 40,000 examined keys. A tip costs at most 4 operations in a `purge` (the tip delete, the
`mgd` marker, and the body and counter deletes) and 5 in a `delete` (one barrier row more). It costs
at most 12 and 13 examined keys. A chunk adds `F + 8` of each for F fields. The node counts the
request before it takes the write gate (`fold_db_core/purge/budget.rs`). A client request that is
too big returns HTTP 400 at once, with the key count that fits, for example: "this purge of 200 keys
of 24 fields needs up to 19328 storage operations and 57728 reads; one request holds at most 16384
operations and 40000 reads; send at most 138 keys of 24 fields in one request".

| Schema | `purge`, fresh home | `delete`, fresh home | `purge`, older home | `delete`, older home |
|---|---|---|---|---|
| 6 fields | 553 keys | 511 keys | 168 keys | 135 keys |
| 24 fields | 138 keys | 128 keys | 42 keys | 33 keys |
| 48 fields | 69 keys | 64 keys | 21 keys | 16 keys |
| 300 fields | 11 keys | 10 keys | 3 keys | 2 keys |

The sweep cuts its batch to the smaller of the pace (`LASTDB_TTL_SWEEP_BATCH`, default 16) and the
key count that fits its schema. The bound is an estimate, so a batch that still hits a size limit
changed nothing: the sweep halves it for that schema and retries, and it does not skip the schema.
The default of 16 fits a schema of up to about 200 fields on a fresh home and about 60 on an older
home. A schema of 300 fields gets batches of 11 keys on a fresh home (measured: 13,487 operations in
the largest batch, no failure).

## The write-time guard (Hash and Single schemas)

The re-check in step 4 reads the stored age for the key.
A native write records the final tips' original clocks in the same durable commit, before its ack.
The erase still checks the tips because a write can occur after selection and before admission.

The sweep sends the cutoff of the batch with each erase of a Hash or Single key. Each verb
compares the write time of the tip itself with the cutoff:

- `purge`: the erase runs inside the native write gate. It reads the tip on disk of every field
  of each key. A key with a tip at or after the cutoff keeps all its tips, and the other keys of
  the batch go on. No writer can acknowledge a write between the read and the batch.
- `delete`: the cutoff is a field of the local delete barrier. The barrier test that runs under
  the tip lock spares a tip written at or after the cutoff. It works for each field, so a rewrite
  of one field spares only that field.

The signed TTL Delete mutation includes its cutoff. Replay preserves that cutoff.
If the Delete spares any current field, the key keeps its TTL and LastSeek entries.
Both installed names of an app schema use the same retention anchor.
Range and HashRange keys take their age from the key. A rewrite does not change it.
An old incorrect Hash age needs explicit backfill or a later nonduplicate write.

## Records that stay after a TTL erase

- `mgd:v1:` is a sparse delete marker. It holds the removed tip and stops a read from showing a
  slot of an immutable base generation of the molecule that the delete removed from the live rows
  (`db_operations/atom_store/molecules/delete.rs`, `generation/slots.rs`). The read looks at it
  only for a molecule that has an active generation (`generation/merge.rs`). Both verbs write it,
  because both use the same slot removal. It is local to the device (`sync/policy.rs`).
  A whole-molecule drain removes its markers. The owner can also remove redundant markers
  from a molecule with no active generation through bounded cleanup pages (below).
- `rdel:v2:` is a delete barrier. It holds the stamp of the delete. It makes a later local write,
  a peer write that is replayed, and an older tip from a restore lose against the delete when
  their clock is not newer (`atom/delete_barrier.rs`, `apply_gate.rs`, `sync/engine/replay`). Only
  `delete` writes it. The current codec uses the caller's exact molecule identity and one key
  (`atom/molecule_key_codec.rs`). It does not substitute hex or base64url spellings.
  Earlier two-spelling measurements describe historical source. Each device builds its own
  barrier from the replayed erase (`sync/policy.rs`).
  Explicit stopped-home cleanup can remove barriers at or before its durable replay cutoff.
  It does not remove historical backup bytes or advance the cutoff on a timer.
- `lastdb db inventory` classifies generation delete markers and delete barriers separately.

### Bounded cleanup of redundant delete markers

Use `lastdb db cleanup-delete-markers --molecule <identity> --json` to preview one page.
Add `--scope <scope>` for a scoped molecule. Omit it for the personal molecule.
Add `--execute` to delete the eligible markers in that page.
Use the returned `next_cursor` as `--after-key` for the next page.
The cursor is opaque text and remains valid after its key is deleted.
An empty page returns `complete: true`. A short nonempty page still needs another call.
`--max-keys` defaults to 128 and accepts 1 to 256.

The command uses the owner socket and native storage.
Each call needs its own native request scope; cleanup refuses an inherited write scope.
It changes no tips, atom counters, atom bodies, conflict records, or Delete barriers.
It returns `blocked: active_generation` while an immutable generation remains selected.
It returns `blocked: generation_preparation` while a generation cut remains in flight.
These responses retain every marker and do not claim completion.
A shared lock covers each generation from its first source read through activation and reclamation.
Cleanup tries the exclusive lock for that molecule after native admission. It never waits for that lock.
Generation preparation also tries its shared lock without a wait.
If cleanup still holds that lock, the complete write refuses with a retryable HTTP 503.
The native request drops its private stages and releases admission before it returns that error.
The client can retry after cleanup commits. No write waits for cleanup while it holds native admission.
Normal keyed writes take no additional lock.
Cleanup checks the generation pointer and deletes one page within the native write scope.
A future generation therefore reads the state after cleanup, or keeps cleanup blocked until its activation ends.
Each page commits before the response. Cancellation or restart can repeat the page safely.
Concurrent erases can add markers before the cursor. A new pass can collect those markers.
Compaction can then reclaim the removed metadata bytes.
This is explicit owner maintenance; it adds no automatic cleanup pass.

### Last writer wins

An ordinary Delete uses its author order. A TTL Delete also carries its signed age cutoff.
A write at or after that cutoff remains eligible even if its author clock predates the sweep.
A purge writes no barrier. An older Put can restore the key.
Its original tip clock remains the TTL age, so an expired restored key can expire again.
Core replay evidence does not establish two-device service behavior or cloud readiness.

## Knobs

Only the node environment sets them. A running process cannot change its environment, so a change
needs a restart of the daemon. The sweep reads them at the start of each pass.

| Name | Default | Clamp | Meaning |
|---|---|---|---|
| `LASTDB_TTL_SWEEP` | on | - | Kill switch. `0`, `false`, `no` or `off` stops the sweep. |
| `LASTDB_TTL_SWEEP_BATCH` | 16 | 1 to 64 | Keys erased in one batch. |
| `LASTDB_TTL_SWEEP_DUTY_PCT` | 25 | 1 to 100 | The share of time that the sweep keeps other writers waiting. |
| `LASTDB_TTL_SWEEP_MIN_PAUSE_MS` | 25 | 0 to 600000 | The shortest rest between two batches. |
| `LASTDB_TTL_SWEEP_VERB` | `purge` | `purge`, `delete` | See Verbs. |

A value that does not parse gives the default.

The rest after a batch is `took * (100 - duty) / duty`, at least the minimum rest and at most
10 minutes. The duty share is exact for a batch of up to 200 seconds at the default duty. After a
slower batch the rest is 10 minutes, and the real share is `took / (took + 600 s)`.

A stop waits for the batch in flight. It cannot cancel it. A batch of 16 keys took 0.24 to 3.1 s
on a quiet host and up to 7.8 s on a busy one.

`RUST_LOG=lastdbd::ttl_sweep=debug` prints one line per batch with the phase times.

## Status

`lastdb status` prints the line `last_pass:` below the `Local retention:` line:

| Field | Meaning |
|---|---|
| `peak_buffered_keys` | The largest expired-key page held by the pass. It excludes schema/hash metadata and temporary query response bytes. |
| `finished_backlog` | The last pass erased or dropped every key that it listed, in every schema, with no failed schema. |
| `keys_left` | The keys that the pass listed and had not reached when it ended. |
| `cut` | The pass ended early: the daemon was stopping, a backup cut was held, or local writes were paused. |
| `schemas_failed` | The schemas that this pass skipped. |
| `missing_dropped` | The keys of a `purge` batch that were already gone. The sweep dropped them. |
| `schema_failures_total` | The skipped schemas for the life of the process. |

A pass that ends with keys left writes one `ttl sweep pass ended with listed keys left over`
line. The next pass lists those keys again. `settled` in the `Local retention:` line stays false
until a pass finishes its backlog.

## Active-TTL latency diagnostics

The CI comparison uses 512 durable calls per case, with eight records per call.
One and four clients send the same request size and total work.
The four states are no policy, age tracking, active TTL, and active TTL with full compaction.
The uninterrupted cases supply the latency and throughput comparison.

Separate diagnostic cases fetch the request ring after each 64-call wave.
Each case requires all 512 exact request records and their reported resident stages.
The diagnostic gaps and native instrumentation can change maintenance exposure.
Their throughput does not replace the uninterrupted comparison.

Native timing adds an opaque process-local identifier when its existing opt-in flag is on.
The request record and native scope carry that identifier; no caller value enters the native log.
The scope then joins its physical commit by sequence and enclosed time.
This separates request queue time, native admission, private preparation, and durable completion.
A physical commit can contain foreground writes and TTL work. The report counts that commit once.
It does not assign the whole shared commit cost to every request.

An older baseline can omit these identifiers. Its native association remains unreported.
The candidate requires complete native association. Missing fields never mean measured zero.
The collector reads the current daemon log and its numeric rotated files in the same output directory.
Rotation does not reduce the required request, scope, sequence or physical-commit coverage.
Missing or ambiguous evidence still fails the campaign.

Private v1 metadata writes use unbuffered private files.
The same-runner before/after comparison found no material latency gain from an eight-page write buffer.
That buffer was removed. Every completed part retains its file barrier and publication barriers.
A private metadata zero-extension experiment reduced issued append bytes but increased request latency.
The full-page append implementation is restored. File, directory and root durability barriers remain.
The accepted largest-wait reduction remains open. Native preparation and physical publication need further diagnosis.
The write queue preserves its sealed prefix until the complete durable result; no acceptance or ordering gate changes.
Opt-in diagnostics separate directory lookups, locator tree edits, and the final directory edit.
Reference resolution, format checksum, and reserved-zero validation timings are nested within those phases.
Reserved-zero validation retains its original early refusal. The provisional bitwise-OR change was removed.
The comparison with identical diagnostics did not establish a consistent active-TTL wait reduction.
Opt-in diagnostics retain its time and validated byte count. Short calls round down to integer microseconds.
Metadata completion now separates private-part sync, object-directory sync, namespace creation, and block-map finalization.
These numeric timings use the existing diagnostic opt-in. Every I/O call, check, error, and barrier keeps its original order.
Checksums and full-page output remain unchanged. This is diagnosis; no new wait reduction is established.
These intervals overlap. Do not add them or treat a diagnostic interval as a per-request disk wait.

The next candidate removes one heap allocation for each existing key descriptor in a leaf edit.
It borrows the checked input page's descriptor bytes until that leaf's output completes.
Inserted keys retain owned descriptors. The output separator still owns its descriptor.
The same split rules, bytes, checksums, full-page writes, and durability barriers remain.
The opt-in `metadata_borrowed_descriptors` count reports this path. A latency gain requires fresh CI measurements.

The v1 maintenance worker releases home ownership before its initial pause.
Format v2 uses background run merges. The explicit v1 pack-compaction route returns a clear HTTP 400 refusal on v2.

A concurrent replacement publication invalidates an older native cleanup plan.
If its obsolete inventory then loses a receipt, cleanup reports deferred work only after it verifies its retained source receipt.
A missing receipt under a valid deletion epoch remains an error.

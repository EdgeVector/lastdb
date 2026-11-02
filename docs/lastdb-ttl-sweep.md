# TTL sweep: verb, pacing and knobs

The node-local TTL sweep (`lastdb_node/src/jobs/ttl_sweep.rs`) erases the expired records of every
schema that has a retention policy. The age rule is in
`fold_db/crates/core/src/schema/schema_types.rs` (`SchemaRetentionPolicy`) and in
`lastdb schema-retention set --help`. This page covers how the sweep finds the expired keys, the
two verbs, what each verb leaves on disk, the guard for rewritten keys, the batch size, the
pacing, the knobs and the status.

A format 8 home stores one record for each key (`rk:`). A TTL erase changes that record into a
tombstone. It does not remove the record.

The default verb is `purge`. It also releases the field history and the kept conflict records of
the record. `delete` keeps them until their own windows end.
The owner can raise the home-wide [replay cutoff](delete-replay-cutoff.md) on a stopped home.

## Schema shape that a TTL needs

A TTL deletes records for good. The sweep finds the expired keys without a scan only when the schema
has the right shape (`docs/app-manifest-retention.md`). A format 8 home has only Hash and HashRange
schemas.

- A HashRange schema needs a range key that starts with 20 digits of unix seconds, zero-padded.
  A key that does not start with 20 ASCII digits has no entry time, and the sweep never expires it.
- Use a day-slice hash for a HashRange log (for example the day of the entry). The sweep lists the
  expired keys hash by hash, in the hashes that it has seen written since the policy exists. A
  day-slice hash keeps each hash small, so one listing reads few keys.
- A Hash schema ages each record by its write time: the newest field stamp of the record. Each
  write records that age in the age index of the schema in the same durable commit.
- Set the policy before the first write, or explicitly backfill existing personal Hash/HashRange records
  with `lastdb schema-retention backfill --schema <name> --confirm-expire-existing`.

CAUTION: Backfill can make an existing record expire at once.
The owner must use `--confirm-expire-existing`.
For a Hash schema, each backfill page reads the current records by key and writes the write time
of each live record as its age. This also corrects an age that an older program recorded from
replay arrival time. The page can move that incorrect age backwards. A newer write keeps its
actual newer age. The native write scope covers the record reads and the age changes.
For a HashRange schema, each page registers the hashes; the range key still gives the age.
A page holds at most 64 keys, and fewer for a wide schema (Batch size and the request limit).

## What a pass does

1. The sampler tick starts at most one pass. Each erase batch uses the normal local mutation
   admission. A snapshot cut keeps that admission until the cut completes.
2. The pass keeps schema metadata and exclusive cursors. It fetches one key page for the schema
   that it serves, then erases that page before it fetches another.
   - A Hash schema reads the age index: at most 256 age entries older than the cutoff in one read.
   - A HashRange schema reads each hash with the normal keyed page reader, from the start of the
     hash to the key `{cutoff:020}`.
   A page holds at most the batch size: 16 keys by default, 64 at most, and fewer for a wide
   schema. Schema and observed-hash metadata still scale with the configured schemas and hashes.
   An exclusive cursor advances across stale age entries and keys without a valid range age.
   Only an empty physical page ends an interval. A short page does not end it.
   The next pass starts from the oldest remaining key; no offset survives an erase.
3. One request examines at most 8,192 record keys, and a tombstone is a key.
   A HashRange page that passes that many tombstones ends early and names the last key it
   examined; the next page starts after that key.
   The erase leaves tombstones at the oldest keys of each hash. So the pass keeps a floor for each
   hash in memory: the key where a page of the leading run of tombstones ended. The next pass of the
   hash starts after that floor. Once in 64 passes of a hash, and after a restart, the pass starts
   at the start of the hash again. A write below the floor (a peer import of old records, or a key
   with an old entry time) waits for that full pass.
4. Each round serves one page from each schema, in schema name order. A size refusal halves the
   batch of that schema and sends the keys again. A failed schema releases its page and retains
   only its outstanding count. Other schemas continue. The status does not call a failed or cut
   pass complete.
5. Before each batch the sweep re-checks its keys, inside the batch admission. The cutoff of the
   re-check is the earlier of the listing cutoff and the cutoff of the current policy.
   - Hash keys take their age from the age index. One batched read drops every key that a client
     rewrote after the pass listed it. A key with no age entry was erased by someone else and is
     dropped too.
   - HashRange keys take their age from the key, so a rewrite does not change it.
   - A policy that is gone ends the schema for this pass. A policy with a longer TTL protects the
     keys that it protects now.
   The log line `ttl sweep re-check dropped keys that are no longer expired` names the count.
6. The erase spares a Hash record that was written at or after the cutoff (see The write-time
   guard).
7. The sweep holds the local mutation admission for one batch at a time, and rests after each
   batch (Pacing). A snapshot cut waits for the admitted batch and blocks later admissions.
   The pass stops between batches when the daemon stops. The next tick lists the rest again.
8. A `purge` refuses a batch that names a key which is already gone, for example a key that a
   client, a peer or another purger removed after the re-check. The refusal reads
   `Purge target not found: ... refusing to silently no-op (compliance verb)`. After such a refusal
   the sweep asks which keys of the batch are absent, counts them as done, drops them, and sends
   the rest. It repeats this until the batch is sent or empty. Each refusal that names an absent key
   makes the batch smaller, so the loop ends. A key that another purger removed is never a failure
   of the sweep. The count is `missing_dropped` in the status. The log line is `ttl sweep drops keys
   that are already gone and sends the rest of the batch`. A refusal that names no absent key is a
   real error and ends the schema for this pass (step 9). A `delete` of an absent record writes
   nothing, so `delete` has no such retry.
9. A listing error or a batch error ends that schema for this pass. The sweep logs it once with
   the schema name, counts it, and goes on with the other schemas. A pass in which no schema
   succeeded counts as failed. After three failed passes one after the other, the sweep waits
   before each next pass: 5 minutes, then 10, 20 and 40 minutes, and 1 hour at most for every pass after that. It
   writes one `ttl sweep waits before the next pass` line at the start of each wait. The `Local
   retention:` line shows `failures`, and a `sweep_waits:` line shows `next_attempt_unix_secs`
   while the wait lasts. A pass in which one schema succeeded ends the wait and resets the count.
   The sweep never stops for the life of the process, so a restart is not needed to resume it.

## Verbs

| `LASTDB_TTL_SWEEP_VERB` | Meaning |
|---|---|
| `purge` (default) | The hard-erasure verb. It changes the record into a tombstone, releases every field history entry of the record, and deletes its kept conflict records. It refuses a key that is already gone. |
| `delete` | The ordinary delete a client uses. It changes the record into a tombstone. A field with a history policy keeps the old value as a history entry. The kept conflict records stay until their window ends. |

Any other value logs one warning at the first sampler tick and uses `purge`. The delete ledger
(`lastdb db delete-ledger`) names the caller of each erase: `ttl-sweep-purge` or `ttl-sweep-delete`.

Both verbs do these steps in the same batch as the tombstone:

- They delete the alternate key entries (`ak:`) of the record.
- They lower the file blob counters of the record (`bref:`). A file blob whose count reaches
  zero enters the blob dwell queue, and the dwell deletes it after `LASTDB_BLOB_DWELL_SECS` (one
  hour by default; `docs/lastdb-blob-counter.md`).
- They remove the age entries of the record from the retention index.
- They apply the same delete floor to the share copies of the record.
- They write one hard-delete audit entry (`dellog`) for each verb, caller and schema group.

Neither verb writes a delete barrier or a delete marker. The tombstone holds the delete floor.

Only `delete` copies the old values into field history (`rh:`) and updates the history count and
expiry queue (`ahk:`, `ahq:`). Only `purge` releases the field history of the record. A purge
releases at most 1,024 history entries of one record in its batch. A record with more refuses the
purge with `purge its field history (field-history purge) before the record`
(`docs/lastdb-field-history.md`).

## Why `purge` is the default

A TTL is a promise that the expired data goes. `purge` keeps that promise in one batch: the
record values, the field history and the kept conflict records go together. `delete` keeps the
old values in field history for each field with a history policy, and keeps the conflict records
until their retention window ends. A TTL schema with a history policy then keeps expired values on disk
after the sweep.

Both verbs leave the same tombstone, and both stop an older write from bringing the key back
(What stays after a TTL erase). So the choice of verb does not change the replay rule.

## What stays after a TTL erase

- The tombstone at `rk:`. It holds no field values. It holds the delete floor: the stamp of the
  erase. A later local write, a replayed peer write, and a restored older record lose against the
  floor when their stamp is at or below it. A peer that receives a replayed `purge` applies it as a
  delete.
- No sweep removes a tombstone. A schema drop removes the records of the schema, tombstones
  included.
- `delete` only: the field history entries and the kept conflict records, until their own windows end.

The default remains `purge`. No timer infers a maximum offline time for a device.
The owner selects the home-wide replay cutoff (`layout:delete-replay-cutoff`) explicitly.
After the cutoff rises, an old or unclocked replay refuses and the peer needs a current snapshot.
The [replay cutoff guide](delete-replay-cutoff.md) gives the steps and the replay refusal rule.

## The write-time guard (Hash schemas)

The re-check in step 5 reads the stored age for the key.
A native write records the age in the same durable commit, before its ack.
The erase still checks the record, because a write can occur after selection and before admission.

The sweep sends the cutoff of the batch with each erase of a Hash key. The erase runs inside the
native write scope and reads the record:

- A record written at or after the cutoff (its newest field stamp) is spared. The erase changes
  nothing for that key and writes no audit entry for it. The other keys of the batch go on.
- An expired record becomes a tombstone with its floor at the cutoff: a local write of this device
  at the cutoff time. A write with a later stamp from any device stays.

No writer can acknowledge a write between the read and the batch.
The guard is for the whole record: a rewrite of one field spares the whole record.
The signed TTL mutation includes its cutoff, and replay preserves it.
Both installed names of an app schema use the same retention anchor.
HashRange keys take their age from the key. A rewrite does not change it, so the sweep sends no
cutoff for them.
An old incorrect Hash age needs explicit backfill or a later write.

## Batch size and the request limit

One erase request stages all its chunks of 64 keys in one native batch. A batch holds at most
16,384 operations, and the reads of the request share a budget of 40,000 examined keys.

The sweep cuts its batch to the smaller of the pace (`LASTDB_TTL_SWEEP_BATCH`, default 16) and the
key count that the estimate in `fold_db_core/purge/budget.rs` allows for the schema. For F fields,
the estimate counts 4 operations and 12 reads for each key and field in a `purge`, 5 and 13 in a
`delete`, and `F + 8` of each for every chunk. The estimate charges a cost for each field. A
format 8 erase writes one record for each key, so the estimate is conservative.

| Schema | `purge` | `delete` |
|---|---|---|
| 6 fields | 553 keys | 511 keys |
| 24 fields | 138 keys | 128 keys |
| 48 fields | 69 keys | 64 keys |
| 300 fields | 11 keys | 10 keys |

The default of 16 fits a schema of up to about 200 fields. A batch that still hits a size limit
(`read resource limit exceeded`, `complete mutation limit exceeded` or `write resource limit`)
changed nothing: the sweep halves it for that schema and sends it again. It does not skip the
schema.

A client request does not use this estimate. A client erase that is too big fails at native
admission, for example with `write resource limit exceeded: complete native batch exceeds 16384
operations`. Send fewer keys in one request.

## Pacing

A sweep batch is one erase request. It retains its durability fence.
The sweep measures the time its write scopes keep other writers waiting.
That measure includes a sealed flush wait and excludes reads before the write gate.
The rest after a batch is `blocked * (100 - duty) / duty`, at least the minimum rest and at most
10 minutes. When no other writer wrote during the batch, the rest is the minimum rest.

The TTL sweep uses the same task-local writer-cost measure as the other maintenance jobs.
A sticky writer watch retains the duty share for sparse writers.
`LASTDB_MAINTENANCE_QUIET_SECS` defaults to 60 seconds.
When no other writer remains, the rest returns to its configured minimum.
The watch persists between passes. A long rest ends early after the quiet period.

Private janitor and dwell batches can share the bounded collection window.
They still return after durability. Their ordinary measured cost includes the gate and shared flush holds.
See `docs/lastdb-eager-durable-queue.md` and `docs/lastdb-time-queue.md`.

A stop waits for the batch in flight. It cannot cancel it.

## Measured cost

This page keeps no measurement. The benchmark workflow measures TTL purge batches during its
write window on every measured run: see the active TTL cases in `scripts/benchmarks/README.md`.
The result is in `ttl-active/` inside the CI artifact, with a separate CI summary.

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

## Logs and status

`RUST_LOG=lastdbd::ttl_sweep=debug` prints one `ttl sweep batch erased` line per batch with the
key counts and `took_ms`.

`lastdb status` prints the `Local retention:` line:

| Field | Meaning |
|---|---|
| `enabled` | The sweep runs: `LASTDB_TTL_SWEEP` does not stop it. |
| `policies` | The schemas with a retention policy. |
| `passes` | The passes in which a schema succeeded, for the life of the process. |
| `rows_reaped` | The keys that the sweep sent to an erase, spared keys included. |
| `last_sweep_unix_secs` | The end of the last pass in which a schema succeeded, or `never`. |
| `failures` | The failed passes since the last pass in which a schema succeeded. |
| `in_flight` | A pass runs now. |
| `settled` | The last pass sent no key and finished its backlog. A new pass sets it to false while it runs. |

The line `last_pass:` follows it:

| Field | Meaning |
|---|---|
| `finished_backlog` | The last pass erased or dropped every key that it listed, in every schema, with no failed schema. |
| `keys_left` | The keys that the pass listed and had not reached when it ended. |
| `cut` | The pass ended early before it reached all listed keys. |
| `schemas_failed` | The schemas that this pass skipped. |
| `missing_dropped` | The keys of a `purge` batch that were already gone. The sweep dropped them. |
| `schema_failures_total` | The skipped schemas for the life of the process. |

The JSON status also gives `peak_buffered_keys`: the largest expired-key page held by the pass.
It excludes schema/hash metadata and temporary query response bytes. The text line does not print it.

A pass that ends with keys left writes one `ttl sweep pass ended with listed keys left over`
line. The next pass lists those keys again.

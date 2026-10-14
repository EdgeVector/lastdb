# TTL sweep: verb, pacing and knobs

The node-local TTL sweep (`lastdb_node/src/ops/ttl_sweep.rs`) erases the expired keys of every
schema that has a retention policy. The age rule is in
`fold_db/crates/core/src/schema/schema_types.rs` (`SchemaRetentionPolicy`) and in
`lastdb schema-retention set --help`. This page covers what the sweep does with the keys it
selected, and the four knobs.

## What a pass does

1. The sampler tick starts at most one pass. A pass skips while a backup cut is held or local
   writes are paused.
2. The pass lists the expired keys of each schema once, hash by hash and oldest first inside a
   hash. The store keeps the rows of one hash in one group, so one batch touches few groups.
3. The pass erases in rounds. Each round takes one batch from each schema that still has keys,
   in schema name order. A large backlog in one schema cannot starve the others.
4. Before each batch the sweep re-checks its keys, inside the batch admission:
   - Hash and Single keys take their age from the write-time index. One batched read drops every
     key that a client rewrote after the pass listed it. A key with no index row was erased by
     someone else and is dropped too.
   - Range and HashRange keys take their age from the key, so a rewrite does not change it.
   - A policy that is gone ends the schema for this pass. A policy with a longer TTL protects the
     keys that it protects now.
   The log line `ttl sweep re-check dropped keys that are no longer expired` names the count.
5. The sweep holds the local mutation admission for one batch at a time, and rests after each
   batch (below). The pass stops between batches when the daemon stops, when a backup cut starts,
   or when local writes pause. The next tick lists the rest again.
6. A listing error or a batch error ends that schema for this pass. The sweep logs it once with
   the schema name, counts it, and goes on with the other schemas. Three passes in a row in which
   no schema succeeded stop the sweep until the daemon restarts. A pass in which one schema
   succeeded resets that count.

## The write-time guard (`delete` verb, Hash and Single schemas)

The re-check in step 4 reads the write-time index. A write reaches that index after the client
gets its ack, in the persist lane of the schema. A rewrite that was acked and not yet indexed
reads as old, and the batch would delete it with a stamp that is newer than the rewrite.

To close this window, a `delete` batch of Hash or Single keys carries the cutoff of the batch.
The delete compares the write time of the tip itself with the cutoff, under the tip lock that
the delete takes. A tip written at or after the cutoff is kept, and its age row stays. The
decision and the erase are in one critical section, so no rewrite can fall between them.

Limits:

- The cutoff is a field of the local delete barrier only. A peer and a cloud restore replay a
  plain delete that is stamped at the time of the sweep. They do not see the guard. On a node
  with sync on, a key that the guard spared stays here, while a peer that replays the delete
  can erase its copy (see Last writer wins). The two devices can then differ for that key.
- The `purge` verb has no guard. It keeps the re-check of step 4 and its window.
- A key written only through another name of the same schema (the catalog hash name) has no age
  row, so the sweep never lists it. This limit is older than the guard.
- Range and HashRange keys take their age from the key. A rewrite does not change it, so they
  need no guard.

## Status

`lastdb status` prints the line `last_pass:` below the `Local retention:` line:

| Field | Meaning |
|---|---|
| `finished_backlog` | The last pass erased or dropped every key that it listed, in every schema, with no failed schema. |
| `keys_left` | The keys that the pass listed and had not reached when it ended. |
| `cut` | The pass ended early: the daemon was stopping, a backup cut was held, or local writes were paused. |
| `schemas_failed` | The schemas that this pass skipped. |
| `schema_failures_total` | The same count for the life of the process. |

A pass that ends with keys left writes one `ttl sweep pass ended with listed keys left over`
line. The next pass lists those keys again. `settled` in the `Local retention:` line stays false
until a pass finishes its backlog.

## Verbs

| `LASTDB_TTL_SWEEP_VERB` | Meaning |
|---|---|
| `delete` (default) | The ordinary delete a client uses. It removes the tips and needs only the keys of the batch. It leaves the atom bodies to garbage collection. It removes the tip-version (`tv:`) chain of each removed tip. It leaves the legacy history rows and the conflict records. It writes delete barrier records (see Records that stay). The delete ledger row has the caller `ttl-sweep-delete`. A client delete has the caller `mutation-pipeline`. |
| `purge` | The hard-erasure verb. It also deletes the atoms that no other key references. A molecule made on a current build has no reverse-reference manifest, so each batch loads every row of every field of the schema, whatever the batch size. It refuses a target that is already gone, so one key that a client or a peer removed fails the batch. |

Any other value logs one warning at the first sampler tick and uses `delete`.

CAUTION: With `delete` the atom bodies stay until `gc-atoms` deletes them. Automatic `gc-atoms`
waits for the grace window (24 hours by default), and it is off on a node that has applied a peer
mutation (see `docs/lastdb-atom-refcount-grace-window-gc.md`, the peer-apply interlock). On such a
node a TTL sweep frees the tips and no atom bytes. Use `purge` where the atom bodies must go at once.

## Records that stay after a TTL delete

CAUTION: The default `delete` verb leaves rows that nothing removes. Measured on a synthetic home
(release build, 2,000 keys with 3 fields in a HashRange schema, all swept by `delete`, then
`lastdb db compact --collection tips --execute`):

| Rows in the `tips` collection | Rows | Bytes after compaction | Per key per field | Verbs |
|---|---|---|---|---|
| `rdel:v2:` delete barrier | 12,000 | about 19.0 MB | about 3,170 B | `delete` only |
| `mgd:v1:` generation delete marker | 6,000 | about 4.2 MB | about 700 B | `delete` and `purge` |

A second run with 400 keys gave 3,170 B per key per field, so the cost is linear.

The delete writes one barrier for each spelling of the molecule key that a read accepts. A
molecule whose id is a 32-byte digest has two spellings, so a key with 3 fields leaves 6 barrier
rows of about 1.6 KB each (key about 720 B, value about 810 B). A barrier row is live to the
store, so compaction cannot reclaim it. `gc-atoms` prunes `tv:` chains and deletes `atom:` rows.
It does not touch `rdel:v2:` or `mgd:v1:`. Seven `gc-atoms --execute` runs and a second compaction
left all 12,000 barrier rows in place. `lastdb db inventory` has no key class for either prefix,
so it does not show them.

Projection for a log that expires 100,000 records per day for 30 days with 3 fields:

- Barrier rows: 3,000,000 records x 3 fields x 3,170 B is about 28.6 GB.
- Generation markers (both verbs): about 6.4 GB.
- The atom bodies also stay until `gc-atoms` runs (see Verbs).

The cost scales with the number of fields. The barrier is the proof that makes a later write
with an older clock lose, so a prune needs a horizon that is safe for every peer. No prune
exists today.

### Last writer wins

A delete is ordered by write clock like any other write. A TTL delete is stamped at the time of
the sweep. It beats every write with an older clock, including a write of a peer that is not
yet synced. That write is lost on both devices. This holds for both verbs. The sweep keeps only
the age rule, so a record that a peer rewrote and did not yet sync can be erased by a sweep on
this device.

## Knobs

Only the node environment sets them. A running process cannot change its environment, so a change
needs a restart of the daemon. The sweep reads them at the start of each pass.

| Name | Default | Clamp | Meaning |
|---|---|---|---|
| `LASTDB_TTL_SWEEP_BATCH` | 16 | 1 to 64 | Keys erased in one batch. |
| `LASTDB_TTL_SWEEP_DUTY_PCT` | 25 | 1 to 100 | The share of wall time that the sweep spends in batches. |
| `LASTDB_TTL_SWEEP_MIN_PAUSE_MS` | 25 | 0 to 600000 | The shortest rest between two batches. |
| `LASTDB_TTL_SWEEP_VERB` | `delete` | `delete`, `purge` | See Verbs. |

A value that does not parse gives the default. `LASTDB_TTL_SWEEP=0` stops the sweep.

The rest after a batch is `took * (100 - duty) / duty`, at least the minimum rest and at most
10 minutes. The duty share is exact for a batch of up to 200 seconds at the default duty. After a
slower batch the rest is 10 minutes, and the real share is `took / (took + 600 s)`.

A stop waits for the batch in flight. It cannot cancel it. A `delete` batch of 16 keys takes
seconds. A `purge` batch takes as long as the proof over the schema, which grows with the schema.

`RUST_LOG=lastdbd::ttl_sweep=debug` prints one line per batch with the phase times.

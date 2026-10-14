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

## Verbs

| `LASTDB_TTL_SWEEP_VERB` | Meaning |
|---|---|
| `delete` (default) | The ordinary delete a client uses. It removes the tips and needs only the keys of the batch. It leaves the atom bodies to garbage collection. It does not touch history rows or conflict records. The delete ledger row has the caller `ttl-sweep-delete`. A client delete has the caller `mutation-pipeline`. |
| `purge` | The hard-erasure verb. It also deletes the atoms that no other key references. A molecule made on a current build has no reverse-reference manifest, so each batch loads every row of every field of the schema, whatever the batch size. It refuses a target that is already gone, so one key that a client or a peer removed fails the batch. |

Any other value logs one warning at the first sampler tick and uses `delete`.

CAUTION: With `delete` the atom bodies stay until `gc-atoms` deletes them. Automatic `gc-atoms`
waits for the grace window (24 hours by default), and it is off on a node that has applied a peer
mutation (see `docs/lastdb-atom-refcount-grace-window-gc.md`, the peer-apply interlock). On such a
node a TTL sweep frees the tips and no atom bytes. Use `purge` where the atom bodies must go at once.

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

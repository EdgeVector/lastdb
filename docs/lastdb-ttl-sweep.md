# TTL sweep: verb, pacing and knobs

The node-local TTL sweep (`lastdb_node/src/ops/ttl_sweep.rs`) erases the expired keys of every
schema that has a retention policy. The age rule is in
`fold_db/crates/core/src/schema/schema_types.rs` (`SchemaRetentionPolicy`) and in
`lastdb schema-retention set --help`. This page covers what the sweep does with the keys it
selected, the two verbs, what each verb leaves on disk, and the four knobs.

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
5. The erase spares a Hash or Single key whose tip is newer than the cutoff (see The write-time
   guard).
6. The sweep holds the local mutation admission for one batch at a time, and rests after each
   batch (below). The pass stops between batches when the daemon stops, when a backup cut starts,
   or when local writes pause. The next tick lists the rest again.
7. A `purge` refuses a batch that names a key which is already gone, for example a key that a
   client or a peer removed after the re-check. After such a refusal the sweep asks which keys of
   the batch are absent, drops them, and sends the rest. It does this up to 3 times for a batch.
   The count is `missing_dropped` in the status. The log line is `ttl sweep drops keys that are
   already gone and sends the rest of the batch`.
8. A listing error or a batch error ends that schema for this pass. The sweep logs it once with
   the schema name, counts it, and goes on with the other schemas. Three passes in a row in which
   no schema succeeded stop the sweep until the daemon restarts. A pass in which one schema
   succeeded resets that count.

## Verbs

| `LASTDB_TTL_SWEEP_VERB` | Meaning |
|---|---|
| `purge` (default) | The hard-erasure verb. It deletes the tips, the tip-version (`tv:`) chain, the legacy history rows of the key and the atom bodies that no other key references. It writes no delete barrier. A molecule made on a current build has no reverse-reference manifest, so each batch loads every row of every field of the schema. A batch holds the schema purge barrier while it runs, and writes to that schema wait for it. It frees atom bytes at once, also on a node that has applied a peer mutation. The delete ledger row has the caller `ttl-sweep-purge`. |
| `delete` | The ordinary delete a client uses. It removes the tips and the `tv:` chain, needs only the keys of the batch, and holds no schema barrier. It leaves the atom bodies and the legacy history rows to garbage collection, and it writes two delete barrier records for each field of each key (see Why `purge` is the default). The delete ledger row has the caller `ttl-sweep-delete`. A client erase has the caller `mutation-pipeline`. |

Both verbs leave the conflict records, and both leave one `mgd:v1:` record for each field of
each key (see Records that stay).

Any other value logs one warning at the first sampler tick and uses `purge`.

CAUTION: With `delete` the atom bodies stay until `gc-atoms` deletes them. Automatic `gc-atoms`
waits for the grace window (24 hours by default), and it is off on a node that has applied a peer
mutation (see `docs/lastdb-atom-refcount-grace-window-gc.md`, the peer-apply interlock). On such a
node a `delete` sweep frees the tips and no atom bytes.

## Why `purge` is the default

`delete` leaves two `rdel:v2:` barrier rows for each field of each key, and nothing prunes them.
On a 3-field schema that is about 3,170 B for each key and field, and about 28.6 GB for a log that
expires 100,000 records per day for 30 days. `purge` writes no barrier. The measured numbers
are in the next section.

What `delete` buys: the barrier stops an older write from bringing the key back. A write that
a peer made before the sweep and did not yet sync, and the replay of an older log, are the cases.
With `purge` such a write is applied as a new write and the key can come back. A Range or
HashRange key takes its age from the key, so the next pass erases it again. A Hash or Single key
takes its age from the write-time index, and whether a replayed peer write gets a fresh index row
is not verified.

Condition to switch the default to `delete`: a prune horizon for the barriers that is safe for
every peer. Today no code knows a maximum offline time for a device and no code prunes a barrier.

## Measured cost of each verb

Synthetic fresh home, release build, EXEMEM_ENV=dev. One HashRange schema with 3 fields. 2,000
expired keys (6,000 key and field pairs), all swept in one pass with the knobs set to
batch 64, duty 100 and no minimum rest. Then two `gc-atoms --execute` laps and
`lastdb db compact --collection <name> --execute` on every plane. Row classes of the `tips`
collection were counted on the stopped home.

| | `purge` | `delete` |
|---|---|---|
| Records per second | 22.1 (90.5 s) | 21.0 (95.2 s) |
| Host load during the run (1 minute average) | 4.9 to 6.8 (mean 5.4) | 8.0 to 11.0 (mean 9.3) |
| `rdel:v2:` barrier rows | 0 | 12,000 (about 3,006 B per key and field) |
| `mgd:v1:` rows | 6,000 (654 B per key and field) | 6,000 (654 B per key and field) |
| Other rows that stay in `tips` | 32 ledger rows, 7 small rows | 32 ledger rows, 4 small rows |
| `tips` on disk after compaction | 4.25 MB (708 B per key and field) | 23.29 MB (3,881 B per key and field) |
| `atoms` on disk after compaction | 34 KB | 1.14 MB (190 B per key and field) |
| After 2 `gc-atoms` laps and compaction | no class shrinks | no class shrinks |

The `gc-atoms` laps scanned 0 atoms and reported `prune_more_remaining`, so they did not reach
the atom scan. Compaction found every `tips` row live. A run with 400 keys gave 3,170 B per key
and field of barrier rows, so the cost is linear.

Throughput at the default knobs (batch 16, duty 25, minimum rest 25 ms) for `purge` on a larger
schema: one HashRange schema with 6 fields and 2,400 rows (2,100 expired) and 150-byte bodies,
one writer on the swept schema and one on a second schema (one small write every 100 ms each),
host load 6 to 13. The run stopped at 780 s.

| | `purge`, default knobs |
|---|---|
| Keys erased | 96 in 13 minutes (6 batches), so 0.10 records per second |
| Time of one batch of 16 keys | 16 to 76 s (median 31 s) |
| Hold of the schema purge barrier per batch | 4.4 to 36.1 s (median 11.9 s) |
| Writes to the swept schema during the sweep | p50 519 ms, p95 912 ms, longest 36.8 s |
| Writes to the second schema during the sweep | p50 239 ms, p95 474 ms, longest 1.7 s |

A `purge` batch loads every row of every field of the schema, so its cost follows the size of the
schema and not the size of the batch. The 3-field run above took 2.9 s for 64 keys. This 6-field
run took 31 s for 16 keys. A larger batch lowers the cost for each key and does not lengthen the
stall for each batch. The default batch is 16 for both verbs.

## The write-time guard (Hash and Single schemas)

The re-check in step 4 reads the write-time index. A write reaches that index after the client
gets its ack, in the persist lane of the schema. A rewrite that was acked and not yet indexed
reads as old, so without a guard the erase would remove it.

The sweep sends the cutoff of the batch with each erase of a Hash or Single key. Each verb
compares the write time of the tip itself with the cutoff:

- `purge`: the enqueue of the erase holds the molecule write locks of the slots. No writer can
  acknowledge a write to them while it holds the locks. It reads the resident tip and the tip on
  disk of every field of each key. A key with a tip at or after the cutoff is dropped from the
  batch, and the batch is planned again without it.
- `delete`: the cutoff is a field of the local delete barrier. The barrier test that runs under
  the tip lock spares a tip written at or after the cutoff. It works for each field, so a rewrite
  of one field spares only that field.

Limits:

- A spared key keeps its age row only if the row is newer than the cutoff. A key whose tip clock
  is at or after the cutoff, and whose age row is old, is listed and spared again by each pass.
  A peer write that carries a clock ahead of this node does this.
- The cutoff is not part of the captured mutation. A peer and a cloud restore replay a plain
  erase that is stamped at the time of the sweep. On a node with sync on, a spared key stays here,
  and a peer that replays the erase can remove its copy. The two devices can then differ.
- A key written only through another name of the same schema (the catalog hash name) has no age
  row, so the sweep never lists it.
- Range and HashRange keys take their age from the key. A rewrite does not change it, so they
  need no guard.

## Records that stay after a TTL erase

- `mgd:v1:` is a sparse delete marker. It holds the removed tip and stops a read from showing a
  slot of an immutable base generation of the molecule that the delete removed from the live rows
  (`db_operations/atom_store/molecules/delete.rs`, `generation/slots.rs`). The read looks at it
  only for a molecule that has an active generation (`generation/merge.rs`). Both verbs write it,
  because both use the same slot removal. It is local to the device (`sync/policy.rs`), and
  nothing removes it.
- `rdel:v2:` is a delete barrier. It holds the stamp of the delete. It makes a later local write,
  a peer write that is replayed, and an older tip from a restore lose against the delete when
  their clock is not newer (`atom/delete_barrier.rs`, `apply_gate.rs`, `sync/engine/replay`). Only
  `delete` writes it. It writes one for each spelling of the molecule key that a read accepts
  (`atom/molecule_key_codec.rs`), which is two on a molecule with a 32-byte digest id. Each
  device builds its own barrier from the replayed erase (`sync/policy.rs`), and nothing removes it.
- `lastdb db inventory` has no key class for either prefix, so it does not show them.

### Last writer wins

An erase is ordered by write clock like any other write. A TTL `delete` is stamped at the time of
the sweep. It beats every write with an older clock, including a peer rewrite that is not yet
synced. That write is lost on both devices. A `purge` has no barrier: an older write that arrives
later is applied. In both cases a peer rewrite that is not yet synced can be lost or can
bring the key back. No test with two devices exists for this.

## Knobs

Only the node environment sets them. A running process cannot change its environment, so a change
needs a restart of the daemon. The sweep reads them at the start of each pass.

| Name | Default | Clamp | Meaning |
|---|---|---|---|
| `LASTDB_TTL_SWEEP_BATCH` | 16 | 1 to 64 | Keys erased in one batch. |
| `LASTDB_TTL_SWEEP_DUTY_PCT` | 25 | 1 to 100 | The share of wall time that the sweep spends in batches. |
| `LASTDB_TTL_SWEEP_MIN_PAUSE_MS` | 25 | 0 to 600000 | The shortest rest between two batches. |
| `LASTDB_TTL_SWEEP_VERB` | `purge` | `purge`, `delete` | See Verbs. |

A value that does not parse gives the default. `LASTDB_TTL_SWEEP=0` stops the sweep.

The rest after a batch is `took * (100 - duty) / duty`, at least the minimum rest and at most
10 minutes. The duty share is exact for a batch of up to 200 seconds at the default duty. After a
slower batch the rest is 10 minutes, and the real share is `took / (took + 600 s)`.

A stop waits for the batch in flight. It cannot cancel it. A `delete` batch of 16 keys takes
seconds. A `purge` batch takes as long as the proof over the schema, which grows with the schema.
Measured stop times: 13.3 s with a `purge` batch in flight (2,400 rows, batch 16), 1.0 s while the
pass rested between batches, and 0.8 to 0.9 s with a `delete` batch. A `purge` batch of 16 keys
took up to 76 s on the 6-field schema above, and a stop can wait that long.

`RUST_LOG=lastdbd::ttl_sweep=debug` prints one line per batch with the phase times.

## Status

`lastdb status` prints the line `last_pass:` below the `Local retention:` line:

| Field | Meaning |
|---|---|
| `finished_backlog` | The last pass erased or dropped every key that it listed, in every schema, with no failed schema. |
| `keys_left` | The keys that the pass listed and had not reached when it ended. |
| `cut` | The pass ended early: the daemon was stopping, a backup cut was held, or local writes were paused. |
| `schemas_failed` | The schemas that this pass skipped. |
| `missing_dropped` | The keys of a `purge` batch that were already gone. The sweep dropped them. |
| `schema_failures_total` | The skipped schemas for the life of the process. |

A pass that ends with keys left writes one `ttl sweep pass ended with listed keys left over`
line. The next pass lists those keys again. `settled` in the `Local retention:` line stays false
until a pass finishes its backlog.

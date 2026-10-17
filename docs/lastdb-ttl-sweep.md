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
| `purge` (default) | The hard-erasure verb. It deletes the tips, the tip-version (`tv:`) chain, the legacy history rows of the key and the atom bodies that no other key references. It writes no delete barrier. A batch reads only the rows of its own keys and the reverse-reference edges of the atoms that it can free (see What a `purge` batch reads). It takes no schema purge barrier, and it occupies the persist lane of the swept schema while it runs. It frees atom bytes at once, also on a node that has applied a peer mutation. The delete ledger row has the caller `ttl-sweep-purge`. |
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

## What a `purge` batch reads

A `purge` batch must not free an atom body that another key still names. It proves this with the
reverse-reference edges (`aref:v2:`). The edges list each tip, tip version and history row that
names an atom. A batch reads:

- the tips of its own keys, in one batch read for each field;
- the `tv:` chain of its own keys, and the `history:` rows of the molecules (a molecule that never
  recorded a conflict and never held a legacy row has none; the first batch of a molecule in a
  process proves it, and a memo keeps that answer; a molecule with such rows is read again by each
  batch, and that read walks the groups of the collection, so it follows the size of the store);
- the edges of each atom that its keys name, as one range read of at most 2 rows for each atom;
- the tips of the schema in memory, which the resident graph holds and which the key budget of the
  warm set bounds (an acked write has no edge on disk until its persist envelope runs).

A batch does not read the other keys of the schema, so its cost follows the batch and not the
schema. It takes no schema purge barrier. It occupies the persist lane of the swept schema while it
runs, so a durable write to that schema waits for the batch.

The proof needs a manifest for each molecule of the schema. The daemon writes the manifest of a
molecule at its first `purge` when the home holds both completion markers of the edge plane. A home
that the daemon created holds them from its first boot. Each later batch reads all manifests of the
schema in one batch read.

A batch falls back to the proof over the whole schema in these cases. The fallback loads every row
of every field, takes the schema purge barrier, and costs as much as the "whole schema" column of
the tables below:

- the node runs with `LASTDB_BARRIERLESS_PURGE=0`;
- the home lacks the completion markers, for example a home that a restore filled without them;
- a field of the schema has a storage prefix (an organization or share namespace);
- the history upgrade of a molecule is not finished (it advances 256 rows for each batch).

The status table `Purge stats` shows the path. A schema that takes the keyed path has
`schema_barrier_acquisitions=0` and a count of `purge_reverse_edge_reads`. A schema that falls back
has a count of `schema_barrier_acquisitions` and a time in `exclusive_hold_us`.

## Measured cost of each verb

Synthetic fresh home, release build, EXEMEM_ENV=dev. One HashRange schema with 3 fields. 2,000
expired keys (6,000 key and field pairs), all swept in one pass with the knobs set to
batch 64, duty 100 and no minimum rest. Then two `gc-atoms --execute` laps and
`lastdb db compact --collection <name> --execute` on every plane. Row classes of the `tips`
collection were counted on the stopped home.

| | `purge` | `delete` |
|---|---|---|
| Records per second | 28.7 (69.6 s) | 21.0 (95.2 s) |
| Host load during the run (1 minute average) | 3.6 to 4.3 (mean 3.8) | 8.0 to 11.0 (mean 9.3) |
| `rdel:v2:` barrier rows | 0 | 12,000 (about 3,006 B per key and field) |
| `mgd:v1:` rows | 6,000 (654 B per key and field) | 6,000 (654 B per key and field) |
| Other rows that stay in `tips` | 32 ledger rows, 7 small rows | 32 ledger rows, 4 small rows |
| `tips` on disk after compaction | 4.25 MB (708 B per key and field) | 23.29 MB (3,881 B per key and field) |
| `atoms` on disk after compaction | 34 KB | 1.14 MB (190 B per key and field) |
| After 2 `gc-atoms` laps and compaction | no class shrinks | no class shrinks |

The `purge` throughput row was measured again for the batch that reads only its own keys. The
batch that read the whole schema gave 25.1 records per second (79.8 s) on this schema at load 3.3
to 5.5. The 3-field schema is small, so the durable delete is most of the batch time (a batch of 64
keys took 2.1 s, against 2.6 s) and the change gains little here. The `delete` column and the rows
about row classes come from the earlier run. The two batches leave the same keys, atoms and row
classes on the 6-field schema below, except for 12 `aref` rows in `atom_ref_edges` (6 manifests and
6 history cursors, one pair for each molecule) that the keyed batch writes at the first `purge`, and
2 bookkeeping rows (`tvr-meta`) in `tips` that the whole-schema proof wrote.

The `gc-atoms` laps scanned 0 atoms and reported `prune_more_remaining`, so they did not reach
the atom scan. Compaction found every `tips` row live. A run with 400 keys gave 3,170 B per key
and field of barrier rows, so the cost is linear.

Throughput at the default knobs (batch 16, duty 25, minimum rest 25 ms) for `purge` on a larger
schema: one HashRange schema with 6 fields and 2,400 rows (2,100 expired) and 150-byte bodies,
one writer on the swept schema and one on a second schema (one small durable write every 100 ms
each). Both runs stopped at 780 s and ran on the same host within one hour. "Whole schema" is the
batch that read every row of every field (main at `afe2b2f5`). "Own keys" is the batch that reads
only its own keys.

| | `purge`, whole schema | `purge`, own keys |
|---|---|---|
| Keys erased | 272 in 13 minutes (17 batches), so 0.35 records per second | 1,760 in 13 minutes (110 batches), so 2.26 records per second |
| Time of one batch of 16 keys | 6.5 to 24.3 s (median 10.1 s) | 1.45 to 2.67 s (median 1.77 s) |
| Hold of the schema purge barrier per batch | 3.4 to 11.3 s (median 4.4 s) | none (the batch takes no barrier) |
| Writes to the swept schema during the sweep | p50 301 ms, p95 484 ms, longest 11.7 s | p50 293 ms, p95 1,722 ms, longest 2.9 s |
| Writes to the second schema during the sweep | p50 294 ms, p95 482 ms, longest 1.7 s | p50 297 ms, p95 777 ms, longest 1.8 s |
| Host load (1 minute average) | 2.3 to 13.1 (mean 6.3) | 2.3 to 11.3 (mean 4.3) |

A durable write took about 250 ms on this host before the sweep started. A durable write to the
swept schema waits for the batch in flight, because the batch occupies the persist lane of the
schema. The duty share is a share of wall time, so the sweep that erases 6.5 times more keys at the
same duty meets more writes, and the p95 of a write rises. The longest wait is about 4 times
shorter. At the same erase rate the stall is shorter: with `LASTDB_TTL_SWEEP_DUTY_PCT=4` the keyed
batch erased 288 keys in 13 minutes (0.37 per second, against 0.35), and the writes to the swept
schema took p50 295 ms, p95 443 ms, longest 2.2 s. The writes to the second schema took p50 297 ms,
p95 458 ms, longest 1.4 s. Lower the duty when the p95 of a write matters more than the speed of
the sweep.

The cost of a batch follows its keys and not the schema. The same 6-field schema with 2,400 rows
and with 9,600 rows (8,400 expired), batch 16, duty 100 and no minimum rest, 240 s, two writers:

| | whole schema, 2,400 rows | whole schema, 9,600 rows | own keys, 2,400 rows | own keys, 9,600 rows |
|---|---|---|---|---|
| Time of one batch of 16 keys | 5.7 to 10.5 s (median 7.6 s) | 22.6 to 42.0 s (median 24.5 s) | 1.5 to 2.5 s (median 1.8 s) | 1.7 to 3.6 s (median 2.0 s) |
| Hold of the schema purge barrier per batch | 3.0 to 4.4 s (median 3.7 s) | 9.9 to 10.6 s (median 10.1 s) | none | none |
| Keys erased in 240 s | 512 | 144 | 2,096 (the pass was nearly done) | 1,968 |
| Stop with a batch in flight | 5.6 s | 13.6 s | 0.9 s | 3.7 s |
| Host load (1 minute average) | 3.0 to 8.8 (mean 4.7) | 3.5 to 4.4 (mean 3.9) | 3.7 to 6.1 (mean 4.7) | 2.8 to 4.3 (mean 3.2) |

With 4 times the rows the whole-schema batch took 3.2 times as long. The batch that reads its own
keys took 1.1 times as long, which is within the noise of the host. The remaining cost of a batch
is the durable delete: a fixed part of about 0.5 s and about 80 ms for each key on 6 fields. A
batch of 16 keys took 1.8 s and a batch of 64 keys took 5.6 s (6 fields, 2,400 rows, duty 100). A larger batch lowers the cost for each key and lengthens
each stall, so the default batch stays 16 for both verbs.

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
seconds, and so does a `purge` batch: about 2 s on the 6-field schema above, at 2,400 rows and at
9,600 rows. Measured stop times with a `purge` batch in flight (batch 16, duty 100): 0.9 s at 2,400
rows and 3.7 s at 9,600 rows. The same stops took 5.6 s and 13.6 s when a batch still read the
whole schema. A stop while the pass rests between batches takes about 1 s, and a stop with a
`delete` batch took 0.8 to 0.9 s.

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

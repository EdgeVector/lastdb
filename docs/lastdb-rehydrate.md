# LastDB Resident Rehydrate

Resident mode is the T0 memory plane for LastDB Mini. It keeps the same ladder
objects the durable store owns, then drains dirty entries to disk on the T1
persist path. T2 cloud sync remains downstream of durable storage.

## Status: production code, not an experiment

`LASTDB_RESIDENT_MODE` defaults to `off`, but that is a rollout default, not a
sign the subsystem is unused. The live node has run `write` mode, and the
restore CLI forces `off` only because a one-shot process has no persist worker.
In every mode the ResidentGraph is the mutation concurrency gate (per-key
revisions), and the persist lane owns the disk writes. Legacy unprefixed reads
check the graph in every mode. Only acknowledge-on-apply depends on write mode.
Native pack reads keep one committed disk root and exclude the live graph.

Three caches exist and are not duplicates:

- ResidentGraph: dirty tips and atoms, write order, and deferred acknowledgment.
- LogicalResidentSet: always-on read cache for tips and atoms inside LastStore,
  capped by `RESIDENT_KEY_CAP`.
- Hash-group warm set: byte-budgeted cache for non-logical collections.

Do not delete, merge, or simplify any of them as cleanup. Changing this is a
design decision for the owner.

## Planes

T0 holds the local schema catalog, molecule tips, atoms, and protein metadata.
The local schema catalog stays in memory. A query uses its fields to select
molecule keys, then fetches only the required atoms. A cold read stops at the
atom. A file result contains the file reference and access metadata. The query
does not open the file or load CAS bytes into memory.

One tip has one normal memory home: the graph while dirty, then the warm set
after its disk write. A disk read admits a clean record only to the warm set.
Schemas, proteins, and pending delete marks remain graph metadata.

T1 is the node-local durable store. Deferred resident writes are acknowledged
only after the mutation has installed the exact ladder objects in T0 and marked
them dirty. The background persist worker later writes those objects to T1 and
removes the matching graph tips and clean atom bodies.

T2 is cloud backup and sync. It sees durable T1 state, not speculative memory.
Resident mode must therefore never treat a T2 upload as evidence that a dirty T0
entry is durable.

## Crash Window

`LASTDB_RESIDENT_MODE=write` allows a bounded crash window: the mutation may be
acknowledged after T0 apply and before T1 persist. There is no journal for that
window. The safety rule is bounded exposure, not replay: dirty entries are
charged, capped, retried on persist failure, and never evicted.

When the number of deferred dirty entries reaches `LASTDB_RESIDENT_MAX_DEFERRED`,
mutations degrade to synchronous durable writes until the backlog drains. This
keeps memory from becoming an unbounded write-ahead store.

## Environment

`LASTDB_RESIDENT_MODE` selects resident graph behavior. The common modes are off
or read-only rehydrate, and write mode for deferred durable persists.

The memory limit for logical keys (tips and atoms) is the used-record cap
`RESIDENT_KEY_CAP = 10000`. `LASTDB_RESIDENT_KEY_CAP` can lower that cap.
Do not tune a byte setting to size that set.

`LASTDB_RESIDENT_BYTES` sets the ResidentGraph byte budget
(`LASTDB_RESIDENT_MODE=write`). A value of `0` disables graph byte-budget
eviction. This bound does not size the logical resident set.

`LASTDB_HASH_GROUP_WARM_BYTES` bounds the hash-group warm set for non-logical
collections (indexes, schema_index, atom_ref_edges_v2, keep_small, metadata,
cas_blobs). Tips and atoms take the unpublished loader.

`LASTDB_RESIDENT_MAX_DEFERRED` bounds the dirty deferred-write backlog before
write mode falls back to synchronous persistence.

`LASTDB_RESIDENT_PERSIST_MS` controls the background persist worker cadence.

## Eviction Invariants

The local schema catalog stays resident. A confirmed disk write removes a graph
tip only when its atom and revision still match. Atom reference counts share
the tip lock, so atom cleanup needs no walk of the other tips.

A cold read or prepared write holds a revision lease. An absent, fully durable
key revision leaves after its final lease. Unique revision identities prevent
an old prepared write from accepting a later key state as its original state.
Revision records do not accumulate for every key that the process wrote.

Dirty entries are never evicted. If every eviction candidate is dirty, the graph
stays over budget and increments `evict_refused_dirty`; the persist worker must
drain the dirty set before budget enforcement can make progress.

Purges must remove both durable rows and any resident tips or atoms that could
answer a read. Eviction helpers that refuse dirty entries are not purge helpers:
purge is erasure, not memory pressure.

## Telemetry

`GET /api/status` exposes resident counters in `status.resident` and labels
the live byte budgets in `status.memory_budget`. `lastdb status` prints a
`Logical keys:` line for the used-record cap, a `Memory budget:` line that
names what each byte budget bounds, and a `Resident:` line for ResidentGraph
activity. The `Logical keys:` line is the memory limit for tips and atoms
(`resident_key_count` / `resident_key_budget`, `RESIDENT_KEY_CAP = 10000`).
The ResidentGraph byte budget is `graph_budget=` on the `Resident:` line and
`resident_graph=` on the `Memory budget:` line. The hit and
rehydrate counters are grouped by schema, molecule, atom, file blob, and
protein. Persist counters show enqueued, flushed, and failed deferred batches.
Eviction counters separate clean ResidentGraph evictions from dirty-budget
refusals.
`revision_slots` reports the key revisions retained by graph state, active
calls, or pending writes. It is separate from the warm-set record count.
The legacy file-blob counters remain present for status compatibility. They
stay at zero because the resident graph cannot hold CAS bytes.

## Complete query intervals

A cold HashRange HashKey, prefix, or range query can certify its complete
requested interval. The certificate contains no tips, atoms, or file bytes.
Later queries use the resident key index and tips for that interval. A prefix
certificate cannot answer a wider partition query. A complete partition can
answer a narrower interval without extra tip copies.

A certificate starts before the required disk read. Slot revisions protect
cold tip installs. Dirty data, a pending delete, or an incomplete resident key
set prevents certification. Writes, durable completion, purge, and eviction
invalidate affected certificates. Durable tip batches, generation changes,
and deletes invalidate again when their work ends, including cancellation.
No storage await holds a certificate lock. No molecule-wide write counter is
required.

Pages do not certify whole partitions. The registry holds at most 1,024
certificates, 32 intervals per pair, and 1,024 key bytes per certificate. Reclaim of these small
certificates only causes a later disk read. The ordinary resident LRU owns tip
and atom bytes. `key_set_marked_complete` reports successful certifications;
`key_set_hits` reports their use.

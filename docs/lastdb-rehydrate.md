# LastDB Resident Rehydrate

Resident mode is the T0 memory plane for LastDB Mini. It keeps the same ladder
objects the durable store owns, then drains dirty entries to disk on the T1
persist path. T2 cloud sync remains downstream of durable storage.

## Planes

T0 holds the local schema catalog, molecule tips, atoms, and protein metadata.
The local schema catalog stays in memory. A query uses its fields to select
molecule keys, then fetches only the required atoms. A cold read stops at the
atom. A file result contains the file reference and access metadata. The query
does not open the file or load CAS bytes into memory.

T1 is the node-local durable store. Deferred resident writes are acknowledged
only after the mutation has installed the exact ladder objects in T0 and marked
them dirty. The background persist worker later writes those objects to T1 and
clears the dirty markers.

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
`RESIDENT_KEY_CAP = 10000`. Tests may lower it with `LASTDB_RESIDENT_KEY_CAP`.
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

The local schema catalog stays resident. Clean molecule and atom entries leave
in LRU order when the resident graph exceeds its byte budget. The byte count
includes the catalog. Thus a catalog larger than the budget sets a fixed floor.
Each eviction pass copies only enough candidate keys to cover the excess bytes.
Persist completion also enforces the budget; it does not need a subsequent write.

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

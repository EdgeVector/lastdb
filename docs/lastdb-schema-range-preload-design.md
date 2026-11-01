# Schema range key preload design

> **Model note (2026-11-01).** This page was written for the field key model:
> schema → field → field key → record → file. Since home format 7 (the record model)
> a schema stores one record per primary key, and separate field-key rows and tips do not exist. The [canonical model](lastdb-canonical-model.md) wins where this
> page differs.

Status: selected route merged in [PR 214](https://github.com/EdgeVector/lastdb/pull/214), 2026-10-18. The policy remains opt-in.
Tom selected first-use preload for selected schemas after the [memory and disk study](lastdb-schema-range-preload-study.md).
The [logical design](lastdb-logical-warm-set-design.md) defines common values, fields, and publication rules.
The [work plan](lastdb-logical-warm-set-plan.md) defines the build sequence.

## First use and later use

A selected HashRange schema uses a preload-on-first-query policy.
Authorize the query scope and resolve the current schema bindings before that gate.
The first query waits for all its required membership keys across all hashes.
Concurrent first queries share one load.
Only keys and essential membership flags enter this complete part of the logical set.
Full tips and record values remain partial and enter the normal warm set when required.
Selected tips can remain without values after a call that does not fetch values.
The same logical record gains its full record after a value fetch; the key preload does not retain all tips.

After the load, ranges select keys from the logical set.
The resolver uses known current tips and records, then fetches missing paths in batches.
No disk key discovery precedes a range over a ready domain.
A query still needs disk when selected tips, values, or predicate fields are unknown.

The explicit policy also puts the first exact query behind this one-time schema gate.
Its actual value lookup remains exact; it never enumerates keys after the gate.
Hash-only exact schemas keep their existing route and need no range preload.
Writes alone do not trigger a schema preload.
No explicit public preload operation is built.
The current policy reads `LASTDB_SCHEMA_PRELOAD_SCHEMAS` at process start.
Its comma-separated values select schema names, schema identity hashes, or `*` for all HashRange schemas.
An empty value selects no schema.
`LASTDB_SCHEMA_KEY_BYTES` accepts a numeric byte allowance; its compiled default is 128 MiB.

Unselected schemas use the bounded disk selector for ranges and sample.
The first-use load is an internal schema lifecycle operation, not a new public query without a key.
Normal range calls still name one hash.
The load cost grows with all required schema keys; it is separate from the subsequent bounded query cost.

## Scope and membership authority

Bind each selected schema to its existing authoritative membership source or sources.
Reuse the query planner's current field selection, source precedence, and known-empty fallback semantics.
Do not add another disk membership file.
One authoritative source can serve all projected fields when their membership contract matches.
Other memberships require their own complete domains; do not silently union field keys and change record existence.

Schema readiness covers all required domains across all hashes, not only the hash in the first call.
If a call can select a different membership source, that source must also be complete before this gate reports ready.
Record the source inventory before implementation.
The study's one-set-per-layout estimate applies only where one source establishes that membership.

A domain identity includes the storage scope, membership field key, key layout, compatible encoding identity, and logical membership rules.
Schema names and definitions reference that domain through the logical owner.
Copied schemas can share it only when all those facts match.
Named alternate layouts retain separate address domains and resolve the primary records.

Ready means that the domain contains the complete candidate keys under its compatible definitions.
It does not mean that every candidate is visible, or that the candidate count is an exact visible-record count.
The selected batch resolves current metadata, active generations, winning stored forms, and `rdel` delete barriers.
A known current warm tip can supply that state without another tip read.
Keep metadata-tombstone keys if the current API can request them.
Discard obsolete physical entries; retain no old-value history.
Unknown or unavailable cloud membership cannot establish a complete local set.

## One logical model

Extend the core field key with a complete ordered key part and a sparse tip/value part.
Do not create another graph, a query-answer list, or a second current-value authority.
Known membership with an unknown RecordEntry becomes a valid core state.
Do not create placeholder record UUIDs for keys whose tips are unknown.

The key part organizes hashes for direct lookup and orders ranges within each hash.
Use the same bounds and comparator as the current core operations.
Start with a mutable B-tree for the ranges.
Use shared key identities for sparse warm paths where practical; do not duplicate all keys in another map.
The existing exact path resolver keeps its direct lookup route.

Keep OPE tokens in a lossless compact byte form where the current codec supports it.
The eight hexadecimal characters of one current word become four big-endian bytes.
Do not change the OPE algorithm as part of this work.
Translate query bounds through the same codec and recover API keys only when required.
Never compare tokens from incompatible field key or key epochs.

The original FieldKeyHashRange couples ordered membership with RecordEntry storage.
The candidate adds a complete key domain beside sparse tips, without one full tip for every preloaded key.
Keep full metadata and record associations for the paths that actually hold them.
Core field operations receive complete selected keys plus resolved values when required.
They must not interpret the sparse value part as the complete field key.

## Schema load states

| State | Query behavior |
|---|---|
| Unloaded | The first query reserves resources and starts one load. |
| Loading | Queries join the same bounded load and wait within their deadlines. |
| Ready | Select keys from the complete logical domains. |
| DiskOnly with reason | Use the bounded disk selector; do not repeat a refused full load on each query. |
| Failed with reason | Return the recorded error until an affected control or process restart resets the domain. |

A successful empty load is Ready with no keys.
A work limit, timeout, source error, or cancelled load never becomes Ready.
Do not serve a normal range from the private partial build.
The first query keeps its common captured disk root and pending changes across the load wait.
If a write changes that boundary, the older query uses its captured disk route when the new key domain cannot serve it.
It does not capture a fresh root in the middle of the batch.
A later query can use the Ready domain at its own common boundary.
A ready range or sample holds its required complete-domain references across the candidate chunks.

The engine owns the shared load and its resources.
One caller timeout does not mark the schema failed or cancel another caller's load.
Bound the load's lifetime, I/O work, temporary changes, and memory independently of caller waits.
Return a retryable timeout when a caller cannot wait for completion.
Cancel or retire abandoned work under the same resource limits.

An initial capacity refusal selects DiskOnly before the query continues through the normal disk route.
An exhausted build budget can also select that route with an explicit status reason.
An I/O failure remains an error; an incomplete source must not masquerade as an empty schema.
The current candidate retains DiskOnly without an automatic full retry on each query.
An affected logical control can reset the domain.
A policy or byte-allowance change takes effect after process restart.
No public preload or retry operation is built.
Expose load state and reason through operational status without schema contents or key values.

The implementation permits one concurrent complete-key load per native runtime.
Each load reserves 64 MiB for temporary work before source capture.
The reservation remains held by any I/O context that still uses it, including work after caller cancellation.
Each read page permits at most 256 records and 16 MiB of read work.
The shared load has a 120-second time limit.

## Concurrent writes during the load

Use the same publication lock as current logical writes.
No disk I/O or whole-schema construction runs under that lock.

1. Reserve a private build and a bounded change allowance.
2. Capture definitions, logical membership controls, disk root, covered logical number, and current pending changes together.
3. Register the load for later accepted membership changes before the capture lock releases.
4. Retain the captured root and pending references while bounded disk pages resolve the initial complete keys.
5. Retain later accepted inserts, deletes, eligibility changes, and both sides of key moves in logical order.
6. Merge those changes into the private key structure outside the lock in bounded chunks.
7. Under the lock, merge a bounded final tail, verify the definitions and change coverage, and publish Ready atomically.
8. Release the build's old-root and temporary change references after publication.

The temporary changes contain only required membership facts or references to existing prepared changes.
They are load work, not another warm value store or saved answer.
A concurrent flush cannot remove changes that the load still requires.
An ordinary write does not restart a whole-schema load.
Bound final lock work; if a small final tail cannot be reached within the deadline, refuse the load explicitly.

If the temporary change allowance fills, cancel the preload before the next write becomes visible.
Complete the write through its normal route and set the schema to DiskOnly with a reason.
Do not block or reject a valid write merely to preserve an optional preload.
Schema rebinding, incompatible encoding, or a logical replacement cancels the affected private load before visibility.
Physical flush and LastDB compaction alone do not invalidate logical membership or restart the load.

## Writes after readiness

Publish key membership, current warm paths, dirty state, and reserved flush work before the ack.
An insert adds its key; a delete removes or changes its current visibility as the existing contract requires.
A key move removes its old membership and adds its new membership in one publication.
An overwrite that preserves membership needs no complete-key rebuild.
Logical-record purge removes its selected tip and value together and leaves complete key membership intact.

The native publication route observes accepted raw changes before its logical boundary becomes visible.
Ordinary tip changes and `mgd` or `mgr` changes maintain complete candidate keys without a full reload.
The selected batch still decides current visibility from the actual tip and control state.
An `mgp` logical generation replacement invalidates the affected domain and requires a later load.
Legacy membership blobs and incompatible encoding can select a recorded fallback state.

The writer inventory includes durable writes, imports, retention, backfill, and administrative changes.
The shared native publication route does not by itself prove every writer's end-to-end behavior.
Current product receipts cover the generated batch writes, not every writer in that inventory.
Keep those evidence gaps visible before compatibility removal or a wider cutover.
Named alternate addresses change with their primary records in the same accepted request.
Do not change their current propagation schedule.
Copied-schema aliases observe the same domain update where their definitions permit sharing.

A logical bulk replacement prepares the new disk state first.
At visibility, publish compatible new complete keys or invalidate affected domains and warm slots together.
The next query passes the gate again when invalidation selects Unloaded.
A physical root replacement with identical logical membership preserves Ready.
Keep file-root lifetime controls separate from the logical readiness fact.

## Memory and purge

Full logical paths keep the current 10,000-record cap and the existing least recently used list.
The complete key part uses an explicit separate byte allowance.
The candidate uses a configurable 128 MiB byte allowance per native runtime by default.
The selected-schema policy remains opt-in at process start.
This development work does not change the installed node.

Charge key allocation capacity, tree nodes, domain definitions, build buffers, temporary changes, and read-held retired domains.
Reserve before allocation and before readiness publication.
Limit concurrent loads as well as total retained bytes.
The existing memory guard remains active.

Track recency for complete schema domains separately from full logical path recency.
Purge a whole complete domain when the key allowance requires it.
Clear Ready under the publication lock before the purge becomes visible.
Existing calls retain only their required lifetime references or retry under the common boundary rule.
Do not copy the whole key tree for each query.
Retain a complete-domain lifetime reference across every candidate chunk of a ready call.
A clean purge does not change the logical change number; that number alone cannot protect the directory lifetime.
The retained reference permits the captured call to finish or retry while new calls see the cleared readiness.

If a ready domain cannot accept a key within the allowance, invalidate it before the write ack.
The write proceeds; later queries use DiskOnly until capacity permits a deliberate new load.
Do not retain a partially complete directory as Ready.
After an ordinary clean domain purge, the next selected-schema query can load it again.
Avoid repeated load/purge cycles with a recorded capacity refusal until resources change.

One-time means once per retained complete domain with compatible definitions in that process.
A restart starts Unloaded; readiness is not an authoritative disk or cloud record.
Schema removal releases its references; shared domains remain while another schema or active call requires them.

## Ranges samples and counts

A ready range finds its hash, seeks its first range key, then walks the required ordered candidates in memory.
The seek depends on keys under that hash, not on keys in other schema hashes.
Filters can require more candidates and additional value fetches.
Lookahead, cursor endpoints, source precedence, and final count rules remain unchanged.

An ordered sample under one hash uses the same complete keys.
Whole-schema preview can use all ready domains and the existing stored order, with its maximum of 10.
Preserve range-major order and stored-hash ties for HashRange preview.
Do not promise constant work for global preview or add another persistent order structure.
If fallback membership differs, resolve it from its own complete domain before the source decision.
The record-envelope R membership is one such source when a call requires it.
The final program includes that source and passes the ordinary record-envelope requests.
An empty domain needs no per-key missing-envelope reads.
A nonempty envelope source needs its complete candidate domain before memory selection can replace disk discovery.

Keys supply candidates, not the results of value filters.
Fetch unknown tips, document-copy state, predicate fields, and sort fields in batches before the final result.
An exact key count can use complete membership only where its visibility contract matches.
Content-level exclusions and sample behavior remain the same.
A random sample needs a separate API decision.

## Current evidence and remaining proof

The final program passes the generated 256-record read, update, insert, delete, key move, flush, and reopen workloads.
The selected 20,000-key workload passes, including the capped preview that exceeds the disk selector's current resource allowance.
Concurrent first calls share two recorded domain loads.
A separate 4,096-byte allowance records DiskOnly and correct disk fallback without repeated load attempts.
A separate 128-path cap purges values while complete keys stay Ready without another load.
The [final results](lastdb-schema-range-preload-results.md) preserve source identities, full responses, costs, size, and regressions.

The extra ordinary requests cover Hash-only reads, record envelopes, pages, bounds, a filter, samples after deletes, and physical LastDB compaction.
The concurrent-load requests return coherent old results, followed by correct current and post-flush results.
Those requests start while Loading, but their acks arrive after Ready.
Thus publication before the load ends remains unobserved.
The wider writer and reader inventory remains incomplete.

Preload reduces repeated range disk work but does not give a general elapsed-time improvement over the final disk selector.
At 20,000 keys, it adds about 0.6–0.8 seconds to the first query in the recorded observations.
Repeated 64-key and 256-key ranges take more time with preload on than off.
The recorded tradeoffs support an opt-in policy, not a default cutover claim.

## Required product evidence

Use fresh synthetic homes and actual product writes and queries.
Keep first-use time separate from later range selection, tip fetch, record fetch, and response work.
Measure the existing engine, the fallback disk route, and the chosen preload route under matched conditions.
Include schemas with many hashes, keys above 10,000, empty membership, long OPE keys, and byte-cap refusal.
Preserve source and binary identities, full response receipts, resource costs, and failures.

Cover concurrent first queries, writes during load, post-load writes, flush, LastDB compaction, purge, restart, and logical replacement.
Establish that later ready ranges perform no disk key discovery.
Establish that unknown values still fetch correctly and current writes appear before their flush.
Use non-test format, syntax, lint, build, and operational checks.
Stop owned synthetic nodes and use supported exact-path cleanup after receipt preservation.

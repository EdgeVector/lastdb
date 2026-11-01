# LastDB key selection study

> **Model note (2026-11-01).** This page was written for the field key model:
> schema → field → field key → record → file. Since home format 7 (the record model)
> a schema stores one record per primary key, and separate field-key rows and tips do not exist. The [canonical model](lastdb-canonical-model.md) wins where this
> page differs.

Status: historical alternatives study, 2026-10-17. Its default-range recommendation is superseded.
The selected [preload design](lastdb-schema-range-preload-design.md) loads complete schema keys before the first query continues.
Later ready ranges and sample select keys from the logical set.
A0 remains the bounded initial-load source and fallback, rather than the normal ready-range key source.
The [logical design](lastdb-logical-warm-set-design.md) and [work plan](lastdb-logical-warm-set-plan.md) define that current direction.
The earlier source findings, alternatives, and product measurements below remain unchanged.

## Finding

A partial field key cannot determine every key in a range.
The core model should expose complete logical operations through a read service.
Its warm objects can remain partial.
The service obtains authoritative keys before it resolves their values.

Keep the existing field key and record types as logical objects.
Put disk fetches and completeness decisions in the engine's read service.
Existing field rules operate on the selected objects, rather than an unrestricted partial field key.

For ranges and sample, start with the existing disk structure plus captured writes that await flush.
Read small current tip records, then reuse warm records and batch-fetch the missing records.
An exact call already supplies its keys. It does not need range selection.
An exact HashRange pair also supplies a complete key.
Trust its current known memory state; fetch only unknown tips or required values.
No routine disk validation follows a known exact result.

The extra completeness step belongs to range and sample key discovery.
A hash alone in a HashRange field key names a range, so it requires discovery.
Pages and counts over that range inherit the discovery step.
A count over supplied exact keys uses the exact route.
Filters over explicit keys use those keys without range enumeration.
Both routes then use the same logical warm set and batch fetch boundary.

This removes the initial need for retained range-completeness facts, empty-gap facts, and saved sample windows.
It preserves a complete logical answer without a complete field key in memory.
Repeated ranges still pay for small tip selection. That cost needs measurement.

## Alternatives

| Option | Key selection | Main benefit | Main cost |
|---|---|---|---|
| A0: bounded tip pages | Disk returns keys and small current tip records; merge captured pending changes. | Reuses existing storage; avoids a second selected-tip lookup. | Every range reads tip metadata, even with warm records. |
| A1: directory keys first | Disk returns keys and temporary locations; warm tip lookup precedes a missing-tip batch. | Can avoid tip-body reads when tips remain warm. | Cold calls add another dependent stage; bare keys do not establish current visibility. |
| B: retained complete intervals | Reuse a partial field key only where retained facts establish complete membership. | A fully covered range can avoid disk key selection. | Writes, purge, empty gaps, generations, and metadata need precise fact maintenance. |
| C: logical ordered cursor | One seek/next interface hides disk selection, pending changes, and batch resolution. | Callers use the core model without storage details. | The interface alone removes no I/O. A second pager or persistent tree adds a separate engine. |

Use C as the interface for A0.
A0 and C are compatible choices at different levels.
Compare A1 after A0 supplies an instrumented reference.
Defer B until measured key-selection cost justifies its additional state.

The ordered field key can still organize pending changes.
It is complete for the accepted changes that await flush, even when clean warm records are partial.
Its ordered references name the existing current slots; they do not contain another copy of the records.

## Read sequence

1. Capture schema definitions, disk state, and accepted changes under one publication boundary.
2. Select keys and current tips from that disk state and the captured changes.
3. Merge inserts, deletes, replacements, and key moves before the page limit.
4. Check each selected logical path in the warm set.
5. Collect unresolved tips and records across the selected fields.
6. Fetch one batch per dependent stage and storage scope.
7. Run predicates and final page rules on the authoritative selected values.
8. Return one complete answer after the common boundary check.

A per-key logical fetch does not require a separate physical request.
The service preserves each result slot while it combines the disk work.
It does not await a disk request for each key.

The baseline uses bounded tip pages, rather than a whole-hash list.
A selected tip carries the canonical key, logical slot, actual source, RecordEntry, KeyMetadata, order token, and record placement hint.
Its continuation names the examined position. Its exhaustion flag distinguishes a completed interval from incomplete work.
Exhaustion includes pending keys after the last disk key.
A pending delete can require more disk candidates before the selected page is full.

The actual source matters when live and generation records coexist.
A reconstructed API key cannot identify which stored record won.
A warm record must match that selected slot association at the captured boundary.
A matching content UUID alone does not prove that the full record metadata matches.

## Current writes and flush

Disk alone omits an insert that receives an ack before flush.
Disk alone can also return a deleted key.
The selector therefore merges all relevant captured changes after the disk state's covered logical change number.

Publish the disk root and its covered logical change number together.
A call sees either the old root with its required changes or the new root that contains those changes.
It must never see the old root after those changes disappear.

A replacement larger than the logical path budget needs a typed root transition.
Prepare its disk state before visibility. Include the accepted prefix or complete its flush first.
Publish the new root, covered logical number, and schema/generation controls atomically.

Invalidate affected clean slots before the ack.
Active calls retain their old root and required immutable entries, or retry.
This avoids an over-cap per-key pending batch.

The physical file frontier and logical publication number use different units.
The adapter must not compare them as the same counter.
The present queue completion signal also follows delivery work; it is not the atomic disk-root publication.

Retain immutable pending entries and values for an active call.
A later flush cannot remove the only copy that an older captured root needs.
Use bounded whole-call retries if a logical publication changes during the call.
Do not force a flush before every range.

This design retains current state only.
Temporary read references do not add an old-value API.

## Use-case results

| Call | Complete-answer rule |
|---|---|
| Exact key or exact batch | Use current known paths; fetch unknown tips and required records in batches. |
| Fixed-hash range or prefix | Merge complete disk streams and pending changes for the named hash and bounds. |
| Empty range | Require authoritative exhaustion. Zero warm keys prove nothing. |
| Page | Continue through N+1 qualifying records or exhaustion; keep the examined and public cursors separate. |
| Predicate | Fetch unknown predicate fields before a decision; select document-copy precedence first. |
| Count | Examine the complete requested domain; fetch values only where visibility or predicates need them. |
| Value order | Inspect the complete candidate domain unless key order already provides the requested order. |
| Ordered sample under one hash | Use a capped first-page selection with the existing sample rules. |
| Explicit schema preview | Preserve its primary-source fallback, stored order, and maximum of 10. Never sample only warm records. |
| Purge | Remove a clean logical path; later ranges still obtain complete membership from the authoritative selector. |
| Cloud-only domain | A local miss stays unknown until an authoritative source establishes absence. |

Whole-schema HashRange preview remains a special cost.
Its required order is range-major, while the existing disk structure is hash-major.
A bounded size-10 selection can still examine many membership records.
Do not add another global disk directory without a separate design decision.
Return a work-limit error when complete selection exceeds the budget.

## Source findings

| Source | Finding | Change for A0 |
|---|---|---|
| [Tip page reader](../fold_db/crates/core/src/db_operations/record_store/filter/partition_page.rs) | Existing bounded live/generation streams precede record fetches. They return vectors without explicit exhaustion or the complete winner source. | Expose a selected-tip page, continuation, exhaustion, and actual source. Add delete-barrier resolution and refill. |
| [Exact candidate loader](../fold_db/crates/core/src/db_operations/record_store/filter/per_key/load.rs) | It collects candidate keys but discards the actual winning encoded key. | Preserve the winner through value resolution. |
| [Field record batches](../fold_db/crates/core/src/db_operations/field_read/fetch/batch.rs) | It checks selected paths and collects record misses before one batch. | Reuse this boundary with complete selected-tip records and one read view. |
| [Native row reader](../fold_db/crates/core/src/storage/disk_pack/rows.rs) | It merges a captured raw write view with the held disk cursor. | Preserve this behavior; later replace raw records with typed pending references. |
| [Publication](../fold_db/crates/core/src/storage/disk_pack/view.rs) and [queue](../fold_db/crates/core/src/storage/disk_pack/queue.rs) | Disk root, accepted state, and queue completion exist, but no explicit root-to-covered-logical-number pair exists. | Define and publish that pair before removal of the raw fallback. |
| [Key-only trait default](../fold_db/crates/core/src/storage/traits/kv.rs) | It loads records before it discards their values. | Do not claim A1 from this method. Prevent unbounded materialization before a page limit. |
| [Native range reader](https://github.com/EdgeVector/lastdb/blob/2f4ba44bd0688b0c0361db325d037524d8bb243a/vendor/laststore/src/disk_pack/hash_range.rs) (disk format v1, removed) | It reads each selected record body. A field key record body is a small tip, not the record payload. | This is valid A0 work. Measure a real directory-only route separately as A1. |
| [Filtered key list](../fold_db/crates/core/src/db_operations/record_store/filter/per_key/list.rs) | Blinded-hash API-key recovery can fetch primary records serially. | Avoid this route; collect required recovery reads into a batch. |
| [Directory decision](../fold_db/crates/core/src/db_operations/record_store/helpers.rs) | The derived secondary directory remains disabled. | Preserve that decision during the first study. |

## Related engine patterns

RocksDB presents an ordered iterator over a consistent point-in-time view.
Its iterator owns resources that keep that view valid.
This supports the read-boundary and cursor pattern; LastDB does not need RocksDB's history API.
Source: [RocksDB Iterator](https://github.com/facebook/rocksdb/wiki/Iterator).

RocksDB's iterator implementation combines ordered memory and disk sources behind one logical iterator.
This supports the proposed merge of pending changes with disk selection.
It does not establish which LastDB implementation is fastest.
Source: [RocksDB Iterator Implementation](https://github.com/facebook/rocksdb/wiki/Iterator-Implementation).

SQLite places a B-tree interface above disk pages and a pager.
This illustrates a logical ordered structure whose complete contents need not reside in memory.
A retained page cache would change LastDB's current storage-object retention rule.
This study proposes no engine or API replacement.
Source: [SQLite Architecture](https://www.sqlite.org/arch.html).

These are structural precedents. They are not LastDB benchmark evidence.

## What the existing receipts establish

The [spike report](lastdb-core-model-warm-spike.md) preserves the initial paired comparison and the later candidate-only replay.
The final replay contains these warm cases:

| Case | Request median | Read operations | Payload bytes |
|---|---:|---:|---:|
| 16 exact keys | 4.335 ms | 42 | 33,510 |
| 16 range keys | 3.569 ms | 51 | 33,510 |
| Four Hash queries in one batch | 6.558 ms | 13 total | 1,447 total |

Each exact/range call records 80 core hits and zero misses.
Each Hash batch records 24 core hits and zero misses.
Warm records coexist with other disk work.

These measurements do not isolate key selection, schema work, document copies, decode, or response construction.
They cannot rank A0, A1, B, and C.
The final replay has no matched final baseline and no controlled operating-system cache state.
Do not predict request-time gains from read-operation counts alone.

## Next product studies

Use ordinary product calls and fresh synthetic homes.
Add no tests, fixture harness, or copy of an existing home.

1. Attribute request cost before another redesign.
2. Implement bounded A0 behind the logical cursor and common read boundary.
3. Compare a real A1 path with A0 under matched conditions.
4. Study B only if key selection remains a material repeated-range cost.

Record schema resolution, controls, document-copy resolution, key selection, warm lookup, missing-tip fetch, missing-record fetch, predicates, count, and response cost separately.
Use per-call counters when calls overlap.
Also record examined keys, selected keys, hidden candidates, pending overrides, decoded tips, distinct records, batches, retries, refusals, queue size, and retained memory.

Use 1, 16, 64, and 256 keys across exact calls and fixed-hash ranges.
Include empty bounds, overlapping ranges, pages, value predicates, shared records, and omitted sort fields.
Include unrelated writes, inserts, deletes, key moves, reads before flush, reads during flush, and reopen.
Compare hot and cold values, several field counts, and data below and above the logical path cap.
Measure explicit preview separately as the source size grows.

Alternate matched before and after product runs.
Preserve source and binary identities, complete response evidence, request times, ack times, flush times, and failures.
Report memory and write costs beside read gains.
Stop owned nodes and use supported exact-path cleanup after receipt preservation.

| Hypothesis | Evidence that can disprove it |
|---|---|
| A0 bounds dense, unfiltered first/keyset pages without exact total count. | Tip work grows with the entire hash before that small page returns. |
| A0 reuses warm values. | A selected warm slot still fetches its record without a changed association. |
| A1 saves useful work. | Its extra stage costs more than the avoided tip reads. |
| B merits its added state. | It supplies little request-time gain or adds material write, memory, or purge cost. |
| The cursor stays simple. | It adds another durable directory, pager, or retained storage-object cache. |
| Every option preserves the logical answer. | Keys, values, metadata, counts, or page flags differ at the same captured state. |

A selective predicate, many hidden keys, a large offset, or exact total count can require broad work.
Measure those costs separately from dense first-page selection.

## Recommendation

Use an authoritative logical selector with bounded tip pages as the next baseline.
Keep the existing core field key and record types for selected values and pending changes.
Keep least recently used references for clean logical records.
Do not retain clean-range completeness facts or saved sample windows in this baseline.

The next code step is cost attribution and the read-view/selected-tip contract.
It precedes broad route conversion or a new memory B-tree.
Choose further complexity from matched product evidence.

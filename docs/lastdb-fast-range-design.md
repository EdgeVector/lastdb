# Fast disk keys for schema preload and fallback

Status: the disk changes are built in the development branch, 2026-10-17. Final product measurements follow.
The [schema key preload study](lastdb-schema-range-preload-study.md) adds synthetic memory and native disk-component measurements.
The selected [preload design](lastdb-schema-range-preload-design.md) uses complete keys with a separate byte cap.
The [component receipts](https://github.com/EdgeVector/lastdb/tree/0b0a79d4c10949e2f0570e82d9bc4fdb9513a285/docs/proofs/schema-preload-product-20261017/disk-component/) cover packed keys, journal changes, compaction, and reopen.

The [logical design](lastdb-logical-warm-set-design.md) remains the common read and publication contract.
This document selects a disk-format target for the first schema load and the fallback disk route.
The [calculation receipt](https://github.com/EdgeVector/lastdb/blob/0b0a79d4c10949e2f0570e82d9bc4fdb9513a285/docs/proofs/fast-range-design-20261017/calculations.json) records assumptions and results.
The [calculation source](https://github.com/EdgeVector/lastdb/blob/0b0a79d4c10949e2f0570e82d9bc4fdb9513a285/docs/proofs/fast-range-design-20261017/calculate.py) uses arithmetic only. It does not call LastDB.

## Recommendation

Use complete logical range keys after the selected schema's first-query preload.
Use bounded current-tip pages for that initial load and for unselected or refused schemas.
Batch tip payload reads, then remove per-key page waste for ordinary OPE keys.
Keep the existing atom references, encrypted tip payloads, shared atoms, and file packing.

Load every required hash and membership source for a selected schema once before its first query continues.
Concurrent queries share that load.
The logical owner keeps complete keys and sparse tips and atom values.
The full logical-record cap remains 10,000, with a separate explicit byte allowance for complete keys.
The earlier tip-retaining preload option is superseded.

A ready range selects keys from memory and uses the common batch resolver for unknown tips and values.
Only the loader and fallback selector perform disk key discovery.
Both sources use the same core field rules, current-write publication, and read boundary.

## Reference engine costs

These costs describe the committed reference engine before this change.
The new format packs ordinary complete keys through 1,024 bytes into tree pages.
It retains the bounded external path for larger keys.
Range reads now send selected locations to the existing batch reader.

| Reference fact | Source |
|---|---|
| Pages are 16,384 bytes; their header is 128 bytes. | [Format constants](../vendor/laststore/src/disk_pack/mod.rs) |
| A locator leaf holds at most 53 key/location entries. | [Format constants](../vendor/laststore/src/disk_pack/mod.rs) |
| Each descriptor stores at most 128 key bytes directly. | [Key output](../vendor/laststore/src/disk_pack/tree_output.rs) |
| Each longer key gets its own page or page chain for its remaining bytes. | [Key output](../vendor/laststore/src/disk_pack/tree_output.rs) |
| OPE emits eight hexadecimal characters for each original range byte. | [Range codec](../fold_db/crates/core/src/crypto/e2e.rs) |
| The blinded hash has 22 characters. | [Hash codec](../fold_db/crates/core/src/crypto/e2e.rs) |
| Range traversal reads each selected tip immediately. | [Range reader](../vendor/laststore/src/disk_pack/hash_range.rs) |
| Exact batches already order blocks and combine adjacent frames up to 64 KiB. | [Batch reader](../vendor/laststore/src/disk_pack/batch.rs), [file reads](../vendor/laststore/src/disk_pack/batch_io.rs) |
| Each call reuses its decoded blocks and pages, then releases them. | [Read call](../vendor/laststore/src/disk_pack/call.rs), [page reader](../vendor/laststore/src/disk_pack/page.rs) |

With a 43-character molecule identity and no storage-scope prefix:

```text
stored key bytes = 70 + 8 × original range bytes
```

The 70 bytes include the molecule, blinded hash, separators, and key prefix.
A longer molecule identity or storage prefix increases the result.

| Original range bytes | Stored key bytes | Current extra allocation per key |
|---:|---:|---:|
| 4 | 102 | None |
| 8 | 134 | 16 KiB |
| 13 | 174 | 16 KiB |
| 24 | 262 | 16 KiB |
| 36 | 358 | 16 KiB |
| 128 | 1,094 | 16 KiB |

For example, a 13-byte range produces a 174-byte key.
The descriptor holds 128 bytes. A separate 16 KiB page holds its remaining 46 bytes.
A read of 256 distinct such keys therefore needs 4 MiB of key-tail pages alone when those pages are cold.

This is a format calculation.
It does not establish the cause of any earlier benchmark result.
The earlier receipts do not isolate key-tail reads.

## Disk design

### First change: batch already selected tip locations

Keep the present format for the first read-path change.

1. Seek each configured file group to the requested start.
2. Collect a bounded page of selected keys and their existing Locations.
3. Send those Locations to the existing block planner.
4. Reuse decoded blocks and combine adjacent physical frames.
5. Resolve and validate tips in their original logical order.
6. Merge captured accepted changes before the final page decision.
7. Check selected values in memory and batch-fetch missing atoms.

Do not turn the selected keys into another exact tree lookup.
The range cursor already knows their Locations.

The existing serial path already shares blocks and file handles.
The added benefit is physical order and adjacent-frame reads.
Scattered journals can still require separate reads.
Compaction already copies each file group's current records in key order.
Preserve that order and measure its range benefit.

### Format target: pack ordinary full keys inside the existing tree pages

Use one new packed-key page format in the current locator tree.
Keep the directory's fixed key representation.
Add no second range file, derived directory, or retained storage-object cache.

A locator leaf contains a small slot array and variable-length entries.
Each entry stores the entire ordinary OPE key beside its existing 128-byte Location.
Pages remain immutable. A rewritten page is packed again from its live entries.
No in-page free list or stable slot address is required.

The initial leaf sizing model is:

| Part | Bytes per entry |
|---|---:|
| Offset/length slot | 4 |
| Length, flags, and reserved fields | 8 |
| Full key digest | 32 |
| Existing Location | 128 |
| Entire stored key | Variable |
| Total | Stored key bytes + 172 |

Internal locator pages use variable-length separators and existing child references.
The cursor keeps its current ancestor stack.
Use one validated accessor for entry offsets, key bytes, digest, and value bytes.

Set the first version's ordinary-key limit to 1 KiB.
The 174-byte and 358-byte examples fit.
Larger keys retain explicit bounded overflow support.
The 1,094-byte example remains an overflow case in this first version.

Its 256-key dense model uses six leaves plus 256 tail pages, or 4,192 KiB.
That is slightly worse than the current 4,176 KiB model.
Do not claim a range gain for this long-key case without a further overflow-layout improvement.

Use an explicit external-key entry for that fallback.
It retains a 128-byte prefix, full key length, digest, and a 16-byte overflow reference.
The reference names the first key-tail page, tail length, and reserved format fields.
Follow the existing bounded chain and verify the full key before comparison or return.
The modeled fallback leaf entry is 316 bytes, including its slot and Location.

The same 1 KiB inline limit applies to internal separator keys.
An ordinary internal entry uses at most 1,076 bytes, including its slot and child reference.
At least 15 maximum-size separators fit in a page.
The editor must preserve the non-root minimum of seven separators while it splits by bytes.
Retain the eight-level limit and reject an over-height result before publication.
Do not raise the inline limit without a new capacity calculation.

Split pages by encoded bytes and validate minimum occupancy and height admission.
Do not reuse the old 53-entry split rule.
Account for slot arrays, maximum key size, and internal separators before publication.
A rejected layout returns a resource error before it becomes visible.

A wider fixed 256-byte key area is a smaller alternative.
It fits the 174-byte example, but moves the overflow threshold to a 24-byte unscoped range.
It still fails the 36-byte example.
The packed-key target handles both without a fixed unused key area for every entry.

Prefix compression can reduce repeated molecule/hash bytes later.
Do not include that optional saving in the first calculation.
Do not change the OPE algorithm as part of this layout work.

### Keep tip values separate initially

The packed-key target retains the current Location and encrypted payload bytes.
It removes the key-tail read for ordinary keys.
It does not remove the separate tip payload read.

A representative tip with author fields is 318 bytes before encryption.
Without compression, the v1 binary form is 352 bytes; the v1 base64 form is 468 bytes.
Custom metadata can increase those sizes substantially.

The current 128-byte Location slot cannot hold that full encrypted tip.
Inline tip values require a further value format and dependency-walk change.
Defer that work until the measured tip payload cost justifies it.
Do not put plaintext tips into pages that currently contain only keys and locations.

## Disk calculations

The following model uses one cold ordered stream and full leaves.
It excludes directory/branch seeks, tip payloads, atom payloads, and file-frame headers.
It also excludes boundary pages, tombstones, filters, and next-page lookahead.

For a 13-byte original range:

```text
current key entries per leaf = 53
proposed key entry = 174 + 172 = 346 bytes
proposed entries per leaf = floor(16,256 / 346) = 46
current key pages = ceil(K / 53) + K overflow pages
proposed key pages = ceil(K / 46)
```

| Examined keys | Current key-page bytes | Proposed key-page bytes |
|---:|---:|---:|
| 16 | 272 KiB | 16 KiB |
| 64 | 1,056 KiB | 32 KiB |
| 256 | 4,176 KiB | 96 KiB |
| 1,024 | 16,704 KiB | 368 KiB |

For 256 keys, the modeled key-page bytes fall by a factor of 43.5.
That factor is not a complete-query speedup.

Partial page occupancy increases the cost:

| Same occupancy assumption for both formats | Current, 256 keys | Proposed, 256 keys |
|---|---:|---:|
| 100% | 4,176 KiB | 96 KiB |
| 75% | 4,208 KiB | 128 KiB |
| 50% | 4,256 KiB | 192 KiB |

A 36-byte range produces a 358-byte key.
The proposed entry is 530 bytes, so a full leaf holds 30 entries.
Its 256-key model needs nine leaf pages, or 144 KiB, instead of per-key tail pages.

One user hash can use up to 16 file groups.
For each group, include the directory and tree seek plus its boundary pages.
The whole hash can therefore need more pages than these one-stream examples.
Do not report six pages as the complete query's total read count.

Missing atom values add their own exact lookup, block, and decode work.
The key-page improvement remains useful when all selected atoms are warm.
It does not make cold atoms free.

### Time model

A simple cold serial model is:

```text
key-stage time ≈ read count × fixed read cost + transferred bytes / transfer rate
```

For illustration only, use 100 microseconds per read and 500 MiB/s transfer.
The dense 256-key model gives about 34.3 ms before and 0.79 ms after for this stage.
These inputs are arbitrary scenarios, not measurements of this machine.
They omit tree seeks, cache effects, CPU work, payloads, and parallel reads.
Use the receipt's sensitivity cases rather than treating these times as a prediction.

## Selected schema key preload

Use the complete key part and first-query gate in the [preload design](lastdb-schema-range-preload-design.md).
The loader resolves current membership through bounded disk pages plus accepted changes.
It retains only keys and essential membership flags, not a full tip and atom for each key.
After atomic readiness publication, ranges select from the logical keys.

The first query intentionally pays for all required schema keys across all hashes.
Later narrow ranges do not repeat that whole-schema work.
An exact query to a selected schema also passes the gate once, then uses direct exact value resolution.
Unselected and explicitly refused schemas keep the bounded fallback selector.

The [memory study](lastdb-schema-range-preload-study.md#measured-memory-cost) measures compact key structures from 10,000 to one million entries.
The complete key byte allowance is separate from the 10,000-full-path cap.
The first implementation uses 128 MiB as a configurable initial measurement setting.
A schema that does not fit selects DiskOnly with a reason rather than another failed preload on each query.

Ordinary writes maintain ready membership before their ack.
The loader retains concurrent membership changes until readiness or explicit cancellation.
Tip and atom purge preserves keys; whole-domain key purge clears readiness first.
Physical flush and LastDB compaction preserve readiness when logical membership stays the same.

The earlier 5,000-tip preload option is superseded.
Its 1 KiB-per-path assumptions remain in the [arithmetic receipt](https://github.com/EdgeVector/lastdb/blob/0b0a79d4c10949e2f0570e82d9bc4fdb9513a285/docs/proofs/fast-range-design-20261017/calculations.json) as historical calculations.
Those figures do not estimate bare key memory.
Do not implement a second tip-retaining preload policy.

## Writes, sharing, and cloud

Keep batch tree edits. Each touched old page is copied once per flush batch.
New packed-key pages change space quotes and split decisions.
Measure changed pages, flush bytes, and ack cost beside read gains.

The key format does not change atom UUIDs or protein links.
Several molecules can retain references to the same atom.
Do not place a copy of the atom payload in each range entry.

Write the new pages before atomic root publication.
Keep old-root file leases until active calls finish.
Add a required format feature so unsupported readers reject the new pages.
Update validators, accessors, editors, compaction, dependency walks, and backup readers together.

Snapshots still name completed disk state and referenced files.
Keep encrypted tip and atom bytes identical across local and cloud copies.
Memory completeness flags are not authoritative cloud data.

Count changed upload parts as well as changed pages.
A 16 KiB page change does not imply a 16 KiB cloud upload.
The current tree uses multi-page part files; upload granularity can increase that cost.
A key-page format change does not itself change the semantic mutation log.

## Execution order and product evidence

1. Define membership authority, the common logical boundary, and the first-query schema gate.
2. Build a correct bounded loader and typed key publication on the current format.
3. Batch already selected tip locations with the shared block reader.
4. Build the packed-key page format and all required readers/writers.
5. Compare first-use loads, ready ranges, writes, and fallback under matched conditions.
6. Evaluate inline encrypted tips only if their remaining cost remains material.

Use fresh synthetic homes for both versions.
Use actual product writes and reads, with no tests or fixture harness.
Do not use the primary, its archive, or an existing home.

Include original range lengths 4, 8, 13, 24, 36, 64, and 128 bytes.
Include scoped keys, one and several file groups, mixed metadata sizes, and data above the path cap.
Measure insert, overwrite, delete, key move, flush, compaction, reopen, and concurrent calls.
Preserve returned values, order, metadata, page flags, counts, failures, and source/binary identities.

Report the key-stage gains separately from full-query latency.
Report write, memory, and cloud-part costs beside the read results.
Stop owned nodes and use supported exact-path cleanup after receipt preservation.

## External design references

RocksDB stores ordered key/value pairs in data blocks and uses a separate block lookup.
This supports grouped ordered reads as a general design pattern.
It does not establish LastDB's expected speed.
Source: [RocksDB block format](https://github.com/facebook/rocksdb/wiki/Rocksdb-BlockBasedTable-Format).

WiredTiger documents the extra reads for overflow keys and the space cost of large allocation units.
It also describes prefix compression and page-size tradeoffs.
These support the problem analysis, not the arithmetic inputs for LastDB.
Source: [WiredTiger page size and compression](https://source.wiredtiger.com/develop/tune_page_size_and_comp.html).

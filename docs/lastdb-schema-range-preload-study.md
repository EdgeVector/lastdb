# Complete range keys for selected schemas

Status: source review and synthetic component measurements, 2026-10-17.
This document preserves the earlier component study.
The branch now contains the complete-key loader and read routes.
The [product receipts](https://github.com/EdgeVector/lastdb/tree/0b0a79d4c10949e2f0570e82d9bc4fdb9513a285/docs/proofs/schema-preload-product-20261017/) keep their separate before and after measurements.
Tom selected the [first-query preload design](lastdb-schema-range-preload-design.md) after this study.
The measurements below remain component evidence, not a complete preload result.

## Recommendation

Preload complete range keys for selected schemas as part of the core logical model.
Keep the atom values and ordinary tips under the existing 10,000-logical-record warm cap.
Give the complete key part its own explicit byte cap.
The separate allowance is part of the selected design; it does not change the installed node.

Use one mutable ordered key structure for each authoritative membership source and key layout.
It is the key part of the logical owner, not another store of current field values or saved query answers.
An initial B-tree avoids a full-array shift for each new key.
Compact key chunks remain a later option if the tree's memory cost matters.

The first query to a selected HashRange schema starts or joins one shared complete preload and waits.
This includes its first exact query; exact value resolution remains direct after the gate.
Unselected or capacity-refused schemas use the bounded disk route.
Select keys from the complete set, reuse known warm tips and atoms, then fetch all missing paths in batches.
Do not preload every declared schema or every field.

## What the old size establishes

Tom estimates the old home at 16–20 GB.
A saved October 8 report records a 19.5 GiB whole home and a 12.5 GiB data directory.
It records 6.72 GiB of tips, 2.75 GiB of atoms, and 29,273 data files.
Its status says measured sample, not census.
Source: saved report, lines 350–356.

That size includes files and values that a range-key preload does not need.
It does not establish a count of distinct range keys for each schema.
A September 26 summary's 17.16 million tips also does not establish that count.
One record can have many field tips and several protein key layouts.
Source: saved tip summary.

The estimates below use explicit synthetic key counts.
They are not a census of the old home.
No primary home or archived home supplies data for this study.

## How the apps use ranges

| App source | Range use | Size input |
|---|---|---|
| Brain | RecordListEntry uses type as hash and slug as range. | A 40-byte slug is one useful scenario. |
| Brain tags | TagMembership uses a tag-derived hash and type:slug as range. | Each tag membership adds another key. |
| F-Kanban | BoardCards uses board as hash and column#position#slug as range. | A four-byte column and 40-byte slug produce 54 range bytes. |
| F-Kanban | MilestoneCards uses milestone as hash with the same range shape. | A card in both layouts needs two membership entries. |
| Routines | RoutineRunSummaryV2 uses routine ID as hash and a 24-character timestamp as range. | The source sets retention defaults to 100 runs and 30 days. |
| LastGit | Pack uses repo as hash and a SHA256 text value as range. | The range has 64 bytes, even if the pack file is large. |

BoardCards declares 24 fields.
The range selector can use one authoritative membership set; it does not need 24 copies of the keys.
Different memberships still count separately, including tags, graph links, and protein key layouts.
A field with different membership cannot reuse another field's completeness claim.

Source snapshots are code evidence, not proof of the installed app state:

- Brain: RecordListEntry, type/slug key, tag key.
- F-Kanban: BoardCards fields and key, range construction, position width.
- Routines: EdgeVector/routines commit `86c320b09f4a919754832f4c6bf05f9438547f25`, `src/publish-status.ts:449–450,1009–1015` and `src/runner.ts:103–104`.
- LastGit: Pack and Ref declarations, commit `165d3dae614507a16b72434b4a934cff1dc5e04f`.

## Measured memory cost

The current OPE text uses eight hexadecimal characters for each original range byte.
The same ordered words need four bytes in a lossless binary form.
Use big-endian words to preserve the stored order.
This changes the memory representation, not the OPE algorithm or its protection.
Store the common hash and molecule identity once per domain where possible.

The standalone prototype measures a mutable B-tree of binary range keys.
It does not retain AtomEntry objects or atom values.
The memory column is the median peak size of a complete fresh process, with no empty-process subtraction.
It does not measure the extra memory inside an integrated LastDB process.

| Keys | Original range bytes | Example shape | Peak process size | File read plus tree construction |
|---:|---:|---|---:|---:|
| 10,000 | 24 | Routine timestamp | 3.20 MiB | 1.30 ms |
| 10,000 | 40 | Brain slug scenario | 3.86 MiB | 1.74 ms |
| 100,000 | 24 | Routine timestamp | 16.27 MiB | 15.02 ms |
| 100,000 | 40 | Brain slug scenario | 22.47 MiB | 19.90 ms |
| 100,000 | 54 | Board range scenario | 28.66 MiB | 26.67 ms |
| 1,000,000 | 40 | Brain slug scenario | 208.52 MiB | 243.01 ms |

These are key-structure measurements, not complete LastDB preload times.
The generated input files remain in the operating-system cache.
The runs exclude current-tip resolution, encryption, schema rules, pending writes, core admission, and concurrent calls.
Real schemas also need hash/domain metadata and variable key lengths.
The source uses five samples per case through 100,000 keys, and three at one million.

A compact fixed-width byte array uses less memory but has a different update cost.
At 100,000 board-shaped keys, binary tree memory is 28.66 MiB; hexadecimal tree memory is 50.97 MiB.
The binary array uses 22.25 MiB and loads in 1.78 ms under the same cached-input conditions.
Those array results do not justify a flat mutable array for production writes.

Full methods and raw receipts: [representation report](https://github.com/EdgeVector/lastdb/blob/0b0a79d4c10949e2f0570e82d9bc4fdb9513a285/docs/proofs/schema-range-preload-20261017/representation-report.md) and [case summary](https://github.com/EdgeVector/lastdb/blob/0b0a79d4c10949e2f0570e82d9bc4fdb9513a285/docs/proofs/schema-range-preload-20261017/representation-summary.json).

## App-shaped estimates

For a rough mutable-tree budget, use `N × (4R + 64)` bytes.
Here N is the key count and R is the average original range length in bytes.
The 64-byte allowance approximates the measured tree overhead; it is not a measured core-object size.
Add per-domain metadata and a 25–50% reserve for size variation and active work.
Many small hashes can require much more metadata than one large hash.

| Synthetic schema scenario | Assumed keys | Assumed R | Mutable key estimate before reserve |
|---|---:|---:|---:|
| Brain RecordListEntry | 50,000 | 40 | 10.68 MiB |
| BoardCards | 10,000 | 54 | 2.67 MiB |
| MilestoneCards | 10,000 | 54 | 2.67 MiB |
| 100 routines, each with 100 run summaries | 10,000 | 24 | 1.53 MiB |
| LastGit Pack | 100,000 | 64 | 30.52 MiB |
| Total of these selected memberships | 180,000 | Mixed | 48.07 MiB |

The total becomes about 60–72 MiB with the stated reserve, before extra per-domain metadata.
This scenario excludes other schemas, tags, graph memberships, and unknown real key distributions.
A 16–20 GB database can fit this scenario or a much larger key count.
The total database size alone cannot select the right scenario.

For one million 40-byte ranges, the tree prototype uses about 209 MiB before real core costs.
A 50% reserve brings that estimate to about 313 MiB.
A 128 MiB cap therefore cannot promise a complete preload for that case.

Calculation source and assumptions: [arithmetic source](https://github.com/EdgeVector/lastdb/blob/0b0a79d4c10949e2f0570e82d9bc4fdb9513a285/docs/proofs/schema-range-preload-20261017/calculate_estimates.py), [results](https://github.com/EdgeVector/lastdb/blob/0b0a79d4c10949e2f0570e82d9bc4fdb9513a285/docs/proofs/schema-range-preload-20261017/estimates.json).

## First schema use: the disk cost matters

The current disk format gives each of these long keys a separate 16 KiB key-tail page.
The native disk CLI measures the existing format on new synthetic homes:

| Keys | Original range bytes | Median component time | Key-tail pages | Bytes read |
|---:|---:|---:|---:|---:|
| 1,000 | 24 | 48.58 ms | 1,000 | 16.09 MiB |
| 1,000 | 54 | 49.33 ms | 1,000 | 16.15 MiB |
| 3,000 | 24 | 142.21 ms | 3,000 | 48.16 MiB |
| 3,000 | 54 | 122.62 ms | 3,000 | 48.32 MiB |

These calls use the actual current OPE transform with public synthetic input.
They read plain synthetic tips and construct the CLI result; they do not perform full schema preload.
The files likely remain in the operating-system cache.
Each case uses one file group and three fresh CLI calls.
Every measured response returns the requested key count and explicit exhaustion.

A simple linear extrapolation from the two 3,000-key cases gives these component costs:

| Keys | Extrapolated current component time |
|---:|---:|
| 10,000 | 0.41–0.47 seconds |
| 100,000 | 4.09–4.74 seconds |
| 1,000,000 | 40.9–47.4 seconds |

These are arithmetic extrapolations, not measured complete preload times or a supported single-call capacity.
Larger trees, several hashes, cold device reads, deletes, and schema work can change the result.
Current operational limits also require bounded work and continuation for large loads.
Do not describe the 20 ms tree result as a 100,000-key first-schema-use time.

The [packed-key disk proposal](lastdb-fast-range-design.md#disk-design) removes the ordinary per-key tail-page waste.
It is not implemented or measured here.
Measure its complete key load before a promise of subsecond schema preload.
The memory results establish feasibility; the disk results identify work that precedes a fast first use.

Native CLI source, binary identity, counters, responses, and cleanup: [disk receipt](https://github.com/EdgeVector/lastdb/blob/0b0a79d4c10949e2f0570e82d9bc4fdb9513a285/docs/proofs/schema-range-preload-20261017/disk-component.json).

## Required behavior

1. Choose the authoritative membership source, schema, fields, key layout, and compatible encoding domains.
2. Capture the disk root, covered logical change number, definitions, controls, and required accepted changes.
3. Read bounded key pages and resolve current membership, including delete barriers and generation precedence.
4. Discard tip payloads when only the key remains; do not silently mark its AtomEntry or values known.
5. Preserve accepted changes throughout the load and publish completeness at one checked logical boundary.
6. Keep queries behind the shared load; use disk fallback only for unselected or explicitly refused schemas.

A query over complete keys still resolves unknown tips and predicate fields in batches.
Key count alone does not establish a count after value filters.
An exact known warm record needs no range-key read.
Ordered range selection uses the complete ordered keys; the existing sample comparator and preview cap remain unchanged.
A range finds its start in memory, then reads the matching keys there.
This removes disk key selection only while that key domain remains complete.

An accepted insert, delete, or key move updates membership before its ack.
An overwrite that preserves membership need not rebuild all keys.
Logical bulk replacement, schema bindings, incompatible encoding, and delete controls update or clear completeness before visibility.
Physical flush and LastDB compaction preserve complete keys when logical membership stays the same.
Flush alone does not remove current memory changes before the base includes them.

Keep one logical owner for the key part and the sparse tip/value part.
Use stable references to that owner's keys where practical; do not maintain independent mutable copies in several maps.
The full logical-record cap remains 10,000.
The separate key byte cap counts all live domains, build buffers, write changes, and copies that active calls still hold.
Charge allocation capacity and tree nodes, not only the nominal key lengths.

Reject or stop a preload before it exceeds the byte cap.
Purge a whole complete key domain after readiness clears.
Tip and atom purge preserves its complete keys.
Never claim a partial key set is a complete range.
An explicit internal schema load does not add a normal no-key query API.
Keep no file handle, hash group, or disk page object as the retained key set.

## Build order

1. Define selected schemas, authoritative membership sources, shared load states, and typed publication.
2. Add the complete logical key part and bounded loader on the current disk format.
3. Route first queries through the gate and later ranges through complete logical keys.
4. Batch selected tip locations, implement packed-key pages, and measure their read and write costs.
5. Compare full-schema loads, many hashes, updates, deletes, key moves, flush, purge, and active reads on synthetic data.

The initial 128 MiB allowance is a measurement setting, not an installed default.
Use a larger configured allowance only when the measured schema needs it and the machine has room.
Record DiskOnly with a reason when the selected schema does not fit; do not retry the failed full load on every query.
The [work plan](lastdb-logical-warm-set-plan.md) defines the common read contract and preload implementation sequence.

The representation scratch root and the disk-component scratch root were removed through supported exact-path cleanup.
No product tests or fixture harness ran.

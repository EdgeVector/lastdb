# Complete disk-pack journal batches

This unit adds the recovery bytes for the disk-pack plan.
It follows the [artifact reader and builder](lastdb-disk-pack-reader.md).
It does not activate a LastStore home writer or change a memory ack.
It does not publish a directory root.
The separate [artifact publisher](lastdb-disk-pack-publication.md) supplies that next step.

The journal contains complete immutable batches.
Each batch contains BEGIN, ordered operations, and COMMIT.
Each operation retains its exact collection, group, storage key, and stored body.
The writer assigns a physical batch sequence and operation number.
It does not replace logical atom identity, author time, device identity, or owner encryption bytes.

The implementation follows Sections 4, 6, 7, and 8 of the [format document](lastdb-disk-pack-format-v1.md).
The plain part and frame writer serves both packs and journal batches.
Journal paths use an exact stream UUID and batch sequence.
Independent 4 KiB blocks retain exact record positions.
No read depends on a previous record to decode its bytes.

## Write order and limits

The artifact writer accepts at most 4,096 operations and 128 MiB of logical bytes per batch.
The complete size includes both markers, operation headers, collection names, keys, and record bytes.
It checks every input before it creates the batch files.
New construction requires the complete-object-bounds artifact feature.
Each map header receives its final bounds and checksum before the durability barriers and TAIL.
Older artifacts remain readable, but this writer refuses a new journal on them.
The suffix cap is 512 MiB of reserved uncompressed file bytes and at most 4,096 complete batches.
Compression does not increase admission capacity.

The reserve includes worst-case frame headers, part headers, and complete map pages.
The writer also reserves 8 KiB for the current and replacement TAIL files.
TAIL records actual complete physical data-part and map bytes.
It does not substitute those compressed bytes for the admission reserve.
These limits apply to the separate artifact constructor.
They do not enable the future production queue, age, disk-space, or protected-record policy.

The writer uses this order:

1. Check the complete batch and suffix limits.
2. Write complete operations and COMMIT into new batch files.
3. Finish part headers and immutable map pages.
4. Execute the data-file and directory barriers.
5. Write and flush a temporary TAIL.
6. Rename TAIL and execute its parent-directory barrier.
7. Advance the in-process durable boundary and return its direct record locations.

One exclusive file lock protects the artifact writer.
A file or publication error poisons that writer.
It cannot reuse a partial batch path or guess an uncertain TAIL result.
The current artifact constructor refuses an existing journal.
Resume and stream rotation remain release dependencies for the online writer.

## Recovery

The recovery reader captures CURRENT and TAIL once.
It reads only exact batch paths within that captured suffix.
It checks the byte and batch limits before work.
It checks each complete BEGIN, operation digest, full key, collection digest, record version, and COMMIT.
It also checks frame order, map bounds, logical end, physical bytes, and the final TAIL marker digest.

The reader returns one batch only after all its operations and COMMIT verify.
An error poisons the recovery object.
The caller must complete the captured suffix before it exposes recovered state.
An error never becomes a successful empty batch or end-of-recovery result.

The reader retains one decoded block and bounded metadata for the current batch.
It does not retain all prior batch payloads.
One batch has at most 64 map pages and 32,768 payload blocks.
The separate cumulative work budget is 768 MiB per batch.
This budget includes file bytes, decoded bytes, operation buffers, and coordinate copies.
Returned mutations remain the caller's bounded current work.

Explicit recovery frame reads increment `journal_replay_bytes`.
Ordinary reads of committed journal extents use payload reads and leave that counter at zero.
The opt-in LastDB read receipt exposes the new additive field.
No LastDB query route invokes recovery in this unit.

## Product commands

The commands require an explicit separate artifact path.
The artifact marker prevents their use as a home writer.
The input file contains generated or explicitly supplied stored mutations.
No primary-home default exists.

```sh
lastdb-journal append --artifact PATH --input mutations.jsonl --batch-size 64
lastdb-journal recover --artifact PATH
lastdb-journal publish --artifact PATH
```

Each input line contains `collection`, `group`, `key`, and `body_hex`.
A delete uses `deleted: true` and omits `body_hex`.
The command limits each line to 2,200,000 bytes.
It collects a complete batch before the append.
Earlier complete batches remain durable if later input fails.

Recovery prints a receipt for each verified batch.
The final `complete: true` receipt establishes completion of the captured suffix.
The inspection output uses storage-id and body digests.
It does not print stored bodies or complete storage keys.
The library returns the original bytes and direct positions to the artifact publisher.

The [operational report](proofs/disk-journal-20261015/report.md) records fresh generated-data results.
The commands exercise real file barriers and reopen the completed files.
They do not establish crash behavior or LastDB node recovery.

## Online write dependency

The current code has more than one write unit:

| Source | Existing unit | Required integration |
|---|---|---|
| `LastStore::put`, `delete`, and compare-and-swap | One keyed change under its write stripe | Reserve and journal one complete accepted unit |
| `transaction` and `transaction_deferred` | All supplied operations, with error restore | Keep the complete unit and its failure result |
| `append_for_resident` | One append and one durability token | Bind tokens to the committed root, not only journal durability |
| Logical `apply_batch` | A loop of resident appends with restore on error | Add an explicit complete batch boundary |
| Persist envelopes | A larger acknowledged mutation job | Reserve all required future store work before the memory ack |
| Restore and maintenance | Direct or destructive file work | Use an explicit supported mode or refuse the new home format |

A per-operation append observer cannot supply the required complete logical batch boundary.
The older per-operation journal proposal uses a different format and contract.
It does not enable this format.

The artifact publisher adds immutable directory updates and atomic root publication.
Online admission, complete token coverage, the home fence, and bridge support follow before activation.
Backup retention, source-free restore, concurrent conversion, and primary preparation remain open.
The primary, archive, factory, and reset stores stay outside this work.

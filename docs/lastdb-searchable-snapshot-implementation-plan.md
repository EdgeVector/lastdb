# Implementation plan: snapshot-owned recovery and sync-only mutation log

Revised 2026-10-23. Local journal removal merged in PR 268 as `4118c7baa`.
PR 271 contains the follow-up erase recovery and cloud repair removal.
The initial implementation commit is `47088b89c`; later fixes and receipts accompany it.
The October 21 growth measurements use `8067843c4f9626470edc971ab10c10e829f021d8`.
The [code review](lastdb-searchable-snapshot-code-review.md) separates implemented behavior, approved policy, and new proposals.

Current main commits native values and their app notices together before acknowledgement.
This integration preserves that write policy, concurrent compaction, exact file holds, and byte-based capacity rules.
The earlier benchmark reports retain their frozen source identities; they do not measure this later integration.
Tom approved the code merges on October 23. A primary upgrade remains separate.

## Completed local scope and remaining scope

The implemented writer creates ordinary payload files and changed search pages.
It commits one generation receipt and one `CURRENT` publication per complete stored batch.
Startup validates the selected snapshot without local mutation replay.
Compaction, backup format `lastdb-native-backup-2`, and restore use the selected snapshot.
The internal journal module, stream checkpoint, dependency walker, and `lastdb-journal` command are removed.
The format requires feature `256`; it refuses old native journal homes before writer file creation.
No old-native-journal converter exists in this change.

The [active protocol](lastdb-snapshot-commits.md) describes the exact implemented local path.
The [operational report](https://github.com/EdgeVector/lastdb/blob/0b0a79d4c10949e2f0570e82d9bc4fdb9513a285/docs/proofs/snapshot-commits-20261022/RESULTS.md) records the generated-data product results and their limits.
The later [matched benchmark](https://github.com/EdgeVector/lastdb/blob/0b0a79d4c10949e2f0570e82d9bc4fdb9513a285/docs/proofs/journal-removal-benchmark-20261022/RESULTS.md) records three controlled before/after pairs.
The [repeat benchmark and review](https://github.com/EdgeVector/lastdb/blob/0b0a79d4c10949e2f0570e82d9bc4fdb9513a285/docs/proofs/journal-removal-review-benchmark-20261022/RESULTS.md) uses final product source `5919abbfb`.
It confirms lower update write bytes; batch latency and compaction pauses show mixed results.
The separate erase capture checks aggregate restore after a forced stop, before any value fetch.
The second review removes erase debit intents, replay, prune, and journal bootstrap fields.
Erase accounting writes its final aggregate header and changed schema slices with the delete.
The live meter debit follows the durable snapshot root.
Meter snapshot version 2 marks older values incomplete without journal replay.
The ordinary atom counter retains its source schema for a correct debit when a protein peer removes the last reference.
An older counter without that source makes schema meter state incomplete instead of a guessed owner debit.
Erase preparation checks an 8 MiB aggregate copy quote before a full-state copy.
It retains one aggregate candidate and allows at most eight aggregate working allocations, with a conservative 64 MiB total quote.
Retained post-root debit effects have a separate 8 MiB cap per request.
Either cap returns `WriteResourceLimit` and refuses the complete private erase batch.
This limit can refuse an erase when the aggregate meter state is too large.

The journal-based cloud repair workers, selected-device replacement, peer adoption, and atom-rewrite commands are removed.
Retired repair state and cloud generations that require adoption receive an explicit refusal.
Keep the sync mutation log and authenticated retired sync source records.

Native cloud activation remains disabled. The sync contract and proposed resource limits below remain separate work.
Receipt part removal and periodic cleanup are implemented in PR 273.
Commit `e79b71151` adds the bounded cache within each publication.
Dirty-share enforcement and the other proposed workspace quotas remain separate work.

## Receipt part removal

The follow-up starts from the journal-free `08d8436cc` source.
It replaces the inherited dependency list with exact live-part counts in each new receipt.
Feature bit `512` distinguishes the counted receipt; older programs refuse the bit.
The receipt keeps its existing 80-byte dependency width and uses bytes 24 through 31 for the count.
Counted generation prefixes store semantic version 1 in bytes 96 through 103.
Pre-merge counted prototypes with version zero receive an explicit refusal; they lack the finalized allocator-reference semantics.
Supported uncounted snapshots keep their zero reserved prefix and receive a streamed count pass at writer open.
The format adds no journal, side file, or receipt chain for recovery.

- Replace conservative final receipts with exact counts; retain bounded private dependency inventories where compaction needs them.
- Retire old page references as `TreeEdit` replaces each page.
- Read only fresh private metadata pages to add their references.
- Change payload counts only for keys that the batch inserts or replaces.
- Resolve each page's overflow references before the next page enters the ledger.
- Resolve each changed key's old and new payload references before the next key.
- Keep a shared file part until its last current reference goes.
- Keep each locator object's part-zero allocator template while a directory entry references that object.
- Leave old receipts, read holds, backup references, and reclaim barriers intact.
- Schedule periodic reclaim below the compaction trigger; holds, grace periods, and backoff can delay cleanup.
- Repeat the orphan sweep for parts that no receipt names, with unchanged deletion cutoffs.
- Collect older receipt counts at writer open, before writer admission.
- Transfer those counts into the first counted commit.
- Collect complete new counts after a full snapshot build or compaction.

The receipt and each signed part map retain the existing 65,536-coordinate ceiling.
The separate pending-reference ceiling is 4,096 entries per page collector.
The existing mutation quote reserves current plus additional coordinates before admission.
Full count passes use a bounded stack and page-local ledgers, without a directory-entry or logical-record quota.
Their work bound derives from the supported physical format; the arbitrary older-format step quota is removed.
Final full and tier outputs receive one complete seed, including grafted subtrees.
The normal staged compaction path performs that seed before its final publication gate.
The synchronous fallback also performs a complete seed within its caller's boundary.
Backup manifests accept optional feature 512, preserve supported older roots, and reject unknown feature bits.
The [protocol](lastdb-snapshot-commits.md#live-part-limits) states the memory quotes and work limits.

### Implemented bounded reference cache

Commit `e79b711517519c9ce27d0f6f1b405a364be3ba91` shares checked maps, overflow pages, and part headers within one publication.
The six source files under `vendor/laststore/src/disk_pack/` are:

- `live_parts.rs`
- `live_parts/overflow.rs`
- `live_parts/scan.rs`
- `tree_edit.rs`
- `locator_forest.rs`
- `publish.rs`

The cache serves changed-key references and the fresh-part reference pass.
It resets before the next reference step at 24 cached pages or 24 part-header identities.
Its bounds remain 26 pages, 25 header identities, and 32 file handles.
Every resolution keeps its own existing work limit; cache hits still incur the logical block or overflow-page charge.
The cache retains no payload bodies and drops before final count changes and receipt output.
Bootstrap retains its format-derived work bound and allowed-part checks.
Integrity, root publication, recovery, file lifetime, and warm-data policy remain unchanged.
The change adds no global cache, format bit, journal, or side file.

The separate [measurement plan](https://github.com/EdgeVector/lastdb/blob/0b0a79d4c10949e2f0570e82d9bc4fdb9513a285/docs/proofs/receipt-part-pruning-benchmark-20261022/METHOD.md) freezes equal build commands and generated workloads.
It compares three before/after pairs for disk growth, process write bytes, and request latency.
Additional operations cover shared payload parts, long keys, older receipt conversion, reclaim holds, backup restore, and crash reopen.
An automatic-maintenance operation checks idle reclaim below the compaction trigger, without a manual pack or reclaim request.
The source and measurement receipts must identify the exact executable used for each result.

The [earlier receipt comparison](https://github.com/EdgeVector/lastdb/blob/0b0a79d4c10949e2f0570e82d9bc4fdb9513a285/docs/proofs/receipt-part-pruning-benchmark-20261022/RESULTS.md) uses uncached candidate `a1941e3a8` against journal-free `08d8436cc`.
All six operational checks and three benchmark pairs pass their data checks.
Durable-single growth falls from 668.35 to 640.36 KiB per update.
The durable 32-record batch p50 rises from 99.96 to 164.65 ms.
Those measurements describe the uncached candidate and do not measure `e79b71151`.
The cache is implemented; the separate [follow-up report](https://github.com/EdgeVector/lastdb/blob/0b0a79d4c10949e2f0570e82d9bc4fdb9513a285/docs/proofs/receipt-cache-followup-20261022/RESULTS.md) contains its final profiles, controlled pairs, and default-maintenance comparison.
Three controlled pairs show durable-single growth of 670.86 versus 639.35 KiB per update and batch32 request p50 of 113.33 versus 131.24 ms.
The focused largest compaction write rises from 1.249 to 1.488 seconds.
One default-maintenance run per variant retains 1,253.27 versus 175.05 MiB after the quiet tail.
Those frozen comparisons establish a space/latency tradeoff for their workloads; later main changes require separate measurements.
Use existing durable batches for ready independent updates.
Before release, verify a representative generated home through older-root admission, actual compaction, exact reads, ranges, reopen, and further durable writes.
State its directory coordinates, file parts, record counts, memory, space reserve, and request delays.
The historical [scale review](https://github.com/EdgeVector/lastdb/blob/0b0a79d4c10949e2f0570e82d9bc4fdb9513a285/docs/proofs/receipt-cache-followup-20261022/SCALE-REVIEW.md) describes the earlier quotas, which this integration removes.
Physical directory coordinates are not logical records.
Main already combines native app notices with their data commit; the earlier [source note](https://github.com/EdgeVector/lastdb/blob/0b0a79d4c10949e2f0570e82d9bc4fdb9513a285/docs/proofs/receipt-cache-followup-20261022/NEXT-CHANGE-REVIEW.md) describes the older path.

## 1. End state and scope

- The local searchable snapshot owns keyed reads, ranges, local commits, and crash recovery.
- The mutation log serves sync only. Local recovery never replays it.
- An ordinary native ack follows the complete local snapshot commit and clean core memory publication.
- Native data and app notices share that commit; no collection timer delays the request.
- The ack confirms local durability. Required sync retention remains a separate incomplete contract, and the ack does not confirm remote delivery.
- Keep stable schema, field, molecule, atom, and file identities. Keep encryption, proteins, conflict order, deletes, and reference rules.
- Keep the current-value model and the existing 30-day conflict-record exception. Add no history query.
- Keep directory and locator search pages initially. File consolidation is a separate change.
- Keep cloud activation off until the new sync commit contract has operational proof.

The October 19 byte-limit decisions replace the older 10,000-logical-record rule, including its stale workspace wording.
Keep the byte-based policy and its always-admit read rule. Do not restore the count cap.
The approved 25% dirty-write share still needs enforcement; the audited source exposes its threshold in status.
See [the byte-limit decisions](lastdb-warm-set-byte-limit-design.md#decisions-tom-2026-10-19).

## 2. Snapshot commit protocol

The implemented `SnapshotWriter` uses complete `SnapshotReservation` and `MutationReservation` ownership.
Steps that concern required sync entries describe the remaining sync contract; the local writer does not implement those steps.
Retain the existing exclusive writer ownership and immutable page helpers.
Retain `complete_native_commit`, ordered publication barriers, and the final macOS full device flush from PR 266.
Wrap the complete new direct-write path in that durability scope.
Do not restore a full device flush for every individual file.

1. Collect one complete logical change, including related molecules, reference changes, deletes, and required sync work.
2. Check conditions and original write order under one writer scope.
3. Reserve the complete operation count, logical paths, owned bytes, disk output, and replacement metadata.
4. Reserve sync retention and any required file holds before acceptance, when that sync contract is enabled.
5. Retain the complete private change and its ownership. Caller cancellation does not cancel accepted work.
6. Write bodies through ordinary payload files with `Source::Pack` locations.
7. Copy only changed directory and locator pages. Retain the old selected pages.
8. Write the outgoing sync entries and their required file holds.
9. Write one complete generation receipt that names every required snapshot dependency.
10. Order required writes before atomic `CURRENT` publication, then complete all final platform durability barriers.
11. Install the committed disk view and clean core memory state, then notify waiters and return the durable receipt.

Advance the runtime's durable-root handle only after every publication barrier, including the final macOS full device flush, succeeds.
Sync export captures that handle under the publication gate.
It never treats a renamed but not yet durable `CURRENT` as export authority.

One root publication covers every operation of an atomic stored batch.
An API batch can still return independent item results where the existing API permits them.
Group complete accepted units for a flush without a change to their conditions or order.
Never split an atomic unit to satisfy a cap.
Preserve author-clock reservations and their existing durability barriers before the native request enters its storage gate.

The end-state root must record these facts, directly or through its committed metadata.
The local implementation records the format, generation, frontier, roots, and integrity facts.
It does not add the outgoing or incoming sync boundaries below.

| Fact | Purpose |
|---|---|
| Store identity, format epoch, generation, roots, integrity check | Select and validate the local snapshot |
| Local commit sequence and complete commit identity | Resolve an uncertain publication without duplicate work |
| Outgoing device identity, committed sequence, file, and byte end | Bound sync export |
| Required sync targets, acknowledgements, and durable file holds | Retain required outgoing data |
| Received sequence per source device | Commit imported values and progress together |
| Confirmed cloud base identity and per-device boundaries | Define the retained sync interval |

Local generations and sync sequences are distinct.
Compaction and imports can advance the local generation without a new outgoing mutation.
The root may reference bounded metadata pages; it need not contain an unbounded device list.

## 3. Reads and ranges

Capture one selected disk generation and one complete accepted memory view for each call.
An exact read checks the named logical paths, then fetches only missing paths from the snapshot.
Batch all known tip and atom keys at each dependency step.
Keep the product keyed-access complexity contract; report physical page and byte costs separately.

A range supplies the molecule, one hash, and encoded range bounds.
Seek to the first required key and return a bounded page.
Merge relevant accepted memory changes in key order. A delete masks its older disk value.
The unselected disk route must not load a complete hash group, schema, or field to answer a page.
Preserve the existing opt-in first-query membership preload for selected HashRange schemas.
It has a separate 128 MiB default key budget and does not preload atom values.
Measure that first-query lifecycle cost separately from later keyed and range calls.

An in-call cursor holds one view through its pages.
A supported ascending keyed continuation uses the last returned key with the same hash and bounds, then captures a fresh view.
It does not promise one snapshot across requests.
Concurrent changes before that last key need not appear in later requests.
Descending and other unsupported cursor forms retain their existing offset behavior and explicit cursor refusals.
Retain existing encoded key order, prefix bounds, and inclusive or exclusive endpoint semantics.

## 4. Crash recovery, compaction, and backup

Startup validates `CURRENT` and opens its named snapshot metadata.
A counted snapshot needs no full metadata walk at writer open.
An older supported snapshot needs one bounded walk to collect live counts before writer admission.
Neither route opens the sync log for local recovery.
Search and payload integrity checks occur before affected data reaches a caller.

| Interruption | Required result |
|---|---|
| Before new output becomes durable | The previous selected snapshot remains usable |
| After sync append, before root publication | The previous snapshot wins; sync export ignores the extra suffix |
| During root publication | Either complete root can survive; no partial atomic batch appears |
| After successful durable publication | Every covered operation survives restart |
| Disk error with uncertain publication | Stop write admission; reopen the selected root and resolve the commit identity |
| Corrupt committed file | Return an explicit integrity error; do not silently report absence or select stale data |

Unreferenced partial files are cleanup work, not recovery instructions.
The sync worker separately checks its committed log interval before export.
Missing required sync bytes produce a sync-integrity error. They do not become instructions to rebuild local values.

Compaction uses the same new-files-and-root protocol.
Compaction retains the current whole-home write lock for replacement publication; report its pause explicitly.
The change retains PR 267's concurrent reclaim after the new disk view enters service.
A later performance change can build replacement output outside a short publication lock.
That later publication must preserve newer commits or retry from a newer source view.
It must never replace newer data with an older compaction result.

Active readers and backups hold the exact snapshot files they need.
Reclaim removes a file only after no selected root, active call, backup, or required sync reference needs it.
A backup captures the snapshot and required sync state from the same committed view.
For an active sync file, copy only that view's committed byte prefix.
Later appends do not change the captured prefix or its integrity facts.
Do not require the active file's eventual whole-file size or digest.
Restore opens that snapshot directly; it performs no internal-journal recovery.

## 5. Sync-only mutation log

Prefer self-contained entries with schema, key, operation, values, and original mutation identity.
Large file references need durable holds until the required sync publication completes.
A reference to the current atom alone is insufficient after an overwrite or delete.
Use an immutable file-identity header and complete append frames; never rewrite a committed prefix.

The sender exports only through the runtime's durable root and its committed log end.
It ignores an uncommitted suffix and retries delivery with the same mutation identity.
Before new sync append admission after restart, discard only the suffix beyond that exact end or start a fresh sync file.
Use a fresh encryption identity where required; never reuse an encryption nonce.
A later commit must never include abandoned events through an accidental advance across the old physical file end.

Imports retain the source time, device, counter, and mutation identity.
Commit imported values, conflict effects, and received progress together.
Advance received progress only through contiguous per-device coverage.
Commit the receipt even when an accepted losing or duplicate event makes no value change.
Bound gaps and request missing events; a maximum seen sequence cannot stand for complete coverage.
Repeated delivery has the same result. Do not create a new outgoing identity for an imported envelope.

The cloud base records one included boundary for each source device.
Retain the log interval after the latest confirmed cloud base, including already-uploaded entries still required by that base.
Upload all replacement-base dependencies before the atomic cloud base selection.
An upload receipt alone does not permit log removal.
An older retained cloud base keeps any log interval that its supported restore path still requires.
Keep local unsent mutations through any cloud-base replacement.
An older device stages a supported cloud base with its own committed unsent mutations before the replacement local root becomes visible.
Preserve those local mutation identities and required holds. Publish no intermediate base-only local state.
A confirmed base advances received progress only after its complete included state enters the local snapshot.
Cloud-base export streams a held snapshot with bounded buffers; it needs no full local duplicate of the database.

The retained asynchronous capture code does not establish this atomic contract today.
Remove any policy that silently drops an oversized required outgoing entry.
Reject an unsupported mutation before local acceptance instead.

## 6. Initial limits

The implemented local change preserves the current limits:

| Resource | Implemented limit |
|---|---|
| Native stored batch | 16,384 operations; 128 MiB encoded dimensions |
| Physical CLI write and standalone artifact | 4,096 operations; 128 MiB encoded dimensions |
| Snapshot reservation queue | 512 MiB and 4,096 pending batches; these are reservations, not sync history |
| Warm retention | 256 MiB default; protected-record overshoot remains counted |
| Ordinary native ack | Complete local commit; no collection delay |
| Generation dependencies | 65,536-file ceiling; adaptive maintenance retains its current policy |

The following limits remain proposals, except where the table names an existing or approved rule.
Keep all limits explicit in status and receipts. Byte values use binary units.

| Resource | Initial limit or rule |
|---|---|
| Background flush schedule | Preserve the existing configured background policy; ordinary native requests wait for their complete commit |
| Warm logical records | No fixed record-count cap; charge each path and shared value by bytes |
| Warm bytes | 256 MiB default retention budget; charge paths, slots, links, atoms, and values held by calls; permit the approved temporary overshoot |
| Dirty-write share | Approved target: 25% of warm bytes, about 64 MiB; admission enforcement remains separate work; ordinary native requests already use durable completion |
| Prepared write-queue bytes | 64 MiB; count actual additional ownership and reservations |
| Temporary workspace | 128 MiB shared pool; at most 64 MiB for one writer, compactor, or snapshot worker |
| Other retained database metadata | Preserve separate defaults: 16 MiB definitions, 128 MiB selected keys, and 16 MiB write-model metadata; no new merged cap |
| Total tracked database memory | Report the aggregate and warm overshoot; retain the process memory guard; this plan sets no new hard whole-process cap |
| One read call | 16 MiB workspace within the shared pool; at most 64 file handles per call and 256 across calls; close them at call end |
| One range page | At most 1,000 returned records and 4 MiB encoded output, with an explicit continuation |
| Atomic stored batch | 16,384 storage operations and 128 MiB complete encoded bytes, including required side records |
| Selected generation dependencies | Preserve the 65,536-file ceiling and adaptive maintenance policy; the default starting high-water mark is 4,096 files |
| Sync mutation batch | At most 128 MiB complete encoded bytes; reserve it with the snapshot work |
| Retained sync log | 512 MiB, 4,096 complete batches, and 16 files; the first reached cap stops dependent writes |
| Sync file size | 64 MiB target, 256 MiB hard maximum; a complete large batch stays intact |
| Incoming gaps | 4,096 events and 64 MiB total disk bytes; refuse extra work and request retry without false progress |
| Cloud-base work | One pending upload; retain the two latest confirmed bases plus any older base with a required hold |
| Local disk | 20 GiB initial configurable cap; count live, old, partial, sync, blob, and temporary files |
| Disk reserve | Reserve worst-case flush output before acceptance; reserve full replacement output before compaction starts |
| Sync control reserve | 64 MiB disk and 8 MiB workspace within their total caps; user work cannot consume this reserve |
| File blob transfer | Stream in bounded buffers charged to the workspace pool; keep the existing atom-size contract |

The outgoing limits count retained history after the confirmed base, not only entries that lack an upload acknowledgement.
Start replacement cloud-base work at half of any retained-log bound: 256 MiB, 2,048 batches, or eight files.
Wake that work on a capacity refusal too.
Never discard required log entries, held data, or an accepted write to satisfy a limit.
The outgoing quota does not block acknowledgements, confirmed-base commits, required hold releases, or imports with no new outgoing entry.
These control commits use the reserved allowance and their own complete quote.
Count incoming gaps and held old cloud bases within the local disk cap.

Charge the path before fetch and its atom at admission. Shared values receive one charge.
Each read enters the warm set; there is no warm-capacity error or no-retention read fallback.
Purge least recently used clean records that no call holds.
If held or unflushed records prevent enough purge, admit the read above the warm budget and count that admission.
Keep those bytes charged until their last owner releases them. Purge again after call completion or flush.

Reserve complete private-write ownership and the required work allowances before acceptance.
Ordinary native writes finish on disk before success or return a separate operation-resource error.
Never refuse a read solely because the warm set is full.
Queue, workspace, and disk reservations retain their separate bounds; warm overshoot does not authorize excess in those resources.
Release all reservations when their ownership ends.
If one projected record exceeds the range-page output cap, return an explicit capacity error.
Do not return an empty continuation that cannot advance.

## 7. Code and disk files

The paths below map the original plan to the affected source.
The local writer, format, reader, maintenance, backup, and command changes are complete in this branch.
The converter, sync contract, dirty share, and new quotas remain future work.
Keep the generic pack, encryption, page, ownership, and durability helpers.

| Action | Code or files | Required change |
|---|---|---|
| Remove | `vendor/laststore/src/disk_pack/journal/` | Remove internal batch markers, suffix admission, replay, tail archives, and stream rotation after all callers move |
| Remove | `vendor/laststore/src/disk_pack/stream_checkpoint.rs` | Remove the second empty-stream receipt and root publication |
| Remove | `vendor/laststore/src/bin/lastdb-journal.rs` and its entry in `vendor/laststore/Cargo.toml` | Retire the internal-journal command; update dependent recovery commands |
| Remove | `db_operations/atom_store/hard_erase_journal.rs` and its commit helper | Replace recovery records with final aggregate state in the native erase batch |
| Remove | Node `cloud_purge*` modules, commands, routes, and core repair/adoption modules | Remove journal-based repair and restart workers; keep generation guards as explicit refusals |
| Remove | Node and core `backup_atom_rewrite.rs` | Retire the resumable cloud rewrite command; keep old backup identity reads |
| Replace | `vendor/laststore/src/disk_pack/home/write.rs`, `home/admission.rs`, `home/mutation_quote.rs` | Replace `HomeJournal` with direct snapshot reservations and commits |
| Rewrite | `vendor/laststore/src/disk_pack/publish.rs` | Accept prepared locations directly; keep one receipt, durable root publication, and commit-identity resolution |
| Rewrite | `vendor/laststore/src/disk_pack/current.rs`, `artifact.rs`, `mod.rs`, `home.rs`, `build.rs`, `home/build.rs` | Add the journal-free format fence and snapshot commit metadata; remove mandatory journal initialization |
| Rewrite; walker removed | `vendor/laststore/src/disk_pack/pack_write.rs`, `location.rs`, `frame.rs`, `call.rs`, `object.rs`, `block_plan.rs`, `dependency_walk.rs` | Journal output and `Source::Journal` are removed; direct payload access remains |
| Rewrite | `vendor/laststore/src/disk_pack/maintenance/`, `home/maintenance.rs`, `backup/` | Remove tail requirements; preserve roots, file holds, space reservations, and complete backup state |
| Refusal complete; converter future | `vendor/laststore/src/disk_pack/{conversion.rs,conversion_select.rs,conversion_fence.rs}`, `vendor/laststore/src/store/native_conversion.rs` | Preserve explicit format refusal; add an old-native-journal converter separately |
| Rewrite | `vendor/laststore/src/bin/{lastdb-disk-home.rs,lastdb-disk-recovery.rs,lastdb-pack-maintain.rs}`, `vendor/laststore/src/bin/disk_home/io.rs` | Remove internal append-versus-publish commands and journal output |
| Rewrite | `fold_db/crates/core/src/storage/disk_pack/{mod.rs,write.rs,queue.rs,initialization.rs,view.rs,space.rs,transaction/,maintenance.rs,backup.rs,parts/orphans.rs}` | Replace journal ownership, tokens, admission, startup, status, and maintenance edges |
| Rewrite | `vendor/laststore/src/disk_pack/dependency.rs`, `generation/dependency_record.rs` | Remove internal-journal dependency kinds; retain snapshot and required sync file holds |
| Rewrite | `lastdb_node/src/exec/db_compact_routes/{backup.rs,native.rs,status.rs,flush.rs}` | Adapt command and status surfaces to snapshot commits |
| Rewrite | `fold_db/crates/core/src/resident/` and native read-view admission | Preserve byte accounting, always-admit reads, and counted overshoot; complete dirty-share enforcement and retain complete views |
| Retain and adapt | `fold_db/crates/core/src/storage/disk_pack/schema_keys/`, `resident/core_keys*` | Preserve opt-in selected-schema membership preload, its separate budget, and common captured views |
| Rewrite | `fold_db/crates/core/src/sync/` capture, outgoing retention, import progress, and cloud-base metadata | Implement the common commit boundary; remove silent required-entry drops |
| Retain | Pack-write helpers, `tree_edit.rs`, directory and locator helpers, `generation.rs`, `lifetime/`, `write_lock.rs`, read and integrity helpers | Reuse immutable payloads, search pages, root holds, and exclusive writer ownership |
| Retain | `vendor/laststore/src/durability.rs`, `durability/ordered.rs`, and the macOS filesystem dependency | Keep ordered file sets, publication barriers, final device flush, and failure propagation |
| Update | Disk-format, journal, publication, backup, recovery, and operations documents | Mark the old journal format historical; remove obsolete active commands and requirements |

In the new disk format, remove `JOURNAL_TAIL`, `journal/<stream>/batch-*`, and archived `tail-*` files.
Keep `CURRENT`, `generations/`, `directory/`, `locator/`, ordinary payload files under `packs/`, and required file blobs.
Keep the separate sync mutation code. Native cloud activation remains refused.
The new format creates no internal-journal files, even when sync is disabled.

## 8. Compatibility and implementation sequence

1. Complete: add the journal-free format, direct commits, and explicit old-format refusal.
2. Complete: retain memory publication, flush, durable receipts, reservations, and the byte-based warm policy.
3. Complete: use snapshot ownership for startup, backup, restore, compaction, and reclaim; remove the native journal and unused callers.
4. Record the operational results and limits. Keep primary activation separate.
5. Future: add an explicit offline converter for known old native journal homes, with a separate destination and ownership checks.
6. Future: connect required sync entries, file holds, and received progress to the same root commit.
7. Future: enforce the approved dirty share and agree on the other proposed quotas before their implementation.
8. Review new performance receipts before a performance claim. Review complete sync receipts before cloud activation.

A future converter must read one held old `CURRENT` and copy all required journal-backed bodies into ordinary snapshot payload files.
It preserves storage keys, atom identities, author facts, deletes, reference counts, and required sync state.
The existing generic conversion command does not establish this conversion.
Keep any required old-format parser isolated in the conversion tool; the normal new engine has no internal-journal module.
Normal startup does not migrate a home or delete an old format.
Older binaries must refuse the new mandatory format before data access.
Rollback retains an untouched old-format home only within an explicit authorized conversion procedure.

The primary, its archive, shared services, CI, and release settings remain outside this plan's authorized execution.
Do not copy the primary or run a candidate on its data.

## 9. Operational verification and completion

Use real product commands and requests on fresh owned homes with generated data.
Do not add or run a test suite or fixture harness.
Keep exact process ownership, source revision, binary identity, resource limits, request results, and cleanup receipts.

| Area | Required observation |
|---|---|
| Keyed reads | Create, overwrite, delete, and recreate known keys; request selected fields; batch exact keys across hashes; verify absence and values before and after restart |
| Ranges | Use one hash, prefix and endpoint bounds, and page size one; verify order, delete masks, and continuation rules without unrelated-hash reads |
| Optional preload | Select one HashRange schema; measure first-use key loading separately; verify later ranges and concurrent writes preserve membership and view semantics |
| Complete views | Read several fields and protein-related molecules during a batch; observe a complete old or new accepted unit |
| Held views | Keep an in-call cursor active during new commits and compaction; its files and selected values remain valid |
| Native ack | Observe only the complete accepted request; require disk completion and correct reopen after the ack |
| Durable receipt | Restart after the receipt; every covered operation remains present |
| Interrupted publication | Stop an owned node at observable output, file-sync, root-publication, and directory-sync stages; reopen a complete snapshot without log replay |
| Committed-file integrity | Corrupt an explicitly owned generated snapshot file while its node is stopped; reopen or read returns an integrity error, never false absence |
| Sync isolation | Disable the sync worker; record actual file-source reads and opens; local reads and recovery use snapshot files only |
| Sync consistency | Observe log append before root publication; export ignores that suffix; restart and commit another mutation; abandoned events never export |
| Sync publication order | Observe root rename before directory sync; the sender retains its previous durable boundary until publication completes |
| Sync import progress | Deliver out-of-order, repeated, and losing events; advance only contiguous coverage and commit progress with the corresponding state |
| Cloud-base replacement | Keep committed unsent local mutations visible throughout a newer cloud-base install; preserve their original identities and holds |
| Sync retention | Overwrite and delete an atom before delayed export; its required entry or retained bytes remain complete; cap exhaustion refuses dependent writes before acceptance |
| Sync capacity recovery | Reach each outgoing cap; acknowledgements and confirmed-base commits still run, release space, and restore write admission |
| Warm memory | Read enough small records to exceed 10,000 paths without a count-cap error; fill the byte budget with larger records; verify least recently used purge |
| Held memory | Hold records during new reads and flushes; verify honest overshoot counts and charges, then purge after their owners release them |
| Write memory and work limits | Reach the dirty-share and separate queue/workspace limits; verify durable-first writes, complete reservations, and release without lost accepted work |
| Growth | Increase the owned database beyond the warm set; startup and known-key reads retain bounded memory; range pages stay bounded |
| Compaction and backup | Hold a reader and backup during replacement; stop compaction before and after publication; restore every required current value and sync interval |
| Active sync backup | Append during backup; restore only the captured committed prefix with its snapshot, without unfinished events |
| Disk capacity | Fill the configured reserve with owned generated data; keep required files; refuse work before an unfulfillable write acknowledgement |

Process interruption receipts do not establish power-loss behavior.
Report which publication edges the operational exercise reaches.
Retain platform durability reasoning separately and report any unobserved edge.
Zero replay counters alone were insufficient for the former format: that reader could fetch journal payloads without replay.
The new format refuses journal payload source tags and inventories show no internal-journal files.

Repeat the saved 512-record workload against both the reviewed current source and the new writer where practical.
The historical October 21 run is not a current-main measurement.
The October 22 report for PR 266 measures a separate 1,000-call queued workload; do not compare those medians directly.
Compare separate updates with batches, and record complete request acknowledgement and durable completion.
Report median and p95 latency, process disk I/O, logical and allocated growth, metadata bytes, file count, and compaction write pauses.
Record operating-system cache uncertainty and shared-host load.
Do not claim physical SSD writes from process counters.

Local journal-removal completion requires one receipt/root publication per nonempty stored batch, correct product results, and zero internal-journal dependencies.
The full design, including future sync and resource work, has additional requirements below.
Require correct byte accounting, approved read admission, bounded work allowances, valid backup restore, and complete sync retention receipts.
Run appropriate non-test format, syntax, lint, build, secret, and security checks for changed code.
Use compiler failures and a bounded zero-reference source review to complete dead-code removal.
Quantify the actual write and space benefit before a release recommendation.
Stop owned ephemeral nodes and remove owned scratch data after receipts are saved.

References: [one-pager](lastdb-searchable-snapshot-one-pager.md), [code review](lastdb-searchable-snapshot-code-review.md), [baseline receipts](https://github.com/EdgeVector/lastdb/blob/0b0a79d4c10949e2f0570e82d9bc4fdb9513a285/docs/proofs/write-overhead-20261021/summary.json), [barrier report](https://github.com/EdgeVector/lastdb/blob/0b0a79d4c10949e2f0570e82d9bc4fdb9513a285/docs/proofs/native-commit-barriers-20261022/RESULTS.md), [canonical model](lastdb-canonical-model.md), [storage boundary](lastdb-laststore-storage-boundary.md).

# Native snapshot commits

Status: integrated local protocol on main, reviewed at `8a0a40b4f` on October 24, 2026.
The [native summary](lastdb-disk-native-node.md) describes current writes, reads, and the separate native backup gate.
The [operational report](proofs/snapshot-commits-20261022/RESULTS.md) records the generated-data product results and their limits.
This protocol describes source guarantees. The product results do not establish power-loss behavior or a primary result.

This protocol replaces the internal native journal protocol.
Here, snapshot means the complete local disk state selected by `CURRENT`.
The older journal documents remain historical references.
The schema, field, molecule, atom, and file model stays in the [canonical model](lastdb-canonical-model.md).

## Selected snapshot and format fence

`CURRENT` selects one complete immutable snapshot generation.
Its directory and locator pages find the exact payload files for a key.
A normal payload location uses `Source::Pack`; a delete uses `Source::Tombstone`.
An ordinary read does not open a mutation log or replay a suffix.

The mandatory `FEATURE_SNAPSHOT_COMMIT` bit is `256`, or `0x100`.
The current reader requires that bit and refuses the retired journal feature bit `2`.
New publications also require `FEATURE_LIVE_PART_COUNTS`, bit `512`.
That bit permits positive reference counts in each receipt dependency record.
A program without support for this bit refuses it before it creates writer files.

Counted receipts also require semantic version `1` at generation-prefix bytes `96..103`.
The reader refuses prototype version `0` and unknown versions before it allocates the dependency vectors.
Each receipt page must match the complete expected prefix.
Supported snapshots without bit `512` retain zero in that version field.

The other required feature checks still control packed keys, complete object bounds, native home identity, and atom records.
The home descriptor still controls the key and placement codecs.

The active native format has no `journal/` directory, `JOURNAL_TAIL`, batch marker, archived tail, or journal stream.
The exclusive writer lease uses the root file `SNAPSHOT_LOCK`.
The separate `LIFETIME_LOCK` protects files that a call still needs.
The `lastdb-journal` command is removed.

## One complete commit

One commit has one physical batch frontier and one new generation.
The frontier remains monotonic across mutations.
Compaction changes the generation but keeps that frontier.
The physical `(batch, operation)` version does not replace an atom identity, author clock, or encryption key.

The write follows this order:

1. Validate the complete stored batch and obtain its capacity reservations.
2. Write the stored bodies into new private payload files.
3. Complete the payload headers and block maps.
4. Write the changed locator and directory pages into new private files.
5. Write one complete generation receipt for the selected root and its dependency files.
6. Complete the required file and directory barriers before root publication.
7. Write and flush a private `CURRENT.pending-<uuid>` file.
8. Rename that file over `CURRENT` and complete the root directory barrier.
9. Complete the platform durability scope before the commit returns durable success.

The publisher carries the prior live counts and adds the exact changed-page deltas.
It removes a part from the new receipt when that part has no live reference.
Each changed key removes its old payload references and adds its new payload references.
An unchanged value causes no payload-map work when its search page receives a copy.
The resolver reads payload block maps and part headers; it does not decode record bodies.

Tree edits borrow complete inline keys from their checked immutable pages during validation and leaf merge.
The key reader retains the same byte charges, digest checks, and overflow-key validation.
Keys with overflow pages retain their bounded owned buffer. Borrowed keys leave scope before the page drops.
The output pages, live counts, selected root, and durability barriers retain their existing rules.

Commit `e79b71151` shares checked maps, overflow pages, and part headers through one bounded cache within each publication.
The cache serves changed-key references and the fresh-part reference pass.
It drops before final count changes and receipt output; no cache survives the commit or an error.
Bootstrap retains its standalone resolver, cumulative work limit, and allowed-part checks.
Integrity, warm-data admission, file lifetime, root publication, and recovery retain their existing rules.

Overflow references follow the actual next-page links.
One live record keeps every payload and map part that its bytes require.
An obsolete record cannot remove a part that another live record still requires.

Each directory edge also retains locator part `0` as the shared allocator template.
That template remains necessary when every selected nonempty locator root occupies another part.
Each receipt contains the complete `CURRENT` bytes and the exact dependency identities.
No published payload or metadata part receives an append.
An unchanged tree path can retain its existing pages.

The engine quote covers the complete stored batch, private map pages, a complete receipt, and root publication.
The node checks disk space and required memory reservations before it admits the complete request.
Admission transfers the accepted work to the queue; it does not send an early memory ack.

## Durability and failure

The change preserves the existing native durability helpers.
On macOS, the complete commit scope retains ordered file barriers and a final full device flush.
The scope completes each dependency before the visible root can depend on it.
The final device flush precedes durable success.
Other platforms retain their existing file and directory barriers.

The writer marks itself poisoned before the first commit file work.
An uncertain file or publication result leaves it poisoned.
The caller must reopen it before another write.
The caller must not infer success from an uncertain return value.

Each native request waits for the complete snapshot and clean core memory publication before ack.
The request seals its queue without a collection timer.
The native flush receipt identifies the completed sequence, generation, and frontier.
These native ack rules already form part of main.

Captured native app notices and their feed tip share the product snapshot.
Subscriber delivery follows the durable commit.
This app feed is separate from the cloud sync mutation log.

## Reopen and file lifetime

A reopen captures `CURRENT` and verifies its complete generation receipt and dependency files.
It does not replay another local file to recover newer mutations.
Files from an interrupted private commit stay outside the selected snapshot.
Their existence does not authorize their use as committed data.
A damaged or incomplete selected generation causes an error; the code does not guess another root.

An older supported snapshot without bit `512` receives one metadata walk when the writer opens.
The walk precedes writer admission and checks all reachable parts against the older receipt.
The first commit transfers those counts into the new complete receipt.
Counted snapshots use receipt counts and page deltas; an ordinary commit does not walk the full database.

Full snapshot creation collects counts from its complete new tree.
Every published compaction result receives a full final count pass after its last graft.
This rule covers full copies, tier copies, and concurrent catch-up.

Each read holds its selected generation until its last file access.
A read lease from the writer also retains the system writer lease.
Backup and conversion references retain their exact selected generation.

Compaction copies a held snapshot outside the node write gate.
It merges newer stored locations from another held snapshot into its private replacement.
The staged path completes the full count pass and receipt outside the final publication gate.
The final gate checks the exact included snapshot, publishes the replacement, and installs the read view.
A changed boundary causes another private catch-up, with at most eight attempts.
Continued writes can defer compaction with a capacity refusal; they never authorize an older replacement.

An addition of bit `512` during an older-root copy causes a retry with a new source.
An unknown feature change causes an error.
The synchronous fallback still performs its full count pass under the caller's writer boundary.
Reclaim starts after the complete view handoff.

The concurrent reclaim plan owns a hold on its captured generation.
It retains active read generations and durable references.
Its generation floor excludes new output from deletion.
An orphan sweep also checks the file creation generation before concurrent deletion.
An incomplete or unknown new file stays in place.
These rules preserve the concurrent reclaim handoff from issue 267.

An older receipt retains its own parts after a newer receipt omits them.
Removal from a receipt does not itself delete a file.
Reclaim still checks all retained roots, read holds, durable references, and its generation floor.

The automatic maintenance worker schedules reclaim after a 30-second idle interval, even when the live-part count stays below its compaction trigger.
The worker uses its existing bounded drain and installed-root hold; it does not pack the live data for this cleanup.
The orphan sweep repeats after each result, with an interval between 30 seconds and ten minutes.
Its default interval is ten minutes; the configured minimum file age and grace remain the deletion cutoffs.
The repeat covers young files that the startup sweep retains and private files that no receipt ever names.

Short retries still handle a deferred sweep before the next regular interval.
Packs, holds, grace periods, and error backoff can delay the scheduled cleanup.
Automatic maintenance off still disables the worker.

The [earlier receipt removal report](proofs/receipt-part-pruning-benchmark-20261022/RESULTS.md) records six operational checks and three uncached benchmark pairs at `a1941e3a8`.
The part-lifetime checks pass, including the periodic retry after startup retains a young orphan.
The matched foreground comparison shows lower file growth with slower batch writes.

The separate [cache follow-up report](proofs/receipt-cache-followup-20261022/RESULTS.md) covers `e79b71151`.
Three controlled baseline/cache pairs show durable-single growth of 670.86 and 639.35 KiB per update.
Durable batch32 request p50 rises from 113.33 to 131.24 ms; the focused worst write rises from 1.249 to 1.488 seconds.
Those are medians across runs.
The default-maintenance run retains 1,253.27 MiB for the baseline and 175.05 MiB for the cache after its quiet tail.
Each variant has one finite run; this is no steady-state size claim.

This cache comparison uses frozen `08d8436cc` and `e79b71151` source; it does not measure the October 23 integration.
The results do not establish the performance of main's native ack, atomic app notices, or concurrent tier compaction.
Tom approved the code merges, subject to the required checks.
The primary stays unchanged; these code merges do not authorize a primary upgrade.

## Live-part limits

One receipt admits at most 65,536 dependency parts.
The existing mutation quote checks current parts plus worst-case additional parts before admission.
Live counts and each signed part-delta map use that same entry ceiling.
A conservative 256-byte entry quote gives 16 MiB per map.
Page references resolve before the next page enters the ledger.
Their separate 4,096-entry ceiling quotes at most 1 MiB per active collector.

The publication cache resets before a reference step at 24 pages or 24 part-header identities.
Its bounds remain 26 pages, 25 header identities, and 32 file handles.
Each resolution retains its existing work limit; logical block and overflow-page charges still occur on cache hits.
These are storage workspace limits; they do not change the logical warm-data byte budget.

The normal publisher can hold one live-count map and two part-delta maps at the same time.
The additional count workspace has a conservative 64 MiB quote, with receipt vectors and resolver caches included.
That quote excludes existing mutation inputs and search-page workspaces; it is not a total process memory cap.

Full count passes stream the selected trees with a height-bounded stack and page-local reference ledgers.
There is no separate 65,536-directory-entry cap or 67,108,864-step bootstrap quota.
The full count pass retains a loose format-derived work bound and verifies its allowed part coordinates.
The 65,536-part ceiling still bounds receipts, count maps, and signed part-delta maps.
It is not a logical-record or directory-entry cap.

An older-root count error occurs at writer open, before writer admission.
The first write still uses the older receipt count for conservative part admission.
An older receipt at the part ceiling can require compaction before that write.
Normal page resolution uses a format-derived work ceiling that covers every representable pending extent and overflow chain.
The resolver checks all count additions and removals before it writes the receipt.
An invalid count or missing required part causes an error before `CURRENT` publication.

## Backup and restore

The local bundle format is `lastdb-native-backup-2`.
The bundle contains `CURRENT`, the exact generation inventory, and a manifest with file lengths and checksums.
It contains no journal tail or empty journal stream.
A source reference and generation hold protect the complete copy.
The restore verifies the manifest and exact inventory before it publishes a new destination.

The native manifest permits bit `512` as an optional supported feature.
It retains the mandatory older feature mask and refuses unknown feature bits.
The exact `CURRENT` bytes and receipt semantic version remain authoritative.

The source home is not required for restore.
The destination must not exist.
The bundle excludes the node identity, encryption keys, and cloud grants.
Keep those resources separately for a node recovery.
This local bundle does not establish a complete cloud recovery protocol.

For an existing supported native root, the explicit commands are:

```sh
lastdb-disk-home write --home ROOT --input mutations.jsonl
lastdb-disk-recovery backup --home ROOT --backup NEW_BUNDLE
lastdb-disk-recovery restore --home NEW_ROOT --backup BUNDLE
lastdb-pack-maintain compact --home ROOT
```

The write input contains one JSON object per line:

```json
{"collection":"docs","key":"a","body_hex":"6162"}
{"collection":"docs","key":"b","deleted":true}
```

The command admits at most 4,096 operations and 128 MiB of complete batch dimensions.
A command body has at most 1 MiB of decoded bytes.
Hex uses complete bytes and lowercase digits.
The command derives each physical hash group from the home descriptor.

## Compatibility and scope

Old native journal formats are refused before writer file creation.
Startup does not change those homes.
Startup does not convert an old native journal home to a snapshot home.
The separate [native transfer command](../vendor/laststore/src/bin/lastdb-native-transfer.rs) supports explicit export, import, and verification of compatible stored records.
It requires a compatible source reader, a clean source stop, retained identity and keys, and a nonexistent destination.
This route does not make startup accept the older format or provide a generic old-format converter.

The existing generic legacy converter remains subject to its atom-record format fence.
`HomeBuilder::create` refuses that unsupported generic native result before it creates files.
The `lastdb-disk-home build` and generic `convert` commands therefore do not supply a fresh product home or an old-journal conversion.
The product factory creates a fresh home with the required atom-record authority.
Backup restore preserves the feature authority of a supported snapshot bundle.

Legacy Cloud Sync remains blocked by the factory and activation gates.
Local native backup capture saves the exact snapshot, source home, frontier, and durable file hold before network work.
The separate [native backup source](designs/native-cloud-backup-mini-v1.md) includes saved jobs, transfer, restore, and a disabled-by-default gate.
Capture and export do not establish deployed cloud transfer or cloud recovery.
The snapshot commit does not activate multi-device sync, atomic sync imports, or safe mutation-log retirement.

Erase accounting commits its final aggregate snapshot with the erase in one native batch.
The live debit follows the durable root.
No pending debit, commit sequence, replay record, or journal prune remains.
The meter format is version 2. Older snapshots keep their values but lose their trust claim.
Ordinary final deletion audit facts remain in the snapshot.
An atom counter retains the source schema for a debit when a protein peer removes the last reference.
An older counter without that source makes the schema meter state incomplete; the code does not guess its owner.

Erase preparation checks an 8 MiB aggregate copy quote before each full-state copy.
One request retains one aggregate candidate; each new chunk replaces that candidate.
The code allows at most eight aggregate working allocations, with a conservative 64 MiB total quote.
Post-root debit effects have a separate 8 MiB cap per request.
The native batch retains its existing 128 MiB cap for stored bytes and preparation overhead.
A cap refusal returns `WriteResourceLimit`; the complete private erase batch remains uncommitted.
These caps can refuse an erase on a home with a large aggregate meter state.

The journal-based cloud repair, selected-device replacement, peer adoption, and atom-rewrite commands are removed.
Startup refuses retired repair state without a read, replay, or change of the retired records.
Normal session diagnostics can still change files outside that state and the native snapshot.
Cloud generations that need the removed adoption workflow receive an explicit refusal.
The sync mutation log and authenticated retired sync source records remain.
Those records serve sync and never select the local recovery root.

## Source map

| Concern | Source |
|---|---|
| Mandatory format feature and refusal | `vendor/laststore/src/disk_pack/mod.rs`, `current.rs` |
| Exclusive snapshot writer and complete commit | `vendor/laststore/src/disk_pack/snapshot/writer.rs` |
| Changed pages, complete receipt, and root rename | `vendor/laststore/src/disk_pack/publish.rs`, `generation.rs` |
| Native durable ack and clean memory publication | `fold_db/crates/core/src/storage/disk_pack/transaction/request.rs`, `queue.rs`, `view.rs` |
| Atomic native app notices | `fold_db/crates/core/src/db_operations/change_feed/native.rs` |
| Exact live parts, page deltas, and older snapshot count collection | `vendor/laststore/src/disk_pack/live_parts/`, `live_parts.rs`, `tree_edit.rs` |
| Bounded cache within each publication | `vendor/laststore/src/disk_pack/live_parts.rs`, `live_parts/overflow.rs`, `live_parts/scan.rs`, `tree_edit.rs`, `locator_forest.rs`, `publish.rs` |
| Final count seed and conditional compaction publication | `vendor/laststore/src/disk_pack/maintenance/concurrent.rs`, `staged.rs`, `catch_up.rs`, `tier.rs` |
| macOS order and final device flush | `vendor/laststore/src/durability.rs`, `durability/ordered.rs` |
| Read holds and concurrent reclaim | `vendor/laststore/src/disk_pack/lifetime/`, `snapshot/reclaim.rs`, `maintenance/reclaim/` |
| Exact local bundle and restore | `vendor/laststore/src/disk_pack/backup/` |
| Optional counted feature in native backup manifests | `cloud_backup_protocol/src/manifest.rs` |
| Generic conversion format fence | `vendor/laststore/src/disk_pack/home/build.rs` |
| Native cloud activation refusal | `lastdb_node/src/host/boot.rs`, `lastdb_node/src/cloud/resume.rs` |
| Atomic erase accounting | `fold_db/crates/core/src/db_operations/atom_store/erase_meters.rs` |

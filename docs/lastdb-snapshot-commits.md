# Native snapshot commits

Status: implemented local protocol in this branch, October 22, 2026.
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

The publisher carries the prior dependency list and adds the new files.
That list is conservative: it can retain superseded files until compaction.
Each receipt contains the complete `CURRENT` bytes and the exact dependency identities.
No published payload or metadata part receives an append.
An unchanged tree path can retain its existing pages.

The engine quote covers the complete stored batch, private map pages, a complete receipt, and root publication.
The node also reserves disk space, raw memory, and logical records before a memory ack where those resources are required.
An engine reservation alone does not authorize a memory ack.

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

A memory ack still means that the accepted write is correct in memory.
It does not mean that the snapshot commit is durable.
The native flush receipt identifies the completed sequence, generation, and frontier.
This protocol does not change the normal 500 ms flush interval.

## Reopen and file lifetime

A reopen captures `CURRENT` and verifies its complete generation receipt and dependency files.
It does not replay another local file to recover newer mutations.
Files from an interrupted private commit stay outside the selected snapshot.
Their existence does not authorize their use as committed data.
A damaged or incomplete selected generation causes an error; the code does not guess another root.

Each read holds its selected generation until its last file access.
A read lease from the writer also retains the system writer lease.
Backup and conversion references retain their exact selected generation.
Compaction publishes replacement files before the node installs the replacement read view.
Reclaim starts after that handoff.

The concurrent reclaim plan owns a hold on its captured generation.
It retains active read generations and durable references.
Its generation floor excludes new output from deletion.
An orphan sweep also checks the file creation generation before concurrent deletion.
An incomplete or unknown new file stays in place.
These rules preserve the concurrent reclaim handoff from issue 267.

## Backup and restore

The local bundle format is `lastdb-native-backup-2`.
The bundle contains `CURRENT`, the exact generation inventory, and a manifest with file lengths and checksums.
It contains no journal tail or empty journal stream.
A source reference and generation hold protect the complete copy.
The restore verifies the manifest and exact inventory before it publishes a new destination.

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
This change does not implement a converter from an old native journal home to a snapshot home.
The refusal message describes the required future explicit conversion, not an available command.

The existing generic legacy converter remains subject to its atom-record format fence.
`HomeBuilder::create` refuses that unsupported generic native result before it creates files.
The `lastdb-disk-home build` and generic `convert` commands therefore do not supply a fresh product home or an old-journal conversion.
The product factory creates a fresh home with the required atom-record authority.
Backup restore preserves the feature authority of a supported snapshot bundle.

Native cloud sync remains blocked by the factory and cloud activation gates.
The snapshot commit does not activate cloud upload, import, or a sync-only mutation log.
Atomic capture of a snapshot and its included cloud sync boundary remains separate work.
No claim here establishes cloud retention, import idempotence, or cloud recovery.

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
| macOS order and final device flush | `vendor/laststore/src/durability.rs`, `durability/ordered.rs` |
| Read holds and concurrent reclaim | `vendor/laststore/src/disk_pack/lifetime/`, `snapshot/reclaim.rs`, `maintenance/reclaim/` |
| Exact local bundle and restore | `vendor/laststore/src/disk_pack/backup/` |
| Generic conversion format fence | `vendor/laststore/src/disk_pack/home/build.rs` |
| Native cloud activation refusal | `lastdb_node/src/host/boot.rs`, `lastdb_node/src/cloud/resume.rs` |
| Atomic erase accounting | `fold_db/crates/core/src/db_operations/atom_store/erase_meters.rs` |

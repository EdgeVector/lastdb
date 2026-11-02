# Disk V2 client backup

Status: accepted client implementation. The client has no disk V1 profile.
Restore refuses a backup of a disk format v1 home.

The cloud V2 service is already independent of the local file format.
The client captures and restores disk V2 files and supports an explicit PROD connection.
It keeps the local file bytes, file format, encryption, and cloud service contract.
The [completion design](cloud-v2-completion-plan.md) defines the release and operational checks.

## Scope

- Use client metadata version 5 for a new disk V2 file backup.
- Read version 3 backups with unknown logical event coverage.
- Keep the cloud descriptor null and require the R2 and B2 destinations.
- Send the existing encrypted file bytes without another encryption or text conversion.
- Send CAS run files to B2. Send all other captured files to R2.
- Keep the current object size and memory configuration. Add no backup-only native file size cap.
- Keep automatic backup off. The legacy sync engine is retired.

## One consistent source

The daemon owns both run homes and their exclusive locks.
An external process must not bypass those locks or read two independent CURRENT files.
The client first completes the native flush.
It then holds the existing Core write gate across the paired capture.
That gate excludes the CAS additions, main write, and CAS deletions of a compound write.

For each run home, the vendor capture method holds its publication mutex.
It refuses a poisoned home and selects its current manifest under that mutex.
It links the exact descriptor, encrypted CURRENT inode, and every referenced immutable run into a fresh private capture directory.
The metadata includes each home identity, generation, frontier, and complete file set.
Background compaction can change physical generations, but it must preserve the selected logical frontier and data.

The capture is flat and uses opaque link names. It is not another runnable database home.
The durable links preserve the bytes after compaction or restart removes original file names.
The client fsyncs the links and the paired capture record before it submits new remote work.
It hashes captured files after it releases the Core write gate.
It uses bounded reads and the existing transfer buffers.

The vendor API is `RunHome::capture_files(&Path) -> CapturedRunHome`.
The caller supplies an absent private capture directory on the source filesystem.
The result identifies the selected home and each captured file.
Each file identifies its native relative path, opaque held name, kind, and byte length.
The vendor API does not depend on cloud types or perform remote work.

## Client profile

Core keeps the public `ByteBackupCapture` operations.
The disk V2 implementation uses captured file segments, not a stored-record conversion.
Core owns the new `RunBackupProfile` and its validation.
The shared cloud protocol receives no new native metadata type.
Run profile 6 is the format 8 home, the only profile.
The client checks the node marker against that profile.
The profile adds no cloud format field and changes no stored file.

`RunBackupProfile` contains one main-home binding and one CAS-home binding.
Each binding contains its home ID, generation, frontier, descriptor hash, CURRENT hash, file count, and byte count.
The encrypted client root binds this profile to the common native-root summary and recovery descriptor.
The native file pages contain the original destination paths and exact receipt extents.
They use the existing `ClientNativeFile` shape.
Version 6 permits native files with a null cloud descriptor and no stored-record profile.

The only installed native paths are:

- `laststore-pack-node-v1`, with the exact disk V2 node marker.
- `disk-run/laststore-run-home-v2` and `disk-run/CURRENT`.
- `disk-run/runs/<canonical UUID>.ldrun`.
- `disk-cas/laststore-run-home-v2` and `disk-cas/CURRENT`.
- `disk-cas/runs/<canonical UUID>.ldrun`.

The descriptor must identify the supported LDRHOME3 format.
Both home identities and complete referenced run sets must match the captured profile.
The common native-root summary uses the main identity, generation, and frontier.
Its CURRENT hash binds the main CURRENT. Its home digest binds both home bindings.
The metadata requires this exact relationship and the supported native feature set.
It does not weaken the older native-root validator.

## Resume and release

The saved source state identifies the profile, exact paired cut, held files, and submitted remote job.
A restart resumes those same identities and files. It does not select a new cut for the existing request.
A submitted or uncertain request retains its source until the existing terminal and source-release rules permit release.
Capture cleanup waits for all readers to finish.
An incomplete capture cannot become an accepted backup.
If a crash interrupts the paired capture, resume refuses the incomplete pair.
Explicit close can remove that unsubmitted capture. A new request can then select a new cut.

## Read resources

Backup verification uses separate bounded readers. Ordinary reads keep their existing path.
The readers first authenticate each encrypted frame through fixed buffers.
A second pass returns the existing decoded bytes and checks authentication again at the exact end.
Large uncompressed file values use the stream without a file size cap.
The existing compressed-frame limits stay unchanged.

The client admits both retained manifests, file coordinates, decoder buffers, and output before allocation.
CURRENT and footer readers check expanded key and array sizes before allocation.
The client uses the central backup memory allowance and transfer buffers.
It keeps raw file bytes outside the client metadata JSON.

The verifier keeps the selected key and value together across proof records.
DEV and primary operations record actual backup duration as well as completion.
Local file admission uses the current product policy.
The backup adds no separate native file size cap.
A proof records actual file sizes and distinguishes a file value from a native run file.
An aggregate database size does not prove one large native file.

## Restore

The client freezes one accepted backup before it fetches its objects.
It verifies the complete receipt set, object hashes, extent bounds, and final native file hashes.
It restores both run homes into a fresh absent destination.
It creates fresh ephemeral LOCK files; it does not copy a source owner lock.
It validates both descriptors, encrypted CURRENT files, exact referenced run closure, account key proof, and record-to-file dependencies.
It publishes the node wrapper and recovery receipt only after those checks pass.
It preserves the captured native identities and ciphertext bytes.
It does not convert disk V2 into disk V1 or create a node on a primary copy.

## Explicit primary connection

The new CLI path is `cloud backup-v2 connect --existing --env prod --api-key-stdin --json`.
The old fresh DEV and restored-home commands retain their behavior.
The CLI sends a separate sensitive request to an owner-only connection route.
The ordinary backup request enum must not contain or log the API key.

The daemon derives its account and native source identity.
It verifies the supplied key through the compiled environment's V2 context endpoint.
The response must match that account, its current generation, both destinations, and permission for new work.
A rejected or wrong-account key causes no registration or credential publication.
Account registration remains a separate explicit fallback with a durable device intent.

Under the backup transition lock, connection refuses active work.
It atomically writes the existing private paused credential format and a source-bound manual connection record.
The record binds the account, native source, environment, endpoint, and credential hash.
It does not copy an identity, reset data, create `.cloud_resume_required`, or enable automatic work.
The existing local-write state remains unchanged.

New code can admit explicit manual work from the existing environment opt-in or a valid owner connection record.
It checks the record against the current source and credentials before each request.
Remote authority and quota checks still govern every job.
Connection and startup never start a backup.

## Release sequence

1. Review this contract and implement the vendor, Core, connection, and restore adapters.
2. Use CI for compilation, required checks, benchmarks, and the exact Mac artifact.
3. Verify a fresh disk V2 DEV backup and source-free restore with supported file blobs.
4. Verify exact raw file bytes, both providers, restart, resume, and allocation closure.
5. Prepare the exact client package and primary restart procedure before any primary upgrade.
6. Connect the existing primary identity without a format conversion or local data copy.
7. Create one new canonical request UUID and finish its PROD backup.
8. Verify publication, accepted identity, provider receipts, and complete local source release.

No local Rust build, benchmark, or test is part of this sequence.
The primary and its archive remain outside DEV proof data.

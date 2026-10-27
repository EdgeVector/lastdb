# Cloud byte transport V2

Status: the manual V2 backup and restore path is accepted code.
This change adds explicit R2 and B2 destinations and a client record export.
CI and a real provider backup and restore must verify this change before activation.
The route namespace is `/api/cloud/backups/v2/`.

The retired legacy engine's nonserialized `SyncConfig` and `CaptureMode` Rust
APIs have been removed: their upload, bootstrap, coalescing, and grace-window
knobs had no remaining consumers. V2 transport settings and serialized status
models keep their existing definitions; those status models are passive metadata.

## Purpose and boundary

The cloud stores exact opaque bytes and returns verified receipts.
The client defines its format, encryption, native home, durable frontier, files, and recovery rules.
The service does not decode CURRENT, native features, file kinds, or recovery keys.
The client sends file blob bytes to B2 and all other bytes to R2.
The local pack format and file encryption do not change.

The canonical types are in `cloud_backup_protocol/src/v2/`.
The client and service use this same source through the accepted LastDB revision.
No handwritten second copy is permitted.
V2 reuses only scalar primitives, `CloudScope`, and generic error types from the parent crate.
V2 does not use V1 native types, object limits, or identity hash domains.

## Identity and exact bytes

Every V2 version field is the JSON integer `2`.
`Stream` contains an account scope and a fresh stream UUID.
`Job` adds the immutable commit UUID.
The scope contains the authenticated personal account and its account generation.
Provider resources and credential authority remain private service state.
Context reads bind the requested account to its current scope and return `new_work_allowed`.
The client can require R2 and B2 before it starts a local capture.
Begin freezes that same destination requirement.
This value is advisory. Mutation transactions still enforce exact current authority.

`Object` contains a SHA-256 digest, a positive canonical decimal byte count, and an optional destination.
New routed objects require an explicit `r2` or `b2` destination.
The service uses the destination without knowledge of the object contents.
The count permits the positive `u64` range.
The wire type imposes no native file-size limit.
The provider, quota, and resource policy must admit each object before an effect starts.
Clients can split large files into objects without a cloud file-kind field.

`ClientDescriptor` is an opaque bounded JSON value.
The service preserves the value without knowledge of its fields.
Old descriptors retain their exact JSON shape and identity.
New LastDB backups use a null descriptor.
The encrypted client root contains the client format and recovery information.
The recovery phrase identifies the account before the client decrypts that root.

Canonical JSON uses `json-canon` over the complete typed value.
Identity hashes frame the domain length as big-endian `u32`, then the domain bytes.
They then frame the canonical body length as big-endian `u64`, followed by the body bytes.
The digest is SHA-256 of that complete frame.
Domains use `lastdb:cloud-byte-backup:`, one operation name, and the suffix `:v2`.
Operation names are `client-descriptor`, `begin`, `transfer`, `object-receipt`, `inventory-put`, and `commit`.
Stored object and canonical inventory byte digests use plain SHA-256.

## Job and object sequence

1. Save one job UUID, the descriptor, the destinations, and the exact predecessor before the first request.
2. Submit `Begin` with those immutable values. Retry with the same input.
3. Save exact object bytes and immutable transfer inputs before a PUT.
4. Prepare at most four transfers from one job per request.
5. Use the exact upload grant and its mode, with a signed `Content-Length`.
6. Complete each transfer. Poll its saved identity while verification remains pending.
7. Receive authoritative receipts only after exact length and SHA-256 verification or verified canonical reuse.
8. Upload and verify the client root object before the final inventory.
9. Include its ordinary receipt once, then freeze the complete inventory and submit `Commit`.

Each transfer input binds its request UUID, complete job, destination, object digest, and object byte count.
Resume and complete select that exact saved input digest.
The private provider target must bind the full transfer digest, so independent requests cannot alias an untracked target.
Canonical reuse requires the saved verified allocation, not a caller-computed receipt hash.
The target identity separates R2 and B2 allocations, credentials, and target epochs.
The service must save each authoritative receipt before acceptance.

`Pending` means awaiting bytes, verification, or an unknown outcome.
A timeout does not prove failure. The client preserves the same input and byte source.
An explicit resume may retry one exact upload grant and complete request.
Status polls do not repeat PUT or queue admission.
The existing generic terminal failure codes describe only permanent frozen-input failures.
They do not authorize format interpretation or release unresolved reservations.

R2 grants use a conditional PUT with signed `If-None-Match: *`.
B2 grants permit only part one of a service-owned multipart upload.
The service alone creates and completes that upload.
It saves the upload identity before it grants access to the part.
It verifies the exact part, then binds reads to the completed object version.
An unknown provider outcome retains the reservation and requires reconciliation.
The service does not repeat an unknown create or completion to assume success.
The B2 part limit applies to one upload object, not to a local file.

## Finite inventory and publication

Each inventory page contains sorted unique `ObjectReceipt` values from one scope.
Pages use consecutive indices from zero.
Each descriptor binds exact canonical page bytes, count, byte sum, and first and last receipt IDs.
The index orders descriptors and requires strict receipt order across page boundaries.
All count and byte sums use checked arithmetic. Overflow refuses the input.
The root contains the exact index digest, page count, object count, and stored-object byte sum.
The byte sum includes the root object once.

`InventoryVerifier` consumes the actual pages in descriptor order.
It checks each exact descriptor and requires the root receipt to occur once.
Only a complete pass creates the private, non-deserializable `InventoryProof`.
`Commit::validate_inventory` and `AcceptedBackup::validate_inventory` require this proof.
These source checks prove structure. The service must also resolve every receipt through its verified receipt store.

`Commit` binds the Begin digest, root receipt ID, and full inventory root.
The root plaintext does not commit to the final inventory that contains the root ciphertext receipt.
This sequence avoids a hash cycle.
The client root may describe its content dependencies. The accepted record supplies the final transport inventory.
The client root binds the Begin digest and descriptor digest, without the final inventory commitment.

The accepted record contains the complete frozen Begin once, including the predecessor and descriptor.
Its job, descriptor, and Begin digest accessors derive those values from that Begin.
Standalone validation reconstructs Commit and checks the Begin, ordinal, and commit digest.
Disaster recovery does not need a hidden Begin record to verify this internal consistency.
The service's accepted-store authority still supplies the receipt authority.

One service transaction compares the predecessor and job authority.
It writes the immutable accepted record, head, and Published job, then clears the active job.
The transaction also checks current account/resource authority and all required quota and inventory evidence.
No public hash or client proof replaces those authority checks.
The service does not compare native frontiers or native generations.

The job states are Active, Published, and Closed.
Close atomically fences future acceptance and clears active admission.
An accepted result wins a close race.
Close preserves old provider reservations and obligations, even when a PUT outcome is unknown.
Head reads return the prior accepted backup and a separate optional active commit ID.
A pending job never masks the accepted backup.
Commit status before a saved submission returns `NotFound`; it does not prove an in-flight request failed.

## Reads and restore

Discovery returns bounded account-scoped stream pages. A directory entry is not acceptance proof.
The caller freezes one authoritative accepted record before byte reads.
Temporary read selectors name an ordinary receipt, the accepted inventory index, or one page from that index.
The service grants reads only after exact accepted membership checks.
A caller-supplied digest alone never authorizes a read.

The root grant can precede inventory downloads because the accepted record names its receipt.
The grant's destination, digest, and byte count must reconstruct that same canonical receipt identity.
The client then verifies all inventory pages and the root's exact membership.
Before install, the client compares its complete used receipt set with the accepted inventory, including the root.
It refuses missing receipts, extra receipts, duplicate receipts, or unchecked bytes.
The client alone checks native identity, feature support, recovery keys, dependency closure, and fresh destination rules.
Restore retains the same accepted selection after restart.

## Bounds, quota, and compatibility

V2 caps control JSON and each canonical inventory value at 256 KiB.
It caps a page at 512 receipts and an index at 1024 page descriptors.
It caps an opaque descriptor at 256 KiB.
The complete request must also fit the control cap; an envelope can reduce the effective metadata capacity.
Transfers and read grants use batches of one to four. Discovery permits at most 100 streams per page.
Transport code must cap input bytes before JSON decode and cap output bytes before response decode.
Pure type validation does not supply runtime memory, disk, or provider admission.

Quota counts actual physical allocations and transport metadata separately.
One shared account ledger covers V1, V2, and other accounted bytes.
Usage reports disjoint other-used, reserved, and retained bytes, plus the quota.
The derived used total uses checked addition. A quota downgrade does not invalidate a usage response.
The object inventory excludes its own pages and index to avoid self-reference.
This exclusion never grants free metadata: the service charges and retains those bytes independently.
The inventory byte sum is not an account quota total. Reuse, duplicate allocations, and unresolved obligations can differ.
Unknown provider outcomes and legacy obligations keep their charges until independent authoritative closure.
V2 creates no deletion, debit, cutover Ready, or legacy drain proof.

V1 messages, routes, hashes, receipts, plans, and unfinished jobs remain unchanged.
The V2 route namespace is separate from V1.
The client completes V1 terminal cleanup before it creates a fresh V2 stream.
V1 restore remains available. No conversion invents V2 authority from V1 hashes.
Old V2 Begin values omit destinations and keep their exact identity.
Old V2 object and read values omit destinations and keep their exact identity.
Those jobs retain their saved AWS target, even after the service enables R2 and B2.
New configuration cannot move an old receipt or change an accepted backup.
Inventory pages and the inventory index use R2 for routed jobs.
The service keeps provider credentials in secret references, outside the protocol.
DEV deployment and real DEV recovery remain separate operational work.

## Client adoption

The typed HTTP client is `sync::auth::byte_backup::ByteBackupClient`.
It uses only V2 requests, response checks, and control limits.
The private byte transport facade shares the existing credentials, URL checks, deadlines, and bounded streams.
The V1 wrappers retain their exact validators, limits, routes, and public methods.
The V2 upload requires an exact resume response. Commit requires the concrete inventory proof.
Read grants require the frozen accepted record. The client does not choose a newer head during restore.

The client format lives in `storage::disk_pack::byte_backup`.
Its encrypted root and pages bind the exact Begin and descriptor digests.
Only the client interprets their contents.

A native pack can contain both file blob bytes and other records in one compressed block.
One destination for that whole pack cannot separate those bytes.
The client therefore exports the stored records through the existing native transfer format.
This export does not change the local pack format, stored bytes, or encryption keys.
It does not create a second home or copy the primary.

The client uses four ordered streams:

1. Metadata contains the existing transfer header, record prefixes, and footer. It goes to R2.
2. R2 bodies contain the exact stored bytes for all records outside `cas_blobs`.
3. B2 bodies contain the exact stored bytes for `cas_blobs`.
4. Sequence contains the stream choice and byte count for each transfer run. It goes to R2.

The existing client metadata encryption protects the metadata and sequence chunks, root, and pages.
Body bytes retain their existing encryption. The client adds no file encryption layer.
Client pages describe chunk order and receipt identities without a record-count cap.
The cloud sees only opaque objects and their destinations.

The adapter retains one durable native cut before the export and network work.
A retry keeps that cut, job UUID, transfer UUIDs, and object bytes.
Body objects combine adjacent stored bodies and split at the existing 64 MiB upload chunk size.
Metadata and sequence chunks use the central 1 MiB stream buffer size.
These chunk sizes impose no local file-size limit.
The existing shared allowance controls memory, concurrent streams, and temporary disk space.
Large keys reduce upload concurrency within that allowance.
Encrypted metadata and source coordinates use the existing bounded source spool.
The source retains its files until exact cloud terminal authority permits release.
Local cancellation requires proof that Begin was not submitted.

The manual command is `lastdb --data-dir <absolute-home> cloud backup-v2`.
The node requires `LASTDB_BYTE_BACKUP_V2_ENABLED=true` for new manual work.
Status and exact close operations remain available without that flag.
The node status reports V2 manual work and disables automatic backup claims.
It reads only the local V2 coordinator and reports unknown freshness separately.
A missing local receipt cannot prove that an external coordinator backup is absent.
The health check keeps unknown freshness distinct from a healthy or failed backup.
Start, resume, status, and close use the owner-only local socket route.
The worker has a separate controller and participates in normal node shutdown.
The V1 backup, restore, automatic controller, and legacy cloud activation commands are retired.
Fresh DEV setup uses cloud backup-v2 init and cloud backup-v2 connect.
Restored DEV setup uses cloud backup-v2 connect --dev --restored.
These commands keep credentials paused and preserve the existing identity and file formats.

Each exact control request permits six attempts within one 30-second monotonic deadline.
Only validated ProviderPending, RetryLater, and OutcomeUnknown errors permit a retry.
Bounded random delays separate attempts. Pause checks precede attempts and follow each delay.
A successful Pending result starts status-only continuation for the admitted transfer or commit.
Status requests keep separate control deadlines; the 30-second limit does not cover the whole worker.
Pending and typed temporary status errors continue with delays of 1, 2, 4, 8, 16, then 30 seconds.
Close and shutdown checks precede each status request and interrupt each delay.
A permanent refusal stops the worker. The source hold stays until exact terminal authority permits release.
The helper never repeats PUT. It repeats exact Complete requests only when queue admission remains unknown.
A failed batch keeps verified sibling receipts. Other control failures keep the last typed temporary error and source hold.
A daemon exit retains the saved job and cut. An explicit resume uses their exact identities and bytes.

Restore freezes one accepted V2 backup and verifies every inventory page and downloaded byte.
The client verifies the complete chunk order, stream digests, and used receipt set.
Four bounded streams reconstruct the existing native transfer input.
The existing importer rebuilds a fresh home and checks the complete stored record receipt.
The rebuilt home preserves stored records, versions, deletions, account keys, and native format.
Its physical file names and home identity can differ from the source.
The client checks that rebuilt home through the existing native validator and fresh-destination installer.
All feeder tasks and the importer settle before any failure returns.

A durable completion marker binds the accepted backup and fully verified import.
A resumed restore rechecks the concrete pages and rebuilt record receipt before installation.
A partial import without that marker requires a fresh restore session.
The old client profile still restores native files through their exact extents and file digests.
Neither restore path reads source files or the primary.
The client refuses remote-only dependencies before backup eligibility.

Use exact CI client and service packages for the real DEV proof.
Verify B2 file blob objects, R2 other objects, a 100 MiB file, and interrupted upload recovery.
Stop the source and restore a fresh home. Verify normal reads and complete file bytes after two boots.
Verify old accepted backup reads through their saved target.
Activate PROD only after the DEV proof succeeds.

The unused sealed-chunk backup progress tracker, cycle samples, and ETA estimator are removed.
The legacy progress snapshot and shared CLI formatter retain their status behavior.
Active V2 byte-backup progress does not use that tracker.

The client capture supports only disk V2 run homes.
The [disk V2 client design](cloud-run-v2-client.md) describes that adapter.
The generic cloud contract requires no change for that adapter.

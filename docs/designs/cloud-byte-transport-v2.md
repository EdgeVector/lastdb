# Cloud byte transport V2

Status: the manual V2 backup and restore path is accepted code.
The service supports explicit R2 and B2 destinations. The client captures native disk V2 files.
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

V1 routes and client operations are retired. Historical records and byte obligations remain protected.
A new V2 job never invents authority from a historical native hash or receipt.
Old V2 Begin values omit destinations and keep their exact identity.
Old V2 object and read values omit destinations and keep their exact identity.
Those jobs retain their saved AWS target, even after the service enables R2 and B2.
New configuration cannot move an old receipt or change an accepted backup.
Inventory pages and the inventory index use R2 for routed jobs.
The service keeps provider credentials in secret references, outside the protocol.
DEV deployment and real DEV recovery remain separate operational work.

## Client adoption

The typed HTTP client is `sync::auth::byte_backup::ByteBackupClient`.
It uses V2 requests, response checks, and control limits.
The private byte transport facade shares the existing credentials, URL checks, deadlines, and bounded streams.
An upload keeps its exact resume response. Commit requires the concrete complete inventory proof.
Read grants keep the frozen accepted record throughout restore.

The client format lives in `storage::disk_pack::byte_backup`.
Its encrypted root and pages bind the exact Begin and descriptor digests.
Only the client interprets their native metadata.
The [disk V2 client contract](cloud-run-v2-client.md) defines the paired main and CAS capture.
The [completion plan](cloud-v2-completion-plan.md) defines the remaining operational bar.

The client captures existing native files without a stored-record export.
It sends CAS run files to B2 and all other captured files to R2.
The CAS descriptor and CURRENT file remain in R2.
Small-file packs never combine destinations. Large files use exact extents across bounded objects.
The native path, file hash, byte count, and extent order remain client metadata.
The cloud sees objects and their destinations without a native file-kind field.

The file bytes keep their existing encryption and representation.
The cloud adds no encryption. The client adds no second encryption around a native file.
The existing client metadata encryption protects the root and pages.
The adapter keeps one durable paired cut before remote admission.
A retry keeps that cut, job UUID, transfer UUIDs, and exact bytes.
The existing shared allowance controls memory, concurrent streams, and temporary disk space.
The source retains its files until exact terminal authority permits release.
Local cancellation requires proof that Begin was not submitted.

The manual command is `lastdb --data-dir <absolute-home> cloud backup-v2`.
New manual work requires the explicit environment opt-in or a valid source-bound owner connection record.
`connect --existing --env dev|prod --api-key-stdin` verifies existing credentials on the owner daemon.
It keeps automatic backup off and performs no account enrollment or backup start.
Status and exact close operations remain available without the opt-in flag.
The status path reads only the local V2 coordinator and distinguishes unknown freshness.
A missing local receipt does not prove that an external coordinator backup is absent.

Start, resume, status, and close use the owner-only local socket route.
The worker has a separate controller and participates in normal node shutdown.
The V1 backup, restore, automatic controller, and legacy cloud activation commands are retired.
Fresh DEV setup uses `cloud backup-v2 init` and `cloud backup-v2 connect --dev`.
Restored DEV setup uses `cloud backup-v2 connect --dev --restored`.
These commands keep credentials paused and preserve the existing identity and file formats.

Each exact control request permits six attempts within one 30-second monotonic deadline.
Only validated ProviderPending, RetryLater, and OutcomeUnknown errors permit a retry.
Bounded random delays separate attempts. Pause checks precede attempts and follow each delay.
A successful Pending result starts status-only continuation for the admitted transfer or commit.
Status requests keep separate deadlines; the 30-second limit does not cover the whole worker.
Pending and typed temporary status errors continue with delays of 1, 2, 4, 8, 16, then 30 seconds.
Close and shutdown checks precede each status request and interrupt each delay.
A permanent refusal stops the worker and preserves the source until terminal authority permits release.
The helper never repeats PUT. It repeats exact Complete requests only while queue admission remains unknown.
A failed batch keeps verified sibling receipts and the last typed error.
A daemon exit retains the saved job and cut. An explicit resume uses those same identities and bytes.

Restore freezes one accepted V2 backup and verifies every inventory page and downloaded byte.
The client reconstructs exact native files through their extents and checks each complete file hash.
It validates the paired stores, account key, complete native dependencies, and exact used receipt set before publication.
Native identities and file bytes stay unchanged. A restore does not convert the local format.
A durable restore receipt binds the selected accepted backup and the fully verified destination.
A resumed restore keeps that same selection and refuses an unrelated destination.

Use exact CI client and service packages for real DEV proof.
Verify actual B2 file objects, R2 other objects, same-cut resume, and exact storage accounting.
Stop the source and restore an absent home. Verify native files and normal file reads after two boots.
Record actual individual file sizes; an aggregate byte total does not prove one large file.
Verify historical accepted reads through their saved targets without a migration or account repair.
PROD and primary operations follow the accepted DEV proof and their authorized scope.

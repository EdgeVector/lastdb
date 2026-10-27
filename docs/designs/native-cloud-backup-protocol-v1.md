# Native cloud backup protocol, version 1

Status: unreleased draft, revision 7. Runtime support stays disabled.
Since 2026-10-26, the V1 client, capture, upload, and restore code is removed. The active backup is the [V2 byte transport](cloud-byte-transport-v2.md). Some shared types of this protocol stay in the V2 code.
This revision separates bounded closure observations from exact allocation comparison and live provider proof.
Wire Version remains 1. Source consumers exist, but this work does not activate or deploy them.
Source baseline: LastDB 4118c7baa12539c8c6bcf4391c497159f2a1529a and Exemem Infra a4ecf4d73465370a8b3e08cbfb44fed677b4ca62.

## 1. Release boundary and authority

The first release provides complete personal native backups on R2 with the Mini portable recovery profile.
It preserves native packed bytes and reuses verified backup-owned objects.
Every required main file, thumbnail, and poster belongs to that protected backup object set.
The cloud control store owns the sole accepted backup pointer.

The [shared crate](../../cloud_backup_protocol/src/lib.rs) defines wire fields, exact encodings, bounded results, and pure validation.
This document defines authority, required durable actions, recovery rules, and completion.
The crate does not activate routes, verify cloud bytes, change a daemon, or prove recovery.
The existing native cloud refusal stays in place.

Each part owns one result:

| Part | Owner | Authoritative result |
| --- | --- | --- |
| Capture | Native store | One committed root and its protected source files |
| Transfer | Cloud object service | One authoritative receipt for exact backup-owned bytes |
| Manifest | Backup capture and restore | One authenticated map from native paths and file pointers to those bytes |
| Acceptance | Cloud control service | One accepted record and pointer from one conditional transaction |
| Restore | Native importer | One verified fresh destination with recoverable record and file data |

Scope, authority, limits, and exact retry identities apply to all five parts.
A memory ack, disk completion, verified object, accepted backup, and local confirmation remain separate facts.

The following functions have no version-one wire contract:
org sync, logical operation replay, B2, publisher takeover, external-object adoption, journal suffix backup, and automatic cloud deletion.
A later release must define its own complete operation contract before it adds those functions.

## 2. Identities and encoding

Use UTF-8 JSON and RFC 8785 canonical JSON for durable protocol bytes and hashes.
Reject unknown fields, duplicate struct fields, unsupported enum values, and oversized raw requests before decode.

| Scalar | Exact representation |
| --- | --- |
| Version | JSON integer 1 |
| DecimalU64 | Canonical decimal JSON string; no leading zero except "0"; maximum 18446744073709551615 |
| Digest | 64 lowercase hexadecimal characters |
| Id | Canonical lowercase nonnil UUID |
| AccountId | 32 lowercase hexadecimal characters |
| RelativePath | At most 512 UTF-8 bytes and eight normal slash-separated components |
| Text<N> | Nonempty UTF-8 text, at most N bytes, without control characters |

RelativePath rejects absolute paths, empty components, backslashes, "." and "..".
The importer also rejects symlinks and paths outside its owned destination.

CloudScope contains account_id and account_generation.
The generation identifies the account incarnation authorized by the cloud service.
A reset or revocation can advance it. Ordinary backups do not advance it.
Provider routing and physical cleanup state remain private service facts.

context/read takes only Version. The authenticated service subject selects the personal account.
It returns CloudScope, open_for_writes, and new_work_enabled without account creation or adoption.
Authoritative control absence means generation zero and an open account.
A present control requires its exact identity, positive generation, and typed authority flag.
An unavailable or malformed record never becomes generation zero.
The client compares the returned account with its independently derived source or recovery account.
This observation does not replace conditional authority checks on any new effect.
The new_work_enabled observation includes account readiness as well as the feature gate.
The current service supplies only a permanent cutoff hold and reports this value as false.
It provides no Ready transition or hold removal.

The private per-account storage authority is separate from the existing generation control.
Its cutoff hold fences new legacy admission and control changes through conditions in their own transactions.
The hold does not prove closure of old grants or provider effects.
Native new effects and acceptance require explicit Ready authority in addition to their generation and job conditions.
A later transition must bind complete old-effect proof to the exact cutoff and account generation.
The internal closure audit reports bounded observations of known legacy records. It grants no closure authority.
Page completion does not prove historical ownership coverage, raw-grant closure, or a complete serving and rollback floor.
Native allocation comparison visits the exact account key across every saved generation, including closed jobs.
Every allocation change advances a billing revision in the same transaction as its obligation and byte totals.
A saved comparison requires that revision to stay unchanged across all pages and its final conditional write.
The comparison checks exact forward and reverse allocation identities. It preserves unknown reservations and never repairs counters or deletes objects.
Earlier service versions without that revision remain an external deployment-floor prerequisite.
Reads, exact close, and positive-evidence recovery remain available during the hold.
Legacy purge has no authority over native backup files or native obligations.

BackupStream adds stream_id and source_home.
The first successful jobs/begin conditionally registers the stream to its authenticated stable service subject and exact native home.
Authentication uses the existing service credential system.
The subject remains stable across token refresh; it is not the access-token bytes.
Another subject cannot adopt that stream through this protocol.
Restore preserves the native home but starts a fresh stream.

BackupJob adds commit_id. The client saves this identity before a cloud request.
The native root, recovery profile, and expected predecessor belong to the immutable JobBeginInput.
The service saves its hash before object transfers.
Changed input under the same identity returns IdentityConflict.

Native generation, native frontier, backup ordinal, and private control revision have different meanings.
The first accepted backup has ordinal 1. A successor has its predecessor's ordinal plus one.
Native generation and frontier cannot decrease within a stream.
An unchanged generation requires the same NativeRoot summary.
Backup capture and restore also compare that summary against exact native bytes.

bytes_sha256 hashes exact stored bytes with SHA-256.
An ETag is not a byte-integrity proof.

input_digest hashes this frame:

    u32_be(domain UTF-8 length) || domain UTF-8 bytes ||
    u64_be(canonical JSON length) || canonical JSON bytes

| Value | Domain |
| --- | --- |
| JobBeginInput | lastdb:native-backup:job-begin:v1 |
| TransferInput | lastdb:native-backup:transfer:v1 |
| CommitInput | lastdb:native-backup:commit:v1 |
| Array [version, scope, object] | lastdb:native-backup:object-receipt:v1 |
| CloudScope | lastdb:native-backup:scope:v1 |
| RecoveryDescriptor | lastdb:native-backup:recovery:v1 |

The receipt hash names one canonical scoped byte identity.
The authoritative private receipt record supplies its fixed provider location and verified evidence.
A client-created receipt with a valid hash does not establish that authority.

## 3. Capture and protected backup files

Backup capture selects one complete committed root and creates its durable file hold.
It selects the complete snapshot CURRENT after the required flush and retains its exact immutable file inventory.
The snapshot has no journal frontier, stream, or tail file.
It releases the local write gate before network work.
Ordinary local reads and writes use their existing memory and disk rules.

The source job saves its hold identity, root, exact inventory, and initial transfer inputs before cloud work.
A restart resumes those files. It does not substitute a newer root.
A full local backup copy is not required.
A generation hold protects that exact cut while later writes and compaction proceed.

NativeRoot is a service-visible summary:
home identity and digest, full CURRENT digest, native generation, frontier, and required native features.
Detailed directory roots, placement, codecs, layout, and logical markers remain in the existing native format.
Backup capture and restore use the native decoder to check those details.
The current native profile supports mask 0x1fd, requires 0x1c5, and uses codecs 0x0101.
The logical marker `layout:v3:keys` requires direct ASCII value `6`.
An explicit encrypted account-key proof authenticates the restore account key.
Only local CAS records use the outer storage envelope.
File bytes retain their existing encryption.
An atom row has no content seal. It is plain JSON inside an encrypted run-file block.
Tip values and other control values remain readable in the stored backup bytes.
Earlier logical formats require an explicit offline conversion; restore does not rewrite them.
The native record transfer converts formats `4` and `5` in a separate inactive target.
The original native reader restores an older physical backup before that transfer.
Unsupported features cause refusal before destination publication.
The snapshot-commit bit 0x100 is required. The retired journal bit 0x2 is refused.
Native readers and this unreleased cloud profile refuse the old journal format without automatic conversion.
Saved job identities, bytes, and hashes cannot change to bypass that format boundary.

Native files retain their exact stored bytes and hashes.
Small metadata files may share an 8 MiB transport object with explicit member extents.
Ordinary native packed files need no second database layout or encryption pass.
The current native physical frames remain plain; upper layers protect stored values where configured.

File-pointer changes and dependency-directory changes share the same native mutation batch.
Backup capture reads that directory from the same protected root.
The current local cache is not the dependency authority.
An old pointer without a certified dependency prevents a complete backup.

A local cas_blobs record already belongs to the native packed file set.
An exact directory record, native key, and authenticated file check can certify those bytes as native-covered.
Those bytes require no second blob upload or external manifest dependency.
The importer verifies that coverage against the restored native root and the original pointer.
An external file needs a verified backup-owned copy and a persistent restore resolver.

All required file blob bytes use backup-owned objects.
Copy authorized external bytes once, then reuse verified backup objects in later snapshots.
Do not retain a dependency on an external owner's deletion policy.
If backup capture lacks permission to copy required bytes, it refuses completion.

Preserve each stored source hash, length, encryption suite, and encrypted key reference.
The dependency map key is the pair logical_pointer_sha256 and FileDependencyKind.
The digest hashes canonical JSON bytes of the exact decrypted whole validated file-pointer atom value, including its outer $lastdb_file wrapper.
It includes all optional pointer members and excludes the atom ID, atom metadata, and stored ciphertext.
Main selects the parent blob_ref, dek, and cipher_suite.
Thumbnail selects thumbnail with kind image. Poster selects thumbnail with kind video-poster.
Both nested forms select their own blob_ref and access.dek / access.cipher_suite.
Backup capture and restore use the existing pointer validator and compare the selected suite with the dependency.
There is no second key-reference digest. The existing encrypted pointer retains the key material.
Legacy $blob_ref or metadata-only file references require certified native coverage or explicit refusal.
Backup capture cannot silently omit them.
Large blob streams use ordered extents over objects of at most 64 MiB.
The first profile supports the existing convergent and legacy DEK file suites.
The account content key does not replace a per-file DEK.
A persistent restore dependency map resolves original file identities through the backup extents without a native-root rewrite.

## 4. Job lifecycle and authority

jobs/begin saves the immutable input and conditionally creates or checks the stream's one active job.
It checks the authenticated subject, current account generation, exact native home, predecessor, and admitted resources.
The same input returns the original job result.

The public lifecycle has three states:

| State | Meaning | Legal terminal result |
| --- | --- | --- |
| Active | The exact job may transfer bytes and submit its manifest | Published or Closed |
| Published | The acceptance transaction committed its AcceptedBackup | The same Published result |
| Closed | A close transaction permanently fenced this job's acceptance | The same Closed result |

Active does not expose private worker phases.
Transfer rejection does not silently close the parent job.
The client can replace a definitively rejected transfer through a new request identity, or explicitly close the job.
A changed frozen commit requires a new job.

jobs/status reads the exact BackupJob and returns its original input hash.
A recovery caller therefore needs no lost local input hash for a status read.
jobs/close requires that saved hash and one CloseReason:
UserRequested, SourceUnavailable, or GenerationChanged.

Close is one conditional control-store transaction:
it marks the exact active job Closed and clears the stream's active-job slot.
It does not change the accepted backup pointer.
If acceptance wins the race, close returns the original Published result.
If close wins, no later worker can accept that job.
Repeated close returns the first terminal result and preserves its original reason.

Close also handles an absent job after a lost begin response.
It conditionally creates a permanent Closed record with the exact BackupJob, begin-input hash, and reason.
That path leaves any other active job and the accepted pointer unchanged.
A later begin must check that record before admission and return Closed for the same input hash.
Changed input returns IdentityConflict. The Closed record has no expiry rule.
If begin wins the race, close reads the saved job and uses the ordinary conditional close path.
NotFound alone never permits the client to release its source hold after an uncertain begin.

Closure may complete while an old PUT outcome remains unknown.
The private byte reservation, fixed target, and accounting obligation remain after closure.
A late write can create an unreferenced object, but it cannot publish the closed job.
Closure authorizes neither deletion nor an assumption that the old write failed.
The source releases its local hold after it saves terminal closure and stops its own source readers.

Authority rules differ by operation:

| Operation | Required authority |
| --- | --- |
| New job, grant, metadata write, or acceptance | Current account generation, registered subject, and exact Active job |
| Read an accepted backup | Authenticated read permission for that selected scope and backup |
| Close a job | Its normal write authority or explicit scoped recovery authority |
| Record an old provider outcome | Trusted service recovery authority over the exact saved private attempt |

Trusted recovery may record evidence and close old admissions after a generation change.
It cannot issue a new old-generation grant or publish a closed job.
A fence must not prevent evidence records from resolving old reserved bytes.
The authenticated personal account owner supplies scoped recovery authority for its own old jobs.
The service compares the authenticated account identity with the exact scope; the request body supplies no authority.

## 5. Small immutable transfers

Each TransferInput contains version, request_id, BackupJob, and ObjectIdentity.
ObjectIdentity contains the stored SHA-256, byte length, and kind.
The kind determines the format; there is no separate encoding choice.

| Kind | Stored format |
| --- | --- |
| NativeFile, MetadataPack | Existing native stored bytes |
| ManifestPage, ManifestIndex | Existing account envelope v1 |
| BlobPart | Existing stored blob bytes |

Before a grant, the service saves the frozen transfer input, its hash, its fixed unique target, and the byte reservation.
The same request and input returns the same transfer.
Provider addresses and worker records stay private.
The service explicitly routes the new namespace to R2; the legacy unknown-prefix selector cannot choose its backend.

Use one signed conditional PUT per object.
The client sends signed If-None-Match: * and the exact signed Content-Length.
The URL signature must enforce both values.
No grant permits overwrite, deletion, arbitrary keys, or control metadata writes.
The client streams bytes with bounded memory.
A retry repeats the bounded object through the same saved target; it does not resume a multipart upload.

R2 evaluates create-once conditional writes against committed state.
The service still verifies full stored SHA-256 and length before it issues a receipt.
A provider existence response or ETag alone is insufficient.
See [R2 conditional writes and uncertainty](https://developers.cloudflare.com/r2/reference/consistency/).

A 412 response can mean the earlier request succeeded.
The service checks the exact fixed target and expected bytes.
A negative read, timeout, expired URL, or elapsed interval does not prove that an in-flight PUT failed.
The service may repeat the same conditional PUT without a new target.
At most one write creates that target; later retries verify the winning bytes.

| Public transfer result | Meaning |
| --- | --- |
| Pending / AwaitingBytes | Required verified bytes do not exist yet |
| Pending / Verifying | The service checks the stored bytes |
| Pending / OutcomeUnknown | The service cannot yet establish the exact outcome |
| Verified | The authoritative receipt is available, through upload or safe reuse |
| Rejected | The exact request has a known terminal failure |

Verified and Rejected remain terminal for that request identity.
Missing bytes remain Pending. A missing local source uses jobs/close.
TerminalFailureCode includes only InvalidInput, UnsupportedFormat, UnsupportedKeyMode, and IntegrityMismatch.
Pending and retryable error codes cannot appear in TerminalFailure.

Resume returns a temporary grant, the current transfer state, or a typed request error.
A verified-state race therefore returns the receipt instead of an unnecessary upload.
Status never contains a URL.
The complete operation triggers byte verification; it performs no provider multipart completion.
Temporary read and upload URLs never enter durable manifests or job records.

Independent requests use bounded batches or parallel calls.
The service uses batched keyed control reads.
Every transfer prepare batch selects one exact BackupJob, with at most four items.
The service sums new reservations and admits that batch through one conditional transaction.
It does not send one read and write sequence for each item.
Usage charges and uncertain byte reservations use independent durable obligation identities.
A charge retry cannot duplicate usage or keep the stream's active-job slot indefinitely.

## 6. Manifest and finite reference closure

The encrypted manifest describes private native paths, original file pointers, key references, hashes, and extents.
The public reference tree describes the exact verified cloud object set.
Their shared reference digest binds the two views.
The service checks byte availability; the importer checks native meaning after decryption.

The public tree contains each full ObjectReceipt once in strict receipt order.
Manifest page references and file extents use receipt IDs.
Receipt scope must equal the backup scope.
Every extent must fit its selected receipt and use the required object kind.
The complete native inventory must have no duplicate or extra path.

Build the closure in this order:

1. Obtain receipts for native files, metadata objects, and backup-owned blob parts.
2. Save each exact encrypted manifest page, then obtain its receipt.
3. Sort those content and page receipts into bounded public ReferencePages.
4. Save their exact canonical bytes through references/put.
5. Create and save a bounded ReferenceIndex over those pages.
6. Compute ReferenceRoot from the exact canonical index and its checked totals.
7. Put that root and the recovery commitment into the encrypted manifest index.
8. Save its exact encrypted bytes and obtain its separate receipt.
9. Submit CommitInput with the reference root and encrypted index receipt ID.

Each reference descriptor binds page hash, length, count, object bytes, and first and last receipt IDs.
Page indices are consecutive from zero. Receipt ranges cannot overlap.
ReferenceRoot includes content and encrypted page objects.
It excludes public reference metadata and the encrypted manifest index itself.
The finite tree and separate index receipt avoid a circular hash.
Quota admission adds the excluded metadata and temporary bytes separately.

The service verifies the full reference tree and the separate authoritative ManifestIndex receipt.
That check does not prove encrypted native semantics.
The importer verifies the decrypted index, all page contexts, receipt membership, file hashes, extents, totals, and exact native inventory.
It checks that the index job, native root, reference root, and recovery commitment equal the selected AcceptedBackup values.
It calls validate_for_accepted only after byte-hash verification and authenticated decryption of that selected index object.

Manifest encryption uses existing account envelope v1:
one version byte, 12-byte random nonce, ciphertext, and 16-byte tag.
It has 29 bytes of overhead, no AAD, and no key ID.
Its authenticated plaintext includes version, BackupJob, exact CURRENT hash, and page index.
Save each exact ciphertext before that object's transfer preparation.
A retry does not encrypt that object again.

## 7. One atomic acceptance point

CommitInput selects an existing Active job through its exact begin-input hash.
It adds the final ReferenceRoot and encrypted manifest index receipt ID.
The first submission freezes those bytes under that commit identity.
A different submission returns IdentityConflict.

The service verifies all required authoritative receipts and exact reference metadata before acceptance.
These objects and records remain immutable and protected with cloud deletion disabled.
The private verification certificate binds the complete tree and submission hash.
This certificate permits a small final transaction; it does not replace receipt verification.
The transaction need not read thousands of object records at once.

One Dynamo DB transaction performs these actions:

1. Check current account authority and generation.
2. Conditionally update the stream from the exact predecessor and Active job to the new BackupHead.
3. Put the immutable AcceptedBackup record under the exact commit identity.
4. Mark that job Published and clear its active-job admission.

Combine conditions and updates for the same item into one action.
Check the frozen begin hash, frozen commit hash, verification certificate, native order, and registered subject in those conditions.
No action can target the same item twice.
Add deterministic usage obligations within the bounded transaction or retain them independently through the job.
Never lose an obligation between accepted bytes and charge completion.

AcceptedBackup is also the commit receipt.
There is no second receipt object or mutable R2 head.
The stream head contains only the accepted commit ID, digest, and ordinal.
Private storage records may reference that immutable accepted record instead of duplicating its complete contents.

Dynamo DB supports conditional all-or-nothing transactions.
Its token window is not the durable retry identity.
See [TransactWriteItems](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html).

A lost transaction response returns Pending / OutcomeUnknown.
The client reads status through the same job and retries only the same frozen CommitInput.
An accepted status returns the original AcceptedBackup, even after a newer job starts.
NotFound means no saved submission is visible; it does not prove that a request cannot still complete.
The client repeats the same input or closes the job to fence later acceptance.

heads/read returns the last accepted backup plus the separate active commit ID.
A pending, rejected, or closed new job cannot mask the previous accepted backup.
Before the first accepted backup, accepted is null; an active job can still exist.
Use strongly consistent keyed reads and a transactional read when several changing records require one coherent view.
A private worker event or provider object list is not acceptance evidence.

The client saves AcceptedBackup on local disk before it releases its native source hold.
If that local flush fails, it retains the hold and retrieves the same accepted result.
Local confirmation and remote acceptance remain separate states.

## 8. Recovery and restore

MiniPortableV1 is the only account recovery profile.
RecoveryDescriptor contains that profile tag and the account public key.
The surrounding job supplies scope and native identity.

The existing derivation remains:

    24 BIP39 words -> 32 raw entropy bytes -> Ed25519 seed -> existing E2eKeys derivation

Do not use BIP39's PBKDF seed output.
Preserve the existing Ed25519-to-X25519 conversion and HKDF-SHA256 derivation.
The salt remains fold:e2e:v1.
The info values remain fold:content-key, fold:index-key, and fold:range-ope-v1.

The public key contains 32 raw Ed25519 bytes in lowercase hexadecimal.
The account ID equals the first sixteen SHA-256 bytes of that raw public key.
The importer derives the key from the supplied recovery secret and compares it.
A public descriptor check alone does not prove secret possession.

The recovery secret stays outside the backup.
Recovery also requires cloud service authority and its accepted metadata. A bucket and phrase alone do not satisfy this profile.
Each per-file DEK remains in its existing encrypted pointer data.
No public descriptor, receipt, or error contains seeds, DEKs, API keys, record values, or signed URLs.
Embedded store-keyring recovery requires a later profile.

Authentication supplies authorized account generations after loss of the local home.
streams/list reads bounded pages from the account's keyed stream directory.
Directory entries are discovery information.
heads/read or commits/status supplies the accepted result.
A RecoveryLocator is an optional convenience, not a required surviving local file.

objects/read grants temporary access only to identities in the selected accepted backup's exact closure.
It includes the separate encrypted index and public reference metadata.
It cannot accept an arbitrary client provider key.
The client compares every grant's expected hash and length to its authoritative receipt or reference descriptor.

The first encrypted manifest index read starts with AcceptedBackup's receipt ID, without a local full receipt.
Reconstruct its ObjectReceipt from Version 1, the accepted scope, ManifestIndex kind, and the grant's hash and length.
Verify that reconstructed receipt against the accepted receipt ID before download.
This check also enforces the ManifestIndex envelope byte bound.
Use ReadGrantResponse::manifest_index_grant_for for this first response.
That helper binds the selected job and receipt selector, then verifies the reconstructed receipt before it returns the grant.
An echoed receipt selector alone cannot establish the grant's byte identity.
Other receipt reads use the full receipt from the verified reference tree.

The restore job saves its selected BackupJob, accepted digest, progress, and fresh destination before downloads.
A restart resumes that selection.
The importer verifies encryption context, recovered keys, complete inventory, every digest and extent, and file blob access.
It uses the native validator and completes disk writes before it exposes the destination.
The persistent dependency map must still resolve all file bytes after restart.

Restore preserves the account and native home identity.
It obtains separate authorized credentials, leaves cloud sync off, and starts a fresh stream for any later backup.
It does not adopt the source's private job or registered subject binding.

## 9. API and private record boundary

All operations are reserved POST routes under /api/cloud/native-backup/v1/.
No route becomes available through this contract change.
An authenticated request body never grants authority by itself.

| Route | Request | Successful or operation result |
| --- | --- | --- |
| context/read | ContextReadRequest | ContextReadResponse |
| jobs/begin | JobBeginRequest | JobBeginResult |
| jobs/status | JobStatusRequest | JobStatusResult |
| jobs/close | JobCloseRequest | JobCloseResult |
| transfers/prepare | TransferPrepareRequest | TransferBatchResponse |
| transfers/status | TransferStatusRequest | TransferBatchResponse |
| transfers/resume | TransferResumeRequest | TransferResumeResult |
| transfers/complete | TransferCompleteRequest | TransferItemResult |
| references/put | ReferencePutRequest | ReferencePutResult |
| commits/prepare | CommitRequest | CommitResponse |
| commits/status | CommitStatusRequest | CommitResponse |
| heads/read | HeadReadRequest | HeadReadResult |
| streams/list | StreamsListRequest | StreamsListResponse |
| objects/read | ReadGrantRequest | ReadGrantResponse |
| source/read | SourceReadRequest | SourceReadResponse |
| usage/read | UsageReadRequest | UsageReadResponse |

ProtocolError is the typed error body where no operation result exists.
NotFound maps to 404; input errors to 400; authority failure to 403; identity, generation, job, and predecessor conflicts to 409.
Resource and quota refusal maps to 413.
Single known pending results map to 202; an unknown or retryable backend failure maps to 503 with its stable identity.
Batch results preserve the requested order and keep each item's success or error.
Valid batch envelopes return 200 with each item's state or error; top-level request failures retain their error code.
No response code permits a new identity merely because an earlier outcome is unknown.

Private service state includes stream subject bindings, revisions, provider targets, verification evidence, quota reservations, submission certificates, and usage obligations.

source/read authorizes only an existing personal consumer copy, under the current account generation.
One bounded transactional read binds current control to at most four exact reference records.
Each selector names its original owner scope, optional app, file identity, kind, and expected ciphertext length.
The service derives the consumer from authentication and verifies the exact saved copy identity, path, provider, length, and proof.
The first source profile supports Main only. Other kinds and missing copies receive explicit refusal.
The grant uses the configured B2 target and one saved strong ETag through signed If-Match.
No source request creates, registers, releases, adopts, or copies an object.
The capture still checks the complete original encryption, plaintext identity, exact length, and EOF.

usage/read binds an account-wide observation to the authenticated personal account.
It reads the same reserved and retained counters that native quota admission changes.
Its total also includes observed legacy bytes. Those bytes exclude future unreserved legacy grants.
The response supplies quota bytes and a checked total across all generations.
Mini saves the observation with a timestamp. Missing, invalid, or failed observations stay unknown or retain their prior timestamp.
A usage observation cannot change transfer receipts, acceptance, source release, or byte obligations.
These records stay outside the shared wire crate.
Worker implementation must define conditional durable transitions before its provider code runs.

Use account key nb1#ACCOUNT_ID.
Let S be the framed scope digest.

| Private record | record_id |
| --- | --- |
| Stream directory, subject, head, and active job | s#S#stream#STREAM_ID |
| Frozen begin input and job lifecycle | s#S#job#COMMIT_ID |
| Frozen submission and verification certificate | s#S#submission#COMMIT_ID |
| AcceptedBackup | s#S#accepted#COMMIT_ID |
| Transfer and fixed target | s#S#job#COMMIT_ID#transfer#REQUEST_ID |
| Canonical object receipt | s#S#receipt#RECEIPT_ID |
| Public metadata binding | s#S#reference#page-or-index#SHA256 |
| Private byte reservation or usage obligation | s#S#job#COMMIT_ID#obligation#OBLIGATION_ID |

All substitutions use the canonical scalar encodings.
Directory discovery uses the exact account key and s#S#stream# range prefix.
Trusted recovery uses the exact job prefix to find its transfer and obligation records after loss of local metadata.
Aggregate account quota retains unresolved reservations across account generations.
Keyed reads, not raw provider lists, establish record identity.

The fixed R2 object names are:

    native-backup/v1/S/objects/TRANSFER_SHA256/OBJECT_SHA256-BYTE_LENGTH
    native-backup/v1/S/references/pages/PAGE_SHA256.json
    native-backup/v1/S/references/indices/INDEX_SHA256.json

TRANSFER_SHA256 is TransferInput::input_sha256 over the complete frozen transfer input.
It includes the scope, stream, commit ID, request ID, object kind, hash, and length.
Two jobs with the same request ID and bytes therefore use different targets and independent byte obligations.
A refreshed URL keeps the same target.
Existing verified receipts reuse their saved backup-owned location.
Concurrent first uploads can leave a redundant object at another unique target.
The service keeps the first canonical receipt binding and never overwrites it with that second location.
Each physical allocation has one saved reservation and charge identity before its grant.
Quota counts all retained physical objects and unresolved reservations, including unreferenced bytes after close.
Receipt reuse creates no new payload allocation; it cannot erase an older independent obligation.
Public metadata uses service-only conditional creation over its exact canonical bytes.
A pre-existing metadata object must match its hash and length.

## 10. Bounds, checks, and next work

These are conservative v1 policy limits, not measured speed or capacity claims.
The first reached byte or count bound controls admission.
All totals use checked arithmetic and complete final encodings.

| Resource | Bound |
| --- | --- |
| Native file or metadata object | 8 MiB |
| Blob object | 64 MiB |
| Control request or response | 256 KiB |
| Object receipt or AcceptedBackup | 16 KiB |
| Manifest plaintext page or index | 256 KiB |
| Manifest envelope | Plaintext plus 29 bytes |
| Public reference page or index | 256 KiB |
| Entries per page | 512 |
| Manifest and reference pages | 1024 each, subject to index byte bounds |
| Native files | 65,552 |
| Referenced backup bytes | 1 TiB, plus separately admitted metadata and temporary bytes |
| One external file | At most 512 extents and 32 GiB, subject to page bytes |
| Active backup jobs | One per process and one per stream |
| Concurrent object streams and request batches | Four |
| Cloud-owned working allocations | 96 MiB total |
| Decoded cloud values within that budget | 64 MiB |
| Stream buffer | 1 MiB per stream |
| Tracked local metadata or temporary object cache | 1 GiB |
| Large payload and decode scratch files | Separate filesystem capacity admission |
| Full stored control item | 256 KiB |
| Control transaction | 16 unique items and 2 MiB |
| Account and record keys | 256 and 512 UTF-8 bytes |

The aggregate memory budget includes serialization, encryption copies, decoded values, and stream buffers.
It does not cap the whole LastDB process.
The [resource policy](native-backup-resource-policy-v1.md) identifies `lastdb_storage_policy/src/lib.rs` as the shared source for native sizes and resource values.
Native V1 pages and physical parts remain fixed at 16 KiB and 8 MiB.
A native format change requires compatible versioned readers; resource values are editable source policy, not runtime memory settings.
Capture and restore use bounded buffers and private disk files for the existing encryption formats.
The former 16 MiB backup authentication and 32 MiB native-CAS allocation guards no longer control those stream paths.

The existing native record body bound remains `u32::MAX` bytes.
External ciphertext still obeys the 64 MiB object, 512 extent, manifest page byte, and total backup limits.
The normal local file API has no separate configured file size cap.
The [file capture design](native-backup-file-capture-v1.md) records the exact disk, allocation, and dependency count rules.
The native hold also requires native disk admission and visible retained-byte accounting.
Backup capture cannot delete held files to satisfy a cap.

The current service stores control JSON as a Dynamo DB string.
Count complete stored attribute names and values, including base64 inside that string.
Do not count SDK outer JSON escaping as stored bytes.
The full stored-item and transaction bounds apply to private worker records too.

A service implementation must prove signed create-once headers and exact byte verification before transfer activation.
Native byte admission includes retained native bytes, unresolved reservations, and observed legacy use under one account quota.
Legacy grants without reservations require isolation or the same allocation ledger before activation.
An old usage counter alone cannot bound concurrent legacy uploads.
A restore implementation must prove native record and file recovery through normal product commands.
Format, type, lint, and build checks establish source consistency only.
No new test suite or fixture harness supplies product proof.

B1 backup capture, S1 cloud jobs and transfers, R1 restore, B2 file coverage, and S2 acceptance now have disabled source implementations.
The [second source wave](https://github.com/EdgeVector/lastdb/blob/2f4ba44bd0688b0c0361db325d037524d8bb243a/docs/designs/native-cloud-backup-wave-two.md) (removed) recorded their original boundaries.
The [Mini integration wave](https://github.com/EdgeVector/lastdb/blob/2f4ba44bd0688b0c0361db325d037524d8bb243a/docs/designs/native-cloud-backup-mini-v1.md) (removed) supplied the saved worker, manifest assembler, native client, and normal commands.
The source now supplies read-only scoped Main-file grants and timestamped native usage observations.
Legacy grant isolation, complete allocation reconciliation, and purge exclusion remain release gates.
Scoped thumbnails and posters require a separate exact source grant. Missing consumer copies refuse before backup completion.
P1 proves cloud-only recovery on fresh synthetic data in an approved service environment.

The S1 service worktree imports this crate through its existing fold submodule.
Its dependency path is ../../../fold/cloud_backup_protocol from exemem_service/lambdas/storage_service/Cargo.toml.
That consumer build requires the Lambda's explicit manifest path.
The storage and storage-admin Lambda builds and strict Clippy checks pass in that worktree.
Those source checks do not prove deployed behavior or provider effects.

Automatic cloud deletion and required-object expiry remain disabled.
Unresolved byte reservations and accepted roots cannot disappear through TTL.
Expose retained bytes, pending obligations, job state, blocked reason, and the last accepted backup.
Shared service, CI, routines, deployment, provider lifecycle, or primary changes require Tom's approval after concrete source work is ready.

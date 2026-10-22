# Native cloud backup protocol, version 1

Status: stage-one contract. Runtime support stays disabled.
Source baseline: LastDB `8067843c4f9626470edc971ab10c10e829f021d8` and Exemem Infra `a4ecf4d73465370a8b3e08cbfb44fed677b4ca62`.

The first release copies complete native roots into a personal backup.
It preserves native packed file bytes and reuses verified cloud objects.
A complete backup includes all required file blob bytes and recovery keys.
Device sync, org sync, automatic cloud deletion, and journal suffix backup remain separate releases.

## 1. Contract authority

The [cloud_backup_protocol crate](../../cloud_backup_protocol/src/lib.rs) defines the wire fields, enum values, scalar encodings, and local validation rules.
This document defines the required authority, storage effects, recovery actions, and completion rules.
Neither source alone proves a live cloud result.
Change this document and the types together before a client or service uses a changed contract.

The crate has no runtime, HTTP, provider, or database dependency.
It does not activate a route or remove the native cloud refusal.
Its four capability constants remain `false`.

The client later imports this workspace crate.
The service later imports the same crate through its existing `fold` submodule:

```toml
cloud_backup_protocol = { path = "../../../fold/cloud_backup_protocol" }
```

That path belongs in `exemem_service/lambdas/storage_service/Cargo.toml` after the canonical LastDB change merges.
The service must not copy the types into a second source file.
The service consumer build requires its explicit manifest path; the service workspace excludes that Lambda.
Stage one does not change that submodule or prove that consumer build.

## 2. Encodings and identities

Use UTF-8 JSON. Reject unknown fields, duplicate struct fields, and unsupported enum values.
Reject an oversized raw request before JSON decode.
Use RFC 8785 canonical JSON bytes for hashes and durable protocol objects.
Do not hash a transport JSON string with arbitrary whitespace.

| Scalar | Exact representation |
| --- | --- |
| `Version` | JSON integer `1` |
| `DecimalU64` | Decimal JSON string; no leading zero except `"0"`; maximum `18446744073709551615` |
| `Digest` | 64 lowercase hexadecimal characters for 32 bytes |
| `Id` | Canonical lowercase, nonnil UUID string |
| `AccountId` | Existing 32- or 64-character lowercase hexadecimal identity |
| `RelativePath` | At most 512 UTF-8 bytes and eight normal slash-separated components |
| `Text<N>` | Nonempty UTF-8 text, at most N bytes, with no control characters |

`RelativePath` rejects absolute paths, empty components, backslashes, `.` and `..`.
The importer also rejects symlinks and paths outside its owned destination.
The scalar alone does not establish safe file access.

`CloudScope` contains the account kind, account identity, account generation, and physical cloud generation.
The physical generation cannot exceed the account generation.
The service compares both generations with current authority before each new effect.
Generation values in a request do not grant authority.

`BackupStream` adds `stream_id` and the exact native `source_home` identity.
`Publisher` adds the authenticated principal, durable lease identity, and positive publisher epoch.
The service binds that value to its stored authority and the authenticated request.
The client cannot choose a publisher epoch to replace another publisher.

One stream has one active backup commit and one authorized publisher.
A restore preserves the native home identity but allocates a fresh backup `stream_id`.
Old publisher takeover and device join have no version-one operation.
An elapsed lease timeout does not close an uncertain provider effect.

Native generation, native journal frontier, backup ordinal, and logical writer sequence have different meanings.
Never use one value as another value's progress.
An ordinary write ack confirms the current memory result under the existing local durability rules.
A committed native root confirms the disk boundary. Neither result confirms a cloud backup.
Verified object bytes, a confirmed cloud commit, and a locally flushed confirmation are separate results.
Stored control revisions start at one. Each changed record advances its revision by exactly one.

## 3. Hash rules and object names

`bytes_sha256` hashes the exact stored bytes with SHA-256.
Use it for native files, stored blob streams, encrypted manifests, reference metadata, and candidate head bytes.
An object ETag is an opaque conditional-write token. It is not a SHA-256 proof.

`input_digest(domain, value)` hashes this frame:

```text
u32_be(domain UTF-8 byte length) || domain UTF-8 bytes ||
u64_be(canonical JSON byte length) || canonical JSON bytes
```

| Input | Domain and value |
| --- | --- |
| Transfer | `lastdb:native-backup:transfer:v1`, complete `TransferInput` |
| Commit | `lastdb:native-backup:commit:v1`, complete `CommitInput` |
| Object receipt | `lastdb:native-backup:object-receipt:v1`, array `[version, scope, object, provider, bucket, key, etag]` |
| Scope namespace | `lastdb:native-backup:scope:v1`, complete `CloudScope` |
| Recovery commitment | `lastdb:native-backup:recovery:v1`, complete `RecoveryDescriptor` |

The receipt identity excludes `receipt_id` and `verified_ms`.
A repeated verification returns the original authoritative receipt, including its original time.
The service accepts a receipt only through its authoritative receipt record.
A client-supplied, internally consistent receipt does not establish byte verification.

Let `S` be the scope namespace digest.
Use these version-one object names inside the selected provider bucket:

```text
native-backup/v1/S/streams/STREAM_ID/head.json
native-backup/v1/S/objects/REQUEST_ID/OBJECT_SHA256-BYTE_LENGTH
native-backup/v1/S/references/pages/PAGE_SHA256.json
native-backup/v1/S/references/indices/INDEX_SHA256.json
```

`S`, UUIDs, hashes, and lengths use the scalar encodings above.
The fixed provider, bucket, and complete key survive a restart in durable records.
Current provider defaults must not change a saved location.
There is no automatic provider fallback.
New native namespace objects and heads use R2 for the first implementation.
Do not use the legacy prefix selector; it routes this unknown prefix to B2.
S1 must add explicit native namespace routing, account ownership, quota admission, and usage classification before a provider effect.
The B2 wire value reserves a supported location identity; it does not enable new native transfers on B2.

Each new transfer owns a unique object key.
A later transfer can reuse an authoritative verified receipt for the same scope, hash, length, kind, and encoding.
It must not create another multipart upload against the accepted object's key.
This rule prevents a late completion from overwriting accepted bytes.
Temporary URLs permit part upload only. They never permit head writes, object deletion, or multipart completion.

Reference metadata uses service-only conditional creation with exact canonical bytes.
Its authoritative metadata record saves the fixed provider binding before that effect.
An existing metadata object requires an exact hash and length match.
Its key hash cannot substitute for that match.

## 4. Native root and packed file cut

Use the native backup cut and its exact inventory as the exporter boundary.
The cut completes the native flush, creates a durable generation hold, and captures one full root.
Release the local write gate before network work.
Stream retained files directly; do not require a full local backup copy.

The durable job saves the exact source root, inventory, hold identity, and initial object identities before cloud requests.
Each later manifest object saves its exact bytes before its own transfer preparation.
A restart resumes that cut. It cannot substitute the latest root.
A source hold protects the captured files. It does not protect journal files from later writes.

`NativeRoot` mirrors the complete `CURRENT` and home descriptor fields.
Hash all 4096 bytes of `CURRENT` and the home descriptor.
Verify all mirrors against those bytes during export and import.
Require `frontier == journal_frontier` and the native snapshot's empty `JOURNAL_TAIL` boundary.
No version-one manifest advertises a journal suffix.

Version one accepts the native supported feature mask `0xff` and requires the current base features `0xc7`.
Placement and key codecs equal `0x0101`.
The hash fanout is a power of two from one through sixteen.
The logical key marker is key `layout:v3:keys`, with ASCII value `4`.
Unknown features or a different logical key format cause refusal before destination publication.

Preserve native packed files without a second physical format or encryption pass.
Native frames use the current plain physical format; upper layers protect stored values where configured.
Do not describe every native file as ciphertext.
The native reader remains the authority for checksums, tree structure, and readable keys.

Small metadata files may share a bounded transport object.
The manifest retains each path, native hash, native length, creation generation, object receipt, offset, and extent length.
Use the native header's creation generation when that format contains one.
For fixed metadata without that field, use the captured root generation.
Do not repack ordinary native pack files into a new database layout.
Every extent must stay inside its verified object.
The complete manifest must contain exactly the native backup inventory, including required descriptor and journal files.

Keep the backup job outside the captured database contents.
Node credentials, temporary grants, and private job authority are not restore data.
The client flushes the confirmed cloud receipt before it releases the source hold.

## 5. Manifest and finite reference closure

`ManifestPage` and `NativeBackupManifest` describe encrypted plaintext.
Use the current account content envelope version one: AES-256-GCM, 12-byte random nonce, and 16-byte tag.
The stored envelope has a one-byte version prefix and 29 bytes of total overhead.
This envelope has no AAD or key identifier.
Its authenticated plaintext therefore contains the version, stream, commit identity, exact root digest, and page ordinal.
The importer compares that context after decryption.

Save the exact encrypted page and index bytes before transfer preparation.
A retry reuses those bytes and hashes. It does not encrypt the plaintext again.
The encrypted index lists encrypted page receipts, native totals, dependency totals, recovery commitment, and the same `ReferenceRoot` as the commit.

Build the reference closure in this order:

1. Obtain receipts for native file objects, metadata objects, and required blob objects.
2. Encrypt manifest pages and obtain their receipts.
3. Sort all those receipts by `receipt_id`, with no duplicate identity.
4. Write bounded `ReferencePage` objects and a bounded `ReferenceIndex`.
5. Compute `ReferenceRoot` from the exact canonical index bytes and its totals.
6. Put that root in the encrypted manifest index, then obtain the index receipt.
7. Put the root and the separate index receipt in `CommitInput`.

The reference root includes encrypted manifest pages but excludes the encrypted index itself.
It also excludes its own public reference pages and index.
Those metadata objects form a finite tree, with hashes and lengths in their parent descriptors.
The separate encrypted index receipt closes the manifest dependency without a circular hash.

Each public page contains scoped authoritative `ObjectReceipt` values in strict receipt order.
It also contains sorted `RetentionHoldReference` values for any reused external objects.
Each public hold names an object receipt in that page.
The authoritative hold record binds the same scope, receipt, beneficiary stream, and commit identity.
Each index descriptor commits to page hash, byte length, count, stored object bytes, and first and last receipt identities.
Page ranges cannot overlap. Page indices are consecutive from zero.
`ReferenceRoot.stored_bytes` sums receipt object lengths once per listed receipt.
It excludes reference metadata and the separate encrypted index bytes.
Quota admission adds those excluded bytes and any temporary provider allocations separately.

Before publication, the service verifies the complete public reference tree, every authoritative receipt, and all required retention holds.
It also verifies the separate encrypted index receipt.
After decryption, the client verifies the complete native and file dependency inventory against that same tree.
The service cannot certify encrypted native semantics without the account key.
It must not claim that transport closure alone proves record recovery.

## 6. File blob dependencies and recovery keys

A file pointer alone is not a recoverable backup.
Each main file, thumbnail, and poster requires its exact stored hash, stored length, scope, key reference, and recoverable source.
The exporter captures a durable dependency directory with the same native root.
Each file-pointer change and its dependency-directory change share one native mutation batch.
It must not derive that directory from the current local cache.
An old pointer without a certified dependency prevents complete publication.

Use an existing remote object only when an enforceable hold protects its bytes and key access for the backup lifetime.
The service checks that hold against the exact receipt and owner scope.
Version one permits retained receipts from the same backup scope only.
Another scope requires an authorized copy into the backup scope.
`holds/prepare` creates or returns the same hold for the exact stream, commit, and receipt.
The public reference page carries that hold; the encrypted manifest must name the same hold.
Provider expiry and old owner deletion cannot defeat the hold.
Otherwise, copy authorized stored bytes into protected backup objects and record their ordered extents.
Refuse publication when neither option is authorized.

Version one supports the existing convergent and legacy DEK file encryption suites only.
Keep each existing per-file key in its encrypted pointer or encrypted manifest data.
The account content key does not replace a file DEK.
Public descriptors, receipts, and errors contain no seed, DEK, API key, or signed URL.

The restore path resolves original stored-file identities through the authenticated manifest dependency map.
It does not rewrite the native root merely to replace a provider address.
That map must provide file access after restore and after restart.
If a required resolver capability is absent, the importer refuses completion.

The only version-one account recovery scheme is Mini's portable identity scheme:

```text
24 BIP39 words -> 32 raw entropy bytes -> Ed25519 seed -> existing E2eKeys derivation
```

Do not use BIP39's PBKDF seed output.
Preserve the existing Ed25519-to-X25519 conversion and HKDF-SHA256 salts and info values.
The existing salt is `fold:e2e:v1`.
The existing info values are `fold:content-key`, `fold:index-key`, and `fold:range-ope-v1`.

`account_public_key` is the raw 32-byte Ed25519 public key in lowercase hexadecimal.
The Mini account identity equals the first sixteen SHA-256 bytes of that raw public key, in lowercase hexadecimal.
The importer derives the public key from the supplied recovery secret and compares it before destination publication.
The public descriptor's own consistency check cannot prove possession of that secret.
Embedded store-keyring recovery has no version-one variant.

Disaster recovery preserves the account and native home identity in a fresh destination.
It obtains separate authorized cloud credentials and leaves cloud sync off.
It does not resume a source publisher lease or private upload job.

## 7. Transfers and uncertain provider effects

The service writes the complete `TransferInput`, its input hash, revision, and effect identity before any provider call.
Each transfer carries the pending backup `commit_id` before its manifest exists.
The first transfer prepare conditionally creates or checks the stream's active job and publisher authority.
Later transfer, hold, metadata, and commit requests must name that same active job.
This job admission precedes provider work; final commit preparation checks and advances that existing admission.
The same request identity and input returns the same transfer.
A different input under that identity returns `IdentityConflict`.
Every durable transition uses the expected revision and current account, generation, publisher, and admission authority.

| State | Required action or evidence before the next state |
| --- | --- |
| `prepared` | Reserve quota and the exact provider target before Create |
| `reused` | Return an existing authoritative receipt without Create, part upload, or Complete |
| `creating` | Save the in-flight creation effect before the provider request |
| `create_uncertain` | Recover the same owned effect; do not create another upload |
| `ready` | Save the exact provider upload identity |
| `uploading` | Refresh temporary part URLs for that upload only |
| `completing` | Save the immutable ordered part set and completion effect before Complete |
| `complete_uncertain` | Recover that exact completion; preserve the source and admission |
| `verifying` | Verify stored SHA-256 and length with bounded reads or an equivalent trusted full-byte checksum |
| `verified` | Save the immutable authoritative object receipt |
| `rejected` | Record a definite failure with conclusive closure of every possible provider effect |

Parts use consecutive provider numbers from one.
Each nonfinal part has exactly 8 MiB; the final part contains the remaining bytes.
The maximum object has eight parts.
The service owns Complete and all final object writes.
A successful part response or object existence response cannot create an object receipt.
Before `prepared` leaves for `reused`, the service verifies the receipt identity, byte identity, authority, and required retention.
That terminal path has no new provider binding or completion effect.
The original provider object stays immutable and protected for the new backup.

A timeout, missing list result, 404, expired URL, or elapsed lease does not prove a provider request failed.
The effect ledger records the exact target, exclusive owner, saved request, provider evidence, and closed prior effects.
It exists under the stable effect identity named in the transfer record.
The worker cannot infer that evidence from the phase enum.

Create has no assumed provider idempotency token.
After a lost Create response, reconcile the unique owned key and the original provider request.
Do not repeat Create while that effect can still succeed.
If the provider cannot establish its outcome, retain `create_uncertain` and report `OutcomeUnknown`.
Do not exchange safety for a timed retry against a new key.
This case may require operator evidence or a later provider-specific recovery design.

Complete retries use the saved upload identity and exact frozen parts only.
A confirmed final object still requires byte verification.
Rejected work releases admission only after positive evidence closes all possible effects.
Uncertain work has no TTL deletion rule.

Usage charges have separate durable obligation identities.
Charge retries cannot duplicate a charge or keep a verified transfer's upload admission active.
The charge obligation remains visible until its own terminal result.

## 8. One authoritative head and complete publication

The object-store head is the sole mutable publication head for each stream.
Do not maintain a second independently mutable `latest` value in Dynamo DB.
Keep the legacy namespace outside this protocol.
Only the service worker can write the native head.

`CommitInput` binds the exact root, recovery descriptor, reference root, encrypted index receipt, publisher, and predecessor.
The service validates that predecessor against the authenticated prior candidate.
Native generation and frontier cannot decrease within one stream.
An unchanged generation requires an identical native root.
Compaction may increase generation without a frontier change.

`HeadCandidate` contains the complete commit input, its input hash, and the next ordinal.
It has no volatile timestamp.
Save its exact canonical bytes, SHA-256, expected predecessor ETag, and head effect before the conditional provider write.
The first head uses `If-None-Match: *`.
A later head uses `If-Match` with the exact predecessor ETag.
Never perform an unconditional head write.

| State | Required action or evidence before the next state |
| --- | --- |
| `prepared` | Save the immutable commit and verify the same exclusive active-job admission |
| `references_verified` | Verify the full reference closure, separate index, holds, generations, and authority |
| `writing_head` | Save exact bytes and the effect before the conditional head request |
| `head_uncertain` | Reconcile only the saved candidate and predecessor |
| `head_confirmed` | Confirm the exact candidate hash, bytes, ordinal, and provider ETag |
| `confirmed` | Atomically save the immutable commit receipt and close the exact active admission in the control store |
| `rejected` | Record a definite refusal after all possible head effects are conclusively closed |

One unresolved head effect prevents a later commit on that stream.
A retry may send the same conditional request with the same saved bytes and predecessor.
It cannot change the candidate, predecessor, or effect identity.
A precondition failure requires reconciliation: the original request may already own the head.
A different head or a greater ordinal does not confirm this commit.

Provider head writes and Dynamo DB transactions cannot form one atomic operation.
The service therefore preserves the uncertain states between those operations.
After a crash, it completes the same receipt and admission closure.
It never erases an uncertain record or starts a replacement publisher to infer success.

The head-read operation returns a `PublishedHead` only with its matching confirmed `CommitReceipt`.
The client validates that receipt's complete linkage and the exact candidate hash.
An unconfirmed head returns `ProviderPending` or `OutcomeUnknown`.
It does not return an empty head or silently accept a raw object list.
An empty result means authoritative absence with no unresolved head effect.

After remote confirmation, the client flushes that receipt locally, then releases the durable source hold.
If the local flush fails, the client retains the hold and repeats confirmation through the same commit identity.
That local state is separate from the service's confirmed state.

## 9. Reserved API and control-store rules

All routes below are reserved `POST` operations under `/api/cloud/native-backup/v1/`.
They require authenticated authority and remain unavailable until their implementation and release gate complete.
No request body grants authority by itself.

| Suffix | Request | Result |
| --- | --- | --- |
| `transfers/prepare` | `TransferPrepareRequest` | `TransferBatchResponse` |
| `transfers/status` | `TransferStatusRequest` | `TransferBatchResponse` |
| `transfers/resume` | `TransferResumeRequest` | Temporary `TransferResumeResponse` or typed error |
| `transfers/complete` | `TransferCompleteRequest` | `TransferItemResult` |
| `references/put` | `ReferencePutRequest` | `ReferencePutResult` |
| `holds/prepare` | `RetentionPrepareRequest` | `RetentionPrepareResult` |
| `commits/prepare` | `CommitRequest` | `CommitResponse` |
| `commits/status` | `CommitStatusRequest` | `CommitResponse` |
| `heads/read` | `HeadReadRequest` | `HeadReadResult` |
| `streams/list` | `StreamsListRequest` | `StreamsListResponse` or typed error |
| `objects/read` | `ReadGrantRequest` | `ReadGrantResponse` |

Prepare accepts at most four transfers and preserves each item's result.
The general transfer route rejects `ReferencePage` and `LogicalPayload` object kinds.
Public reference metadata uses its dedicated route. Logical sync remains unavailable.
Independent keyed work uses bounded batches and parallel calls.
Do not send one serial control call for every native record.
Status contains no signed URL. Resume returns fresh temporary URLs for the original upload.
`references/put` sends bounded public page or index bytes for the deterministic service-only metadata write.
Its receipt returns the fixed provider location, exact hash, and stored byte length.
`objects/read` accepts at most four selected receipt or reference metadata identities from one confirmed commit.
The service proves membership in that commit's exact closure before it returns a temporary read URL.
The request cannot specify an arbitrary provider key.
Read URLs remain temporary response data and never enter durable manifests or status records.

`streams/list` reads the authenticated account's keyed stream directory with bounded pages.
It permits discovery after loss of the local home and private jobs.
Authentication and current account authority supply the authorized scope generations before this request.
The user does not need a saved local scope digest or locator.
Follow every directory page needed for the user's selection, then call `heads/read` for confirmation.
The directory is not publication evidence and does not replace the authoritative head.
The backup command also returns a nonsecret `RecoveryLocator` for the confirmed commit.

Map identity, revision, predecessor, and generation conflicts to HTTP 409.
Map invalid input to 400, failed authority to 403, and resource or quota refusal to 413.
Use 202 for known pending work, and 503 with the saved identity for an unknown provider outcome or retryable control failure.
Use a typed `ProtocolError` in error results. Do not put credentials or record values in its `field` text.
These codes do not authorize a client to allocate a new identity after an uncertain result.

The current service stores control JSON in a Dynamo DB string attribute.
Count UTF-8 attribute names and stored string values, including base64 text inside that string.
Do not count the SDK's outer escaped request JSON as the stored item size.
The full final item, not only its payload JSON, must pass admission.
Each transaction names unique items once and uses conditional revision checks.

Use account key `nb1#ACCOUNT_KIND#ACCOUNT_ID` and the following record keys.
`S` is the scope digest from section 3.

| Record | `record_id` |
| --- | --- |
| Stream directory and publisher authority | `s#S#stream#STREAM_ID` |
| Transfer | `s#S#transfer#REQUEST_ID` |
| Provider effect | `s#S#effect#EFFECT_ID` |
| Object receipt | `s#S#receipt#RECEIPT_ID` |
| Commit | `s#S#commit#COMMIT_ID` |
| Public reference page | `s#S#reference#page#PAGE_SHA256` |
| Public reference index | `s#S#reference#index#INDEX_SHA256` |
| Retention hold | `s#S#hold#STREAM_ID#COMMIT_ID#RECEIPT_ID` |
| Usage obligation | `s#S#charge#CHARGE_ID` |

All substitutions use the canonical scalar encodings.
The service creates or checks stream directory authority before any upload effect.
Directory discovery uses the exact account key and `s#S#stream#` range prefix.
It does not inspect provider object lists for accepted roots.
Use exact keyed reads. Do not infer membership or progress from a partial list.
No active, uncertain, confirmed-root, or required receipt record receives an expiry rule that defeats recovery.
The shared ten-minute transaction token is insufficient as the durable request identity.

## 10. Bounds and admission

These values are conservative version-one policy bounds, not measured speed or capacity claims.
The first reached byte or count bound controls admission.
All totals use checked arithmetic.
The complete final encoding must fit even when a count stays below its separate maximum.

| Resource | Version-one bound |
| --- | --- |
| Native file or metadata transport object | 8 MiB |
| Other cloud object | 64 MiB |
| Multipart size and count | 8 MiB; at most eight parts |
| Raw control request | 256 KiB |
| Transfer session record | 64 KiB canonical JSON |
| Object or commit receipt | 16 KiB canonical JSON |
| Candidate head | 16 KiB canonical JSON |
| Manifest page or index plaintext | 256 KiB |
| Encrypted manifest object | Plaintext plus 29 bytes |
| Public reference page or index | 256 KiB |
| Entries per manifest or reference page | At most 512 |
| Manifest pages and reference pages | At most 1024 each, subject to index byte bounds |
| Native file count | 65,552 |
| Referenced stored bytes per backup | 1 TiB, plus separately admitted metadata and temporary bytes |
| Copied external file | At most 512 extents; at most 32 GiB, subject to page bytes |
| Reused single external object | At most 64 MiB |
| Active backup jobs | One per process and one per stream; coalesce later requests |
| Concurrent streams and transfer batch size | Four |
| Cloud-owned working allocations | 96 MiB total |
| Decoded cloud values within that budget | 64 MiB |
| Stream buffer | 1 MiB per stream |
| Durable local cloud spool | 1 GiB |
| Reserved logical unit | 16 MiB |
| Reserved logical queue | 1 GiB and 1024 requests |
| Full stored control item | 256 KiB |
| Control transaction | 16 unique items and 2 MiB |
| Account key and record key | 256 and 512 UTF-8 bytes |

The cloud allocation budget includes serialization, encryption copies, decoded values, and stream buffers together.
It is not a bound on the whole LastDB process.
The native hold also requires the native disk admission rule and visible retained-byte accounting.
The source may refuse a cut when the hold or spool exceeds its admitted capacity.
It must not delete required held files to meet that cap.

The native 8 MiB part and 65,552-file ceilings come from the current native format.
Dynamo DB permits 400 KiB per item and 100 unique items or 4 MiB per transaction.
Its key ceilings are 2048 and 1024 bytes.
The smaller contract limits leave space for complete item attributes and conditional operations.
See [Dynamo DB constraints](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Constraints.html) and [TransactWriteItems](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html).

R2 permits 1024-byte object keys, 8192-byte metadata, and 10,000 multipart parts.
Its nonfinal multipart minimum is 5 MiB; the contract uses 8 MiB.
See [R2 limits](https://developers.cloudflare.com/r2/platform/limits/) and [multipart uploads](https://developers.cloudflare.com/r2/objects/upload-objects/).
Conditional head recovery uses committed provider state, with the uncertainty rules above.
See [R2 consistency](https://developers.cloudflare.com/r2/reference/consistency/).
The provider implementation must demonstrate equivalent required semantics before a later contract revision enables B2 for this namespace.

## 11. Restore completion and later logical sync

The restore job saves the selected stream, commit, root, receipt tree, completed work, and fresh destination before downloads.
A restart resumes that selected root.
It never combines files from different heads.
It verifies authenticated manifest context, source keys, every file digest and extent, exact inventory, and required blob access.
It then uses the native validator and completes disk writes before destination publication.
The command reports completion only when records and all required file bytes are available after restart.

Format and build checks do not supply that recovery proof.
The later personal release gate uses normal product commands, fresh synthetic data, and cloud-only recovery in an approved environment.
It does not use a primary copy, an archive, an existing source cache, or a new test harness.
Shared service, deployment, provider lifecycle, and primary changes require Tom's approval after the concrete source result is ready.

Logical sync has reserved identity types only in version one.
`LogicalStream` scopes account, generations, database, target, writer identity, and writer epoch.
`LogicalUnitReference` names one complete source commit, not one transport part.
Its dependency root includes the encrypted payload itself, even when no external file exists.
Sparse writer sequence values do not define accepted publication order.
The eventual accepted receipt predecessor chain defines that order.

Preserve existing author signature version three and the native mutation content format.
The new canonical JSON hashes do not replace those signatures.
Raw native `HomeMutation` journal entries do not define a cross-node merge operation.
The later operation contract must cover record transactions, schema authority, proteins, file dependencies, and org authority.

The later local write must commit the complete logical unit and its data in the same native transaction.
The later importer must commit data and its exact scoped `AppliedCheckpoint` together.
Applied progress cannot be shared between personal and org targets.
No logical route, queue worker, replay decoder, or sync capability starts in stage one.

## 12. Next work and ownership

The next stage has three parallel work packages:

- B1: the native owner extracts the retained cut and durable exporter job.
- S1: the service owner adds durable transfer identities, provider effects, and exact object receipts.
- R1: the restore owner adds bounded import and account recovery from this contract.

The integration owner controls protocol changes and shared native transaction interfaces.
B2 then closes file dependencies through the exporter and restore interfaces.
S2 then implements the confirmed head and complete reference rules.
P1 proves personal cloud recovery before a separate activation decision.

Keep automatic cloud deletion disabled through P1.
No first-release provider expiry rule may remove a required object.
Cloud size, retained local bytes, unresolved effects, queue age, blocked reason, and last confirmed root must remain visible.
G1 deletion, J1 journal suffixes, and L1-L3 logical sync each require their own complete design and release evidence.

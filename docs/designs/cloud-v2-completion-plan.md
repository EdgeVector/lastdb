# Complete disk V2 cloud backup and restore

Status: implementation contract and completion plan.
The cloud byte service and the disk V2 client adapter exist.
An accepted source revision alone does not prove an operational backup.
CI supplies the exact executable packages. Real DEV operations supply recovery proof.

## Boundary

LastDB owns the local format, consistent capture, file selection, keys, and restore rules.
The cloud owns account access, byte transfers, verified receipts, storage limits, and accepted backups.
The service does not decode database files, schemas, atoms, molecules, or recovery keys.
Disk V2 and the cloud protocol have separate versions.
A new disk format requires a client adapter change only when the byte contract remains sufficient.

The shared cloud contract stays in `cloud_backup_protocol/src/v2`.
The LastDB descriptor stays null. Client recovery metadata carries the local format and required features.
The service preserves that metadata as opaque bytes.
The client and service use the shared types and hash rules rather than separate handwritten contracts.

## Complete database scope

A backup contains the main store, CAS store, schemas, records, required key records, and local recovery metadata.
The selected main and CAS stores describe one consistent durable point.
The backup also includes every file that those selected stores require.
Installed programs, logs, sockets, process locks, temporary files, and machine settings are outside this database backup.
Recovery material provides access to the account and its encrypted keys.

Each accepted backup describes a complete file set.
Restore does not require a mutation log or a chain of earlier backup records.
Verified object reuse can avoid another physical upload without a dependency on an older backup selection.
Org backup and delivery remain separate layers after this foundation.

## Provider split

The client selects the destination before upload.
`run_backup_destination` is the single client rule for captured native paths.

- B2 holds `disk-cas/runs/<UUID>.ldrun` file bytes.
- R2 holds main-store run files, both store descriptors and CURRENT files, and the node marker.
- R2 also holds the client manifest, its pages, and transport inventory pages and index.

The CAS descriptor and CURRENT file belong in R2, even though they describe CAS data.
Small files share an upload object only when their destinations match.
Large native files use bounded objects and exact file extents.
The backup uses the existing central object, buffer, memory, concurrency, and disk-space policy.
An upload chunk size does not become a native file size limit.

New jobs require both R2 and B2 before capture and remote admission.
Begin keeps the exact provider targets and their distinct identities.
The service refuses an unavailable required destination instead of an alternate-provider upload.
A later configuration change cannot move an accepted object or retarget a saved job.
Existing accepted backups retain their exact receipts and saved targets.

## Encryption and privacy

The client sends existing file bytes without another encryption step or text conversion.
The cloud never decrypts, transforms, or encrypts those file bytes.
Content and encryption keys must remain protected before upload.
The existing client manifest protection stays in place. Non-sensitive transport metadata can remain readable.
No cloud repair path substitutes another encryption layer for a client defect.

The service uses opaque account credentials, job identities, object hashes, lengths, destinations, and storage usage.
It collects no names, email addresses, phone numbers, or payment-customer links for this backup path.
Original filenames and native paths stay inside client recovery metadata.
This layer introduces no sender, receiver, or delivery records.
Provider credentials and operational secrets stay in secret stores and never enter receipts or logs.

Recovery requires the user's recovery material and the encrypted key records in the backup.
Restore cannot depend on a secret that exists only on the lost source device.
The cloud receives no clear encryption key as a recovery shortcut.

## Capture

The owner daemon completes the required flush and selects both stores under the existing Core write control.
The paired capture excludes compound-write changes between the main and CAS selections.
The vendor capture method selects each manifest under its publication control.
The client keeps the exact immutable files through the existing private file-hold mechanism.
The capture is a set of retained links, not a second runnable database home.

The client records both store identities, generations, frontiers, descriptors, CURRENT hashes, and exact file membership.
It flushes the retained links and capture record before new remote effects.
It then releases the Core write control and hashes the selected files through bounded readers.
Normal writes continue while the captured bytes remain available.
No record export or disk format conversion belongs in this path.

## Transfer and acceptance

The client saves one request identity, capture identity, and exact transfer inputs before upload.
Each input binds its job, destination, hash, and byte count.
The client uploads directly through exact provider grants.
R2 keeps its conditional create-once PUT. B2 keeps its service-owned multipart completion and exact object version.
Only verified length and full hash checks permit an authoritative receipt or verified reuse.

The client root precedes the final inventory, which includes that root exactly once.
The service verifies every inventory page and authoritative receipt before acceptance.
One conditional transaction records acceptance, updates the head, marks publication, and clears active admission.
The transaction keeps the current account, predecessor, resource, and storage checks.
An incomplete backup never replaces the previous accepted backup.

## Interruption, retained bytes, and storage limits

A restart resumes the same request, capture, transfer inputs, and bytes.
It never selects newer files under an existing request.
A status read causes no upload, new job, or repeated provider effect.
An unknown provider result requires reconciliation through its saved identity.
A timeout or close does not prove that bytes disappeared.

Each physical allocation and inventory allocation keeps its exact storage obligation.
Unknown results retain their charges and reservations.
The client releases its captured files only after the existing terminal checks permit release and all readers finish.
Successful completion requires cloud acceptance and zero retained local source bytes.
Existing backups, local org data, counters, holds, and unknown allocations remain protected.

## Restore

Restore selects one immutable accepted backup and keeps that selection after a restart.
It verifies the root, complete inventory, object receipts, exact used receipt set, and downloaded hashes.
It reconstructs both stores in a private temporary directory through the recorded file extents.
It checks every final native file hash and the complete referenced file set.
It also checks supported formats, the account key proof, and record-to-file dependencies.

Restore creates fresh process locks and refuses an existing destination.
Only the complete valid result becomes the destination home.
The client preserves native identities and file bytes without a format conversion.
The restored database starts with automatic backup off.
Recovery must succeed after the source stops and without access to its database files.
Normal reads and exact file reads must succeed after two separate boots.

## Product status

The existing product command owns start, resume, status, and close.
Status reports the saved phase, verified objects, native file totals, retained source bytes, and any explicit block.
Local capture, network transfer, cloud acceptance, and local source release remain separate facts.
CPU activity alone does not prove progress.
An observer deadline never authorizes cancellation, another request, or a new capture.

The first milestone is a reliable manual backup and restore.
A later automatic controller calls the same pipeline and admits one active job at a time.
It does not introduce a separate backup implementation.

## Work order and completion

1. Bind the exact client, CI package, service package, protocol, provider targets, and account.
2. Reconcile any earlier request before new work. Preserve incomplete historical evidence and unknown obligations.
3. Correct stale design text and close demonstrated client or service defects through normal PR gates.
4. Use the exact CI client for a real DEV backup with both providers and an interrupted same-capture resume.
5. Stop the DEV source. Restore an absent home, compare every captured native file, and verify records and file bytes after two boots.
6. Verify the complete cloud receipt and allocation inventory, provider identities and versions, exact account delta, and zero request reservations.
7. Use the accepted client for the separately authorized primary connection and one production backup.
8. Verify production publication, stored provider bytes, unchanged primary identity and process, and complete local source release.

Use fresh DEV data for recovery proof while the primary-copy prohibition remains in effect.
Production byte verification does not require a second primary home or a production restore.
Use CI for builds and required product benchmarks. Do not run tests or local LastDB builds, checks, clippy, or benchmarks.
Keep raw operational evidence outside Git. Use CI summaries and artifacts for CI evidence.
Keep PROD canaries off until Tom changes that instruction; retain alarms, rollback, and source/package checks.

Completion means one product path reaches both providers, publishes a complete backup, releases its local source, and restores correctly in DEV.
The production primary must separately publish its own verified backup through that accepted path.

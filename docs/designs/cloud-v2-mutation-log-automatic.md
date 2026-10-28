# Automatic backup: mutation log and daily snapshots

Status: implementation design. Automatic backup remains off until the complete DEV recovery proof passes.
This design replaces automatic full snapshots as the frequent backup path.

## Result

The daemon uploads new mutation-log batches without a manual request.
It uploads a full snapshot once per day by default.
The owner can configure the snapshot interval separately from the log upload interval.
The defaults are 86400 seconds for snapshots and 60 seconds for log uploads.
The command is `lastdb cloud backup-v2 enable-automatic --log-interval-seconds 60 --snapshot-interval-seconds 86400`.
Both intervals accept 60 through 604800 seconds.
The owner uses `disable-automatic` to stop new requests and retries.
An initial full snapshot establishes the recovery base when automatic backup starts.

LastDB owns the mutation log, its order, native formats, encryption, capture, and replay.
The cloud continues to accept opaque byte objects, verify receipts, enforce storage limits, and accept immutable recovery points.
The service never decodes a mutation, a native file, or a recovery key.
The existing R2 and B2 transport remains the common path.

## Native mutation log

A native write batch already produces encrypted immutable run files.
Those files hold the exact stored mutations, including deletes, counters, schemas, original identities, and file references.
The new journal retains the new commit files before the writer releases its references.
Compaction can then replace the ordinary files without removing the journal bytes.
This is a log of actual durable write batches, not a repeated complete file inventory or a record export.

One journal entry describes one complete batch across the main and CAS stores.
It contains an epoch, a sequence, the preceding entry hash, both previous frontiers, both resulting frontiers, and the ordered commit-file identities.
The native order remains CAS additions, main records, then CAS deletions.
The entry and its retained files become durable before the batch reaches its normal completion boundary.
An entry becomes eligible for upload only after its complete commit marker is durable.

The journal preserves the existing encrypted file bytes.
CAS commit-file bytes go to B2. Main commit-file bytes go to R2.
Client manifests and non-sensitive transport metadata go to R2.
The cloud adds no encryption. Existing client manifest protection stays in place.
Content and keys retain their current native protection.

## Capture, gaps, and resource limits

The native writer and snapshot capture use the same commit control.
A snapshot selects both native stores at one complete durable point.
Its existing client profile records each store's frontier.
The journal cursor for that snapshot must match both frontiers exactly.
Entries after that cursor belong after the snapshot.

A crash can leave an incomplete journal entry after a native write.
Recovery accepts only an exact complete next entry, or reports a journal gap.
A gap never becomes a claimed uploaded frontier.
The controller requires a new complete snapshot before it publishes later log entries.
It preserves any submitted job and uncertain provider result through the existing reconciliation rules.

The journal uses the central local backup resource limits and the existing disk-space checks.
A full local log stops log progress and reports the gap or resource block.
It does not silently discard unprotected changes or claim current cloud protection.
Local ordinary writes retain their existing disk and memory rules and never wait for a cloud request.
The status distinguishes local durability, pending log work, cloud acceptance, and retained source bytes.

## Snapshot and ordered log

A snapshot remains a complete native backup through the accepted V2 byte pipeline.
A mutation-log checkpoint references that snapshot and the preceding accepted checkpoint.
Its inventory contains the new complete log batches, their client metadata, and their exact object receipts.
The client checks a continuous sequence from the snapshot through the selected recovery point.
It uploads new log bytes and metadata instead of another full inventory of the growing log prefix.
The daily snapshot starts a new recovery base and ends the earlier replay chain.

The new log root is a client metadata version, not a cloud protocol or native file-format change.
It binds the base snapshot, preceding accepted point, account scope, epoch, entry sequence, entry hashes, and final frontiers.
Its pages describe exact log file extents and receipts.
The byte service applies the same receipt, length, hash, quota, predecessor, acceptance, and storage checks.
It receives no mutation-log schema or native-format field.

One conditional cloud acceptance publishes the new recovery point after its complete batch inventory passes verification.
An incomplete log upload leaves the earlier accepted recovery point available.
The controller releases local journal bytes only after acceptance covers those exact entries and all readers stop.
An accepted daily snapshot replaces the recovery base and covers the earlier log prefix.
The snapshot and every accepted checkpoint required for replay remain protected.
The current service retains accepted backups; this change adds no cloud backup deletion policy.
A future deletion policy must preserve the complete dependency set for each retained recovery point.

## Controller and owner controls

The owner explicitly enables automatic backup through the daemon socket.
The private policy binds the current account, native home, endpoint, and credential hash.
It records the log interval, snapshot interval, recovery base, uploaded journal cursor, pending request, next due time, and any block.
Old credentials and a restored home never enable automatic work.

The controller uses one active request and the existing backup runtime.
A temporary failure or restart resumes the same request, files, transfer inputs, and provider obligations.
Quota, credential, identity, integrity, and unsupported-format errors stop automatic retries and appear in status.
An unfinished manual request is never adopted or replaced.
Automatic requests use a reserved ID prefix.
After source release, the next request removes the previous automatic request's local transfer and terminal files.
An expired automatic ID returns absent and cannot start a new manual backup.
The cloud keeps the accepted backup and its receipts.
Older UUID requests retain their existing terminal records; new automatic work does not add to that history.
Disable stops new automatic requests and retries while an active invocation can finish safely.

New mutations make the log due for upload within the configured log interval.
An unchanged journal causes no repeated upload.
The daily snapshot schedule remains independent from successful log uploads.
An active snapshot, an offline connection, or a required retry can delay a log upload.
The local journal retains those mutations and status reports the pending frontier.
A journal gap or a recovery-manifest resource limit can require an earlier snapshot.
Status reports that reason and the actual last successful snapshot and log checkpoint.

## Restore

Restore freezes one accepted recovery point and follows its exact accepted predecessors to the named snapshot.
It checks the account, hashes, sequence, repeated references, and resource bounds before replay.
It saves the finite replay plan outside the destination and never follows a newer head.
Each checkpoint supplies its own complete batch inventory and authoritative receipts.
It restores the base snapshot into an absent private destination through the current native checks.
It then replays complete journal entries in order through the native file-import path.
Each entry must match the preceding entry hash and both expected native frontiers.
A missing, repeated, reordered, wrong-account, or corrupt entry prevents publication.

The client saves its replay position after each original native part.
An exact repeated part is safe after a crash.
After a complete checkpoint and a durable cursor, native compaction can remove replaced runs.
Compaction changes no logical frontier and keeps the replay file count bounded.
The restore starts no TTL, conflict, dwell, or daemon worker before the final proof.

Replay preserves the original stored mutations and encryption keys.
It creates fresh process locks and does not use the source database.
After replay, the client verifies the complete native closure, account key proof, counters, schemas, records, and file dependencies.
Only the complete valid destination becomes available as a restored home.
The restored home starts with automatic backup off.

## Work and acceptance

1. Add the optional native journal and bounded commit-file retention. Keep it off by default.
2. Add the client log manifest and native replay path. Keep the cloud byte contract unchanged.
3. Extend the saved byte pipeline to publish exact log checkpoints with complete batch inventories and accepted predecessor references.
4. Add the owner controls and controller with separate log and snapshot intervals.
5. Use the exact CI client for real DEV writes, an initial snapshot, automatic log uploads, restart recovery, and a later scheduled snapshot.
6. Stop the source. Restore a snapshot plus multiple log batches, including an update, a delete, counters, and an actual CAS file.
7. Verify exact keyed values and full file bytes after two restored boots. Verify gap refusal, source release, provider identities, complete stored-byte hashes, quotas, and reservations.
8. Complete CI, review, and required product benchmarks. Use the supported compatible primary upgrade route and a current cloud recovery point.
9. Enable automatic backup on the primary. Verify a daemon-initiated log checkpoint and the configured daily snapshot schedule.

Do not run tests or local LastDB builds, checks, clippy, or benchmarks.
Do not copy the primary, open its archive, convert its native format, or add cloud encryption.
Keep raw operational evidence outside Git and preserve accepted backups and uncertain obligations.
Keep PROD canaries off until Tom changes that instruction.

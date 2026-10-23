# Automatic native cloud backup V1

Status: proposed design only. Source basis: `191fed435e32289bf3cb1c943fae8f9c12e8d050`.
This document changes no product behavior, release gate, account authority, or service.
Implementation depends on the first approved fresh DEV backup and restore roundtrip.
That result must include normal record reads, packed native files, and exact recovered file bytes.

## Current state

The [native owner requests](../../lastdb_node/src/native_cloud_backup/model.rs#L8) support Start, Resume, Close, and Status.
The [daemon background tasks](../../lastdb_node/src/main.rs#L175) include no automatic backup controller.
The [worker result path](../../lastdb_node/src/native_cloud_backup/control.rs#L127) saves Waiting without an automatic retry.
The existing job, capture, transfer, manifest, and acceptance contracts remain the backup contract.
This proposal covers personal native backup. It adds no device or org mutation replay.

## Proposed normal commands

Each command requires an explicit absolute home and uses that home's owner socket.

```text
lastdb --data-dir HOME cloud backup auto on [--json]
lastdb --data-dir HOME cloud backup auto off [--json]
lastdb --data-dir HOME cloud backup auto status [--json]
```

`on` validates the actual account, native home, existing credentials, endpoint, and current cloud context.
It requires the native release gate, current Ready authority, and an account open for writes.
It saves explicit consent before it permits automatic work.
It resumes a compatible saved job first; otherwise, it requests a backup when the durable frontier needs one.
Repeated `on` is idempotent for the same saved consent and account scope.

`off` saves disabled consent before it returns. It requires no cloud request or release gate.
It stops future automatic starts and resumes. An already active worker may finish its current invocation.
It does not close a cloud job, delete files, release reservations, or remove receipts.
The existing `cloud backup close` command disables automatic mode before it saves the close intent.
Thus an explicit Close cannot cause an immediate replacement backup.

`status` reads bounded local state. It performs no cloud request and requires no release gate.
It shows enabled state, pause reason, bound scope, current frontier, accepted frontier, active request, phase, and next retry.
It also shows the existing saved byte totals and pack counts. It labels cached cloud usage with its observation time.
It never presents cached usage as acceptance, closure, or released-byte authority.

## Consent and local authority

The default is off. Missing credentials, paused credentials, and an environment flag do not imply consent.
The [source gate](../../lastdb_node/src/native_cloud_backup/mod.rs#L20) remains necessary but does not replace per-home consent.
Use one small versioned file beneath [the existing manager directory](../../lastdb_node/src/native_cloud_backup/store.rs#L36).
Bind consent to the account ID, account generation, source-home ID, and configured native endpoint.
Store no credential or recovery phrase there. Require the existing private-file and durable replacement rules.
Invalid or mismatched state refuses automatic work; it never defaults to enabled.
Keep this file outside the captured native root, so its own updates do not request another backup.

Use one short owner transition lock for consent changes and automatic job admission.
Check enabled consent inside that lock before each automatic start or resume.
Use the existing separate [worker permit and home lock](../../lastdb_node/src/native_cloud_backup/control.rs#L71) for active work.
Auto-off does not wait for a cloud request. After its response, the controller admits no further invocation.
Manual Start and Resume retain their existing explicit authorization and worker serialization.

## Durable change signal and coverage

Add a narrow native boundary getter and watch; do not query records or enumerate files to detect a change.
Publish the watch only after [complete native publication](../../fold_db/crates/core/src/storage/disk_pack/view.rs#L159).
Initialize it from the saved CURRENT, including its native home and frontier.
The watch is a wake signal. The saved native boundary and AcceptedBackup determine coverage.
The existing [queue completion watch starts at zero](../../fold_db/crates/core/src/storage/disk_pack/mod.rs#L161); it is not this boundary.
App change-feed events also cannot prove complete native coverage.

Coverage requires the same account scope and source home, through the endpoint bound by consent.
At enable, reconcile the local accepted receipt with that endpoint's exact stream head before it supplies coverage.
For that scope and home, a current frontier above the accepted frontier requires another backup.
No accepted backup in that scope requires an initial backup, even when the frontier is zero.
A lower current frontier or a different home requires explicit review; it cannot silently start a replacement stream.
Compaction alone requires no new backup because [it preserves the frontier](../../vendor/laststore/src/disk_pack/maintenance/concurrent.rs#L208).
Schema changes, file records, deletes, and internal native mutations use the same durable boundary.
Cloud work never delays a local write ack or holds the local write gate across network work.

## One active job and one successor

The controller subscribes before its first boundary read, then reconciles saved consent, active job, and accepted coverage.
It repeats that comparison after each wake and worker completion. A lost wake or daemon restart loses no durable change.
When idle and dirty, it saves one request UUID before it calls the existing Start path.
Reuse that UUID after an uncertain local admission or response; never generate a replacement retry identity.
After restart, reconcile this saved UUID before the active selector; a crash can occur between the job-state and selector writes.
If another request is active, wait. Never replace its selector.
Clear the scheduled UUID only after terminal evidence and source cleanup reconcile.
Keep at most one active job and one latest pending frontier. Do not keep one job per mutation.
New writes do not change an active job's frozen cut, exact ciphertext, transfer inputs, or predecessor.

After Published, reconcile its receipt and finish source cleanup before a successor starts.
Only [validated Published acceptance](../../lastdb_node/src/native_cloud_backup/terminal.rs#L69) advances the accepted frontier.
Then read the current frontier again. Start one successor if the accepted cut does not cover it.
Closed, rejected, failed, or cached status never advances coverage.
The accepted receipt remains the coverage record; the controller needs no second mutation queue.

## Retry, authority, and quota

Add a typed controller retry class. Do not parse the current human-readable `blocked_reason` string.
Waiting, transport failure, RetryLater, ProviderPending, and OutcomeUnknown retain and retry the exact saved job.
Reuse the [current capped pause sequence](../../lastdb_node/src/native_cloud_backup/transfer.rs#L152): 1, 2, 4, 8, 16, then 30 seconds.
Apply it between controller invocations; no wake from another local write bypasses an active retry delay.
The [15-minute transfer wait](../../lastdb_node/src/native_cloud_backup/transfer.rs#L8) remains nonterminal, with status-only polls after submission.
An invocation may repeat one exact conditional upload and completion request to repair an uncertain prior submission.

Quota, disk, credential, integrity, unsupported-format, and missing-source refusals pause automatic retries with an explicit reason.
After repair, explicit `auto on` retries the same active job before any new cut.
A changed generation or endpoint also pauses; explicit re-enable cannot rewrite an old job's frozen scope.
Close or reconcile an incompatible old job before new-scope work. Preserve all unresolved obligations.
Before every automatic Start or Resume, check consent, endpoint, native gate, account scope, generation, Ready, and open-for-writes state.
A saved intent does not waive the [current context check](../../lastdb_node/src/native_cloud_backup/setup.rs#L25).
Read context outside the short transition lock; then recheck the consent revision inside it before admission.
The service remains authoritative for each effect.
No automatic retention, deletion, quota debit, Ready update, or reserve release enters this stage.

## Shutdown and restore

Register the controller beside the daemon's existing background tasks and add it to the shutdown sequence.
Shutdown stops new automatic admissions and retries before the final local flush.
Use a nonterminal pause signal for shutdown. Never encode this pause as Close.
Track both controller and worker completion in the daemon drain.
Pause stops new requests; active sibling uploads settle before the worker saves progress.
Retain each in-flight source guard until its readers and upload feeders stop.
Do not wait for cloud acceptance or interpret process exit as provider failure. Preserve the exact saved job for restart.
If the drain exceeds the daemon budget, report an incomplete drain and refuse the clean-shutdown receipt.
The next enabled daemon reconciles terminal evidence first, then resumes the same job before another cut.
New [restore output](../../lastdb_node/src/native_cloud_restore/install.rs#L86) contains no credentials or automatic consent.
A restored home stays off until explicit enrollment and `auto on`; it uses the existing fresh-stream restore rule.

## Sequence and completion conditions

1. Complete and review the first fresh DEV manual backup and restore roundtrip before automatic source work.
2. Add the native boundary API, then the node consent/controller, commands, status, and shutdown seam.
3. Use GitHub CI for type, lint, and build checks. Run no local Cargo, benchmarks, tests, or fixture harnesses.
4. Use approved normal fresh DEV commands to change records and a real large localCAS file without a backup Start command.
5. Confirm accepted frontier advance, packed-file counts, exact restored records, and normal recovered file reads.
6. Change data during an active backup; confirm one active job and one successor that covers the latest durable frontier.
7. Restart the fresh daemon during Pending verification; confirm the same cut, request IDs, and retained source resume.
8. Confirm idle time and compaction alone create no job; confirm off, Close, generation change, and quota refusal stop new automatic work.
9. Confirm the restored home remains off. Record source checks separately from actual DEV results outside Git.

This document authorizes no primary access, cloud call, deployment, CI change, or account mutation.

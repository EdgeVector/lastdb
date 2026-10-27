# Automatic V2 backups

Status: owner-controlled client support. The cloud byte service stays unchanged.

The daemon runs the existing V2 capture, transfer, acceptance, and source-release path.
Automatic backup introduces no second backup implementation, file format, or encryption step.
The existing client rule sends CAS run bytes to B2 and the other backup objects to R2.

## Owner control

`lastdb cloud backup-v2 enable-automatic` enables an hourly check by default.
`--interval-seconds` accepts 60 through 604800 seconds.
The first check starts at once. Later checks start after the configured interval.
The daemon must run for an automatic check to occur.

The owner socket accepts this change only from the local owner.
An existing explicit connection must bind the account, native home, endpoint, and credential hash.
The private policy stores that binding, a revision, the interval, and progress facts.
It stores no secret value, personal information, or provider credential.
Old credentials alone never enable automatic backup.
A restored home starts with automatic backup off.

`lastdb cloud backup-v2 disable-automatic` stops new automatic requests and retries.
An active invocation can finish and release its source through the normal checks.
Disable does not delete its capture, close its cloud job, or remove a backup.
Enable can resume a saved automatic request after an explicit owner review.

## One request

The controller saves one request identity and its current durable frontier before admission.
The existing runtime and file lock admit one worker.
The controller never adopts or replaces an unfinished manual request.
Manual start, resume, and close keep their owner controls.

A restart or a temporary failure resumes the same saved request, captured files, and transfer inputs.
The controller uses a bounded retry interval from two minutes through one hour.
Quota, credential, identity, source, integrity, and other attention errors stop automatic retries.
Status reports the explicit pause. An owner enable request permits a reviewed retry.
Provider outcomes that remain unknown keep their existing identities and storage obligations.

Automatic completion requires publication and zero retained local source bytes.
The controller records the completed request and schedules the next check after the interval.
It then skips a new request when the durable frontier stays unchanged.
The frontier covers both native stores and survives a restart.
Compaction alone does not require a new logical backup.
Writes during capture remain eligible for another backup after the interval.

## Storage and status

The existing service can reuse objects only through its verified receipt rules.
The automatic controller keeps all current quota, reservation, retention, and accounting checks.
It adds no backup deletion or retention policy.
Accepted backups remain protected. A quota error requires an explicit storage decision.

`lastdb cloud backup-v2 status --json` reports the automatic policy, pending request, next check, attempts, last completion, and pause.
The normal backup phase and byte counts remain separate from the automatic policy.
An enabled policy alone does not prove a successful backup.

## Release proof

Use the exact CI client for real DEV operations before a primary upgrade.
Verify automatic start, same-request resume after interruption, and publication without manual start or resume.
Verify disable, a quiet unchanged interval, and a later write followed by another automatic backup.
Stop the source and restore the accepted result into an absent DEV home.
Compare native file hashes, ordinary keyed records, and complete file bytes after two boots.
Verify the saved provider targets, accepted object bytes, account delta, and reservations.

Use the required primary upgrade route with the accepted executable pair and a current cloud recovery point.
Enable the primary policy only after the compatible DEV checks pass.
Completion requires an automatic primary request, cloud acceptance, actual stored-byte checks, and local source release.
Keep the primary identity, native file format, encryption, and existing backups intact.

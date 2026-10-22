# Native cloud backup: Mini integration

Status: M1 source integration in DEV. Cloud sync stays off.
The [shared contract](native-cloud-backup-protocol-v1.md) defines the authority and byte rules.
The [resource policy](native-backup-resource-policy-v1.md) defines the file, memory, and metadata bounds.

## Current state and change

The earlier wave supplies capture, restore, transfer receipts, and cloud acceptance.
This wave connects those components through one saved Mini job.
It adds an exact native service client, manifest assembly, owner commands, and offline restore commands.
The native V1 files keep their existing eight MiB parts and original encryption.
Native file bytes already in the capture need no second payload copy.

The owner derives the account, recovery descriptor, content key, and source home from the actual Mini identity.
It reads the current cloud account generation through a read-only context request.
That request neither creates account state nor adopts a generation.
The first stream treats an authoritative missing head as an empty predecessor.
Unavailable or malformed authority never becomes an empty predecessor.

## Saved sequence

1. Save the owner request, stream, predecessor, and exact capture intent.
2. Flush memory to disk. Retain the native cut and save its frozen job input.
3. Read the exact cloud job status. Reconcile a terminal result before new local proof work.
4. Certify the file dependencies. Begin the exact cloud job when no prior job exists.
5. Upload the native bytes in batches of at most four sources. Save authoritative receipts.
6. Copy one external ciphertext to a private disk file. Freeze its part inputs before each upload.
7. Save every verified part receipt before removal of that local ciphertext copy.
8. Audit all file dependencies. Freeze encrypted manifest pages and exact public reference pages.
9. Upload those exact pages. Save the commit input before the acceptance request.
10. Save the authoritative terminal result before release of the source hold and private metadata cleanup.

The saved job uses private owner files with atomic writes and disk barriers.
An owner file lock and one process permit prevent two active workers.
The worker does not depend on the command connection for its lifetime.
A process restart requires an explicit resume of the same saved job.
Resume reuses the capture, transfer identities, ciphertext, nonces, receipts, and commit input.
It never creates a new source cut for a terminal job.

Manifest assembly checks its exact source inventory and file coverage.
It bounds page bytes, receipt identities, restore memory, and combined metadata before exposure of new work.
The assembler freezes ciphertext before the client can request a grant.
The client sends the native protocol unchanged and validates every result against its exact request.
The client neither calls legacy enrollment nor creates a legacy consumer copy.

## Unknown results and close

An upload response does not establish a receipt.
Only the service's verified receipt establishes stored bytes.
The client checks completion after an uncertain upload response.
Eight bounded settlement attempts can leave a saved job ready for explicit resume.
Every upload batch settles all siblings before return of an error or the next phase.
The upload feeder retains its source guard until cancellation and task completion finish.

Close saves a separate intent before its request.
It remains available when the new-work flag turns off.
A request with no retained cut or cloud admission closes locally without a new capture.
A retained capture requires the exact authoritative cloud close result before source release.
The service's close-on-absence fence prevents a late begin from admission.
Unknown cloud byte obligations stay reserved after close.

Close reloads the latest saved commit before a terminal result or a resumed worker can use it.
This prevents a concurrent accepted result from loss through an older local state write.
A delayed close cannot restart an older job after a newer job becomes active.
Terminal cleanup can repeat after a lost local response.
An archived terminal request cannot select a new active capture.

## Product commands and status

`lastdb cloud backup` supplies `start`, `resume`, `status`, `close`, `discover`, and `restore`.
Start prints its request ID before submission. A repeat can use that exact ID.
Status reads fixed local records. It does not open credentials, audit dependencies, or call the cloud.
Status shows the phase, verified counts, retained bytes, blocked reason, and last accepted backup.
Reserved cloud bytes remain unknown until a native usage view exists.

Discovery uses an explicit phrase-derived account and one bounded directory page.
Restore selects an explicit stream and source home, with an optional exact commit and generation.
The selected accepted backup reaches disk before the first payload read.
Restore resumes only that saved selection and publishes only a fresh destination.
It keeps cloud sync off.
The phrase enters through standard input and does not enter the saved job or command arguments.

## Source review and remaining gates

Source review fixed first-stream absence, terminal retry order, close state races, and restart reconstruction of a private external copy.
It also replaced repeated metadata inventory reads with saved size counters.
File-part readers now share four source slots while confirmation and cleanup keep exclusive guards.
Proof and upload phases use separate memory allowances from the central policy.

Owner-scoped remote file sources currently refuse before a request.
The legacy scoped download action creates a consumer copy and cannot serve as a read-only backup source grant.
A read-only grant contract remains a release requirement for those files.
An exact verified local recovery copy remains usable without that grant.
Uncertified old pointers also receive a clear refusal.

The grant can initially authorize only an existing independent consumer reference.
The service must derive the personal consumer from the authenticated account.
It must read the exact current control and reference records with consistent authority.
The saved logical identity, generation, verified copy, provider, and deterministic copy path must all match.
It can then sign only that existing consumer copy, with exact ciphertext proof at the client.
An absent reference must refuse. The original owner field alone cannot authorize a read.

Native usage views, legacy grant isolation, legacy purge exclusion, API Gateway routes, and activation configuration remain release gates.
Both native activation flags remain off by default. Legacy Cloud On stays disabled.
No deployment, shared infrastructure change, primary access, tests, or runtime cloud recovery occurs in this wave.
Format, type, lint, and build results establish source checks only.

The next source step supplies the read-only file grant and the native usage and legacy isolation boundaries.
P1 then uses normal product commands with fresh synthetic data in an approved service environment.
Tom must approve shared service and infrastructure changes before that operational step.

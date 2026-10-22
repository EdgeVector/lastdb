# Native cloud backup: Mini integration

Status: M1 source integration in DEV. Cloud sync stays off.
This source integrates journal removal from main commit `4118c7baa12539c8c6bcf4391c497159f2a1529a` (PR #268).
The source release still requires the remaining legacy isolation and allocation boundaries.
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
Status includes the last cloud usage observation and its timestamp, with reserved, retained, observed legacy, and quota byte totals.
The worker refreshes that observation at start, after upload batches, and after completion.
Status itself makes no cloud call. A missing or invalid observation stays unknown.
A failed refresh cannot replace the last accepted result or release a source hold.

Discovery uses an explicit phrase-derived account and one bounded directory page.
Restore selects an explicit stream and source home, with an optional exact commit and generation.
The selected accepted backup reaches disk before the first payload read.
Restore resumes only that saved selection and publishes only a fresh destination.
It keeps cloud sync off.
The phrase enters through standard input and does not enter the saved job or command arguments.

## Journal removal integration

The cloud branch rebases onto the merged snapshot commit design from PR #268.
Capture uses the snapshot writer and its disk-space reservations.
It saves exact CURRENT bytes and the durable source reference under the write gate, then releases that gate before network work.
Export contains CURRENT and the exact immutable generation inventory. It creates no journal tail or empty journal directory.
Restore checks the same inventory and the exact home, generation, frontier, feature mask, file length, and file digest.

The shared root summary carries no journal field.
Its supported feature mask is 0x1fd and its required mask is 0x1c5.
The snapshot commit bit is required, and the old journal bit is refused.
The native decoder refuses old journal homes before writer creation.
The unreleased cloud schema also refuses old journal-shaped metadata; no automatic conversion or hash substitution exists.
Any pre-release old jobs require an explicit compatibility decision before service deployment.

Exact saved-cut resume, protected file reads, and terminal source release keep their original sequence.
Compaction and file reclaim still respect each durable source reference and active reader.
The bounded file reader accepts only native packed payload locations.

The 96 MiB source memory policy stays in place after review.
The decode trees, metadata, file proof, and upload buffers still require their separate phase allowances.
Journal removal changes the publication source and does not remove those allocations.

## Source review and remaining gates

Source review fixed first-stream absence, terminal retry order, close state races, and restart reconstruction of a private external copy.
It also replaced repeated metadata inventory reads with saved size counters.
File-part readers now share four source slots while confirmation and cleanup keep exclusive guards.
Proof and upload phases use separate memory allowances from the central policy.

The new source grant reads only an existing personal consumer copy for a scoped Main file.
The service binds current control and exact references in one bounded transactional read.
The saved owner, logical identity, generation, verified copy, provider, length, path, and strong ETag must all match.
It signs a GET with exact If-Match and creates no new consumer copy.
Capture checks original encryption, plaintext identity, complete length, and EOF before a backup-owned upload.

Missing consumer copies receive a clear source refusal.
Scoped thumbnails and posters still refuse. Unscoped authorized reads and exact local recovery copies keep their existing paths.
Uncertified old pointers also refuse.

Complete native allocation reconciliation, legacy grant isolation, legacy purge exclusion, API Gateway routes, and activation configuration remain release gates.
Both native activation flags remain off by default. Legacy Cloud On stays disabled.
No deployment, shared infrastructure change, primary access, tests, or runtime cloud recovery occurs in this wave.
Format, type, lint, and build results establish source checks only.

The next source step closes legacy grant isolation, allocation reconciliation, and legacy purge coverage.
P1 then uses normal product commands with fresh synthetic data in an approved service environment.
Tom must approve shared service and infrastructure changes before that operational step.

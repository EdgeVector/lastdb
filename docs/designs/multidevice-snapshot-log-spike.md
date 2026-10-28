# Shared snapshot and device log spike

Status: isolated implementation spike. Tom approved the concept and requested this prototype.
The prototype implementation is prepared for review. Tom approved the manual CI job and run on 2026-10-28.
Build and operational evidence remain pending.
The spike PR closes after review of that evidence; it does not merge into the product.

## Scope

The `peer-sync-spike` Cargo feature exposes the experimental `peer_sync_spike` core module and example command.
The feature stays off by default. Normal node startup and the product CLI do not use the module.
The CI operation creates fresh native V2 homes and synthetic device mutations in the `__peer_sync_spike_v1` namespace.
The command accepts marked, private prototype roots. The primary and its archive remain outside this work.

This prototype uses signed Put and Delete events, ordered device streams, and a shared snapshot boundary.
It does not implement the production schema, molecule, atom, counter, or CAS mutation path.
It accepts unconditional Put and Delete outcomes. Production source preconditions, Purge, TTL, proteins, shares, and file blobs remain outside the prototype.
It therefore cannot establish production multi-device correctness or replace the current backup path.

The physical V2 format and encryption stay unchanged.
Current native requests complete their durable run commit and clean core publication before the ack.
The spike keeps that rule and the current run writer. It adds no internal recovery journal or delayed ack.
See the [disk contract](../lastdb-disk-format-v2.md) for the corrected current writer, crash boundaries, and limits.

## Prototype protocol

Each device owns one writer identity and one ordered stream.
The origin combines its Ed25519 public key and a random UUID generation.
Each signed event contains its origin, consecutive sequence, predecessor hash, key, value or delete, and conflict order.
An import retains the original author identity and order. It does not create another local event for the same mutation.
The command refuses a gap, a fork, an invalid signature, and a conflicting repeated event.

A snapshot contains the state and a vector of included source positions.
Each vector member binds the device, stream generation, last consecutive included sequence, and prefix hash.
The state and positions must describe one durable cut. A position never means merely received or uploaded.
The snapshot retains the winner and delete metadata needed to refuse stale changes.

The vector expresses per-device order. It does not claim an exact creation order across disconnected clocks.

A peer adopts the shared state plus every uncovered local and remote tail.
Its own unpublished events remain available throughout adoption.
The peer saves imported state and source positions at one local publication boundary.
Local physical files can differ after the peers reach the same logical state.
The prototype holds an exclusive root lock during a command, including snapshot adoption.
It retains signed events for ancestry checks. It does not remove the covered log prefix.
Snapshot admission assumes a trusted producer within the same account key scope.
It verifies encrypted files, a complete logical digest, head signatures, clocks, and matching positions.
It does not recompute every winner or every historical link from the full signed log.

For example, `{A:84, B:31}` includes A through 84 and B through 31.
A peer resumes those streams at 85 and 32. Its unpublished A events 85 through 87 remain part of the adopted state.

## Opaque file boundary

The prototype represents cloud file transfer with a private local directory.
Snapshot data uses the real encrypted V2 files. The client owns the snapshot metadata and the file references.
Log packets use AES-256-GCM with the account key and a random nonce. The packet format authenticates a fixed domain.
The client owns snapshot interpretation and tail merge.
No request reaches the cloud service, and no service source or deployment changes.

The approved target model uses a conditional head update for one canonical shared snapshot.
Its vector covers the previous head's vector. Competing, incomparable snapshots need a merge before another publication attempt.
The prototype does not implement that shared-head update.

In the target model, an accepted snapshot can replace the log prefix that its vector covers.
Uncovered tails, retained recovery roots, active readers, and incomplete uploads must still protect their files.
A locally completed upload alone cannot authorize release of its only durable source.
The prototype retains the signed prefix instead of implementing that retention contract.
Local file operations cannot establish provider verification, service transactions, durable remote holds, or actual provider deletion.

## Limits

| Bound | Value |
| --- | --- |
| Origins | 128 |
| Changes per signed event | 128 |
| Encoded signed event | 256 KiB |
| Hash or range | 1 KiB each |
| Value | 64 KiB |
| Import page or requested export limit | 128 events |
| Actual native export fetch | At most four events, further limited by encoded bytes and changes |
| One import | 1 MiB and 1024 changes |
| CLI batch | 128 changes, each value at most 16 KiB |
| Encrypted packet | 1 MiB plaintext plus 28 bytes for nonce and tag |
| Tail during snapshot adoption | 128 events, 1 MiB, and 1024 changes in total |
| Snapshot namespace records | 2047, including winners, Deletes, signed events, and state; the descriptor uses a separate slot |
| Snapshot work | 128 pages of 16 records, with a bounded final lookahead |
| Retained local generations | 128 |
| CI operation | 256 commands, 64 pages per transfer, 128 pages per range |

Imports validate their bounds before one native publication.
The CI corpus uses 64 changes of 256 bytes in each batch and at most two events per transfer page.
Tail and ancestry reads fetch at most four events at a time.
Tail admission stops as soon as its cumulative byte or change cap fails.
The import byte cap does not define a complete process memory cap.

These prototype limits do not change the product warm-set budget, pending-write allowance, disk caps, or native read limits.
No result from this bounded corpus establishes a maximum complete process memory size or a production throughput guarantee.

## CI operation

The proposed command sequence initializes two fresh homes and their distinct device identities.
It writes independent keys and competing versions, exports and imports ordered tails, then exports a snapshot.
It gives an offline peer an unpublished local tail before snapshot adoption.
It then reopens each home and reads the expected keys and ranges.
The operation also requests duplicate delivery and a sequence gap through real exported log packets.
It compares actual canonical dumps, keyed results, paged ranges, and source positions.
These requests exercise the standalone prototype command and real native files; they do not install a test suite or fixture harness.

The durable command script uses one language and bounded work.
It records commands, expected refusals, keyed results, positions, times, disk totals, and cleanup status.
Private keys, native homes, and program files never enter result artifacts.
The script stops its owned processes and removes its own temporary homes before it returns.
The operation does not interrupt a process during a write. Normal reopen after each command is not crash evidence.

The standalone command contract is:

```text
key KEYFILE
init ROOT KEYFILE
status ROOT
put ROOT HASH RANGE VALUE
delete ROOT HASH RANGE
batch ROOT HASH START COUNT VALUE_BYTES
export ROOT ORIGIN AFTER OUTFILE [LIMIT]
import ROOT INFILE
get ROOT HASH RANGE
range ROOT HASH AFTER_OR_DASH LIMIT
dump ROOT HASH OUTFILE
snapshot ROOT OUTDIR
adopt ROOT SNAPSHOT
```

`get` returns a JSON string or null. `range` returns `items` and `next`; a dash starts the first page.
Each returned cursor has an `after:` prefix and an encoded key. Pass that cursor unchanged to continue, including after a literal dash key.
An empty range page can still return a cursor after retained Delete records.
`batch` returns its native durable write time. Process time also includes command startup and reopen.
`export` returns `entries`, `bytes`, and `last_sequence`. Its file contains the encrypted packet.
`dump` writes actual live Change records as JSON lines through bounded range pages.
The CI script records Linux's largest completed child-process memory peak across the complete operation.
That value has no per-command attribution and does not establish a hard memory bound.

The proposed CI entry is the manual `peer_sync_spike` option in `disk-run-proof.yml`.
Its default is false. A separate job uses a standard GitHub-hosted Ubuntu runner in the public repository.
The job builds only the feature-gated example and executes the operational script.
It records the exact source, program checksum, artifact checksum, download URL, and expiration.
Results remain in CI artifacts for 14 days. Raw results do not enter Git.

Tom approved this opt-in CI patch and its execution on 2026-10-28. The current disk proof jobs remain unchanged.
Builds, clippy, and all prototype execution stay in CI. No local product command supplies substitute evidence.

## Remaining production work

- Connect logical peer events to the normal atom, molecule, counter, and file dependency rules.
- Define durable device enrollment, writer replacement, authorization, and encryption key changes.
- Bind complete snapshot and log files to accepted cloud receipts and conditional shared-head publication.
- Define causal dependencies for operations that need them; the current signed event contains no dependency vector.
- Preserve local pending mutations, imported positions, and required files through every crash boundary.
- Complete cloud retention, active-use protection, provider deletion, and exact quota release.
- Establish production resource costs and larger multi-device convergence through approved CI operations.

This spike explores the protocol and its cost. It does not enable automatic peer sync, change the primary, or authorize a production rollout.

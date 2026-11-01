# Work Plan For Encrypted Range Keys And Local Preload

Status: Alternative implementation sequence, 2026-10-24. Mandatory preload is no longer the current recommendation.
Use the [OPE recommendation and implementation plan](ope-range-keys-implementation-plan.md) for the current proposal.
The [design](encrypted-range-keys-local-preload.md) defines the target behavior.
The [reader and writer inventory](encrypted-range-keys-reader-writer-inventory.md) maps the current source dependencies and preload exceptions.

The first release replaces the reversible range codec with encrypted key directories and complete local key trees.
It keeps hash blinding, run-file block encryption, and file encryption.
A later release adds direct cloud reads and a browser client.
That release must add per-atom encryption first: disk format v2 atom rows are plain JSON inside encrypted run-file blocks (see the gap section of the [design](encrypted-range-keys-local-preload.md)).
The local release does not depend on a cloud deployment.

## Current state and scope

The present preload route is optional and retains encoded ranges.
It can select an ordered disk route after a capacity refusal.
The new format requires complete decrypted keys and an explicit refusal when they cannot fit.

The current SDK imports Node HTTP, crypto, and file APIs.
A browser client therefore needs its own supported entry point and portable encryption code.
The cloud byte service supplies backup access, rather than the logical read protocol in this design.

[PR 321](https://github.com/EdgeVector/lastdb/pull/321) remains open at the time of this plan.
It changes the generic storage wrapper and is narrower than this work.
Keep the range-format work separate from that PR.
Refresh the accepted base and cloud protocol state before implementation, then record any dependency on its final result.

## Delivery order

| Package | Result | Depends on |
|---|---|---|
| 0 Contract and source inventory | Fixed format choices and complete reader and writer inventory | Accepted design |
| 1 Portable encryption and lookup identifiers | One versioned format for native and browser clients | 0 |
| 2 Local directory storage | Encrypted authoritative membership and exact opaque lookups | 1 |
| 3 All membership writers | Atomic directory, tip, and value changes | 2 |
| 4 Complete preload and queries | Plaintext local B-trees with mandatory readiness | 2 and 3 |
| 5 Conversion and recovery | Separate-target conversion, backup, and restore | 3 and 4 |
| 6 Local release | Measured default cutover for the new format | 0 through 5 |
| 7 Cloud revision contract and publisher | Authenticated directories and exact read requests | 0, 3, and 5 |
| 8 Cloud logical read service | Authorized reads of one coherent published revision | 7 |
| 9 Browser client | Local key selection and direct encrypted cloud reads | 4, 6, and 8 |

Packages define reviewable outcomes, rather than empty PR placeholders.
Split a package when its source changes exceed a practical review size.
Keep each intermediate PR usable with the current accepted format.
Select the new format only after packages 1 through 5 work together.

## Package 0 Contract and source inventory

Use a dedicated DEV worktree from accepted main when implementation starts.
Carry this design and plan into its documentation PR.
Do not combine new product code with the paused wrapper worktree.

Fix these choices before the first format writer merges:

- The new format identifier, version, and old-reader refusal.
- The complete membership domains for each supported query, including record envelopes and shared fields.
- Exact unsigned UTF-8 byte order, prefix bounds, empty values, maximum key size, and cursor semantics.
- Directory chunk structure, byte limits, duplicate rules, counts, and complete-manifest checks.
- Stable opaque token framing, size, collision handling, and key epochs.
- The directory encryption suite, separate key purposes, nonce rules, and rotation limits.
- Memory accounting, refusal behavior, load deadlines, and the first-use gate for HashRange reads.

Use an established keyed hash for opaque identifiers and an established authenticated encryption implementation for directory bytes.
Keep the existing atom and file encryption suites unless a separate defect requires a change.
Review the encryption contract before selection of the new stored form.

Inventory each reader and writer with its source path, required change, and operational check.
Include raw administrative writes, imports, alternate-key changes, retention, conflicts, replay, conversion, backup, and restore.
Locate plaintext range copies in controls, cursors, metadata, logs, and object names.
Include durable app notifications, conflict values, retained sync markers, and the local search inbox privacy boundary.
The old decoder search alone cannot establish complete coverage.

Completion: the format choices and full inventory have explicit outcomes, with no unresolved choice that changes stored bytes.
Cloud head authority can remain a later decision because local storage does not depend on it.

## Package 1 Portable encryption and lookup identifiers

Create a small portable module or crate for directory types, identifiers, encryption context, and validation.
The location is a package 0 decision; it must not require LastStore, AWS, a daemon, or Node APIs.
Use the same implementation from the native client and the later browser build.

Keep the blinded hash and add a stable opaque identifier for each exact range key.
Bind the identifier to the molecule, blinded hash, exact range bytes, format, and key epoch.
Keep an identifier stable across value-only updates.
Use distinct key purposes for range identifiers and directory encryption.
Extend the explicit molecule key bundles and access-domain encryption accordingly.

Validate directory entries against their expected scope and key context.
Freeze encrypted bytes before retry of an upload.
Reject a wrong key or invalid protected bytes without a plaintext fallback.
Retain the original range bytes inside encrypted storage for API recovery.

Completion: native and browser build targets share the same format and ordering rules through a browser-compatible Rust build.
CI establishes build support; source inspection does not establish browser compatibility.
The current home format remains selected until the complete local route exists.

## Package 2 Local directory storage

Make the encrypted directory the stored form of authoritative membership, rather than an independently maintained second membership authority.
Retain the existing membership precedence for fields and record envelopes.
Named alternate address layouts keep distinct domains.

Store encrypted directory entries and a complete directory root in the native snapshot.
Use exact opaque keys for tip and control lookups.
Keep plaintext ranges out of physical lookup keys and public file locations.
Give the local tree one shared key identity that also supplies its opaque storage identifier.

Choose chunk boundaries through opaque identifiers, independent of plaintext range order.
An insert must rewrite bounded affected chunks and root metadata, rather than the complete schema directory.
Use the existing native snapshot and transaction mechanism.
Do not add a local mutation replay journal.

Completion: a fresh candidate home stores encrypted membership and resolves exact opaque keys through the product command.
A reopen verifies the same directory root, entries, and required controls.
Format support remains inactive for ordinary existing homes.

## Package 3 All membership writers

Stage directory membership, tips, delete barriers, atom references, and required counters at one native publication boundary.
Preserve the current acknowledgement and durability contract.
Pass verified membership changes from codec-aware writers to the complete key domain.
Raw opaque keys cannot reconstruct a plaintext range through the former decoder.

Cover these writer families before cutover:

- Ordinary insert, overwrite, delete, purge, and key move.
- Named alternate-key changes, explicit catalog references, shared scopes, and backfill.
- Conflict retention, expiry, and the release of atom or file references.
- Imports, replay, administrative replacements, and generation changes.
- File-bearing writes and the controls that preserve their dependencies.

An insert or delete updates a ready key domain at the same visible boundary as its tip.
A key move removes the old membership and adds the new membership together.
An overwrite keeps membership and changes the current atom.
A writer that cannot preserve completeness invalidates readiness before its change becomes visible.
Schema replacement or a new key epoch invalidates incompatible domains.

Writes remain valid when no complete directory fits in memory.
Capacity failure clears readiness; later reads receive an explicit refusal rather than an incomplete answer.

Completion: every writer inventory entry has a supported route or an explicit refusal before any partial change.
Concurrent writes preserve the existing current-value and reference-count rules.

## Package 4 Complete preload and queries

Extend the existing loader and key memory owner instead of adding another memory model.
Load all required authorized membership domains before the first HashRange read proceeds.
Keep Hash-only reads direct and keep writes independent of a preload.

Decrypt bounded chunks into private local B-trees under their hashes.
Verify complete chunk coverage and merge concurrent membership changes before publication of Ready.
Do not perform disk I/O or a complete tree build under the publication lock.
Concurrent callers join one bounded load.

Update all query routes that currently depend on encoded order or prefixes:

- Exact HashRange reads and batch reads.
- Range bounds, prefixes, cursors, limits, and pages under one hash.
- Record envelopes, samples, counts, and field-source fallback.
- Predicate and sort-field fetches, explicit catalog references, and named alternate layouts.
- Administrative key output and any supported API that returns original keys.

Select plaintext ranges in memory, then batch-fetch missing tips and atoms by opaque identifiers.
Keep complete keys separate from sparse warm tips and values.
Retain key-domain lifetime references throughout a query.

Add explicit status for load progress, key bytes, retained bytes, deadlines, and refusal reasons without secret values.
Remove the new format's capacity fallback to ordered disk ranges.
Keep legacy reads separate until the supported conversion path replaces them.

Completion: the candidate serves supported keyed reads after a verified load and never serves a partial directory as complete.
A used schema does not fetch all atom contents or file blobs.

## Package 5 Conversion and recovery

Allocate the new format version after the base audit, rather than assuming the wrapper PR's version is the latest version.
Normal startup refuses incompatible homes before any rewrite.
Conversion uses an explicit inactive target and leaves its source unchanged.

Use the source format's supported reader to recover its logical membership and required control state.
Rewrite every affected tip, barrier, generation, conflict, and dependency reference to the new identifiers.
Preserve atom identities, shared references, counters, and file access.
Publish the new format marker only after complete target validation.

Make interrupted conversion resumable or explicitly discardable without ambiguity.
A wrong key must leave the source and target's accepted state unchanged.
An old reader must refuse the converted target.
The original reader must still open the unchanged source.

Include directory roots, chunks, key bundles, and revision dependencies in backup capture and restore.
Reject missing chunks, mixed epochs, and incomplete restored directories before destination publication.
Physical compaction preserves logical directory membership and exact lookup identities.
New encryption cannot repair prior copies that already expose range values.

Completion: fresh synthetic DEV data survives conversion, backup, restore, compaction, and reopen through the product commands.
No primary data, archive, or copy of the primary supplies this validation.

## Package 6 Local release

Compare accepted main and the complete candidate through the existing CI benchmark workflows.
Use matching source helpers and identical logical inputs for both formats.
Measure the first query separately from later queries.

Record load time, key memory, process memory, disk size, mutation cost, and repeated-query cost.
Include many hashes, long ranges, empty schemas, and keys above the warm-set count.
Measure capacity refusal and concurrent first use separately.
Do not infer a general speed improvement from fewer disk reads.

Select the complete new format for fresh homes only after the prior packages pass their required checks.
Keep an explicit separate-target path for existing homes.
Remove OPE encoding and decoding from the new format's ordinary routes.
Keep only legacy support that the conversion contract still requires.

Completion: the local product serves the new format through ordinary commands and exposes clear readiness and capacity state.
The release record includes required CI checks and valid benchmark artifacts.
A primary upgrade remains a separate action.

## Package 7 Cloud revision contract and publisher

Define logical reads separately from the existing generic byte backup contract.
Share wire types and canonical framing between the client and service.
Do not reinterpret a backup object hash as permission to read a logical record.

Fix the current-head authority before service implementation.
The proposed initial model trusts the authenticated control service for the current head and verifies writer-authenticated directory and tip facts.
It does not claim protection against that control service deliberately replaying an older valid head.
Stronger protection requires a separate trusted witness or equivalent protocol before that claim becomes valid.

Publish encrypted data dependencies first, then commit one complete directory and tip revision.
Define concurrent-writer conflict handling, revision expiry, bounded reads, and client refresh after a rejected old revision.
Authenticate the link between the requested opaque identifier, tip, atom, and revision.
Define explicit proofs of absence or an equally strong complete-revision rule.

Reuse the accepted sync change mechanism for directory updates.
Do not create a second local write journal.
Bound changed-chunk upload work and keep unchanged encrypted atoms reusable.

Completion: the client can produce a complete authenticated revision and can validate a frozen exact-read response through the product interface.
No service activation occurs in this package.

## Package 8 Cloud logical read service

Implement in `EdgeVector/exemem-infra` against the accepted shared protocol revision from `EdgeVector/lastdb`.
Reconcile current cloud byte work before selecting integration points.
Keep account generations, share permissions, object grants, quotas, and resource limits authoritative.

Add authorized directory discovery and chunk reads by opaque domain and revision.
Add batched exact tip and atom reads by opaque key identifiers.
Choose individual objects, authenticated pack byte ranges, or both through the package 7 contract.
The service receives no private identity key, directory key, or atom content key (the per-atom encryption that the design gap section requires).

Bind every returned location and byte range to the requested revision and its authenticated content.
Reject requests across accounts, expired revisions, and grants outside their shared scope.
Keep existing backup and restore routes compatible.

Completion: the DEV service returns only the selected encrypted records and the required integrity information.
A stale or inaccessible revision produces the specified refresh or permission result.

## Package 9 Browser client

Add a browser-safe client entry point with fetch-based transport and the portable encryption implementation.
Keep Node socket and filesystem code outside its dependency path.
Unlock only the molecule keys that the browser's authorized scope requires.
Define key recovery, session end, reload, and cache behavior without plaintext keys in URLs, logs, or persisted directories.

Load and decrypt the complete selected directory, build the local trees, and batch-fetch selected opaque identifiers.
Verify returned tips and atoms against one revision before local decryption.
An explicit file request fetches its selected encrypted file blob.
Keep the initial browser scope to reads; browser mutations require a separate complete write contract.

Completion: an actual browser opens a fresh synthetic account, reads a HashRange schema, and fetches selected atoms without a local daemon.
It also handles an update from another authorized client, a shared field, key refusal, missing chunks, and memory refusal.
The initial key download remains visible as a separate cost from later exact fetches.

## Source map

| Area | Current source to inspect first |
|---|---|
| Range codec and key purposes | `fold_db/crates/core/src/crypto/e2e.rs`, `atom/molecule_key_codec/`, `db_operations/molecule_key_store.rs` |
| Complete key memory | `fold_db/crates/core/src/resident/core_keys.rs` and `resident/core_keys/` |
| Load and publication | `fold_db/crates/core/src/storage/disk_pack/schema_keys.rs` and `storage/disk_pack/schema_keys/` |
| Query selection | `fold_db/crates/core/src/db_operations/atom_store/schema_keys/`, `filter/`, `schema/types/field/filter_utils/` |
| Tip and control writers | `fold_db/crates/core/src/fold_db_core/mutation_manager/`, `db_operations/atom_store/` |
| Format and recovery | `fold_db/crates/core/src/storage/current_keys.rs`, `mini_cutover/`, `storage/disk_pack/byte_backup/` |
| Cloud contract and client | `cloud_backup_protocol/src/`, `fold_db/crates/core/src/sync/auth/`, `sync/engine/` |
| Cloud service | `exemem_service/lambdas/storage_service/src/` in `exemem-infra` |
| SDK split | `lastdb_app_sdk/src/client.ts`, `transport.ts`, and `capabilityStore.ts` |

Paths in a cell after the first full core path share the `fold_db/crates/core/src/` root.
These are integration anchors, rather than a claim that the new directory protocol already exists.

## Validation and publication rules

Use local source, syntax, and format checks only for LastDB.
Use CI for builds, lint that compiles code, and all benchmarks.
Do not write, restore, run, or require tests or a replacement fixture harness.
Use existing product commands, operational workflows, and fresh synthetic DEV data for behavior checks.

The required operational record covers initial load, complete membership, normal writes, key moves, prefix bounds, concurrent changes, and reopen.
It also covers wrong keys, tampered bytes, missing chunks, old revisions, capacity refusal, backup, and restore.
Cloud checks include account separation, shared access, revision checks, and actual browser reads.
Preserve each unresolved writer or reader gap before cutover.

Every LastDB product PR requires the applicable non-test gates and `benchmark-complete` before merge.
Service PRs require their repository's current non-test gates.
Documentation-only PRs skip measurements under the existing policy.
Link the CI summary and artifacts; keep raw output outside Git and avoid duplicate receipts.
After publication, check comments, reviews, and CI until the PR merges or closes.

CAUTION: A merge to `exemem-infra` main deploys DEV and can start the automatic 5% production canary.

Read its current `deploy.yml` before publication of service changes.
Obtain Tom's approval before changes to shared CI, shared configuration, service activation, or deployment.
The primary reset prevents the usual Situations preflight; this plan does not replace that required authorization.
Prepare each concrete shared change for review before that approval step.

## First implementation action

Start package 0 on accepted main and close the local format choices and reader and writer inventory.
Keep cloud and browser packages after the local storage checkpoint.
Plan approval does not itself start deployment, change CI, or upgrade the primary.

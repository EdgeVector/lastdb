# Native storage contract review — 2026-10-19

This round checks shared rules across memory, disk, related molecules, and named databases.
Independent reviews cover the source, release identity, operational results, and cleanup.

## Changes

- Memory relief now uses the native core model. It waits for the accepted disk prefix, then removes unused clean logical records.
- Held records and unflushed records remain protected. Reports distinguish the warm set from definitions, complete keys, and write metadata.
- Equal complete atoms can share memory. The byte report also retains each original allocation that an immutable write still owns.
- Declared key fields must match the caller's key. Missing key fields still use that key. A conflicting source batch changes no member.
- Related molecules preserve empty keys and spaces. Delete and Purge create no key-field atoms.
- Purge and must-exist Delete retain the caller's database scope through presence checks, bodies, file keys, and the deletion ledger.
- Both erasure verbs honor their accepted conditions. Purge removes schema markers from their own namespace under each atom's source schema.
- Reference completion proof describes the whole home. Actual reference reads and atom removal remain inside the selected database.
- Descending pages preserve the specified hash order when ranges are equal. Reads with tombstones order their selected set before the offset.
- Prefix reads handle the largest Unicode character. Counts use selected-schema key preload, and warm range windows use the single current encoding.
- Duplicate source mutation identities receive a refusal before preparation. Derived deletion peers retain their source identity.
- Redundant ordinary CAS locks, obsolete cache controls, reservation-count polling, and old encoding branches are removed.

Aggregate locks remain because their work spans multiple native transactions.
Explicit physical maintenance retains its separate role.
The conservative purge reference check remains a safety path; it cannot select an older serving engine.

## Ordinary product operations

Each home starts empty and uses synthetic records.
No primary, archive, existing home, migration, or cloud supplies data.
The literal requests and expected results stay unchanged between the failed and repaired releases.
Raw direct responses are saved before decode. Independent queries use batches.
The existing cohort retains decoded responses and file-read receipts.
No new executable check helper or test suite forms part of this change.

| Run | Result |
|---|---|
| First candidate, `e3e9c0f4` | A valid named Purge returns 503. The run stops at this failure. |
| Repaired normal encoding | 92 planned observations pass. |
| Repaired plain range encoding | 92 planned observations pass. |
| Existing warm-read cohort | 74 read observations pass. All 29 required zero-file-read checks pass. |

The first failure exposes a global proof lookup under a database prefix.
Its original response and error remain in the failure seal.
That trial node stops without a forced kill and returns exit code 1.
It does not count as a successful normal session.

The repaired runs cover ordered pages, Unicode prefixes, empty keys, spaces, batch refusal, erasure conditions, and named data isolation.
Their normal reopen checks repeat the expected records with cold and warm memory.
All eight repaired node sessions stop normally with exit code zero.
Supported exact cleanup removes all five owned roots after independent review.
The final independent check confirms their absence and the unchanged receipt hashes.

The normal-format memory relief removes 76 clean logical records.
Its reported warm-set size falls from 604,848 bytes to zero, and the report confirms its target.
The plain-format run removes 76 records and reports a change from 591,616 bytes to zero.
Definitions and complete keys remain available under their separate allowances.

## Release and limits

The repaired executable uses source commit `7d57939888b39f7651331c81f6aa9346d25995bd`.
The [release identity](release-identity.json) records the exact source commit and reconstructs all 1,885 product inputs.
Repeated captures and immutable Git objects agree before and after the build and product operations.

- Executable SHA-256: `e59db226ac5eea1cdef524dffaf48ad8c07302b377529e3e087a31e4e41a592b`.
- Source inventory SHA-256: `8f8ee19f50e01c201652031b76911df8fb71768ee44bd70f1241d1d50fbd7360`.
- Operation plan SHA-256: `63ab1e74287b40dde2c201cd54d33b372295d5ecac7b5dd0a0384e0cb4c1ab03`.

Workspace and off-lane Clippy pass with warnings refused.
The normal release build, format, file size, function size, new-allow, and build-artifact checks pass.
The app registry, write-path, workspace feature, and lint-coverage checks also pass.
No tests run, as required by the repository instructions.

These observations establish correctness for the listed operations. They do not establish throughput or a speed improvement.
The byte gauge remains report-only. Memory relief does not establish a whole-process cap.
Held-call pressure, duplicate library mutation identities, and physical schema-marker removal receive source and compiler review.
The tombstone flag exercises the count-unavailable route without an actual tombstone record.
Descending reads without a live count can resolve more of the selected set before the final page.
Count-only projection fallback remains correct through disk reads; it does not preload its fallback molecule.
Cloud stays disabled. Its restore proof rules require review before activation.

The [current design](../../lastdb-rehydrate.md) describes the memory and disk responsibilities.
The [cleanup seal](cleanup-root-seal.json) identifies the accepted raw receipts and exact owned roots.
The separate [failure seal](failed-attempt-seal.json) preserves the rejected attempt without a success claim.

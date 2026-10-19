# Storage system review — 2026-10-19

The earlier reviews checked individual routes and missed assumptions that those routes shared.
This review follows complete reads, writes, deletes, key preload, and flush through memory and disk.
It also checks definition scope, record scope, atom selection, and reservation ownership together.

## Resulting behavior

- A selected tip requires its atom body in every database scope. The common reader refuses missing bodies instead of silently omitting records.
- Schema key lists resolve the caller's database membership inside the captured read. Their keys, tips, bodies, and preload use that same scope.
- Schema publication establishes fixed encryption keys for all final field molecules, including related molecules before their first write.
- Private key creation follows one lock order. A failed schema change cannot publish its keys through the shared cache.
- Protein definitions use the node's global definition namespace. Related writes and deletes keep their records inside the caller's database scope.
- Native memory records use those same definitions and retain the database scope. The named-database exception for related deletes is gone.
- Write conditions retain the selected atom location. They use the same required-body reader as queries.
- Missing lookup keys receive a refusal before batch preparation. Declared key fields can supply the key; record contents cannot invent one.
- Malformed stored keys receive an error. Unsupported query cursors receive an explicit refusal instead of silently repeating a page.
- File and journal queue extensions reserve only the additional bytes. Cancellation preserves the prior reservation; acceptance transfers its owner without a second charge.
- Failed or unavailable schema counts report `null`. A successful empty count still reports zero. These optional counts describe the owner's personal database.
- Retired skipped-body state, previous-key transition fallbacks, obsolete helpers, and misleading comments are removed.

The core model remains the logical value authority. The native queue owns prepared disk output.
Explicit physical maintenance keeps its separate role. It cannot select another logical engine.
No migration, primary change, or cloud activation forms part of this change.

## Product evidence

Every home starts empty and uses synthetic data. No existing database home supplies data.
The operation plans and 15 existing helpers remain unchanged during each accepted run.
Raw response bytes precede response decode. A candidate mismatch stops the run.

| Attempt | Outcome | Interpretation |
|---|---|---|
| Original baseline diagnostic | 79 pass, 36 fail | A dependent CAS request omitted its source-pass guard. This attempt stays recorded but does not supply the accepted comparison. |
| Corrected baseline | 75 pass, 39 fail, 1 blocked | The frozen dependency rule blocks the request after its source fails. Expectations stay unchanged. |
| First candidate, `5d1de0d7` | 30 pass, 1 fail; stopped | A named source update left its related range unchanged. This failure identifies the global-definition scope defect. |
| Repaired candidate, `9d9c787c` | 134 pass, zero fail or blocked | 115 main observations and 19 supplemental observations pass. |
| Existing warm-read cohort | 74 pass; 29 strict zero-file checks pass | Values and membership match. Every required file-read count is zero. |

The repaired checks cover named and personal records, shared atom identities, missing keys, whole-batch refusal, write conditions, cursors, conditions, counts, and normal reopen.
A source-only named update reaches both Hash and HashRange layouts.
A named delete removes both layouts while personal records remain correct before and after reopen.
The dependent ContentHash request uses the atom identity from a source observation that first passes its declared value checks.

All 15 node sessions stop normally, without a forced kill.
The original failures and their exact requests remain in the receipt seal.
The main result, supplemental result, and raw receipts receive an independent review.
All five owned synthetic homes and their private programs are removed through supported exact cleanup.
Independent checks confirm their absence and verify the original receipt hashes after cleanup.

## Source and executable

The repaired executable uses source commit `9d9c787c0d70a886f0938f45ea0408520248f31a`.
All 1,883 product inputs match repeated file captures and immutable Git objects before and after the build and product operations.

- Executable SHA-256: `aaf0a32ae7098847d0d9de1755cc78e1facaff1a384ddb663187526a79de971c`.
- Executable size: 68,205,840 bytes.
- Input inventory SHA-256: `12a9e0f8cf5bf9b953d7d1de2d3e6d318b2e9f75c53c4dd4e0a7dcc7eba78b8b`.
- Main plan SHA-256: `19de67aac9c827db6f44cdeb79fb6839654bf66799e22b65412e9fc34cc6f129`.
- Supplemental plan SHA-256: `34580e1dfc75bab2a19f21bdd38d03e03b3bd3075e665420fd3ec0a105760242`.

The full workspace and off-lane Clippy checks pass with warnings refused.
The normal release build, format, file size, function size, new-allow, and build-artifact checks pass.
The app registry, write-path, workspace feature, and workspace lint-coverage checks also pass.
No tests run. No new executable check helper forms part of this change.
Later commits contain only documentation and proof receipts; their product inputs must match this executable's inputs.

## Limits

These operations establish the observed values and read counts. They do not establish throughput or a speed improvement.
They configure the 10,000-record warm limit but do not exhaust it or prove a whole-process memory cap.
The byte gauge remains report-only.
Missing bodies, duplicate physical copies, disk-cap failures, cancellation, and unavailable-count errors receive source and compiler review only.
No fault injection, crash recovery, concurrency stress, cloud operation, older-home load, or migration runs.

The [current design](../../lastdb-rehydrate.md) describes the memory and disk responsibilities.
The compact [release identity](repaired-release-identity.json) reconstructs the complete input inventory from the baseline manifest and 105 changed inputs.
The [cleanup seal](cleanup-root-seal.json) records the exact receipt hashes and owned roots.

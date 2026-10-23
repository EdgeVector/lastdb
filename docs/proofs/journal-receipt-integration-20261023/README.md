# Journal and counted-receipt integration, October 23

Tom approved PR 271, followed by PR 273. The primary stays unchanged.
This report records the later integration separately from the October 22 benchmarks.

## Source

- Main at integration: `0f07ddeb1`, including concurrent compaction, atomic native values and app notices, per-block compaction reads, and owned query results.
- PR 271 product source: `8dee140cafecb6605c5cff2afb8054ef7ed137f9`.
- PR 273 product source: `5a1f8c17bbb3d1109cc716a691051e5a44e415a5`.
- Earlier benchmark sources remain `08d8436cc`, `a1941e3a8`, and `e79b71151`.

## Change

PR 271 removes the erase recovery records and journal-based cloud repair.
Its conflict resolution preserves current main's backup modules, exact file references, and native acknowledgement policy.
Its CodeQL fix gives the disk command a typed public JSON response and static error categories.
Successful reads retain their requested keys, bodies, versions, costs, and timing fields.
No alert suppression or security-policy change forms part of the fix.

PR 273 carries exact positive live-part counts through current main's publication and compaction paths.
Each directory reference retains the locator object's part-zero allocator template.
Every full build and compaction path seeds counts from the final reachable tree, including grafted subtrees.
The normal staged path prepares these counts before its final publication gate.
The synchronous fallback retains a full seed inside its caller's boundary.

The receipt requires feature 512 and count semantic version 1 at generation prefix bytes 96 through 103.
Version-zero counted prototypes receive an explicit refusal because their allocator references differ.
Supported uncounted snapshots retain their old format and receive a streamed count pass before writer admission.
Backup manifests accept optional feature 512 and reject unknown feature bits.
Native cloud activation stays disabled.

The integration removes arbitrary directory-entry and older-format step quotas.
It retains the format-derived work bound, bounded stacks, page-local ledgers, and the physical dependency and map limits.
The reference cache retains its existing bounds and contains no value bodies.

## Evidence scope

Build logs and generated-data product receipts accompany this report after completion.
Each operation records its exact source, executable hashes, helper hashes, and owned home.
The host build lock serializes the local Cargo work.
No tests or coverage commands run.

The new operations establish no fresh performance comparison, power-loss behavior, maximum-size home, or hard process-memory cap.
The older 86.0% retained-space reduction describes its frozen executables and finite workload.
Its latency regressions remain visible in the original report.
No earlier benchmark receipt or result is rewritten for this integration.

The code merges do not upgrade the primary or its installed programs.
No primary or archive data enters a generated home.

# Final native storage review

The review finds no unresolved correctness blocker in the native read, write, or purge paths.
Separate reviews cover read captures, retained memory owners, complete range keys, write publication, and the native startup boundary.
The primary and its archive remain unchanged.

## Final cleanup

- Remove private query code for earlier values. Public `as_of` fields remain and receive the existing explicit error.
- Remove the query-only tip rewind helper and its final unused unsigned-write helper.
- Keep mutation-event inspection, conflict records, tip-version traversal, and the purge operations that use them.
- Keep one current-value query path after public validation, with the same keys, filters, scopes, windows, and atom locations.
- Update current instructions and telemetry documents to describe native storage and the core model.
- Mark old graph, write-mode, dual-read, and migration proposals as historical.

The final product commit is `5419748a0e8ebdc7044f3420808bbd280b6bb249`.
The [merge source identity](merge-identity.json) records its exact product inputs and build.
The earlier source and measurement receipts stay unchanged.

## Source checks

Workspace clippy passes with warnings denied, including the separate off-lane binaries.
Format, function size, file size, and new-allow checks pass.
The change adds zero allow attributes and removes seven.
Write-path synchronization, workspace dependency, clippy coverage, and app registry checks also pass.

The first compiler pass identifies an unused import and the unsigned helper used only by the removed history path.
Both are removed before the successful final pass.
The removed query helpers and their stale source references have zero remaining Rust references.

## Product scope

Native storage remains the sole logical engine, with memory ACKs when the required reservations fit.
Explicit durability and insufficient coverage or reservations can require disk completion.
The private write queue does not serve ordinary reads.
Empty homes initialize directly. No migration code is added.
Cloud activation remains disabled.

The 10,000-record warm cap and separate metadata, key, and write allowances do not define a whole-process memory maximum.
This review adds no new performance comparison. Earlier benchmark results remain specific to their recorded source.
No tests or coverage gate ran.

## Fresh-data results

The final release SHA256 is `38f7ac6b7ddaa1a03ebec3219355b2c1962322b90bc1c3b3093475355cd512a3`.
The [committed-source check](merge-committed-source-verification.json) matches all 1,882 product inputs to the exact product commit.
The [post-workflow check](validation/merge-post-workflow-verification.json) confirms the same source, host settings, executable, and identity chain.

| Workflow | Result |
|---|---|
| [Hash and HashRange](merge-logical/acceptance.json) | Pass: 86 keyed checks, 39 required zero-file checks, six normal node sessions. |
| [Proteins](merge-protein/acceptance.json) | Pass: 14 keyed checks, nine required zero-file checks, two normal node sessions. |
| [Named databases](merge-scoped/acceptance.json) | Pass: 24 keyed checks, 18 required zero-file checks, two normal node sessions. |

All 124 keyed checks and 66 required zero-file checks pass.
The logical workflow also preserves literal encryption-prefix values, conditional writes, deletion, recreate, complete concurrent values, and normal reopens.
Historical query and count requests still return explicit errors after the private history code is removed.
The first queued-write status reports accepted sequence 15, completed sequence 13, one pending batch, and 512 dirty paths before its first value read.
All ten node sessions stop normally. Each workflow uses a separate fresh synthetic home.

The [separate merge seal](merge-receipt-seal.json) preserves the new raw receipts.
The earlier sealed receipts, executable identities, measurements, and evidence index remain unchanged.
The [cleanup verification](merge-cleanup-verification.json) confirms the four new temporary roots are removed.
All 153 new and 357 earlier sealed files retain their exact sizes and hashes.

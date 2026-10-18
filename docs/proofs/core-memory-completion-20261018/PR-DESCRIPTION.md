Repeated exact reads still fetch schema, molecule, and control state after their values enter memory. Unrelated writes also clear reusable warm paths.
This change uses the existing core model for schema state, molecule state, selected complete keys, and fetched values.
Selected HashRange schemas preload their keys on first use and retain them while the complete key set remains ready.
Later calls use complete memory keys and fetch only unknown tips or values.

The native read path now retains shared, typed core definitions with their memory reservations.
Each call keeps its captured definitions, file root, accepted writes, and reservation owners until that call ends.
Native reads exclude private, uncommitted delete barriers from that captured state.
Writes that wait for disk remain private until their disk commit publishes the changed keys.
Captured pending writes share their bodies rather than repeatedly copy the whole pending write set.

Authorized warm paths retain every possible winning tip source, required delete barriers, and the selected atom source.
Ordinary writes invalidate affected paths instead of all paths. Structural changes still invalidate the affected molecule.
Confirmed missing keys use the same complete source rules. Read errors do not become confirmed absence.
Incomplete receipts retain body reuse and require normal tip resolution.
Equal content UUIDs can have different atom dates or source details. Those full atom copies coexist without displacing each other.
Secondary reads preserve the selected physical atom location through the body fetch.

The complete-key allowance reuses retained tree and key capacity after delete and create.
Definitions that cannot fit the definition allowance are refused before any purge of other definitions.
Packed ordinary range keys and batch location reads reduce file work for preload and disk fallback.
Complete keys, partial tips, and values remain parts of the existing molecule model; this adds no saved query-answer list.

The default allowances are 128 MiB for complete keys, 16 MiB for shared definitions, and 10,000 logical paths.
These are separate charged allowances, not a cap on total process memory.
Preload remains opt-in. Active conflict histories and unknown values can still require file reads.
Ordinary reads use Hash and HashRange. This change adds no history or standalone Range query.
The compatibility write route remains in use. The wider caller inventory and its eventual removal remain separate work.

The final source-presence check removes redundant OPE candidate work when a warm body has no physical receipt to record.
Real physical receipts still need full source checks. This change adds no lookup authority.
Three earlier candidates retain their failed timing results and source identities in the proof reports.

The fresh final comparison uses the previous reviewed v4, not the original engine.
Repeated bounded64 reads move from 3.942 to 4.354 ms with preload OFF, and from 4.997 to 4.902 ms with preload ON.
The OFF difference is +10.5%; the ON difference is -1.9%. ON reads report zero file-read calls.
First and disjoint range reads remain slower in several groups; bounded64 disjoint medians rise by 29.1% OFF and 37.4% ON.
Those groups have two observations each. Write results are mixed, including slower read-primed Hash updates and faster OFF HashRange updates.
The [final report](docs/proofs/core-memory-completion-20261018/RESULTS.md) records all limits and the complete tables. This PR makes no general speed claim.

Validation: full ordinary workspace clippy passes with warnings denied; the two separate fastembed packages also pass clippy.
The app SDK typecheck, lint, and build pass. Product URL, write-path, fastembed, workspace-coverage, app-registry, and staged-artifact checks pass.
The final release and post-workload check match all 1,908 frozen product inputs and executable SHA `a0ad619076570fc472569712d5a40743a5a32823e909d45d4e33991b7c536285`.
Ordinary generated-data checks pass: 219 keyed reads, 58 zero-file-read gates, 30 equal-content reads, and 24 constant-membership cycles.
Seven extra HashKey-on-HashRange calls verify exact keys and values through update, delete, and reopen.
All eight matched cases pass with 696 keyed checks, 112 measured write batches, and no unexpected refusal.
All 141 final node sessions stop normally. Supported exact cleanup preserves the original receipt hashes.
Hidden Atom dates and impossible oversized-definition admission receive source review because normal API calls do not expose direct admission.
No tests run. No primary, archive, or existing data home supplies the proof data.
At the local report freeze, committed-tree checks and GitHub `ci-required` remain pending.

The earlier [core memory report](docs/proofs/core-memory-state-20261018/RESULTS.md) compares executable `e10f4593…` with reviewed v4 `bdb83b38…`.
It excludes the original `aa64f61` engine and preserves its original limits.
The final completion report uses a fresh comparison between that reviewed v4 executable and this completion build.
Those two comparisons use different reference builds; their gains must not be combined.
Raw generated evidence stays local. The compact [evidence index](docs/proofs/core-memory-completion-20261018/EVIDENCE.md) preserves exact hashes, sizes, and locations.

# Merge readiness audit

The first audit reads the dirty worktree against local `origin/main` at `0a45f47ce860f5526e68a24126c11a04d4b9eced`.
It does not fetch the current remote state.
The root agent must refresh the Git base and repeat the required checks before merge.
The final root refresh still identifies `origin/main` as `0a45f47ce860f5526e68a24126c11a04d4b9eced`.
The change does not edit shared CI, lint scripts, Cargo inputs, or repository instructions.

The changed Rust files meet the file and function size rules at this audit.
No changed Rust source contains a test attribute or a test-only module attribute.
These are source checks. They do not prove the product result.
See [readiness-initial.json](readiness-initial.json).

The first artifact audit finds 59 files above the 2 MiB Git blob cap.
The untracked tree totals 613,573,001 bytes across 1,982 files.
Raw generated receipts account for most of this size.
No forbidden build-product path appears in that untracked set.

The compact evidence set retains reports, shared drivers, results, and identities.
An additional index records the exact hashes, sizes, and local paths for the excluded original bytes.
The original files stay in place. See [EVIDENCE.md](EVIDENCE.md).
The root agent reviewed and adopted the ignore rules before the Git stage.
The artifact lint must pass on the staged files and the final commit.

The required GitHub check is `ci-required`.
Its required jobs cover the Mini lane, the app SDK, the full Rust workspace, and changed-file size rules.
The full Rust job includes the two separate fastembed packages after the ordinary workspace check.
The gate runs no tests.
This audit does not change CI, repository rules, or remote state.

The final executable is `a0ad619076570fc472569712d5a40743a5a32823e909d45d4e33991b7c536285`.
The final post-workload source check matches all 1,908 product inputs and build settings.
The API proof passes zero-file-read gates for repeated missing keys, unchanged keys after nearby writes, and alternating equal-content record reads.
It retains the earlier schema lifecycle, molecule-map, flush, reopen, and concurrent-call cases.
All 24 same-key delete/create cycles keep the complete-key allowance and load count stable.
Seven supplementary HashKey-on-HashRange calls check membership and values through update, delete, and reopen.
Hidden Atom metadata and impossible definition admission receive source review because the public API does not expose direct entry admission.

The final eight matched cases pass all value checks. The timing results remain mixed.
The first three candidates retain their blocking performance results. The final candidate removes the measured twofold to threefold range CPU delay.
It still shows a 10.5% warm bounded64 OFF gap and several slower cold or disjoint reads.
See [RESULTS.md](RESULTS.md) for all measured tradeoffs, limits, and exact identities.
At the local report freeze, committed-tree checks and GitHub `ci-required` remain pending.

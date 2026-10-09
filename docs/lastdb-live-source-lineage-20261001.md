# LastDB live source lineage bridge: 2026-10-01

The primary LastDB daemon runs Fold source `be41e547e` from the former Forgejo main branch.
GitHub `main` contains later code, but `be41e547e` is not its ancestor.
The safe-upgrade graph rejects a GitHub main release before its database-copy probe.

This branch adds `be41e547e` as a merge parent with the `ours` strategy.
The merge keeps the GitHub main file tree and records the live source ancestry.
No code from the retired branch replaces the current GitHub main code.

**Merge this PR with a merge commit.** A squash or rebase drops the parent link.
After the merge, verify that `be41e547e` is an ancestor of GitHub `main`.
Then rebuild the release from the new main commit and rerun `lastdb-safe-upgrade`.
The upgrade still requires every real-data, memory, latency, CAS, photograph, and durability gate.

# Owned synthetic data cleanup

All 54 run roots and six program-only containers from this task are reclaimed through the supported exact-path command.
The root agent reviews each owner marker, realpath, process, open file, socket, preview, and receipt seal before authorization.
The cleanup helpers repeat those checks before each action. Every supported cleanup returns zero.
The primary, archive, existing homes, and other agents' roots remain unused.

| Receipt set | Run roots | Normal node sessions | Other exit | Original files verified after cleanup |
|---|---:|---:|---|---:|
| [Early discovery](discovery-cleanup-final.json) | 14 | 44 | One unforced native replay startup refusal | 397 |
| [Superseded V4](superseded-v4-cleanup-final.json) | 9 | 28 | None | 234 |
| [V5](v5-cleanup-final.json) | 15 | 153 | None | 297 |
| [V6](v6-cleanup-final.json) | 16 | 169 | None | 313 |

The total is 394 normal sessions and one preserved startup refusal.
All 1,241 sealed original receipt files match after cleanup.
The replay refusal occurs before health, requests, or owner provision. Its original error remains in the discovery evidence.
No forced node stop occurs.

The six program-only containers hold private executable copies and owner markers, with no node sessions.
Their source and executable identities remain in this proof directory after reclaim:

| Program | Exact container | Cleanup receipt |
|---|---|---|
| First candidate `be38c44c…` | `sp.25f46eeb` | [Result](candidate-v2-program-cleanup/baseline-cleanup.json) |
| V3 `8cff7b4c…` | `sp.jqbf78rg` | [Result](candidate-v3-program-cleanup/baseline-cleanup.json) |
| V4 `c4efed97…` | `sp.t_on9mcq` | [Result](candidate-v4-program-cleanup/baseline-cleanup.json) |
| V5 `8a26322c…` | `sp.482ptgui` | [Result](candidate-v5-program-cleanup/baseline-cleanup.json) |
| V6 `c19a25ea…` | `sp.8vnrtb_r` | [Result](candidate-v6-program-cleanup/baseline-cleanup.json) |
| Before `a0ad6190…` | `sp.51250ird` | [Result](baseline-cleanup.json) |

The cleanup receipts retain the full exact paths under the operating system's temporary root.
The accepted before and final candidate programs stayed until the source and result cross-review accepted the comparison.
Their reclaim followed that review and explicit root authorization.
The current worktree release artifact remains a normal ignored build output. This cleanup does not remove the build tree.

Raw requests, values, logs, source copies, and the original sealed receipt files remain locally available under the proof directory.
The [evidence index](evidence-index.json) records their exact hashes, sizes, paths, and local-only status.
Those retained proof files are distinct from the reclaimed synthetic database homes and private program containers.

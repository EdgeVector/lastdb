# Local evidence and Git evidence

The proof reports use generated data only. No proof opens the primary or the archive.

[evidence-index.json](evidence-index.json) records the exact path, size, and SHA-256 of each local evidence file.
The index also records the Git or local status for each file and the actual Git ignore state.
[evidence-summary.json](evidence-summary.json) records the totals.
The index excludes its own outputs to avoid a circular file identity.

New raw request receipts contain repeated generated values. New raw node logs and source copies also stay local.
The raw CPU `sample.txt` also stays local; the compact stack and profile summaries remain in Git.
These files remain at the `local_root` plus the relative path in the index.
A raw-file link in an earlier report works in that original worktree only.
The compact Git reports, shared workload scripts, result summaries, source identities, and cleanup summaries remain available in the repository.
Existing tracked historical evidence remains tracked. The index marks those files with `tracked_at_head`.
The new-file totals exclude that historical evidence. Git ignore rules do not remove files already in the Git tree.

No original evidence file moves or changes because of this index.
The root agent adopted [proposed-ignore.txt](proposed-ignore.txt) in the repository Git rules.
The index script itself does not alter Git rules.
The summary reports any difference between the stated status and the actual Git rules.

`operations.py` uses the ordinary Mini API on one new generated home.
It includes the prior schema, molecule-map, accepted-write, flush, reopen, and concurrent-call cases.
It also checks repeated absence, unchanged keys after other writes, equal-content atom copies, and the allowance after repeated key delete and create.
The final run adds seven ordinary HashKey calls on a fresh HashRange schema, with exact fields and membership checks.
The final [result](RESULTS.md) and [comparison](attempt4/comparison.json) identify the accepted completion build.
The first three attempts retain their measurements, original receipt hashes, source identities, and cleanup records.
The native status reports charged allowances, not total process memory.
The ordinary query response omits atom creation time, and mutation components omit arbitrary atom metadata.
Source review must check those hidden atom fields and the oversized-definition refusal branch.

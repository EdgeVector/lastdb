# LastDB storage code review

Revised 2026-10-23 for integration with main `0f07ddeb1` and the PR 271 squash merge `192ea406a`.
The earlier October 22 review and performance reports retain their recorded source commits.
Commit `47088b89c0bff5142a519db685051849112a787a` supplies the initial native writer.
The implementation starts from main `6d8aa0d573e91a94fd4fd308a7f0eacca022e852`.
It retains the macOS barriers from PR 266 and the reclaim handoff from PR 267.
This document separates source facts, the first product run, and historical performance evidence.
The second review also removes erase recovery records and journal-based cloud repair.
The native writer merged in PR 268 as `4118c7baa`.
The second review uses final product source `5919abbfbf2263993ea99700b73bd9c8d43d689b` in PR 271.
The next change starts from `08d8436cc` and removes obsolete parts from each new receipt.
The earlier comparison measures uncached product source `a1941e3a8` in PR 273.
Commit `e79b71151` implements the bounded cache within each publication.
Profiles, three controlled pairs, six product operations, and three default-maintenance runs are complete.
Those results precede this integration and do not measure its performance.
Tom approved the code merges on October 23. The primary stays unchanged.
The [integration report](https://github.com/EdgeVector/lastdb/blob/0b0a79d4c10949e2f0570e82d9bc4fdb9513a285/docs/proofs/journal-receipt-integration-20261023/README.md) records the final source checks and their local execution limit.

## Current source

| Area | Current source | Scope left |
|---|---|---|
| Local writes | Native values and app notices commit together before the acknowledgement | Earlier matched pairs measure different source and write policy |
| Publications | Each complete stored batch writes one receipt with exact positive live-part counts and publishes one root | Changed pages still use fixed 16 KiB pages |
| Reference cache | One bounded cache shares checked maps, overflow pages, and part headers within each publication | Earlier profiles show lower CPU cost against the uncached source |
| Current record bytes | A payload locator uses `Source::Pack`; a delete uses `Source::Tombstone` | Periodic reclaim removes wholly obsolete parts; compaction rewrites partly live parts |
| Recovery | Writer open verifies `CURRENT`, its receipt, and dependency files | No local mutation replay or silent receipt reconstruction |
| Internal journal | The journal writer, reader, stream, tail, rotation, and command are removed | Erase aggregates now commit with deletes; debit recovery records are removed |
| Format | Every supported root requires feature `256`; new receipts also require feature `512` and count semantic version 1 | Older supported uncounted snapshots collect counts before writer admission; version-zero counted prototypes receive an explicit refusal |
| macOS durability | Ordered file barriers precede root publication; a final device flush precedes durable success | Process interruption does not prove power-loss behavior |
| File lifetime | Exact read holds, backup references, private output protection, and the installed-root reclaim handoff remain | Staged compaction prepares complete counts before its final publication gate; a synchronous fallback retains its caller's boundary |
| Backup and restore | `lastdb-native-backup-2` contains the exact snapshot inventory and no journal state | A data-only bundle excludes node identity and encryption keys |
| Cloud sync | Native startup and activation still refuse cloud configuration | Atomic snapshot/log capture and a sync-only mutation log remain future work |
| Warm records | No 10,000-record cap; the default byte budget is 256 MiB | Preserve counted temporary overshoot for held and dirty records |
| Dirty share | The approved 25% threshold appears in status | Admission enforcement and durable-first fallback remain incomplete |
| Key preload | Selected HashRange schemas can opt into a complete membership-key load | Its separate default allowance remains 128 MiB |
| Ranges | One captured view merges accepted changes and bounded disk pages | Ascending keyed continuations use the last key; other forms retain offsets |
| History | Queries refuse `as_of`; conflict records remain for 30 days by default | Preserve their required file references |
| Counters | Atom, blob, and molecule counters remain correctness data | Preserve them through each commit and compaction |

The [October 19 byte-limit decision](lastdb-warm-set-byte-limit-design.md#decisions-tom-2026-10-19) replaces the older warm-data count cap.
Other 10,000 values bound individual query work or separate caches.
They do not restore a warm-data count limit.
The companion plan's new queue, workspace, range, sync, and disk limits remain proposals.
This journal removal does not implement those proposed limits.

## Additional removal from the second review

Erase accounting writes the final header and changed schema slices in the same native batch as the delete.
The live meter debit follows the durable root.
No debit intent, sequence checkpoint, replay, or prune remains.
Version 2 prevents an older meter snapshot from claiming complete counts without a replay.
Final deletion audit facts remain ordinary snapshot data.
Atom counters retain their source schema for an exact per-schema erase debit.
Older counters without that metadata mark meter trust incomplete instead of a guessed schema debit.

The node no longer starts a cloud-repair worker or replays a local home switch.
The selected-device replacement, peer adoption, and cloud atom-rewrite commands and routes are removed.
Retired repair state and cloud generations that require adoption receive an explicit refusal.
Ordinary sync mutations and authenticated retired sync source records remain.

The review also removes unused acceptance timestamps, pressure wrappers, and dependency batch fields.
Retired batch bytes remain reserved in older receipts.
New counted receipts use bytes 24 through 31 of each dependency record for a positive live count.
Payload reservations no longer charge a phantom journal prefix.
The complete-batch policy cap stays 128 MiB.
The offline write command now holds a complete disk-space reservation through publication.
Its default free-space and replacement allowances remain 64 MiB each.

The [repeat report](https://github.com/EdgeVector/lastdb/blob/0b0a79d4c10949e2f0570e82d9bc4fdb9513a285/docs/proofs/journal-removal-review-benchmark-20261022/RESULTS.md) records three pairs against the original journal-backed baseline.
All exact-key, range, delete, and graceful-reopen checks pass.
Process write bytes fall across all four update workloads in all pairs.
Batch latency and compaction write delay remain mixed.
The separate erase capture restores numeric counters before any value fetch after a forced stop following explicit flush.
It also checks removed commands and the refusal of retired repair state.
These receipts establish no power-loss or primary result.

## Actual commit route

The active stored-batch route is:

```text
HomeSnapshot::commit_mutation
  SnapshotWriter::commit_reserved
    complete_native_commit
      write immutable payload files with PackWriter
      write changed directory and locator pages
      write one generation receipt
      order dependency writes and publish one CURRENT
      finish the platform durability barriers
  install the committed disk view and retire covered memory changes
  complete the queue receipt
```

This route does not write required sync entries.
That atomic capture contract remains future work, and cloud activation stays blocked.
Author-clock reservations retain their durability requirements before writer admission.
Current main commits ordinary native requests and their app notices before acknowledgement.
The final device barrier and clean core memory publication precede the acknowledgement.
No collection timer delays ordinary native requests.

The writer reserves the complete stored batch before file output.
The payload quote includes worst-case frames, part headers, and complete block-map pages.
The complete mutation quote adds changed tree pages, one maximum-size receipt, and root space.
The writer also checks actual payload dimensions against its admitted quote.
The node retains separate disk and memory reservations.
An engine reservation alone does not authorize a native write acknowledgement.

The receipt no longer inherits every prior file part.
The publisher removes old page references and adds references from its fresh private metadata pages.
The count reaches zero only when no current page, overflow descriptor, or payload block requires that part.
Shared parts stay in the receipt until their last reference goes.
Each page resolves overflow references before the next page enters the bounded ledger.
Each changed key resolves its old and new payload references before the next key.
One publication-local `ReferenceCache` now shares the checked immutable maps, overflow pages, and part headers across those resolutions.
It also serves the fresh-part reference pass and drops before final count changes and receipt output.
The cache resets at 24 pages or part-header identities; its bounds remain 26 pages, 25 header identities, and 32 handles.
Each resolution retains its existing work limit and charges logical steps even on cache hits.
Bootstrap retains its standalone cumulative work and allowed-part checks.
The six changed files are `live_parts.rs`, `live_parts/overflow.rs`, `live_parts/scan.rs`, `tree_edit.rs`, `locator_forest.rs`, and `publish.rs`.
All reside under `vendor/laststore/src/disk_pack/`.
The cache changes no integrity, format, warm-data, root, error, or recovery rule.
The existing root holds and reclaim rules still protect older snapshots and backups.
Every directory reference also retains its locator object's part-zero allocator template.
Complete builds and all compaction paths collect counts from the final reachable tree, including grafted subtrees.
Full count passes retain bounded stacks and page-local ledgers without a directory-entry or logical-record quota.
The supported format defines the cumulative work bound; the arbitrary older-format step quota is removed.
Counted generation prefixes store semantic version 1 at bytes 96 through 103.
Version-zero counted prototypes receive an explicit refusal before the writer loads their dependency ledger.
Native backup manifests accept optional feature 512 and reject unknown feature bits.
Periodic reclaim removes obsolete receipts even below the compaction trigger.
The orphan sweep also repeats, with a default ten-minute interval and unchanged age and grace cutoffs.
This sweep finds private output and superseded batch output that no receipt names.

The writer marks itself poisoned before commit file work.
An uncertain result stops new admission until the caller reopens the writer.
A reopen selects only the committed root; private interrupted output remains unused.
Missing or damaged committed dependencies cause an error.
The code does not infer another root or reconstruct a missing receipt.

The [earlier receipt removal report](https://github.com/EdgeVector/lastdb/blob/0b0a79d4c10949e2f0570e82d9bc4fdb9513a285/docs/proofs/receipt-part-pruning-benchmark-20261022/RESULTS.md) records three uncached `a1941e3a8` pairs against journal-free `08d8436cc`.
Durable-single file growth falls from 668.35 to 640.36 KiB per update.
The durable 32-record batch p50 rises from 99.96 to 164.65 ms; every pair records a slower batch.
The focused worst write increases from 1.198 to 1.314 seconds.
That candidate passes the code and six operational checks, but its performance tradeoff does not support a merge recommendation.
Each changed key in that source creates a fresh resolver, which can repeat map reads for related keys.
That source path is a possible cause; the earlier report does not claim profile attribution.
The separate [cache follow-up report](https://github.com/EdgeVector/lastdb/blob/0b0a79d4c10949e2f0570e82d9bc4fdb9513a285/docs/proofs/receipt-cache-followup-20261022/RESULTS.md) covers `e79b71151`.
Three fresh baseline/cache pairs show durable-single growth of 670.86 and 639.35 KiB per update.
The durable batch32 request p50 is 113.33 and 131.24 ms; the focused worst write is 1.249 and 1.488 seconds.
These are medians across runs, without pooled requests.
The cache profile uses 19.6% less node CPU than the uncached draft and 3.3% more than the fresh baseline.
One default-maintenance run per variant retains 1,253.27 MiB for the baseline and 175.05 MiB for the cache after the quiet tail.
The cache's request p50 is 137.18 ms versus 127.46 ms; two requests exceed 500 ms versus none.
The baseline completes one pack; the cache completes none in this finite window.
The October 22 recommendation kept PR 273 as a draft pending further review.
Tom approved the code merges on October 23, with the primary unchanged.
The integration removes the arbitrary count quotas from the earlier source and preserves main's concurrent compaction paths.
The follow-up [scale review](https://github.com/EdgeVector/lastdb/blob/0b0a79d4c10949e2f0570e82d9bc4fdb9513a285/docs/proofs/receipt-cache-followup-20261022/SCALE-REVIEW.md) describes the frozen source and its former quotas.
Its [next-change review](https://github.com/EdgeVector/lastdb/blob/0b0a79d4c10949e2f0570e82d9bc4fdb9513a285/docs/proofs/receipt-cache-followup-20261022/NEXT-CHANGE-REVIEW.md) proposed one app notice publication per batch.
Current main already commits those notices with the native values.
The historical benchmark reports remain useful comparisons for their recorded executables; they do not measure this integration.

New native and artifact roots require `FEATURE_SNAPSHOT_COMMIT`.
Old journal roots receive an explicit refusal before writer file creation.
Startup performs no migration.
The refusal describes a required future explicit conversion, not an available command.
The generic legacy converter still refuses native output without the atom-record authority.

## First product receipt

The [run-1 inputs](https://github.com/EdgeVector/lastdb/blob/0b0a79d4c10949e2f0570e82d9bc4fdb9513a285/docs/proofs/snapshot-commits-20261022/run-1/inputs.json) identify the executable by its SHA-256 and source commit `47088b89c`.
The run precedes the current fixes after that commit.
Its [summary](https://github.com/EdgeVector/lastdb/blob/0b0a79d4c10949e2f0570e82d9bc4fdb9513a285/docs/proofs/snapshot-commits-20261022/run-1/summary.json) records completion, no error, and all owned processes stopped.
The detailed receipts remain in [`docs/proofs/snapshot-commits-20261022/run-1/receipts.jsonl`](https://github.com/EdgeVector/lastdb/blob/0b0a79d4c10949e2f0570e82d9bc4fdb9513a285/docs/proofs/snapshot-commits-20261022/run-1/receipts.jsonl).

The fresh owned home contains 64 generated records with four fields and 1 KiB bodies.
The run changes all 64 records five times, deletes eight records, and recreates four records.
Checks use one batch of exact keys and bounded ordered range pages under one supplied hash.
They confirm 60 live records before compaction, after compaction, and after graceful reopen.
A separate durable update survives a forced process stop and reopen with the expected values.
All 83 recorded requests return HTTP 200.

The physical inventories report no `journal` directory or `JOURNAL_TAIL`.
Their feature mask is `509`, which includes snapshot bit `256` and excludes retired bit `2`.
The compaction receipt reports 152 dependency parts before and six afterward.
It reports 408 ms for the pack and 90 ms for reclaim.
Those figures describe this run only; they establish no improvement over an equivalent baseline.

The first run does not establish power-loss, backup, restore, sync, or corruption behavior.
It does not exercise old-format refusal or the current fixes after its executable commit.
The filesystem cache is uncontrolled.
No result on the primary is claimed.

The later [backup and restore result](https://github.com/EdgeVector/lastdb/blob/0b0a79d4c10949e2f0570e82d9bc4fdb9513a285/docs/proofs/snapshot-commits-20261022/run-1/backup-restore/result.json) restores the exact snapshot bundle after a source-root rename.
The same generated home retains its identity without a read or copy of the identity key bytes.
Exact and ordered range reads match all 60 current records, including the durable update at key 63.
The [complete operational report](https://github.com/EdgeVector/lastdb/blob/0b0a79d4c10949e2f0570e82d9bc4fdb9513a285/docs/proofs/snapshot-commits-20261022/RESULTS.md) also records the final physical command checks and cleanup.

## Historical performance evidence

The [PR 266 barrier report](https://github.com/EdgeVector/lastdb/blob/0b0a79d4c10949e2f0570e82d9bc4fdb9513a285/docs/proofs/native-commit-barriers-20261022/RESULTS.md) predates journal removal.
It compares old main with the barrier candidate on macOS APFS.
The workload sends 1,000 queued requests across 64 schemas through one client, with 64-byte values.

| Whole-request latency | Old main | Barrier candidate |
|---|---:|---:|
| Median | 142.88 ms | 30.00 ms |
| P95 | 273.23 ms | 42.96 ms |
| P99 | 351.34 ms | 58.06 ms |
| Maximum | 1,430.70 ms | 1,688.54 ms |

All 1,000 requests return memory acks in each run.
These are not durable-write latency samples.
Memory acks can still wait for an author-clock disk commit.
The separate durable mutation and forced-process-stop receipt does not establish power-loss behavior.
This report establishes no benefit from journal removal.

The [October 21 growth receipts](https://github.com/EdgeVector/lastdb/blob/0b0a79d4c10949e2f0570e82d9bc4fdb9513a285/docs/proofs/write-overhead-20261021/summary.json) use revision `8067843c4` and a different 512-record workload.
Metadata accounts for 94.7% of aggregate growth across its five default update phases.
Its compaction times also describe that historical revision.
Neither later run repeats that workload.
Request speed can change flush groups, so an unchanged file envelope would not establish unchanged workload growth.

The old estimate of several removable full device flushes became obsolete with PR 266.
The new source removes duplicate receipts and root publications from the ordinary commit route.
The later [matched benchmark](https://github.com/EdgeVector/lastdb/blob/0b0a79d4c10949e2f0570e82d9bc4fdb9513a285/docs/proofs/journal-removal-benchmark-20261022/RESULTS.md) compares `6d8aa0d57` with `3b53ece33` in three fresh-home pairs.
Every write phase completes sooner and records fewer process write bytes in all three pairs.
Compaction finishes sooner, mainly through reclaim; concurrent write probes establish no consistent pause improvement.
The controlled comparison disables automatic maintenance and retains the 500 ms flush.

Source review does not establish runtime results.
No primary data, archive, shared service, CI setting, or release setting changes in this document pass.

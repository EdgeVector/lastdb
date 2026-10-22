# LastDB storage code review

Reviewed 2026-10-22 against the current branch and its recorded source commits.
Commit `47088b89c0bff5142a519db685051849112a787a` supplies the initial native writer.
The implementation starts from main `6d8aa0d573e91a94fd4fd308a7f0eacca022e852`.
It retains the macOS barriers from PR 266 and the reclaim handoff from PR 267.
This document separates source facts, the first product run, and historical performance evidence.
The second review also removes erase recovery records and journal-based cloud repair.

## Current source

| Area | Current source | Scope left |
|---|---|---|
| Local writes | `HomeSnapshot::commit_mutation` writes immutable payload files and changed search pages | Measure the change against a matched baseline |
| Publications | Each complete stored batch writes one generation receipt and publishes one root | Receipt copies still retain conservative prior dependencies |
| Current record bytes | A payload locator uses `Source::Pack`; a delete uses `Source::Tombstone` | Compaction still removes superseded files |
| Recovery | Writer open verifies `CURRENT`, its receipt, and dependency files | No local mutation replay or silent receipt reconstruction |
| Internal journal | The journal writer, reader, stream, tail, rotation, and command are removed | Erase aggregates now commit with deletes; debit recovery records are removed |
| Format | Every supported root requires snapshot feature `256`; retired feature `2` is refused | No converter for old native journal homes exists |
| macOS durability | Ordered file barriers precede root publication; a final device flush precedes durable success | Process interruption does not prove power-loss behavior |
| File lifetime | Read holds, backup references, and the installed-root reclaim handoff remain | Compaction still has a write pause |
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
Reserved format bytes remain zero and retain their integrity check.
Payload reservations no longer charge a phantom journal prefix.
The complete-batch policy cap stays 128 MiB.
The offline write command now holds a complete disk-space reservation through publication.
Its default free-space and replacement allowances remain 64 MiB each.

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
Author-clock reservations retain their durability requirements before a memory ack.
The ordinary flush interval stays 500 ms.
A memory ack remains volatile until the flush completes.
The interval does not set a guaranteed maximum loss window.

The writer reserves the complete stored batch before file output.
The payload quote includes worst-case frames, part headers, and complete block-map pages.
The complete mutation quote adds changed tree pages, one maximum-size receipt, and root space.
The writer also checks actual payload dimensions against its admitted quote.
The node retains separate disk and memory reservations.
An engine reservation alone does not authorize a memory ack.

The writer marks itself poisoned before commit file work.
An uncertain result stops new admission until the caller reopens the writer.
A reopen selects only the committed root; private interrupted output remains unused.
Missing or damaged committed dependencies cause an error.
The code does not infer another root or reconstruct a missing receipt.

New native and artifact roots require `FEATURE_SNAPSHOT_COMMIT`.
Old journal roots receive an explicit refusal before writer file creation.
Startup performs no migration.
The refusal describes a required future explicit conversion, not an available command.
The generic legacy converter still refuses native output without the atom-record authority.

## First product receipt

The [run-1 inputs](proofs/snapshot-commits-20261022/run-1/inputs.json) identify the executable by its SHA-256 and source commit `47088b89c`.
The run precedes the current fixes after that commit.
Its [summary](proofs/snapshot-commits-20261022/run-1/summary.json) records completion, no error, and all owned processes stopped.
The detailed receipts remain in `proofs/snapshot-commits-20261022/run-1/receipts.jsonl`.

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

The later [backup and restore result](proofs/snapshot-commits-20261022/run-1/backup-restore/result.json) restores the exact snapshot bundle after a source-root rename.
The same generated home retains its identity without a read or copy of the identity key bytes.
Exact and ordered range reads match all 60 current records, including the durable update at key 63.
The [complete operational report](proofs/snapshot-commits-20261022/RESULTS.md) also records the final physical command checks and cleanup.

## Historical performance evidence

The [PR 266 barrier report](proofs/native-commit-barriers-20261022/RESULTS.md) predates journal removal.
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

The [October 21 growth receipts](proofs/write-overhead-20261021/summary.json) use revision `8067843c4` and a different 512-record workload.
Metadata accounts for 94.7% of aggregate growth across its five default update phases.
Its compaction times also describe that historical revision.
Neither later run repeats that workload.
Request speed can change flush groups, so an unchanged file envelope would not establish unchanged workload growth.

The old estimate of several removable full device flushes became obsolete with PR 266.
The new source removes duplicate receipts and root publications from the ordinary commit route.
The later [matched benchmark](proofs/journal-removal-benchmark-20261022/RESULTS.md) compares `6d8aa0d57` with `3b53ece33` in three fresh-home pairs.
Every write phase completes sooner and records fewer process write bytes in all three pairs.
Compaction finishes sooner, mainly through reclaim; concurrent write probes establish no consistent pause improvement.
The controlled comparison disables automatic maintenance and retains the 500 ms flush.

Source review does not establish runtime results.
No primary data, archive, shared service, CI setting, or release setting changes in this document pass.

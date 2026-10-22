# Native snapshot commit result, October 22, 2026

The native internal recovery journal is removed in this branch.
The local writer uses immutable payload files, changed search pages, one generation receipt, and one `CURRENT` publication per stored batch.
The sync mutation code remains separate. Native cloud activation stays disabled.

The branch starts at main `6d8aa0d57` and includes implementation commit `47088b89c` plus the final fixes and receipts.
The first product run records that implementation commit and the exact executable hash in [its inputs](run-1/inputs.json).
The executable was built from the worktree before the implementation commit; its embedded Git label can name the earlier dirty base.
The later source change replaces a one-element loop in backup preparation; it does not change its output.
The final command capture records its own executable hashes.

## Local product result

The [bounded capture](capture.py) starts the real node on a fresh owned home with generated data.
It does not read or copy the primary or its archive.
The run uses one HashRange schema, four fields, 64 keys, and 1 KiB generated bodies.
Its 83 HTTP requests return status 200.

| Product action | Observed result |
|---|---|
| Batch create | All 64 exact keys and ordered range values match immediately after the memory ack |
| Five complete update rounds | Each flush reports complete durable coverage |
| Delete eight keys, then recreate four | Exact reads report four absent keys and 60 correct live records |
| Range continuation | Pages contain at most seven records; membership, values, and order match the exact read |
| Explicit compaction | All 60 current records remain correct |
| Graceful stop and reopen | All 60 current records remain correct |
| Durable update, forced process stop, reopen | All 60 current records remain correct; key 63 retains revision 99 |
| Physical inventory | All three inventories report snapshot feature 256, no retired feature 2, and no internal journal paths |
| Final flush | The writer reports no failure and complete sequence coverage |

The physical inventories report 189 files before compaction and 12 immediately after compaction.
Logical disk size falls from 44,353,125 bytes to 22,711,005 bytes in this small generated run.
Later node work creates more files; the final inventory contains 41 files.
These observations describe one product run. They are not a comparison with the journal-backed engine.

At the final native status read, the warm charge is 422,976 bytes under its 268,435,456-byte default budget.
Dirty and held charges are zero; the write queue is empty.
This small run does not establish memory behavior at the budget or under concurrent held reads.
The source retains the byte-based warm policy and its counted temporary overshoot.
The approved 25% dirty share remains a reported threshold rather than complete admission enforcement.

Detailed requests, results, read costs, durable boundaries, file inventories, and shutdown receipts appear in [the run receipts](run-1/receipts.jsonl).
The [run summary](run-1/summary.json) records the result and owned home.

## Backup and restore

The actual recovery command creates a `lastdb-native-backup-2` bundle from the stopped generated home.
The bundle selects generation 40 and frontier 38.
It contains 37 snapshot files with 23,593,524 bytes, plus its two manifest files.
It contains no journal, node identity, encryption key, or cloud grant file paths.

The procedure renames the source physical root and restores the bundle to its original physical path.
The same generated node home keeps its identity; the procedure does not read or copy the identity key bytes.
The node starts from the restored snapshot.
Exact reads and nine range pages match all 60 records, including key 63 at revision 99.
The node then stops with exit 0 and `flush_ok=true`.
The [backup result](run-1/backup-restore/result.json) and adjacent command receipts record the exact boundary and bundle inventory.

This is a data-only local bundle.
Recovery on another machine also needs the separately retained identity and encryption keys.
This procedure establishes no cloud recovery result.

## Complete physical commands and integrity

The [second capture](command_capture.py) uses the final release binaries and a second fresh product-created home.
The node initializes that home and stops before the physical command changes its files.
Each read supplies all six known keys in one batch.

| Physical command | Observed result |
|---|---|
| Seven operations in one complete batch | Generation 21 becomes 22; frontier 20 becomes 21; exactly one generation receipt appears |
| Repeated key | The final input value wins |
| Put followed by delete | The key is absent |
| Empty value | The empty body remains distinct from absence |
| Three tombstones in one complete batch | Generation 22 becomes 23; frontier 21 becomes 22; exactly one receipt appears; no payload part appears |
| One changed byte in the stopped home's actual `CURRENT` | The read command exits 1 with `checksum mismatch` and no data output |
| Exact restore of the original `CURRENT` | The complete known-key read succeeds with the expected values |

The [second summary](run-2/summary.json), [inputs](run-2/inputs.json), and [detailed receipts](run-2/receipts.jsonl) record those results.
All commands exit. The owned node stops with exit 0 and `flush_ok=true`.
This procedure changes an actual generated snapshot file; it does not simulate a power failure or concurrent reclaim.

## Source and build checks

- Release builds include the node, CLI, and native physical commands.
- Full workspace Clippy passes with warnings denied for libraries and binaries.
- Format, whitespace, byte-slice, redaction, atom-counter writer, and native write-path lints pass.
- App registry, workspace dependency, heavy Clippy coverage, and hardcoded URL checks pass.
- Final source size, function size, new-allow, and tree artifact checks appear in the check receipts.

No tests or fixture suite run. The repository instructions retire those checks.

## Scope and remaining limits

The process-stop result follows a completed durable update.
It does not interrupt each publication stage and does not establish physical power-loss behavior.
The source retains ordered file barriers, atomic root replacement, parent directory barriers, and the final macOS device flush.
The writer stops further admission after an uncertain commit result.

Old native journal homes receive an explicit format refusal before writer file creation.
This change supplies no old-native-journal converter or automatic boot migration.
The generic conversion command remains subject to its existing atom-format fence.
The primary, archive, shared services, CI configuration, and cloud configuration remain unchanged.

No new latency or disk-growth improvement is claimed against the latest main.
The earlier [write-growth measurements](../write-overhead-20261021/summary.json) and [macOS barrier report](../native-commit-barriers-20261022/RESULTS.md) use different revisions and workloads.
Atomic sync capture, dirty-share enforcement, proposed workspace limits, and a journal-home converter remain separate work.

Owned processes stop before cleanup.
The [first cleanup receipt](run-1/cleanup.json) and [second cleanup receipt](run-2/cleanup.json) confirm removal of both exact owned scratch homes.
The saved proof files remain in this worktree.

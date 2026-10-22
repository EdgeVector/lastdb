# Atom lifecycle product receipts

These receipts use fresh generated homes. They use the actual daemon and public local API.
The primary and its archive stay unchanged. No tests run.

The source revision and executable hashes appear in each `inputs.json`.
The request receipts contain response hashes and exact read-result hashes.
The generated values use the deterministic recipe in `product_run.py`.

## First run

`run-1` records a failure in the first candidate.
Concurrent writes and compaction completed. The exact values and list membership remained correct.
A later new hash group required an empty locator template that reclaim removed.
The diagnostic backup completed before recovery.
`run-1/diagnosis.json` records the selected generation, frontier, and missing object.

The change now retains the empty template for each grafted locator object.
`recover_run.py` uses the supported offline compact command on that stopped, owned generated home.
Its result remains separate from the failed first result.

## Second run

`run-2` records a refusal during the concurrent copy. The durable writers continued, and shutdown succeeded.
The copy selected a random future generation after a writer took the next receipt name.
The catch-up read then refused those private pages because their age exceeded its root.
The stopped home confirmed private age 2,604,637,533 and selected generation 59.

The copy now stamps private objects at the source generation plus one.
It selects a receipt generation only under the final publication boundary.
The separate recovery confirmed all accepted values before the offline compact.
It also confirmed a new-key write, zero counter mismatches, and a successful reopen.
Both failed temporary homes were removed after their separate recovery checks.
Their original results remain `complete: false`.

## Final run

`run-3/summary.json` records `complete: true` for 4,096 generated records and two long keys.
The final state contains 4,090 records after eight deletes.
Every exact read and list continuation matched the accepted values.
The 84 list pages include 72 successful partial pages below the requested limit.

The concurrent full copy caught 2,697 changed entries across four catch-up rounds.
Three later update copies and the delete copy used recent-tier boundaries.
The largest observed handoff was 16 ms. This small run establishes no large-home latency bound.
The mixed mutation calls had a 130.9 ms median and a 289.8 ms maximum.
Those calls include all phases and establish no paired speed improvement.

The backup completed during the copy and excluded identity files.
The bundle restore, graceful reopen, forced reopen, and interrupted-copy reopen returned the expected values.
The TTL sweep removed 24 expired records while eight foreground updates completed.
Schema drop and molecule drain completed.
The final verifier read 24,546 counters and 16,360 tips, with a complete tally and zero mismatches.
This run contains no file blobs.

All owned node processes stopped. The owned scratch was removed after the receipt and ownership checks.
The backup manifest and cleanup receipt remain with the result.

## Source checks

The executable uses Rust implementation `8ca6c30f0507eb166c519cb13c20cb9b3990608e`.
Its SHA-256 is `672faa0a41739709524530f2e658c4df045fe571279d5becf23790f6b8654378`.
The `dev-release` build passed for the daemon and both native maintenance commands.
Format, size, function, new-allow, writer-boundary, write-path, byte-slice, redaction, and URL checks passed.
The required GitHub check also passed for that implementation, including the full workspace Rust checks and SDK checks.
The final PR head retains its own required GitHub check.

The automatic GitHub code review completed without findings at `3535235fce28d9190ddec17590ae66ddaf5c09a7`.
The later result commit adds receipts and this report. It changes no Rust implementation.
The separate safety and performance reviews remain necessary before merge.

## Product operations

The capture uses these operations:

- Durable create, update, and delete batches, with exact reads and list continuation.
- Concurrent durable writers during full compaction, followed by recent-tier copies.
- Long keys, tombstones, and a new hash group after concurrent compaction.
- A data-only native backup during the copy, then a local bundle restore.
- Graceful shutdown, forced process interruption, and an interrupted private copy.
- TTL expiry, schema drop, molecule drain, and a complete atom/blob counter verifier pass.

`compactions.json` records the copy duration, catch-up records, and tier boundary.
`latency.json` records mixed calls on a shared host. It establishes no paired speed improvement.
A process interruption establishes no power-loss result.

## Cleanup

The cleanup command checks the exact temporary path, realpath, UID, inode, ownership marker, and stopped process receipts.
It refuses active sockets, symlinks, other owners, and an incomplete capture.
The separate failed-run cleanup also requires a successful recovery receipt.
Each `cleanup.json` records the retained ownership facts and the removal result.

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

## First successful full capture

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

## Independent resource finding

The scale reviewer found that a recent-tier copy kept one output descriptor for each touched tree object.
A source can retain several locator objects after concurrent grafts.
The old bound therefore followed the home size rather than the active operation.

Commit `c1413d3d06256f3a37b84d73eed39a18a94d6136` closes each output descriptor between trees.
A partial file reopens with its exact expected length before the final flush.
A full file already receives its flush before the close.
The final file and directory barriers remain in place.
Both independent reviewers accepted the source correction.
The required GitHub check passed for this revision.

The earlier capture uses the same copy selection and publication behavior.
Its timing results do not measure the additional file reopen cost.
The final capture and review reports retain their own source revisions and executable hashes.

`run-4` passed the full product capture with the descriptor correction.
Its binary SHA-256 is `5d507e6bb664b9d0a35e07fab36e3fc37291694d7ef39dfccc59ec066668aaa4`.
It retains source revision `c1413d3d06256f3a37b84d73eed39a18a94d6136`.
The concurrent full copy caught 2,824 changed entries across four rounds.
The four later recent copies took 36 to 43 ms on this host.
The exact data, backup restore, process interruption, expiry, drain, and counter checks passed.
The list returned 72 successful partial pages across 84 total pages.
The owned node stopped, and the owned home was removed after the receipt checks.

## Independent space finding

The reviewers found that a recent page can reference an old long-key overflow chain.
The tier copy rewrites that chain, but the previous reservation counted only recent file lengths.
That reservation can understate the required space for long keys.

The quote now visits only recent reachable tree pages.
It charges each emitted page and each external key descriptor's complete overflow length.
It uses the same directory and locator inline limits as the output format.
It skips old subtrees and reads no old key contents.
The previous metadata slack and uncompressed payload bound remain in the reservation.
Both independent reviewers accepted the source correction.
The earlier timing receipts exclude this additional quote work.

## Independent recovery finding

The large concurrent copy left more selected parts after concurrent changes joined its output.
Both reviewers found a soft-limit recovery stall in the worker's post-copy growth rule.
A successful copy above the soft limit set a new baseline, but refused writes could not supply the next required growth.

The worker now bypasses that growth rule and its no-progress hold above the soft limit.
It forces a full copy there because a recent copy can retain the older grafted base.
A result that stays above the soft limit records the capacity error and waits on an exponential back-off.
The maximum wait is 60 seconds. Refused-write notifications cannot bypass it.
The retry count resets after the selected count falls below the soft limit.

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

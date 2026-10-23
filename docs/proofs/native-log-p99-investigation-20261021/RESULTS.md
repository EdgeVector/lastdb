# Cause of the remaining 67 ms P99

Date: 2026-10-21. Product source: `f8a917c7c`.
This investigation changes no product code, disk guarantee, timer, or memory cap.
No push, PR, merge, primary change, or shared-system change occurs.

## Result

The current experiment has two distinct request populations.
Most requests complete without a wait for the background disk-file update.
A smaller population waits for that update under the same write gate.
P99 lies in the second population.

The earlier diagnostic run contains 1,000 writes and 44 disk-file updates.
Exactly 44 requests overlap those updates by more than 1 ms.
Every request above 40 ms belongs to that population.
Those 44 requests are 4.4% of the complete sample, which exceeds the upper 1% boundary.
The other 956 requests have an 11.907 ms median and an 18.943 ms P99 within that observed subset.
That subset result is descriptive. It is not a new full-workload P99 or a projected result after a fix.

The actual request at the earlier P99 has these non-overlapping costs:

| Component | Milliseconds |
|---|---:|
| Wait before native write preparation | 54.448 |
| Its own durable log acceptance | 7.863 |
| Other native work | 4.363 |
| Client work outside the native scope | 0.357 |
| Complete client request | 67.031 |

The 54.448 ms wait includes 51.649 ms of disk-file publication and 2.660 ms of post-publication delivery.
The own-request full device flush takes 4.992 ms, inside its 7.863 ms log acceptance.
The complete write delay is therefore dominated by exclusion from the writer.

## Why the background update excludes writes

`fold_db/crates/core/src/storage/disk_pack/queue.rs:173` seals the current queue.
Line 176 takes the write gate. Lines 178-185 retain it through `commit_queue`.
The queue then stays sealed until post-commit delivery completes at lines 199-210.
`enter_prepare` waits for both conditions before it admits another complete mutation.

`journal_ack.rs` calls the existing `HomeJournal::publish_durable` inside that interval.
The native publisher verifies complete pending log batches and updates the disk lookup files.
It writes the generation receipt and changes CURRENT.
The existing checkpoint then archives the old TAIL and changes the journal stream.
The complete durable publication ends with a full device flush.

This interval occurs after the 250 ms collection deadline or another existing queue trigger.
The normal ack does not wait for that deadline.
A later request can encounter the update when it needs the shared gate.
The earlier run publishes a median of 24 queued requests at once.
Its disk-file update has a 48.851 ms median.

The gate also protects generation-specific reservations and accepted memory ownership.
`view.rs::install_committed` replaces CURRENT and clears the complete accepted list.
Removing the gate alone can lose later accepted writes or mark their memory clean too early.
A smaller gate interval needs an explicit captured prefix and ownership for the later writes.

## New profile: avoidable checksum work

A fresh run uses the unchanged optimized daemon, a new generated home, and the same 1,000-write workload.
It repeats an 11.959 ms median, 67.027 ms P99, and exactly 44 publication-blocked requests.
An explicit five-second process sample at a 1 ms interval observes 570 queue-owned publication stack samples.

| Publication component | Samples | Share |
|---|---:|---:|
| Disk lookup-file update | 293 | 51.4% |
| Generation receipt | 62 | 10.9% |
| CURRENT file publication | 54 | 9.5% |
| Final full device flush | 54 | 9.5% |
| Other stream rotation work | 45 | 7.9% |
| Journal read and verification | 39 | 6.8% |
| Other publication work | 23 | 4.0% |

Software SHA-256 is the largest leaf function: 211 of 570 publication samples, or 37.0%.
Local dependency source confirms that sha2 0.10.9 enables its ARM hardware path only with the `asm` feature.
All four local dev-release sha2 feature receipts contain only `default` and `std`.
The actual sampled function is `sha2::sha256::soft::compress`.
The host reports arm64, Apple M4 Max, and `hw.optional.arm.FEAT_SHA256 = 1`.
These independent observations confirm that the measured checksum path omits available hardware support.
The file format and checksums need no change to select the faster implementation.
No accelerated build occurs here, so its latency benefit remains unmeasured.

The profile also observes file open and close work during the lookup-file update.
The publisher creates a fresh bounded read call for each affected hash group.
This repeats directory-page reads and checksum work within one publication.
Reuse inside a bounded publication call is a possible later change.
It must preserve the aggregate read budgets and correct lengths of private output files.

Sample shares identify work within the publication stack; they are not exact phase wall times.
The sample can perturb execution. The earlier unprofiled receipts establish the request attribution separately.

## Next change and limits

First enable the dependency's hardware checksum path on supported ARM builds, then measure the real daemon again.
Keep the same checksums, disk barriers, workload, timer, memory caps, and recovery checks.
If the gate still dominates P99, prepare the lookup files outside the gate for an explicit captured prefix.
Retain every later write and its memory and disk reservation until its own publication completes.

The earlier 3,000-write run remains a separate limit: 237 ms P99 and a 2.06-second maximum.
It contains maintenance pauses and complete disk fallback requests.
The trace does not identify every gate owner or every memory-admission refusal reason.
This short-run profile does not resolve or reclassify those larger delays.

## Receipts and cleanup

`findings.json` contains the old and new request attribution plus the checksum source and feature evidence.
`publication-stacks.json` retains normalized publication frames and sample counts without addresses or library paths.
`explain.py` performs the bounded extraction and reconciles sample counts.
The prior numeric analyzer produces `attribution/` from the explicit generated-node log.
`profiled/` retains every real client response, latency, executable hash, and recovery result.
All 501 expected values match after forced-stop recovery. The next author clock advances.
Final shutdown returns exit code 0 and `flush_ok: true`.
The owned daemon stops before cleanup. Generated homes and raw logs are removed after receipt preservation.
No product build is needed because no product code changes. Python syntax and diff whitespace checks pass.
No tests, fixtures, mutation probes, or coverage gates run.

# Durable writes after native journal removal

The branch uses main `4118c7baa12539c8c6bcf4391c497159f2a1529a`, which merges PR 268.
The prior experiment remains at branch `archive/write-under-10ms-before-snapshot-rebase-20261022`, commit `e2e68219f2e11eb82f5c693e13749a655f87272e`.
The rebase consolidates the older experiment commits and keeps their historical receipts.
Those journal-based latency and recovery receipts do not validate this snapshot version.
This work remains unmerged. The primary and its archive stay unchanged.

## Source changes

- Every native request completes its direct snapshot and clean core memory publication before success.
- The queue starts immediately. Native requests use no 500 ms collection timer or journal experiment flag.
- A native create/update batch includes its author-clock high-water state in the same snapshot.
  The state stays at or above any prior durable clock reservation.
- Compaction copies a held snapshot outside the node write gate.
  It merges newer stored locations, including tombstones, from a later held snapshot.
  The final gate checks the exact included snapshot before the root and read-view handoff.
- Compaction uses at most eight catch-ups. Continued foreground writes can defer it with a retryable capacity refusal.
  The owner route returns the existing structured resource-limit response for that refusal.
- The concurrent disk-cap accounting and old-file cleanup fixes from PR 267 stay on the base.

The port removes the journal-only ack, prefix publication, replay catch-up, and dirty-slot completion paths from the old experiment.
It introduces no disk journal, replay suffix, mutation-log recovery path, or second disk format.
The temporary compaction delta holds stored file locations for private maintenance work only.
Ordinary reads and restart recovery use the selected snapshot.

## Product captures

Each run uses a fresh generated home and the real daemon over its own Unix socket.
The clients use 64 distinct schemas with distinct field identities and 64-byte values.
Each client uses its own schema set. No two clients write the same schema concurrently.
Request latency covers the complete batch, with no division by its record count.
The host Cargo exclusion prevents cooperative compiler overlap during each capture.
Other host work and operating-system caches remain uncontrolled.

The first source, `b41edb294e0dfe3b69ec3a156aa74a77ab06aa81`, completes 1,000 default single-record requests.
All replies report durable success. The average is 41.80 ms; P99 is 92.44 ms.
The first 100 calls average 27.87 ms; the final 100 average 49.65 ms.
Retained dependency parts grow from 720 to 14,014. These observations do not isolate the cause of the higher cost.

After SIGKILL, all 65 final known records match and the next author clock advances.
The stopped home contains neither `journal/` nor `JOURNAL_TAIL`.

Two initial concurrent captures preserve all 256 successful durable write receipts each.
Both stop before recovery verification because the old compact route returns a content-free 500 for a capacity deferral.
The first capture also exposes the old helper's unconditional JSON parser.
These attempts remain in `concurrent-compaction/` and `concurrent-compaction-final/`.
They are excluded from final recovery claims. The route and capture changes address that classification and capture problem.

## Final result

Final source: `a590e2abab0db6a3916fb7de3f9fc155677ac727`.
The optimized daemon hash is `0d62917132e752dd12df47f126ef6f9d04d755b84e4da73899b0b9d5b11cec7a`.
The [build receipt](final-build.json), [non-test checks](final-checks.json), and [summary](summary.json) identify the exact result.

| Default durable workload | Calls | Average | Median | P99 | Maximum |
|---|---:|---:|---:|---:|---:|
| One record, one client | 1,000 | 53.30 ms | 48.33 ms | 120.39 ms | 172.15 ms |
| Eight records, four clients, concurrent compaction attempt | 256 | 157.24 ms | 119.46 ms | 691.18 ms | 847.89 ms |

Every final measured reply reports durable success. No write request fails.
After SIGKILL, all 65 final records in the first home and all 513 in the second home match.
Both author clocks advance after reopen. Both final shutdowns complete their disk flush.
Both stopped homes contain no storage journal directory or journal tail.

The concurrent compaction attempt returns structured HTTP 503 with `retryable: true` and a two-second retry hint.
It reaches the eight-catch-up limit while writes continue.
Ninety-three write batches complete during that compaction request.
This overlap does not identify the exact private-copy interval.

After the measured writes stop, the explicit retry returns HTTP 200.
That retry removes 6,636 obsolete files and 590,671,592 bytes.
Its later work stays outside the write-latency samples.
The [compaction receipts](compaction-final/compaction.json) and [successful retry](compaction-final/compaction-after-writes.json) retain both outcomes.

Format, changed-file size, changed-function size, and new-allow checks pass.
The optimized product build and Clippy checks pass for the affected storage, core, and node packages.
The owned daemons stop before cleanup. Their logs remain in `logs/`.
Cleanup receipts identify the exact generated scratch directory and its removal.

## Limits

The measured single-write average exceeds the 20 ms target.
The older journal-based result does not establish the 20 ms target for snapshots.
Each ack now includes the complete snapshot lookup-file work as well as the payload files.
Further cost attribution and optimization remain separate work before a performance approval.

Continued writes can postpone compaction and increase retained files until a later attempt succeeds.
File work outside the write gate still competes for disk and CPU time.
The final snapshot publication still needs the write gate and its durability barriers.

The process-stop captures establish no power-loss result, cloud recovery result, or primary result.
The platform is macOS/APFS with the existing ordered barriers and final full device flush.
Old journal-backed homes retain the refusal and conversion limits of PR 268.

# Independent scale and resource review

Reviewer: `atom_scale_review`
Date: 2026-10-22 PDT / 2026-10-23 UTC
Source: `93729bfbbe2062711c29008b40516b86fd7fbe72`
Decision: **APPROVE source and product evidence. No reviewer blockers remain.**

Green checks for the final PR head remain a merge gate.

## Scope and method

I independently reviewed the source, design, preserved requests, status records, counter reports, ownership records, and cleanup receipts.
The parent agent ran the generated product operations.
I ran no builds, nodes, or tests for this follow-up review.
I accessed no primary data, archive, brain, situations, or kanban.

The main integration at `b9e7434e48a5a4b752b5c94dde74af317deb9155` preserves the reviewed compaction code.
Its incoming key-codec files match main `c6b4f7ed9`.
The final source adds the reviewed capacity-error correction and accurate source comments.

## Source findings and corrections

The final write boundary contains conditional metadata installation, required durability barriers, and the matching read view.
Copy, catch-up, space checks, receipt construction, and temporary-file flushes occur outside that boundary.
The gate try-acquire occurs inside the active blocking closure, after the thread starts.
The duration sample includes the complete gate hold and view handoff.

Two nonblocking copy permits bound concurrent owners for the native home.
Each copy also enforces a 64 MiB charged path-metadata budget.
The existing copy caches retain finite memory and at most 64 cached source file handles per copy.
These bounds do not describe all memory or file handles in the daemon.

Exact path guards precede output creation and remain through publication or abandonment.
Directory guards prevent directory removal without excluding unrelated files inside those directories.
Source holds and exact graft guards retain every input that the proposed root can reference.
Cleanup claims check both the exact guards and the captured epoch.
Publication invalidates older epochs before the new root becomes visible.
This also covers cleanup plans captured before publication but executed later.

All eight publication attempts include busy-gate, pending-queue, and changed-root cases.
Queue waits occur outside the boundary and do not force a flush.
Compatible equal-frontier roots skip key replay, duplicate payload retention, and delta reservation.
The active job count controls the idle status.

The final allocator retries only the exact active-deletion conflict.
A path-budget refusal retains `compaction_behind` and returns without eight allocation retries.
Its local guard survives the budget error through exact new-file close and unlink.
An initial `AlreadyExists` error never triggers that unlink.
No registry mutex spans file I/O.

## Product evidence

Both final captures use source `93729bfbbe2062711c29008b40516b86fd7fbe72` and daemon SHA256:

`3cc3b86963f5d28c6541850017828efb2582a64c52613276b2d0ec188d7074ef`

| Capture | Confirmed result |
|---|---|
| `lifecycle-final` | All 4,090 expected values agree after copies, deletes, restore, process interruption, TTL, schema removal, and reopen. |
| `lifecycle-final` | Two complete tallies each read 24,546 bodies/counters and 16,360 tips, with zero mismatches. |
| `lifecycle-final` | Both equal-frontier copies publish with zero/one retries and 12 selected parts each. A third copy receives `compaction_busy` HTTP 503. |
| `lifecycle-final` | Seventeen cleanup calls overlap active copies and remove 18 files. The recorded maximum shared boundary is 37 ms. |
| `automatic-final` | The worker publishes during durable writes at the supported 128-file trigger. All updated keys match in the 253-key readback. |
| `automatic-final` | The complete tally reads 24,582 bodies/counters and 16,384 tips, with zero mismatches. |
| `automatic-final` | The final writer is healthy. No maintenance failure or worker restart remains. The observed shared boundary is 16 ms. |

The automatic copy selects 188 parts from a 131-part source during concurrent writes.
Later writes leave 202 selected parts, below the current adaptive trigger.
This result establishes successful automatic publication, not a decrease in every concurrent copy's part count.

The large capture uses integrated source `b9e7434e48a5a4b752b5c94dde74af317deb9155`.
Its home contains 100,000 ordinary user records and one long-key record.
The initial full copy processes 1,200,054 native entries.
All 100,001 expected values agree after interruption, both overlap phases, and final reopen.
Three complete tallies each read 600,108 bodies/counters and 400,004 tips, with zero mismatches.

| Large overlap | Copy duration | Catch-up entries per copy | Retries | Selected parts per copy |
|---|---|---|---|---|
| Concurrent writes | 21.134 / 21.220 seconds | 27,047 | 0 / 1 | 1,318 |
| Unchanged values | 11.972 / 12.005 seconds | 0 | 0 / 1 | 108 |

Of 220 cleanup calls, 218 complete without deferral while copies remain active.
Those calls remove 2,155 files during active copies.
The worker log records three orphan files and 1,540,096 bytes removed at `02:18:18.926130Z`.
Status receipts immediately before and after that event show two active copies.
This confirms orphan removal during the overlap.

The 723 valid large-home resource samples show a peak of 628,572,160 memory bytes and 170 file handles.
Retained overlap status samples show at most 11 ms for the shared boundary.
The final reopen resets those counters, so 11 ms is not an all-run maximum.
The baseline contains 283 writer batches plus two later worker-start batches.
Its original 285-sample comparison remains unchanged.

## Receipt integrity and cleanup

The large archive SHA256 is `814600e3b8003d5c2d2975406ca6e449a375aa7f09eb748c57d660a6d5d7b323`.
Decompression produces 5,696,715 bytes with SHA256 `b939b6485a46675c3b3ef1290776cb4a27fdd63ae79b0f90db9e8e8421e83ae2`.
Both results match `large-run1/receipt-archive.json`.

All six capture summaries report completion, no error, and stopped owned processes.
Each cleanup receipt matches its original ownership record and exact scratch path.
Each records successful removal after the owned processes stop.
The final lifecycle receipt lists five stopped process IDs; the automatic receipt lists one.

## Evidence limits

- The large timings remain measurements of source `b9e7434e`; the final capacity-error correction receives source review and fresh small-home product captures.
- Adjacent phases use one generated home on a shared host, with uncontrolled filesystem cache. They establish no paired speed improvement.
- Resource samples establish observations, not strict daemon-wide peaks between samples or a fixed latency guarantee.
- No capture exhausts the 64 MiB path budget, exercises a disk cap, or crosses the actual 85-percent soft-limit transition.
- No capture observes cleanup invalidation after partial removal. The original count remains zero.
- The driver's stronger all-post-reopen-path orphan result remains false. The worker log establishes only the specific three-file removal above.
- These captures include no external file blobs and establish no power-loss result or every possible race order.

The source safeguards and the preserved product evidence support approval within these limits.
See [RESULTS.md](RESULTS.md), the final capture inputs and receipts, and the design for the complete provenance.

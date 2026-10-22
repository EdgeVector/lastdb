# Native cleanup owner handoff, 2026-10-22

Main kept the write gate, journal mutex, and read-view lock through old-file checks,
inventories, deletion, and directory flushes. This change releases those locks after
it captures an installed root. A separate per-home owner serializes maintenance calls.
The owner completes even if the HTTP caller cancels.

The captured root retains a read hold until cleanup ends. Active older reads retain
their complete roots. New reads at or above the installed floor can continue.
Cleanup ignores newer receipt directories. An orphan file needs a valid older creation
generation before deletion. The active journal stream directory stays present.

Capacity admission also retains reservation releases that overlap an inventory.
A brief lock protects receipt renames across inventory namespaces. The reservation
ledger never holds a mutex across file access. Near a cap, conservative charges can
temporarily refuse a write. Ordinary uncapped writes still avoid file inventories.

## Real daemon comparison

The baseline binary contains PR #266 and matches its merged product code on main
`9f0ab569b`. The candidate adds only the cleanup change. Each run uses a fresh generated
home, 64 schemas, four clients, and eight records per batch. Clients use disjoint schema
sets. All 1,000 measured batches request disk completion and receive durable receipts.
No sample was removed. No compiler or other owned workload ran during either measure.

The setup writes 64 batches and packs the home with a one-day grace. The timed phase
runs writes beside one explicit reclaim with zero grace. All times measure a whole
batch request. They do not divide the time by eight records.

| Measure | Before | After |
|---|---:|---:|
| Average request, ms | 232.15 | 218.95 |
| Median request, ms | 191.94 | 173.39 |
| P99 request, ms | 733.12 | 733.86 |
| Maximum request, ms | 1133.23 | 1149.02 |
| Writes completed during cleanup | 0 | 14 |
| Files removed | 2390 | 2393 |
| Bytes removed | 144100830 | 144642231 |
| Cleanup time, ms | 560 | 1227 |

Writes now proceed during cleanup. Cleanup takes longer beside those writes because
it shares CPU and disk capacity. This run shows no P99 improvement. It does not show a
10 ms average. The separate disk-barrier report records the main write-speed change.

Both runs recover all 512 final records after an immediate forced process stop.
Their final graceful stops complete the disk flush. A process stop checks recovery;
it does not simulate a power failure.

The initial capture attempt put durability on the batch envelope, where the API does
not accept it. The corrected capture puts durability on each mutation. A later setup
with 500 batches exceeded the client timeout during retained-file checks. Those attempts
remain as receipts, but neither contributed a latency sample to this comparison.
The process sample in `setup-stall-diagnostic.json` identifies that retained-file work.

## Limits

The pack copy and root installation still use the write gate. This PR removes cleanup
work from that gate. Main keeps its memory-ack policy and its 500 ms flush interval.
The larger durable-ack experiment remains separate. PR #265 covers other pack costs.

The successful calls check actual records, disk recovery, and maintenance results.
Source review checks the lifetime floor, future-output exclusions, and capacity accounting.
No tests, fixtures, or failure-injection suite run or form part of this change.

## Further operational checks

A 1 GiB cap remained active through a further 64 durable eight-record batches and
concurrent cleanup. All calls succeeded. Cleanup removed 2,416 files. All final values
survived the forced stop, and the graceful stop completed its flush. These receipts
check ordinary capped use; they do not exhaust every capacity race.

A further 1,000-call run used the default memory-ack policy, one record per batch,
one client, and active automatic maintenance. The source and workload match the earlier
disk-barrier comparison. It averaged 27.90 ms, with a 24.23 ms median, 106.19 ms P99,
and 637.43 ms maximum. All calls succeeded, and disk recovery checks passed.

Against the barrier-only run, the average fell from 34.17 ms and the maximum fell
from 1688.54 ms. P99 increased from 58.06 ms. Background work can now share CPU and disk
capacity with foreground calls, so the lock removal does not guarantee lower P99.
Against original main, both the average and P99 remain lower. These are single-run
observations. No run proves a 10 ms target or a power-loss outcome.

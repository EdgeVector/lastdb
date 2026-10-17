# Layer 2 before and after results

Final source: `7110c7f54ea005e33678b5c93e89353d6117b41f`. Local date: 16 October 2026; raw timestamps use UTC.

Both programs start with empty homes. We write the same generated keys and bodies, then read and check the expected bytes.
Large disk batches get faster. One-key disk reads get slower.
Repeated exact queries can use memory with zero file I/O.

Large durable node writes still need seconds.

After maintenance, the node home contains 2 payload files and 2662 locator files.
The primary and cloud remain unchanged.

## Scope

The baseline is an earlier native program, `ca287339` plus compile repair `9cbe67f5`.
It is not the legacy file format.
The [freeze](node-final-freeze.json), [provenance](benchmark-provenance.json), and [command recipe](benchmark-recipe.md) record exact hashes, inputs, and commands.
Both programs receive independent empty homes and the same generated logical input.
The benchmark writes data in, reads data out, and compares returned values with that input.
No existing user database, primary, primary copy, archive, schema service, or cloud upload participates.

The physical matrices use fanout 1 in both programs, 4096 explicit keys, and an unset disk cap.
Their format feature masks are 7 and 47.
The node comparison includes fanout 16→1 and feature masks 7→63.
Valid old node samples and final node samples use separate time windows.
The old 256-write workload fails and supplies no comparison latency; no old node retry follows.

No other team-owned node or build runs during the latency matrices.
Ordinary host activity continues; the primary remains unchanged.
The operating system cache state is unknown. A fresh process does not establish a cold-device read.
Physical times use the product clock; node times include the HTTP call and client JSON decode.
File counters measure userspace work, not device traffic.
The p95 uses nearest rank; no p95 appears for fewer than five samples.

## Disk engine: 1 KiB bodies

Each case uses 31 alternating rounds per program. Times use milliseconds; arrows mean baseline→final.

| Operation | Median ms | p95 ms | File opens | File read calls |
|---|---:|---:|---:|---:|
| One exact key | 0.395 → 0.445 | 0.513 → 0.556 | 11 → 11 | 11 → 12 |
| 16 contiguous keys | 0.573 → 0.574 | 0.641 → 0.609 | 15 → 11 | 15 → 12 |
| 64 contiguous keys | 1.134 → 0.998 | 1.167 → 1.033 | 29 → 11 | 29 → 14 |
| 256 contiguous keys | 3.373 → 2.621 | 3.473 → 2.683 | 87 → 11 | 87 → 21 |
| 256 scattered keys | 9.384 → 5.684 | 9.606 → 6.420 | 418 → 11 | 418 → 341 |
| Range page, 256 | 2.666 → 1.982 | 2.891 → 2.185 | 87 → 11 | 87 → 88 |
| One distinct write | 154.964 → 156.692 | 164.912 → 166.533 | 161 → 159 | 110 → 110 |
| 16 distinct writes | 167.069 → 162.866 | 289.891 → 284.713 | 195 → 159 | 144 → 114 |
| 64 distinct writes | 183.837 → 158.188 | 202.787 → 171.997 | 306 → 159 | 255 → 130 |
| 256 distinct writes | 233.586 → 161.862 | 248.279 → 179.266 | 748 → 159 | 697 → 192 |

The 256-key contiguous median decreases by 22.3 percent; the scattered median decreases by 39.4 percent.
The 256-write median decreases by 30.7 percent.
Median rates are 75902 → 97665, 27282 → 45036, and 1096 → 1582 supplied operations per second.
Contiguous reads fetch 286041 → 286041 payload bytes; scattered reads fetch 1314582 → 1314582.
One-key reads add one 4096-byte format read and about 0.050 ms to the median.
One-write median and p95 also increase slightly. The appendix lists every measured increase.

## Other body sizes

Each case uses 11 rounds. Bodies contain distinct SHA-256 digest bytes, without repeated text.

| Body size and 256-operation case | Median ms | p95 ms |
|---|---:|---:|
| 128 bytes, 256 reads | 2.148 → 1.957 | 2.182 → 2.031 |
| 128 bytes, 256 writes | 231.616 → 156.744 | 237.389 → 162.247 |
| 16 KiB, 256 reads | 24.924 → 14.528 | 25.926 → 15.105 |
| 16 KiB, 256 writes | 302.048 → 237.792 | 312.588 → 254.087 |

The 16 KiB read fetches 4297104 payload bytes in both programs.
Nominal body size does not establish device bytes.
All returned bytes, absent slots, range order, and complete operation counts match the generated input.
Normal reopen checks all known keys after each physical write case.

## Directory and locator pages, and memory

For 256 distinct writes, new 16384-byte directory and locator pages decrease from 513 → 8 per complete batch.
This count excludes journals, generation receipts, and publication markers.
After 31 such batches, logical file length is 278042553→21551034 bytes.
Allocated size is 278167552→21676032 bytes, before maintenance.
Retained file size does not measure all device writes.

For contiguous 256-key reads, temporary-buffer peaks are 447486 → 474135 bytes.
Median whole-process maximum memory is 5029888 → 5308416 bytes.
For scattered reads, those figures are 2687961 → 2690838 and 7372800 → 7733248 bytes.
Product counters measure temporary buffers; macOS process statistics measure the complete process.
Neither measurement proves the 10000-logical-record warm-set limit.

## Node reads

Each steady case uses 21 repeated queries after a prime. Cells show median / p95 in milliseconds.
An exact 256 call is one query with 256 explicit keys, not 256 queries.

| Query | Earlier native | Final off | Final write mode |
|---|---:|---:|---:|
| exact 1 | 1.681 / 1.880 | 0.257 / 0.334 | 0.273 / 0.392 |
| exact 16 | 3.638 / 3.838 | 0.823 / 0.867 | 0.834 / 0.904 |
| exact 64 | 11.143 / 11.622 | 2.979 / 3.094 | 3.133 / 3.297 |
| exact 256 | 39.422 / 41.466 | 11.604 / 11.942 | 11.591 / 12.425 |
| range 256 | 52.531 / 54.085 | 72.510 / 73.247 | 66.584 / 69.302 |
| absent 1 | 1.598 / 1.636 | 2.570 / 2.768 | 2.573 / 2.740 |

Every repeated-query response matches its prime, including response metadata.
Final positive exact cases use zero opens, read calls, metadata work, directory work, and file bytes.
This result covers the same complete positive query after its prime.
New key combinations, ranges, and absence use the normal fetch path.
Post-write and reopen reads match current generated values; their prior response digest can change.

Range 256 is a final regression: the off-mode median rises from 52.531 to 72.510 ms.
It uses 34 opens and 1087 read calls. The final range path still reads disk.
The first exact-one request follows a fresh process start and precedes the complete 320-key verification.
Its times are 4.280 ms earlier, 4.727 ms final off, and 4.700 ms final write mode, with one sample each.
Later cases share a process; their first request has an unknown warm state.

## Durable node writes

All calls use normal async convergence and request a durable guarantee.
Node bodies contain 1024 ASCII SHA-256 hexadecimal bytes, which can compress.
Times use seconds. No p95 applies to these small sample counts.

| Writes | Samples | Earlier native | Final off | Final write mode |
|---|---:|---:|---:|---:|
| 1 | 3 | 0.617 | 0.522 | 0.496 |
| 16 | 3 | 4.519 | 2.003 | 1.885 |
| 64 | 1 | 13.508 | 7.598 | 5.863 |
| 256 | 1 | refused | 15.826 | 16.445 |

The final off-mode 64 receipt reports 23.449 ms preparation and 7123.908 ms ack time.
The 256 receipt reports 79.177 ms preparation and 14321.279 ms ack time.
Their native frontier changes by three; that count includes clock, data, and change-feed work.
Resident-stage `publish: 0` does not mean zero native publication time.
HTTP write file-open and file-barrier counts are unavailable.

The separate debug profile commits 5382 unique internal operations for its first 256 update.
It uses feature 0x20 and the fixed 16384-operation limit; its timings are excluded above.
All 320 values match before stop and after normal reopen.
The [numeric stages](node-final-large-diagnostic-stages.txt) retain this proof.

## Queued ack and explicit flush

These final write-mode calls request memory acceptance, then call the explicit product flush route.
The queued ack does not promise a disk copy.

| Writes | Samples | Ack median ms | Flush median seconds | Request start→durable seconds |
|---|---:|---:|---:|---:|
| 1 | 3 | 150.738 | 0.432 | 0.591 |
| 16 | 3 | 192.792 | 1.668 | 1.842 |
| 64 | 1 | 233.426 | 5.477 | 5.711 |
| 256 | 1 | 398.660 | 17.868 | 18.270 |

All explicit flush replies report durable completion. Normal reopen returns all 320 current values.
Immediate reads after a queued update return the new value; the repeat and normal reopen agree.
This proof does not simulate power loss.

## Normal flush policy

These samples use read-only status polls at 20 ms intervals, without a forced flush.
Every sample observes accepted work after the ack, then a healthy completed frontier.
The intervals bound observed durable completion after the ack; they are not timer precision guarantees.

| Writes | Sample | Ack ms | Completion after ack, seconds |
|---|---:|---:|---:|
| 1 | 1 | 129.311 | 0.219–0.250 |
| 1 | 2 | 264.302 | 0.233–0.263 |
| 1 | 3 | 273.237 | 0.258–0.280 |
| 256 | 1 | 530.832 | 12.635–12.666 |
| 256 | 2 | 2664.310 | 11.892–11.923 |
| 256 | 3 | 2658.680 | 12.892–12.922 |

The policy permits up to 500 ms idle delay. Later writes can wake the writer earlier.
File output adds its actual time. The policy does not promise durable completion within 500 ms.
The later 256-write ack samples also need about 2.66 seconds under normal background work.
The earlier 399 ms queued-ack sample does not describe every workload state.

## Separate 8 GiB cap profile

This fresh write-mode home uses `LASTDB_DISK_CAP_BYTES=8589934592`.
The dataset and mutation order match; this writes-only process first verifies all 320 keys.
The uncapped full profile reopens between its read and write phases.
That warmup difference and the small sample counts prevent an isolated cap-cost claim.
The cap uses a bounded file inventory; this run does not establish cost near capacity.

| Writes | Samples | Durable median seconds | Queued ack median ms |
|---|---:|---:|---:|
| 1 | 3 | 0.725 | 238.699 |
| 16 | 3 | 2.545 | 280.649 |
| 64 | 1 | 7.270 | 349.122 |
| 256 | 1 | 20.817 | 560.750 |

All cap-profile operations and value checks pass; all final candidate node stops exit 0.
Whole-process maximum memory across each profile is below. These figures are not per-call temporary memory.

| Profile | Maximum process memory, bytes |
|---|---:|
| f2 | 245465088 |
| fw | 285687808 |
| fc | 232538112 |

## Ancillary maintenance

Maintenance uses the same final node dataset that accepts the expanded 256-write batch.
It runs after quiet timings; its 40.801-second compaction is a separate operation wall time.
All 11339 internal records retain digest `ea70fe8f364e56cc174476e5c77c3452a8a503c9110f4fe3f73310405143ed92`.

| State | Files | Logical file length, bytes | Allocated bytes |
|---|---:|---:|---:|
| Before | 5136 | 192786872 | 193040384 |
| After compact, with backup hold | 7616 | 283105487 | 284049408 |
| Reclaim with hold | 7617 | 283109583 | 284053504 |
| Release hold, then reclaim | 2772 | 115459898 | 116154368 |

The final home contains 2 packed payload files, with logical length 4019991 bytes.
It also contains 2662 `locator` files, with logical length 109920256 bytes.
The locator files record the positions of records in the packed files.
Those files dominate size and file count. The complete home contains 2772 files, not two.
The backup hold retains old files until release; this is local retention, not a cloud upload.
The [maintenance receipt](maintenance-summary.json) contains the exact commands and results.
This ancillary operation follows the fresh generated-data benchmarks.
Earlier format and recovery checks remain in the history; no further existing-home load follows the user's later instruction.

## Final physical regressions

Each case below has a higher final median, p95, or both.
Small differences can reflect host variation. All raw calls remain available.

| Body size | Operation, shape, batch | Median ms | p95 ms |
|---|---|---:|---:|
| 128 bytes | read contiguous 1 | 0.440 → 0.507 | 0.630 → 0.700 |
| 128 bytes | read contiguous 16 | 0.497 → 0.535 | 0.522 → 0.582 |
| 128 bytes | read scattered 1 | 0.389 → 0.434 | 0.401 → 0.462 |
| 128 bytes | read duplicates 1 | 0.388 → 0.437 | 0.404 → 0.456 |
| 128 bytes | read duplicates 16 | 0.518 → 0.554 | 0.603 → 0.622 |
| 128 bytes | read duplicates 64 | 0.746 → 0.781 | 0.769 → 0.807 |
| 128 bytes | range bounded 1 | 0.444 → 0.485 | 0.646 → 0.675 |
| 128 bytes | range bounded 16 | 0.456 → 0.496 | 0.474 → 0.520 |
| 128 bytes | range bounded 64 | 0.654 → 0.634 | 0.670 → 0.789 |
| 128 bytes | write distinct 1 | 159.232 → 161.938 | 169.246 → 181.625 |
| 128 bytes | write repeated 16 | 166.139 → 167.116 | 178.506 → 229.271 |
| 1 KiB | read contiguous 1 | 0.395 → 0.445 | 0.513 → 0.556 |
| 1 KiB | read contiguous 16 | 0.573 → 0.574 | 0.641 → 0.609 |
| 1 KiB | read scattered 1 | 0.390 → 0.441 | 0.418 → 0.469 |
| 1 KiB | read duplicates 1 | 0.421 → 0.463 | 0.492 → 0.548 |
| 1 KiB | read duplicates 16 | 0.574 → 0.617 | 0.608 → 0.666 |
| 1 KiB | read duplicates 64 | 0.888 → 0.926 | 1.023 → 1.069 |
| 1 KiB | range bounded 1 | 0.399 → 0.446 | 0.458 → 0.534 |
| 1 KiB | range bounded 16 | 0.530 → 0.544 | 0.572 → 0.560 |
| 1 KiB | write distinct 1 | 154.964 → 156.692 | 164.912 → 166.533 |
| 16 KiB | read contiguous 1 | 0.520 → 0.561 | 0.591 → 0.756 |
| 16 KiB | read scattered 1 | 0.504 → 0.511 | 0.567 → 0.593 |
| 16 KiB | read duplicates 1 | 0.485 → 0.499 | 0.576 → 0.573 |
| 16 KiB | read duplicates 256 | 9.673 → 9.400 | 9.777 → 10.324 |
| 16 KiB | range bounded 1 | 0.493 → 0.512 | 0.618 → 0.608 |
| 16 KiB | write distinct 1 | 157.594 → 155.491 | 164.189 → 170.626 |
| 16 KiB | write repeated 1 | 154.378 → 156.018 | 166.172 → 166.304 |

## Receipts and history

| Scope | Inputs | Summary | Raw calls |
|---|---|---|---|
| 128 bytes | [Inputs](physical-128-inputs.json) | [Summary](physical-128-summary.json) | [Calls](physical-128-receipts.jsonl) |
| 1 KiB | [Inputs](physical-1k-inputs.json) | [Summary](physical-1k-summary.json) | [Calls](physical-1k-receipts.jsonl) |
| 16 KiB | [Inputs](physical-16k-inputs.json) | [Summary](physical-16k-summary.json) | [Calls](physical-16k-receipts.jsonl) |
| Final off | [Inputs](node-final-off-inputs.json) | [Summary](node-final-off-summary.json) | [Calls](node-final-off-receipts.jsonl) |
| Final write | [Inputs](node-final-write-inputs.json) | [Summary](node-final-write-summary.json) | [Calls](node-final-write-receipts.jsonl) |
| Final cap | [Inputs](node-final-cap-inputs.json) | [Summary](node-final-cap-summary.json) | [Calls](node-final-cap-receipts.jsonl) |

The [history](benchmark-history.md) separates prior binaries, actual intermediate refusals, excluded driver attempts, and the old failed 256 workload.
The earlier 101-round small-write follow-up belongs to the archived 08409 binary.
Final 31-round batch-16 p95 is 289.891→284.713 ms, so no new focused repeat follows.
No result establishes a universal small-write tail guarantee.

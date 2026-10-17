# Layer 2 before and after measurements

Date: 16 October 2026, local time. The raw timestamps use UTC.

The physical batch changes reduce large-batch time, file opens, and map output.
One-key reads become slightly slower. The 16-write p95 remains a measured regression.
Node, focused-tail, cap, and maintenance results remain pending in this report.

## Programs and scope

The baseline uses `ca287339` plus compile repair `9cbe67f5`.
The candidate uses `d1fae6469`.
The [provenance record](benchmark-provenance.json) contains exact binary hashes and workload notes.

The real `lastdb-disk-home` commands build, read, range, write, and reopen each generated home.
Each program receives its own new home from the same generated input.
Both physical homes use fanout 1.
The build receipts retain the actual feature mask and codec numbers.
The candidate includes its new file-lifetime protection.

These results cover the physical engine, not the complete node API or a memory ack.
Each time uses the command's internal clock before its final output print.
The operating system cache state remains unknown.
A fresh process does not establish a physical cold-disk read.
Read bytes and calls measure userspace file work, not device traffic.

## Generated workload

Each physical home starts with 4096 explicit keys under `generated`.
Keys use a NUL separator followed by `record-N`, with six decimal digits.
The collection is `Layer2Receipt`.
No primary data, primary copy, archive, shared service, or cloud participates.

Initial bodies contain distinct SHA-256 byte chunks for each record and chunk.
Update bodies contain distinct chunks for each repeat, slot, and chunk.
The generator uses no repeated text for these physical bodies.
Every 997th initial body is empty. Every 251st key is a tombstone.
Nominal body sizes are 128 bytes, 1024 bytes, and 16384 bytes.

The read shapes are contiguous keys, scattered keys, duplicate keys with absence, and one bounded range page.
The write shapes address distinct keys or repeat one key inside a complete batch.
Batches contain 1, 16, 64, or 256 supplied operations.
The program preserves every ordered operation even when the final value uses one map entry.
Returned bytes, absent slots, range order, and complete operation counts match the generated input.

The 1 KiB matrix uses 31 alternating rounds for each program.
The 128-byte and 16 KiB matrices use 11 rounds.
Baseline and candidate order alternates each round.
The p95 uses the nearest rank.
These sample counts do not establish a production tail guarantee.

## Main physical results: 1 KiB bodies

Times use milliseconds. An arrow means baseline to candidate.

| Operation | Median ms | p95 ms | File opens | File read calls |
|---|---:|---:|---:|---:|
| One exact key | 0.466 → 0.513 | 0.542 → 0.578 | 11 → 11 | 11 → 12 |
| 16 contiguous keys | 0.663 → 0.665 | 0.773 → 0.711 | 15 → 11 | 15 → 12 |
| 64 contiguous keys | 1.326 → 1.152 | 1.383 → 1.251 | 29 → 11 | 29 → 14 |
| 256 contiguous keys | 3.849 → 2.986 | 3.981 → 3.137 | 87 → 11 | 87 → 21 |
| 256 scattered keys | 10.481 → 6.428 | 10.661 → 6.650 | 418 → 11 | 418 → 341 |
| Range page, 256 records | 3.095 → 2.325 | 3.222 → 2.506 | 87 → 11 | 87 → 88 |
| One distinct write | 159.871 → 158.227 | 251.328 → 255.685 | 161 → 159 | 110 → 110 |
| 16 distinct writes | 158.240 → 154.314 | 181.167 → 222.763 | 195 → 159 | 144 → 114 |
| 64 distinct writes | 170.502 → 150.654 | 193.167 → 195.175 | 306 → 159 | 255 → 130 |
| 256 distinct writes | 222.447 → 157.677 | 268.498 → 180.129 | 748 → 159 | 697 → 192 |

The 256-key contiguous median decreases by 22.4 percent.
The scattered median decreases by 38.7 percent.
The 256-write median decreases by 29.1 percent.
Their median rates change from 66504 to 85739, 24425 to 39824, and 1151 to 1624 records per second.

The one-key read median increases by 0.047 ms.
The final command includes one extra 4096-byte format read.
The one-write medians are similar; its p95 increases slightly.
The 16-write p95 increases from 181.167 to 222.763 ms.
A focused 101-round follow-up remains pending; this report does not assign a cause.

The contiguous 256-key call reads 286041 payload bytes in both programs.
The scattered call reads 1314582 payload bytes in both programs.
The gain therefore includes fewer opens and combined read calls, not fewer payload bytes.
The range path uses fewer opens while its file-read count increases by one.

## Other body sizes

These cases use 11 rounds per program.

| Body size and operation | Median ms | p95 ms |
|---|---:|---:|
| 128 bytes, 256 contiguous reads | 2.388 → 2.139 | 2.561 → 2.400 |
| 128 bytes, 256 distinct writes | 222.269 → 157.139 | 368.668 → 275.132 |
| 16 KiB, 256 contiguous reads | 26.132 → 15.367 | 26.952 → 15.889 |
| 16 KiB, 256 distinct writes | 303.489 → 236.864 | 326.680 → 264.687 |

The 16 KiB contiguous call reads 4297104 payload bytes in both programs.
Its file-open count decreases from 1045 to 11.
Do not interpret a nominal value size as physical-device traffic.

## Map output, disk size, and memory

For 256 distinct 1 KiB writes, new map pages decrease from 513 to 8 per batch.
Each map page contains 16384 bytes.
The count excludes journal payloads, generation receipts, and publication markers.

Both homes start with 5919189 logical file bytes.
After 31 complete 256-write batches, the baseline uses 278042556 bytes and the candidate uses 21551036 bytes.
Their allocated file sizes are 278167552 and 21676032 bytes.
These figures precede compact or reclaim.
File size and new map pages do not measure all bytes written to the device.

The contiguous 256-key temporary-buffer peak changes from 447486 to 474135 bytes.
The whole command's maximum process memory changes from 5046272 to 5373952 bytes.
The scattered temporary peak changes from 2687961 to 2690838 bytes.
Its maximum process memory changes from 7520256 to 7749632 bytes.
Process figures come from macOS `/usr/bin/time -l`; transient figures use the product receipt.
These measures have different scopes and do not establish the node's warm-set cap.

## Refusals and incomplete preparations

The first 16 KiB attempt requests all 4096 verification keys through one default-budget read.
That call returns explicit `read_resource_limit` results.
The timed calls through 256 keys succeed.
The [retained exception](large-verification-refusal.txt) records the refusal; the full unsuccessful response was not saved.
The final 16 KiB matrix verifies the known keys in batches of 1024.
The incomplete attempt contributes no main timing result.

The old node rejects an empty CLI-created native home because it requires its older node-marker path.
No marker or descriptor is fabricated to bypass that refusal.
Its normal fresh node uses fanout 16; the final node uses fanout 1.
Future node comparisons must state this geometry change separately from the physical 1-to-1 comparison.

An interrupted 1024-record old-node preparation exits 1 after its background-drain timeout.
A normal 320-record preparation also exits 1, with two tasks still pending after 30 seconds.
Neither attempt supplies a clean reopen proof or a node latency sample here.
One synchronous-convergence setup attempt remains active but paused.
The root agent temporarily releases the quiet interval for separate operational work.

## Raw receipts

The compact JSON retains each complete physical command receipt and its result digest.

| Size | Inputs | Summary | Raw calls |
|---|---|---|---|
| 128 bytes | [Inputs](physical-128-inputs.json) | [Summary](physical-128-summary.json) | [Calls](physical-128-receipts.jsonl) |
| 1 KiB | [Inputs](physical-1k-inputs.json) | [Summary](physical-1k-summary.json) | [Calls](physical-1k-receipts.jsonl) |
| 16 KiB | [Inputs](physical-16k-inputs.json) | [Summary](physical-16k-summary.json) | [Calls](physical-16k-receipts.jsonl) |

The pending node results must separate the memory ack, explicit durable barrier, and normal automatic flush.
A 500 ms idle delay does not promise durable completion within 500 ms.
Page output and file barriers add their actual time.

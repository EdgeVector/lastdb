# Layer 2 results: shared files, complete write batches, and warm exact reads

The three changes are complete in local source `5b9183550151b3887df73be1b044aa121c101f7a`.
The before program uses the previous native Layer 2 engine at `7110c7f54ea005e33678b5c93e89353d6117b41f`.
Both programs use native LastStore storage and independent empty homes.
Each receives the same synthetic writes and reads.
The final user values match between programs.

The largest gains appear in durable writes, concurrent requests, and exact reads after unrelated writes.
All 485 final query responses match the generated values.
All six owned node stops exit normally with code zero.
No HTTP request reports a refusal.

## Main node results

The dataset has 320 records with 1 KiB generated bodies under one hash.
The full recipe defines the keys, field descriptions, values, request order, and reply boundaries.
Repeated read, write, and cohort rounds alternate the program order.

| Case | Before median | After median | Samples per program |
|---|---:|---:|---:|
| Durable write, 1 records | 475.113 ms | 264.368 ms | 5 |
| Durable write, 16 records | 1805.849 ms | 312.292 ms | 5 |
| Durable write, 64 records | 4853.170 ms | 466.312 ms | 5 |
| Durable write, 256 records | 12031.243 ms | 1142.324 ms | 5 |
| Four concurrent requests of 16 records, all replies | 5344.103 ms | 259.811 ms | 5 |
| Same four requests, all data on disk | 6949.050 ms | 595.832 ms | 5 |
| Exact 64-key query after an unrelated write | 42.931 ms | 6.299 ms | 5 |

The concurrent case reaches disk through an explicit flush after all four replies.
Each cohort has four product requests.
After all four replies, five complete batches remain accepted; internal work also uses that count.
The median native frontier change falls from ten to two per cohort.
Each new flush receipt covers its captured accepted sequence.
These results include the shared durable author-clock reservation and native atom reuse.
They do not isolate the write queue as the sole cause.

After unrelated writes, the new 64-key query reports zero file opens, file reads, and metadata operations in all five rounds.
The before medians are 36 file opens and 441 file reads per query.
The values and complete query flags remain correct.

## Repeated reads

Each case has 21 measured calls after its prime.
All repeated exact-key cases use zero file work in both programs.
Range reads still use files.

| Case | Before median | After median | Samples per program |
|---|---:|---:|---:|
| exact, 1 keys | 0.246 ms | 0.233 ms | 21 |
| exact, 16 keys | 0.793 ms | 0.660 ms | 21 |
| exact, 64 keys | 2.881 ms | 2.257 ms | 21 |
| exact, 256 keys | 11.395 ms | 9.066 ms | 21 |
| range, 1 keys | 3.675 ms | 4.906 ms | 21 |
| range, 16 keys | 7.150 ms | 8.712 ms | 21 |
| range, 64 keys | 19.870 ms | 21.560 ms | 21 |
| range, 256 keys | 70.509 ms | 74.776 ms | 21 |

The steady range medians increase by 33.5%, 21.9%, 8.5%, and 6.1% for those four sizes.
The new range cases use fewer file opens; the measurements do not identify the full cause of the latency increase.
The complete node report lists every matched latency increase and its exact times.
It also separates one-call observations and replies with different guarantees.

## Shared file results

The physical command comparison uses the same vendor storage tree as the final node program.
Its after source is `a1d03310`; the vendor tree stays unchanged through `5b918355`.
The many-map dataset has 256 maps and 16 keys per map.
A 256-write request changes one key in each map.

| Many-map case | Before | After | Samples per program |
|---|---:|---:|---:|
| 256-write median | 1835.126 ms | 221.883 ms | 11 |
| 256-write p95 | 2050.177 ms | 247.008 ms | 11 |
| New locator/directory file parts per write | 257 | 2 | 11 |
| New locator/directory pages per write | 262 | 262 | 11 |
| Scattered 256-key read median | 13.712 ms | 11.179 ms | 11 |
| Compaction | 4788.887 ms | 1062.681 ms | 1 |
| Reclaim after backup release | 18071.448 ms | 474.562 ms | 1 |
| Locator files after compaction and reclaim | 256 | 1 | 1 |
| Total files after compaction and reclaim | 291 | 36 | 1 |
| Logical file bytes after compaction and reclaim | 14,259,035 | 10,064,731 | 1 |

Each map keeps its own root and leaf pages inside a shared file.
The change reduces files and empty pages; it does not create a global map tree.
The eight physical maintenance runs retain identical 4096-record digests through compaction and reclaim.
A backup reference protects the earlier files until release.

Single-map results stay close in many cases.
The 16 KiB repeated 256-write median increases from 253.539 to 269.446 ms.
The 128-byte repeated 256-write p95 increases from 225.309 to 308.196 ms.
The physical report includes all 46 cases with a higher median, p95, or both.

## Final node compaction and file size

Both stopped nodes use their own complete generated workload from the final request run.
Each maintenance run protects the old generation with a backup reference, compacts, reclaims, releases the reference, and reclaims again.
Each home retains its own complete ordered record digest.
These are single matched command observations, without a p95.

| Case | Before | After |
|---|---:|---:|
| Compaction | 82665.667 ms | 12627.970 ms |
| Reclaim with backup hold | 2485.774 ms | 192.520 ms |
| Reclaim after release | 211826.860 ms | 16410.830 ms |
| Total files after release | 4,415 | 207 |
| Logical file bytes after release | 187,055,993 | 118,544,176 |
| Allocated file bytes after release | 187,064,320 | 119,799,808 |
| Locator files after release | 4,132 | 12 |
| Locator logical bytes after release | 165,593,088 | 97,763,328 |

These file sizes include native metadata and retained data for the same user writes.
They exclude the node home outside its native storage directory.
The app-storage estimates below remain separate and incomplete.

## Delete and automatic flush boundaries

Both programs receive the same durable Delete, queued Delete, and recreate writes.
The new native Delete reply waits for disk and reports `durability: "durable"`.
Its immediate read shows the deletion.
For queued Delete, the earlier program returns a queued reply; its final comparison read waits for flush.
The earlier incomplete run records its stale read after a queued Delete reply.

The new queued Delete reply takes 372.732 ms in one call.
The earlier queued reply takes 24.900 ms in one call, with a weaker guarantee.
Those reply times are separate observations.
The matched durable Delete calls take 1708.405 and 371.181 ms.

Three normal-flush observations use no forced flush during the measurement.
Their disk-after-reply upper bounds are 1308.445, 1346.966, and 1355.417 ms before the change.
They are 560.585, 434.323, and 763.199 ms after the change.
These values are status-poll observations. They are not exact commit timestamps.
Those three samples raise the acknowledgement median from 203.852 to 308.003 ms.
The five exact64 mixed-prime samples also rise from 4.159 to 5.782 ms.

## Validation and limits

The release build, package Clippy, format checks, and source guards pass.
No tests run, under the retired-test instruction.
Each final node restarts its own generated home and returns all 320 expected records.
The recorded program hashes, recipes, inputs, and receipts identify the measured source.

The app-storage report marks aggregate size estimates incomplete in both programs.
The new cause is `reused_atom_live_transition_requires_repair` after atom reuse.
Per-molecule contribution evidence and authoritative disk-cap accounting keep their existing paths.
The reported aggregate byte estimates cannot establish a storage reduction.
File-count and file-byte results above come from the actual bounded file inventory.

The operating-system cache state is unknown.
A fresh process or node restart does not establish a cold-device read.
File-work counters measure program calls, not device traffic or write barriers.
The p95 uses nearest rank and requires at least five samples.
Small samples and ordinary host activity limit conclusions about tail latency.
Power-loss recovery, near-cap behavior, and cloud transfer time remain unmeasured.

The queue pauses new admission during physical output and ordered delivery.
Aggregate preparation costs grow with the bounded accepted prefix.
A reservation replacement can briefly require capacity for both old and new output.
Bounded warm history removal can cause a correct fallback to files.

## Receipts and source

- [Node results and every latency increase](node/node-report.md).
- [Node summaries](node/summary.json), [all requests](node/receipts.jsonl), and [completion](node/completed.json).
- [Physical report and exact receipts](physical/index.md).
- [Before node maintenance](node/maintenance-before/summary.json) and [after node maintenance](node/maintenance-after/summary.json).
- [Source implementation and practical limits](implementation.md).
- [Complete comparison recipe](benchmark-recipe.md).
- [Source and program provenance](provenance.json) and [non-test checks](checks.json).
- [Final node request recipe](drivers/node_compare.py).
- [Failed intermediate run](intermediate-a1d03310/status.json) and [first diagnostic](diagnostic-eed8b2cc/status.json).

Earlier incomplete and debug measurements remain separate from the final comparison.

# Repeated durable snapshot write measurements

This round repeats the previous workload without a product change.
The source remains `a590e2abab0db6a3916fb7de3f9fc155677ac727`, with journal-free main `4118c7baa` below it.
The daemon SHA-256 remains `0d62917132e752dd12df47f126ef6f9d04d755b84e4da73899b0b9d5b11cec7a`.
The prior [snapshot report](../snapshot-durable-rebase-20261022/RESULTS.md) records the first measurements.
Nothing in this round changes or merges product code.

## Results

All six captures completed with exit code zero.
Every measured write received a durable ack.
The journal stays removed.
The repeated single-write average exceeds the 20 ms target.

| Workload | Requests | Average | Median | P95 | P99 | Maximum |
|---|---:|---:|---:|---:|---:|---:|
| One record, one client, 64 schemas | 3,000 | 64.09 ms | 57.24 ms | 114.42 ms | 142.77 ms | 301.48 ms |
| Eight-record batch, four clients, with compaction | 768 | 281.97 ms | 202.76 ms | 779.75 ms | 1,145.71 ms | 1,708.58 ms |

These values pool the three repeats for each workload.
The batch values describe one complete eight-record request.
They do not describe one record within the batch.
The [summary](summary.json) retains the exact values and the per-run status.

| Run | Average | P99 | Write errors | Final records after forced stop |
|---|---:|---:|---:|---:|
| single-1 | 66.00 ms | 130.24 ms | 0 | 65 |
| single-2 | 68.14 ms | 143.28 ms | 0 | 65 |
| single-3 | 58.13 ms | 150.92 ms | 0 | 65 |
| compaction-1 | 280.29 ms | 1,181.68 ms | 0 | 513 |
| compaction-2 | 293.74 ms | 1,113.66 ms | 0 | 513 |
| compaction-3 | 271.88 ms | 1,145.71 ms | 0 | 513 |

The single-write run averages range from 58.13 to 68.14 ms.
Their P99 values range from 130.24 to 150.92 ms.
The batch run averages range from 271.88 to 293.74 ms.
Their P99 values range from 1,113.66 to 1,181.68 ms.

### Previous capture

| Workload | Previous average | Repeat average | Previous P99 | Repeat P99 |
|---|---:|---:|---:|---:|
| One record | 53.30 ms | 64.09 ms | 120.39 ms | 142.77 ms |
| Eight-record batch with compaction | 157.24 ms | 281.97 ms | 691.18 ms | 1,145.71 ms |

This round records higher latency on the same source and the same binary.
The measurements do not isolate the cause of the difference.
Host work and OS cache state vary between rounds.
These results do not establish a code regression.

### Compaction and recovery

All three explicit compaction calls returned a retryable HTTP 503 after eight attempts to capture the latest disk state.
Each call returned the structured `write_resource_limit` response with `retry_after_secs: 2`.
The retry after foreground writes stopped returned HTTP 200 in all three runs.
The original calls lasted 11.33, 8.00, and 6.29 seconds.
During those request intervals, 163, 117, and 118 batch requests completed.
These intervals include the full compaction request and do not isolate its file-copy phase.

Across the six captures, 3,768 measured requests contain 9,144 record mutations.
There are zero measured write errors.
All 1,734 final records match their expected values after forced process stops.
The next durable write advances the author clock in every run.
All six final shutdowns report a successful flush and exit code zero.
All six stopped homes contain no storage journal directory or journal tail.

The results support process-stop recovery for these generated records.
The results do not establish a power-loss, cloud, or primary result.
The branch needs further work to meet the 20 ms average target.
Nothing is pushed or merged.

## Method

The round uses six fresh generated homes and one daemon at a time.
Each home uses 64 distinct schemas with distinct field identities and 64-byte values.
The single-write workload sends 1,000 requests from one client.
The compaction workload sends 256 batches of eight records from four clients.
Concurrent clients use disjoint schema sets.
The run order is single/compaction, compaction/single, then single/compaction.

Every request omits the durability option and must receive a durable reply.
Each run verifies final values after SIGKILL, checks the next author clock, and checks the final shutdown flush.
The stopped homes must contain no storage journal directory or journal tail.
Every latency describes the complete batch request; none is divided by its record count.
The source calls remain the same as the previous round.

The host Cargo exclusion prevents cooperative compiler overlap throughout all six runs.
Operating-system caches and other host work remain uncontrolled.
The [plan](plan.json) records the binary, source, platform, load, and exact run order.
The [outcomes](outcomes.json) record each capture exit and its elapsed time.
The [driver](run.py) and the original [capture](../snapshot-durable-rebase-20261022/capture.py) retain the method.
The six `*.daemon.txt` files retain the generated-node logs.
The cleanup receipts record the owned paths and confirm removal of the temporary homes and binary.

## Interpretation

Pooled averages and percentiles describe all measured requests for each workload.
The ranges of run averages and run P99 values describe variation across the three repeats.
These are observed results, not confidence intervals or guarantees for another database size.
The primary and its archive stay unchanged.

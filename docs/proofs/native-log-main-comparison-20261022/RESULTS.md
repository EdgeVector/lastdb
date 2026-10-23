The durable candidate has lower average latency and P99 than the latest main in all three measured workloads.
Small writes average 169.65 ms on main and 17.16 ms on the candidate.
That is an 89.9% reduction in average latency and approximately 9.9 times the request rate.
The under-10 ms average target remains unmet.
The candidate stays isolated and unmerged. The primary and its archive stayed unchanged.

GitHub main is `8067843c4f9626470edc971ab10c10e829f021d8`.
The main reference stayed at that commit when the comparison ended.
The candidate is `756c4f09b1ad7e74acf49ca1ca749f8a01246ff2`, with product changes through `550836f1b`.
`builds.json` records the executable hashes and build settings.
Both optimized daemon builds passed with optimization level 3, 16 code units, and LTO off.
The candidate is not rebased onto main; this compares the two complete implementations, including other main changes.

| Workload | Main average ms | Candidate average ms | Main P99 ms | Candidate P99 ms | Average latency reduction |
|---|---:|---:|---:|---:|---:|
| One record, one client | 169.65 | 17.16 | 447.30 | 38.10 | 89.9% |
| Eight records, one client | 179.39 | 28.35 | 471.60 | 54.96 | 84.2% |
| Eight records, four clients | 310.64 | 112.16 | 854.52 | 331.19 | 63.9% |

Each number measures one complete request, including the full batch where present.
No batch latency is divided by the record count.
Each version receives 1,000 calls for each workload. Every sample and outlier stays in the result.
P99 uses the nearest rank, the 990th sample of 1,000 after the sort.
The results cover this Mac, these generated records, and these workloads. They do not establish a universal latency limit.

Main returned 3,000 `queued` receipts. The candidate returned 3,000 `durable` receipts.
The client omitted the durability parameter for both versions.
Thus, main keeps its default memory-ack policy, while the candidate supplies the disk-before-ack guarantee.
Main can still wait for disk under its existing resource rules; a `queued` receipt does not promise a data flush before the ack.

The source explains why a memory ack still has substantial request cost on main.
Main completes a separate author-clock metadata write and its disk flush before the logical data request.
Its periodic flush and compaction also use the global write gate.
The candidate stores the clock and data in one complete native write and keeps compaction file work outside that gate.
These source differences explain possible causes of the improvement; this comparison does not isolate each cause's contribution.

Each run uses a fresh generated home and 64 separate schemas with distinct field descriptions.
The metadata receipts retain the schema and molecule identities.
Each client rotates through its own schemas. The captured request intervals show no simultaneous calls to the same schema.
Requests create and update known keys with 64-byte values and three fields.
Each request uses the real batch API over the owner Unix socket.
The clock includes client JSON encode, connection, response, and JSON decode.
No initial measured request is excluded.

Both versions use eight request workers, the 4,096-file maintenance threshold, and the existing memory caps.
Both retain the 500 ms memory-flush policy setting. The candidate does not use that timer to authorize its ack.
The automatic maintenance start delay is zero on both, so background work can occur during each run.
No disk cap is set. Numeric debug traces are off on both versions.
All inherited LastDB settings are cleared before the explicit comparison settings enter the owned daemon.
The run order alternates versions across workloads. `plan.json` records that order before the measurements.
No owned compiler or lint runs during the measurements.

All 6,000 calls succeeded, for 34,000 record mutations in total.
Each run returned the correct final values by exact keys.
The candidate stopped with SIGKILL immediately after a final durable sentinel ack and recovered all expected values.
Its next write advanced the author clock in every run.
Main completed an explicit flush after the latency measurement, then stopped gracefully and recovered every final value.
That later flush is outside all latency samples and does not establish durability at main's original ack.
All final owned shutdowns returned exit 0 with a successful flush receipt.
These process-stop checks do not include a physical power interruption.

`comparison.json` contains the paired latency and throughput results.
`matrix.json` contains the complete summaries, receipt types, file counters, and recovery results.
Each run folder contains every client sample, native status, schema metadata, and value receipt.
Raw daemon logs and their hashes remain with the receipts.
The capture and analysis syntax checks passed. No test suite, fixture, mutation probe, or primary copy ran.

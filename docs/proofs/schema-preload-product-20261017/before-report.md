# Before engine: product measurements

Date: 2026-10-17. Source: `aa64f61cd3d2d01a3f8953d4a1b3cf21f9940bab`.
The isolated reference checkout is clean.
The executable SHA256 is `3f591bae95065fd31a507dcbab12aa6e40e30b24ff346a55fa5a2b0d126b13cb`.
`reference.json` records the build and source identity.

The measurements use generated data in fresh private homes.
Each record has four fields, a 54-byte range, and a 1 KiB body.
The read requests return the title and body.
The ordinary full-path cap is 10,000.
The operating-system file cache is uncontrolled.
The machine has no exclusive reservation.

## Complete 256-record cohort

`before-main-256/` contains the raw requests, results, costs, and exact driver sources.
All 54 read requests return the expected values and membership.
The range requests return the expected order.
The returned fields contain atom and molecule identities.
All five owned node processes stop normally with exit code zero.

| Request | Keys | Samples | Median ms | Observed p95 ms | Median disk read operations |
|---|---:|---:|---:|---:|---:|
| First exact after reopen | 16 | 1 | 17.912 | — | 214 |
| Repeated exact | 16 | 5 | 0.955 | 1.082 | 0 |
| Repeated exact | 64 | 5 | 2.860 | 2.893 | 0 |
| First range after reopen | 16 | 1 | 12.951 | — | 144 |
| Repeated range | 16 | 5 | 10.983 | 11.353 | 127 |
| Repeated range | 64 | 5 | 31.971 | 32.252 | 307 |
| Concurrent first range | 16 per call | 4 | 16.690 | — | 141.5 |

The capped preview returns ten distinct generated keys and correct values.
The update, insert, delete, key move, explicit flush, and normal reopen requests pass.
The update and insert read observations find pending writes before and after the read.
The delete read observation does not establish that condition.
The key move observation also finds pending writes; its batch request asks for durable writes.
These observations describe the native status, not a per-mutation flush boundary.

| Mutation | Records | Requested ack | Actual ack | Elapsed ms |
|---|---:|---|---|---:|
| Update | 4 | queued | queued | 179.934 |
| Insert | 4 | queued | queued | 12.463 |
| Delete | 2 | durable | durable | 264.003 |
| Key move | 2 mutations | durable | durable | 266.484 |

These are single mutation observations, not repeated-write latency estimates.
The final native status reports equal accepted and completed sequences, both 14.
The highest sampled process memory is 177,995,776 bytes.
This is not the complete peak-memory measurement.

## 10,000-record cohort

`before-main-10000/` contains 42 correct exact and range read requests.
The later capped preview request receives HTTP 503 with `read_resource_limit`.
The message is `read resource limit exceeded: disk-pack v1: read resource limit exceeded`.
The preview is incomplete; this cohort is not a complete workload success.
All four owned node processes stop normally with exit code zero.

| Request | Keys | Samples | Median ms | Observed p95 ms | Median disk read operations |
|---|---:|---:|---:|---:|---:|
| First exact after reopen | 16 | 1 | 23.085 | — | 304 |
| Repeated exact | 16 | 5 | 0.995 | 1.008 | 0 |
| Repeated exact | 64 | 5 | 2.657 | 2.755 | 0 |
| Repeated exact | 256 | 5 | 9.958 | 10.326 | 0 |
| First range after reopen | 16 | 1 | 13.796 | — | 179 |
| Repeated range | 16 | 5 | 10.540 | 10.884 | 135 |
| Repeated range | 64 | 5 | 31.147 | 32.465 | 330 |
| Repeated range | 256 | 5 | 120.321 | 125.114 | 1,171 |
| Concurrent first range | 16 per call | 4 | 16.717 | — | 183.5 |

The highest sampled process memory is 819,658,752 bytes.
This is not the complete peak-memory measurement.
The reference engine does not expose the new core or schema-key counters.

## 20,000-record cohort

`before-main-20000/` contains 42 correct exact and range read requests.
This cohort contains more logical keys than the ordinary full-path cap.
The later capped preview request receives the same HTTP 503 `read_resource_limit`.
The preview is incomplete; this cohort is not a complete workload success.
All four owned node processes stop normally with exit code zero.

| Request | Keys | Samples | Median ms | Observed p95 ms | Median disk read operations |
|---|---:|---:|---:|---:|---:|
| First exact after reopen | 16 | 1 | 39.666 | — | 331 |
| Repeated exact | 16 | 5 | 0.935 | 0.985 | 0 |
| Repeated exact | 64 | 5 | 2.692 | 2.799 | 0 |
| Repeated exact | 256 | 5 | 10.137 | 10.178 | 0 |
| First range after reopen | 16 | 1 | 16.224 | — | 198 |
| Repeated range | 16 | 5 | 10.942 | 11.167 | 147 |
| Repeated range | 64 | 5 | 32.247 | 32.364 | 332 |
| Repeated range | 256 | 5 | 119.908 | 120.880 | 1,173 |
| Concurrent first range | 16 per call | 4 | 19.728 | — | 196 |

The highest sampled process memory is 1,442,021,376 bytes.
This is not the complete peak-memory measurement.

## Separate low-cap refusal

`before-256/` preserves the first 128-path cohort and its original driver.
Its first and repeated exact and range reads return correct generated values.
Four concurrent 16-key calls then receive the active-call capacity refusal.
The lowered cap cannot admit that aggregate request.
This result is separate from the ordinary 10,000-path-cap measurements.

## Limits

Five repeated requests supply the median and the observed p95.
With five samples, the observed p95 is the maximum sample.
These observations do not establish a latency guarantee.
Reopen clears process memory but does not clear the operating-system file cache.
Disk read operation counts are product receipt counters, not physical device commands.
The after engine must use fresh generated homes and the same request shapes.

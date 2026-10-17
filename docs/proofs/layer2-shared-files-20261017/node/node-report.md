# Final node request comparison

Source: `5b9183550151b3887df73be1b044aa121c101f7a`.

The driver uses fresh generated data through real HTTP requests over owned local sockets.
The values are elapsed wall time. Each median is descriptive. No significance check is part of this report.
A case with one sample is a single call. Cases with fewer than five samples have no p95 value.
Normal flush cases use three observed upper bounds. Their min and max values stay in the JSON.
The file cache state is unknown. A completion file confirms that the driver reached its final step.

## All matched latency cases

| Case | Before ms | After ms | Change ms | Change % | Samples before/after |
|---|---:|---:|---:|---:|---:|
| concurrent-queued / 4 requests / 16 / all_acks | 5344.103 | 259.811 | -5084.291 | -95.138 | 5/5 |
| concurrent-queued / 4 requests / 16 / all_durable | 6949.050 | 595.832 | -6353.218 | -91.426 | 5/5 |
| concurrent-queued / 4 requests / 16 / client_collection | 5344.358 | 260.006 | -5084.352 | -95.135 | 5/5 |
| concurrent-queued / 4 requests / 16 / explicit_flush | 1635.706 | 343.937 | -1291.769 | -78.973 | 5/5 |
| concurrent-queued / 4 requests / 16 / individual_ack | 2946.577 | 164.870 | -2781.707 | -94.405 | 20/20 |
| delete-request / 16 / ack | 1708.405 | 371.181 | -1337.224 | -78.273 | 1/1 |
| mutation / durable / 1 / ack | 475.113 | 264.368 | -210.745 | -44.357 | 5/5 |
| mutation / durable / 1 / explicit_flush | 132.362 | 0.338 | -132.023 | -99.745 | 5/5 |
| mutation / durable / 1 / request_to_explicit_durable | 627.674 | 265.180 | -362.494 | -57.752 | 5/5 |
| mutation / durable / 16 / ack | 1805.849 | 312.292 | -1493.557 | -82.707 | 5/5 |
| mutation / durable / 16 / explicit_flush | 308.527 | 0.156 | -308.371 | -99.949 | 5/5 |
| mutation / durable / 16 / request_to_explicit_durable | 2086.370 | 312.857 | -1773.513 | -85.005 | 5/5 |
| mutation / durable / 64 / ack | 4853.170 | 466.312 | -4386.859 | -90.392 | 5/5 |
| mutation / durable / 64 / explicit_flush | 786.210 | 122.204 | -664.006 | -84.457 | 5/5 |
| mutation / durable / 64 / request_to_explicit_durable | 5697.500 | 597.423 | -5100.076 | -89.514 | 5/5 |
| mutation / durable / 256 / ack | 12031.243 | 1142.324 | -10888.919 | -90.505 | 5/5 |
| mutation / durable / 256 / explicit_flush | 2108.815 | 185.260 | -1923.556 | -91.215 | 5/5 |
| mutation / durable / 256 / request_to_explicit_durable | 14338.466 | 1334.035 | -13004.431 | -90.696 | 5/5 |
| mutation / queued / 1 / ack | 157.714 | 131.025 | -26.689 | -16.922 | 5/5 |
| mutation / queued / 1 / explicit_flush | 387.617 | 131.713 | -255.905 | -66.020 | 5/5 |
| mutation / queued / 1 / request_to_explicit_durable | 549.606 | 261.649 | -287.956 | -52.393 | 5/5 |
| mutation / queued / 16 / ack | 181.890 | 155.356 | -26.534 | -14.588 | 5/5 |
| mutation / queued / 16 / explicit_flush | 1475.919 | 201.558 | -1274.361 | -86.344 | 5/5 |
| mutation / queued / 16 / request_to_explicit_durable | 1661.873 | 357.376 | -1304.497 | -78.496 | 5/5 |
| mutation / queued / 64 / ack | 233.780 | 204.529 | -29.251 | -12.512 | 5/5 |
| mutation / queued / 64 / explicit_flush | 4246.646 | 354.423 | -3892.223 | -91.654 | 5/5 |
| mutation / queued / 64 / request_to_explicit_durable | 4467.060 | 536.877 | -3930.182 | -87.981 | 5/5 |
| mutation / queued / 256 / ack | 477.056 | 442.955 | -34.101 | -7.148 | 5/5 |
| mutation / queued / 256 / explicit_flush | 13946.659 | 730.090 | -13216.569 | -94.765 | 5/5 |
| mutation / queued / 256 / request_to_explicit_durable | 14422.353 | 1186.664 | -13235.689 | -91.772 | 5/5 |
| normal-flush / queued request / 16 / ack | 203.852 | 308.003 | 104.151 | 51.092 | 3/3 |
| normal-flush / queued request / 16 / disk_after_ack_upper_bound | 1346.966 | 560.585 | -786.381 | -58.382 | 3/3 |
| normal-flush / queued request / 16 / request_to_disk_upper_bound | 1550.817 | 868.588 | -682.230 | -43.992 | 3/3 |
| query / after-accepted-recreate / exact / 64 / response | 33.031 | 33.416 | 0.385 | 1.166 | 1/1 |
| query / after-concurrent-flush / exact / 64 / response | 43.593 | 40.860 | -2.733 | -6.270 | 5/5 |
| query / after-durable-delete / range / 64 / response | 34.016 | 26.925 | -7.091 | -20.846 | 1/1 |
| query / after-durable-delete-ack / exact / 64 / response | 36.261 | 34.922 | -1.339 | -3.692 | 1/1 |
| query / after-durable-recreate / exact / 320 / response | 159.384 | 151.322 | -8.061 | -5.058 | 1/1 |
| query / after-native-delete-recreate / exact / 64 / response | 33.662 | 32.867 | -0.795 | -2.362 | 1/1 |
| query / after-normal-flush / exact / 16 / response | 18.425 | 19.813 | 1.388 | 7.533 | 3/3 |
| query / after-queued-delete-flush / exact / 64 / response | 37.922 | 28.690 | -9.232 | -24.344 | 1/1 |
| query / after-unrelated-write / exact / 64 / response | 42.931 | 6.299 | -36.632 | -85.327 | 5/5 |
| query / after-write / exact / 1 / response | 10.431 | 8.661 | -1.769 | -16.962 | 10/10 |
| query / after-write / exact / 16 / response | 21.181 | 14.474 | -6.707 | -31.664 | 10/10 |
| query / after-write / exact / 64 / response | 47.584 | 41.657 | -5.927 | -12.456 | 10/10 |
| query / after-write / exact / 256 / response | 121.482 | 122.594 | 1.112 | 0.915 | 10/10 |
| query / final-all-known-keys / exact / 320 / response | 151.552 | 152.800 | 1.248 | 0.823 | 1/1 |
| query / mixed-prime / exact / 64 / response | 4.159 | 5.782 | 1.623 | 39.035 | 5/5 |
| query / normal-reopen-generated-keys / exact / 320 / response | 158.761 | 151.559 | -7.201 | -4.536 | 1/1 |
| query / prime / exact / 1 / response | 6.022 | 5.674 | -0.349 | -5.788 | 1/1 |
| query / prime / exact / 16 / response | 10.955 | 10.817 | -0.138 | -1.258 | 1/1 |
| query / prime / exact / 64 / response | 28.365 | 28.966 | 0.601 | 2.117 | 1/1 |
| query / prime / exact / 256 / response | 100.211 | 103.225 | 3.014 | 3.008 | 1/1 |
| query / prime / range / 1 / response | 3.871 | 5.085 | 1.214 | 31.361 | 1/1 |
| query / prime / range / 16 / response | 7.420 | 10.863 | 3.443 | 46.401 | 1/1 |
| query / prime / range / 64 / response | 20.200 | 27.273 | 7.073 | 35.017 | 1/1 |
| query / prime / range / 256 / response | 70.406 | 120.201 | 49.795 | 70.725 | 1/1 |
| query / steady / exact / 1 / response | 0.246 | 0.233 | -0.013 | -5.291 | 21/21 |
| query / steady / exact / 16 / response | 0.793 | 0.660 | -0.133 | -16.788 | 21/21 |
| query / steady / exact / 64 / response | 2.881 | 2.257 | -0.624 | -21.654 | 21/21 |
| query / steady / exact / 256 / response | 11.395 | 9.066 | -2.329 | -20.439 | 21/21 |
| query / steady / range / 1 / response | 3.675 | 4.906 | 1.231 | 33.493 | 21/21 |
| query / steady / range / 16 / response | 7.150 | 8.712 | 1.562 | 21.853 | 21/21 |
| query / steady / range / 64 / response | 19.870 | 21.560 | 1.690 | 8.507 | 21/21 |
| query / steady / range / 256 / response | 70.509 | 74.776 | 4.266 | 6.051 | 21/21 |
| recreate-request / 16 / ack | 195.299 | 43.258 | -152.041 | -77.850 | 1/1 |
| unrelated-mutation / 16 / ack | 1533.736 | 351.495 | -1182.242 | -77.082 | 5/5 |

## All median latency increases

This list includes every positive median change. Small changes can reflect the host or the request order.
Queued Delete has different ack guarantees. Its times appear only in the separate boundary section.

| Case | Before ms | After ms | Change ms | Change % | Samples before/after |
|---|---:|---:|---:|---:|---:|
| normal-flush / queued request / 16 / ack | 203.852 | 308.003 | 104.151 | 51.092 | 3/3 |
| query / prime / range / 256 / response | 70.406 | 120.201 | 49.795 | 70.725 | 1/1 |
| query / prime / range / 64 / response | 20.200 | 27.273 | 7.073 | 35.017 | 1/1 |
| query / steady / range / 256 / response | 70.509 | 74.776 | 4.266 | 6.051 | 21/21 |
| query / prime / range / 16 / response | 7.420 | 10.863 | 3.443 | 46.401 | 1/1 |
| query / prime / exact / 256 / response | 100.211 | 103.225 | 3.014 | 3.008 | 1/1 |
| query / steady / range / 64 / response | 19.870 | 21.560 | 1.690 | 8.507 | 21/21 |
| query / mixed-prime / exact / 64 / response | 4.159 | 5.782 | 1.623 | 39.035 | 5/5 |
| query / steady / range / 16 / response | 7.150 | 8.712 | 1.562 | 21.853 | 21/21 |
| query / after-normal-flush / exact / 16 / response | 18.425 | 19.813 | 1.388 | 7.533 | 3/3 |
| query / final-all-known-keys / exact / 320 / response | 151.552 | 152.800 | 1.248 | 0.823 | 1/1 |
| query / steady / range / 1 / response | 3.675 | 4.906 | 1.231 | 33.493 | 21/21 |
| query / prime / range / 1 / response | 3.871 | 5.085 | 1.214 | 31.361 | 1/1 |
| query / after-write / exact / 256 / response | 121.482 | 122.594 | 1.112 | 0.915 | 10/10 |
| query / prime / exact / 64 / response | 28.365 | 28.966 | 0.601 | 2.117 | 1/1 |
| query / after-accepted-recreate / exact / 64 / response | 33.031 | 33.416 | 0.385 | 1.166 | 1/1 |

## Warm read costs

Each cell shows median / maximum. Zero costs describe only the operations that the receipt measures.

| Phase / shape / keys | Variant | File opens | Read operations | Metadata operations | Directory reads | Payload bytes |
|---|---|---:|---:|---:|---:|---:|
| after-unrelated-write / exact / 64 | after | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 |
| after-unrelated-write / exact / 64 | before | 36 / 38 | 441 / 441 | 447 / 447 | 0 / 0 | 518443 / 518452 |
| steady / exact / 1 | after | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 |
| steady / exact / 1 | before | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 |
| steady / exact / 16 | after | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 |
| steady / exact / 16 | before | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 |
| steady / exact / 64 | after | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 |
| steady / exact / 64 | before | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 |
| steady / exact / 256 | after | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 |
| steady / exact / 256 | before | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 |
| steady / range / 1 | after | 16 / 16 | 55 / 55 | 59 / 59 | 0 / 0 | 13545 / 13545 |
| steady / range / 1 | before | 25 / 25 | 54 / 54 | 58 / 58 | 0 / 0 | 11566 / 11566 |
| steady / range / 16 | after | 16 / 16 | 125 / 125 | 129 / 129 | 0 / 0 | 102063 / 102063 |
| steady / range / 16 | before | 25 / 25 | 119 / 119 | 123 / 123 | 0 / 0 | 90468 / 90468 |
| steady / range / 64 | after | 19 / 19 | 344 / 344 | 349 / 349 | 0 / 0 | 273763 / 273763 |
| steady / range / 64 | before | 27 / 27 | 343 / 343 | 348 / 348 | 0 / 0 | 255113 / 255113 |
| steady / range / 256 | after | 24 / 24 | 1086 / 1086 | 1093 / 1093 | 0 / 0 | 698011 / 698011 |
| steady / range / 256 | before | 34 / 34 | 1092 / 1092 | 1099 / 1099 | 0 / 0 | 681544 / 681544 |

## Concurrent complete requests

All acks use the last response timestamp. All durable spans end at the explicit flush response.
The JSON includes each request span and the exact accepted and completed sequence fields.
An accepted batch count above one proves that multiple complete requests share the queue.
Clock and change-feed requests can also advance the sequence and the durable frontier.

| Variant / round | All acks ms | All durable ms | Flush ms | Accepted batches after acks | Flush target / completed | Frontier change |
|---|---:|---:|---:|---:|---|---:|
| before / 0 | 5280.817 | 6943.745 | 1662.321 | — | — / — | 10 |
| after / 0 | 284.955 | 629.275 | 343.937 | 5 | 141 / 141 | 2 |
| after / 1 | 159.342 | 521.684 | 361.956 | 5 | 147 / 147 | 1 |
| before / 1 | 5265.135 | 6837.936 | 1572.372 | — | — / — | 10 |
| before / 2 | 5344.103 | 6949.050 | 1604.508 | — | — / — | 10 |
| after / 2 | 268.730 | 614.763 | 345.673 | 5 | 154 / 154 | 2 |
| after / 3 | 156.928 | 493.457 | 336.163 | 5 | 160 / 160 | 1 |
| before / 3 | 5600.802 | 7237.504 | 1636.258 | — | — / — | 10 |
| before / 4 | 6533.596 | 8169.763 | 1635.706 | — | — / — | 10 |
| after / 4 | 259.811 | 595.832 | 335.663 | 5 | 167 / 167 | 2 |

## Normal flush observations

No explicit flush occurs during these samples. The first complete status response gives an upper bound after the ack.
The bound includes status request time and the 20 ms gap between polls. It is not a disk commit timestamp.

| Variant / round | Ack ms | Disk after ack upper bound ms | Polls | Reply guarantee |
|---|---:|---:|---:|---|
| before / 0 | 189.511 | 1308.445 | 48 | queued |
| after / 0 | 308.003 | 560.585 | 21 | queued |
| after / 1 | 46.241 | 434.322 | 17 | queued |
| before / 1 | 203.852 | 1346.966 | 49 | queued |
| before / 2 | 223.923 | 1355.417 | 51 | queued |
| after / 2 | 319.384 | 763.199 | 28 | queued |

## Delete reply boundary

The Delete payload uses the schema and the hash plus range key. Empty fields are valid.
The candidate requires native Delete to complete the durable erase before the reply, even when the request asks for queued durability.
The baseline queued reply can precede that erase. The driver checks the baseline result after a flush.
The driver checks the candidate result directly after the stronger reply, then checks the result after a flush.
Mixed native write and erase requests also flush the captured prefix before a durable reply.

| Variant / operation | Ack ms | Actual guarantee |
|---|---:|---|
| before / delete-request | 1708.405 | durable |
| after / delete-request | 371.181 | durable |
| before / queued-delete-boundary | 24.900 | queued |
| after / queued-delete-boundary | 372.732 | durable |

## App storage trust

These values are the final app storage responses after normal reopen.
An incomplete response does not prove exact live-byte totals. Keep the trust fields with the byte values.

| Variant | Complete | Meters origin | Reason | Live bytes | Unattributed bytes | Heavy |
|---|---|---|---|---:|---:|---|
| before | False | incomplete | molecule_counters_not_bootstrapped | 1751157 | 0 | False |
| after | False | incomplete | reused_atom_live_transition_requires_repair | 920729 | 0 | False |

## Receipt checks and limits

- Query receipts: 485.
- Query receipts with exact generated values: 485.
- Owned node stops: 6.
- Forced node stops: 0.
- Refused requests: 0.
- Unmatched latency cases: 1.

The read receipt measures named file and read costs. It does not measure every CPU operation, lock wait, allocation, or kernel cache cost.
Source review indicates that warm reads still perform request validation, dependency checks, response copies, and JSON conversion.
The queue still requotes its accepted prefix for each request and pauses admission during the disk commit and post-commit delivery.
Those source statements explain possible costs. The elapsed receipts do not isolate their separate time.

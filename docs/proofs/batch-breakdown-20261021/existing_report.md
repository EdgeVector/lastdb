The batch contains the stated record count. One HTTP call sends the whole batch.
Each result combines 64 calls: two runs, two write types, and 16 calls per cell.
Other disk jobs ran at the same time. These results do not show an isolated machine limit.

| Key | Records/call | Version | Calls/s | Mean ms/call | p50 ms | p95 ms | Records/s |
|---|---:|---|---:|---:|---:|---:|---:|
| hash | 1 | before | 0.983 | 1017.54 | 1035.49 | 1491.45 | 0.98 |
| hash | 1 | after | 0.993 | 1006.53 | 997.16 | 1431.76 | 0.99 |
| hash | 32 | before | 0.303 | 3297.27 | 3215.34 | 5408.46 | 9.70 |
| hash | 32 | after | 0.333 | 2999.41 | 2656.14 | 5372.67 | 10.67 |
| hash | 128 | before | 0.104 | 9654.98 | 8971.01 | 17379.43 | 13.26 |
| hash | 128 | after | 0.109 | 9163.60 | 9690.10 | 15702.19 | 13.97 |
| hashrange | 1 | before | 0.610 | 1638.73 | 1585.62 | 2352.30 | 0.61 |
| hashrange | 1 | after | 0.759 | 1317.58 | 1231.90 | 2836.97 | 0.76 |
| hashrange | 32 | before | 0.239 | 4191.47 | 4163.54 | 7251.32 | 7.63 |
| hashrange | 32 | after | 0.327 | 3057.60 | 3162.91 | 4481.86 | 10.47 |
| hashrange | 128 | before | 0.126 | 7936.95 | 8858.99 | 12258.79 | 16.13 |
| hashrange | 128 | after | 0.109 | 9160.67 | 8698.03 | 12807.18 | 13.97 |

The mean call time uses the sum of HTTP durations divided by the call count.
The call rate uses the sum of cell durations. Each cell spans its first send through its last reply.
The small difference consists of time between calls. Schema setup, read checks, and notice checks sit outside these cell durations.

| Key | Records/call | Version | Prepare ms/call | Inner receipt ms/call | HTTP ms/call | Data barriers/call | Directory barriers/call |
|---|---:|---|---:|---:|---:|---:|---:|
| hash | 1 | before | 5.88 | 1011.99 | 1017.54 | 32.52 | 63.48 |
| hash | 1 | after | 6.60 | 998.28 | 1006.53 | 20.91 | 40.47 |
| hash | 32 | before | 39.67 | 3283.42 | 3297.27 | 61.86 | 95.11 |
| hash | 32 | after | 35.82 | 2985.05 | 2999.41 | 48.95 | 70.95 |
| hash | 128 | before | 139.64 | 9620.44 | 9654.98 | 124.22 | 156.39 |
| hash | 128 | after | 101.53 | 9128.39 | 9163.60 | 92.88 | 114.72 |
| hashrange | 1 | before | 7.84 | 1633.00 | 1638.73 | 40.92 | 69.36 |
| hashrange | 1 | after | 7.82 | 1310.91 | 1317.58 | 30.86 | 48.98 |
| hashrange | 32 | before | 17.09 | 4175.14 | 4191.47 | 84.03 | 113.56 |
| hashrange | 32 | after | 14.07 | 3042.74 | 3057.60 | 68.86 | 89.11 |
| hashrange | 128 | before | 43.59 | 7902.36 | 7936.95 | 123.89 | 153.50 |
| hashrange | 128 | after | 45.37 | 9125.22 | 9160.67 | 94.45 | 114.61 |

These are shared-host observations, not an M5 or hardware-limit forecast.
The combined notice candidate is the frozen after-v2 binary; this matrix does not benchmark later main integration changes.
Prepare is elapsed work with awaits, not CPU time. The inner receipt clock is not the HTTP reply clock.
Prepare overlaps the inner receipt clock. Clock differences do not measure a disk or queue wait.
Gate-wait and publish fields report unmeasured zeros; they do not prove zero work.
Barrier counters cover all LastStore homes in the node process and status endpoints outside the timed HTTP interval.
Barrier durations count completed attempts, include failures, and can overlap threads. They cannot form a call-latency partition.
Baseline notices can add work after the HTTP reply. Both post-status and notice-drain windows remain in the JSON.
The complete HTTP reply follows the required durable local commit. No crash, power-loss, cloud or hardware-cold proof is claimed.

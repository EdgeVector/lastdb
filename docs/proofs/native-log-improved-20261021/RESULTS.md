# Combine disk lookup changes to shorten the write pause

Date: 2026-10-21. Branch: `kanban/write-under-10ms-20261021`.
Source before this change: `5090df77e`.
No push, PR, merge, primary change, or shared-system change occurs.

## Result

The change combines disk lookup changes across complete, verified log batches.
It removes repeated intermediate lookup-page work during one disk-file update.
The two 1,000-write small-value runs report **67.031 ms** and **69.983 ms** P99.
Their typical latency stays near 12 ms.
The previous equivalent diagnostic and control runs report 124.954 ms and 119.022 ms P99.

The longer run exposes other limits. Its P99 is **237.460 ms** and its maximum is **2,061.148 ms**.
The change does not remove the shared write gate, compaction pauses, or the complete disk fallback path.
It does not establish a general P99 bound. The candidate remains unmerged.

| Run | Value bytes | Writes | Median ms | Average ms | P95 ms | P99 ms | Maximum ms |
|---|---:|---:|---:|---:|---:|---:|---:|
| Before: diagnostic | 64 | 1,000 | 11.071 | 16.136 | 18.874 | 124.954 | 154.225 |
| After: diagnostic | 64 | 1,000 | 11.925 | 14.074 | 18.961 | 67.031 | 74.112 |
| Before: control | 64 | 1,000 | 11.903 | 16.410 | 20.029 | 119.022 | 130.993 |
| After: control | 64 | 1,000 | 12.021 | 14.712 | 19.945 | 69.983 | 76.958 |
| After: larger value | 4,096 | 1,000 | 12.938 | 15.433 | 20.870 | 73.050 | 77.893 |
| After: longer workload | 64 | 3,000 | 21.200 | 38.949 | 80.377 | 237.460 | 2,061.148 |

Every measured write succeeds with a durable receipt.
The runs retain all samples. There is no warm-up exclusion or removal of slow requests.
The earlier 4,096-byte run has a 125.459 ms P99; its receipts remain in `native-log-ack-20261021/large/`.
The earlier 563.964 ms small-write result remains historical evidence, not a matched baseline for this change.

## Change

Previously, the publisher updates the disk lookup trees once for each complete pending log batch.
One disk-file update can touch the same lookup pages repeatedly before CURRENT changes.
The publisher now collects bounded key and location metadata across those verified batches.
It sorts by collection, hash group, key, and original mutation version.
For each key, the last stored version supplies the final lookup value, including a tombstone.
It then writes the combined lookup changes through the existing tree writer.

Every original batch still contributes its payload-file dependencies.
The stored atom bytes, log frames, mutation versions, CURRENT format, and recovery protocol stay unchanged.
Counters and author clocks retain their final stored values.
This change combines complete stored changes; it does not combine or reorder user commands before their log writes.
The final CURRENT switch still follows complete recovery verification and the existing disk barriers.

The temporary buffer admits no more than the current complete-batch operation limit.
Its populated entries and retained string capacities have an 8 MiB budget.
The operation limit also bounds spare vector capacity.
The publisher completes one bounded window before it accepts another window.
The buffer retains keys and locations, not the recovered body bytes.
It adds no long-lived cache, disk log, disk format, or recovery implementation.

The foreground ack still follows the durable native log append and the memory update.
The 250 ms disk-file deadline, capacity triggers, quotas, and memory protection rules stay unchanged.
No write-queue timer or existing admission cap changes to obtain these results.

## Measured pause

The diagnostic run maps every client request to its complete native scope and accepted sequence.
The publisher completes 44 disk-file updates during those 1,000 writes.
Their median duration is **48.851 ms**; the previous diagnostic run reports **107.166 ms**.
Their maximum is 59.867 ms, versus 134.947 ms in that earlier run.
The native write-gate wait has a 55.122 ms P99, versus 112.438 ms before this change.
The durable log acceptance itself has an 8.041 ms P99.

No request in the new 1,000-write diagnostic run exceeds 80 ms.
That earlier run has 42 such requests, all dominated by the disk-file pause.
These observations support the reduction in repeated publication work.
They do not establish the latency of every workload or isolate all changes in host conditions.

## Longer workload and remaining limits

The 3,000-write run contains 1,500 creates and 1,500 updates.
It performs three automatic native maintenance runs, with no reported maintenance failure.
The last maintenance run takes 4,525 ms. Its work can overlap more than one request.
The largest client request takes 2,061.148 ms, including a 2,028.713 ms wait for the native write gate.
That wait does not overlap the ordinary disk-file publication interval in the numeric trace.
The current diagnostics do not separately time every other gate owner.

Of 164 requests above 80 ms, the largest measured component is:

- Time after the log append for 90 requests.
- The native write-gate wait for 46 requests.
- Time outside the native write scope for 28 requests.

Time after the log append has a 46.566 ms P99 in this run.
The overlap with disk-file publication after the append has a 45.709 ms P99.
This confirms that some requests still wait for the complete disk-file path before success.
The source retains that fallback when complete typed-memory coverage or a memory reservation is unavailable.
The trace does not record the exact admission-refusal reason for each request.

The memory reservation count cap and byte caps stay unchanged.
Owners can retain those reservations while memory or a read still holds their values after the flush.
The source can therefore select the disk fallback even when the pending write queue is short.
This investigation does not change that ownership rule or raise the caps.
The complete client result includes costs before entry into the native scope, which this disk publisher change does not remove.

The longer run's native log acceptance has a 30.253 ms P99.
Its full device flush has a 28.233 ms P99, so the disk cost also varies during that run.
A concurrent directory-walk process is observed on this host.
That observation does not establish a cause for any individual delay.
No other process is stopped or changed.

Further work must address these paths before a broad P99 claim or a merge.
The shared pause still needs a fixed-boundary publication design if new log writes must continue through every disk update.
That design must retain later writes, their quotas, and their memory protection across the CURRENT and journal-stream changes.

## Method, recovery, and checks

All runs use the real optimized daemon on this Mac, fresh generated homes, and the real Unix socket.
The measurements include client JSON encode, connection, response, and response JSON decode.
Each request supplies the same three schema fields. Updates address 128 existing keys.
The dependency high water stays at 4,096. Other settings match the previous diagnostic and control runs.
No build or lint command runs during a latency measurement.
The primary continues its normal work and stays unchanged.

The prior capture and measurement drivers remain in `native-log-tail-20261021/`.
The numeric attribution helper in this folder also distinguishes capacity, post-append, and pre-scope delays.
`inputs.json` in each run records the measured executable hash and generated home.
Each run preserves every response and raw latency in `writes.json`.
The attribution folders preserve numeric events and per-request component timings.

All 501 expected values match after forced-stop recovery in each 1,000-write run.
All 1,501 expected values match after forced-stop recovery in the longer run.
Every next author clock advances. All four final shutdowns return exit code 0 and `flush_ok: true`.
No physical power-loss check occurs.

The first attempted home path exceeds the Unix socket path limit before boot.
That attempt starts no requests. Its inputs and reason remain in `startup-path-rejection/`.
The successful runs use shorter home names within the same owned scratch directory.

The optimized product build and format check pass.
Clippy for laststore, core, and node passes with warnings denied.
The committed file-size, function-size, and new-allow checks pass.
No tests, fixture suite, mutation probes, or coverage gate run.
All four successful owned daemons stop before cleanup.
The exact scratch realpath, owner, and process state are checked before removal.
All generated homes, raw logs, and redundant intermediate reports are removed after the numeric receipts are saved.
The failed startup's empty generated home is removed in the same cleanup.
No primary data, archive, shared service, or other agent's data participates.

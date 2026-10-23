# P99 investigation: the normal disk-file update pauses new writes

Date: 2026-10-21. Branch: `kanban/write-under-10ms-20261021`.
The investigation starts from local commit `4d9bda3c4`.
No push, PR, merge, primary change, or shared-system change occurs.

## Confirmed cause

The prototype uses one write gate for new writes and the normal disk-file update.
The update runs after the first accepted request's 250 ms deadline, or after a capacity trigger.
It closes the queue and pauses later writes until the complete update and its callbacks finish.
The ack does not wait for the timer itself.
A request that arrives during the update must wait for the update to finish.

The new 1,000-write diagnostic run contains 42 requests above 80 ms.
All 42 have the gate wait as their largest component.
Every one overlaps the normal disk-file update by more than 50 ms.
This directly explains the reproduced 125 ms P99.
It is not an inference from the overall sync counters.

| Component | Median for the 42 requests above 80 ms |
|---|---:|
| Client request | 121.160 ms |
| Wait for the native write gate | 109.496 ms |
| Durable log append and memory acceptance | 6.941 ms |
| Other work inside the native scope | 4.400 ms |
| Client time outside that scope | 0.455 ms |

These are separate component medians. They are not an exact sum for one request.

Request 617 is the sample at P99. It takes 124.954 ms:

- 113.221 ms waits for the native write gate.
- 6.522 ms completes the durable log append and memory acceptance.
- 4.763 ms completes other work inside the native scope.
- 0.448 ms occurs outside that scope.

Of its gate wait, 110.392 ms overlaps the disk-file update.
Another 2.680 ms overlaps the callbacks after that update.
Its own full device flush takes 4.524 ms, inside the 6.522 ms acceptance interval.
The client connection and response do not account for the large delay in this sample.

## Work behind the gate

The queue worker holds the exclusive write gate through `commit_queue`.
It calls `publish_logged_queue`, which holds the journal mutex through `publish_durable`.
The existing native publisher reads the complete pending batches and updates the disk lookup files.
It writes a generation receipt, replaces CURRENT, archives TAIL, and rotates the journal stream.
The memory view then changes to the new complete disk state.
The queue stays closed through the callbacks after that disk state becomes current.

The 42 disk-file updates take a median of 107.166 ms.
Their final full device flush takes a median of 5.827 ms.
Their order barriers take a median total of 3.885 ms per update.
Thus, the complete update does much more work than its final full device flush.
The current diagnostics do not split the remaining time into individual file reads, file writes, and CPU work.
They do establish that this whole interval blocks the next foreground write.

The native log acceptance itself has an 8.692 ms P99 across all 1,000 writes.
Its full device flush has a 5.740 ms P99.
The complete client request has a 124.954 ms P99.
These percentile values describe different sample sets and cannot be subtracted as individual request timings.

The native capacity wait is zero for all measured requests.
No automatic native maintenance run occurs.
The status reports no maintenance failure or warm-set over-limit admission.
Those observations do not establish behavior under a larger production workload.

The earlier receipt's `resident_stages_us.gate_wait` reports zero for native writes.
That field does not measure this storage gate. The receipt assigns zero on this path.
The new diagnostic events measure the storage wait directly and do not change the receipt format.

## Reproduction and control

| Run | Writes | Median | Average | P95 | P99 | Maximum |
|---|---:|---:|---:|---:|---:|---:|
| Numeric diagnostics on | 1,000 | 11.071 ms | 16.136 ms | 18.874 ms | 124.954 ms | 154.225 ms |
| Numeric diagnostics off | 1,000 | 11.903 ms | 16.410 ms | 20.029 ms | 119.022 ms | 130.993 ms |

The delay remains with the diagnostic output off.
Host conditions can change between runs. These runs do not measure the exact overhead of the diagnostics.
The earlier 127.204 ms result shows the same periodic delay pattern in its client samples.
The earlier 563.964 ms result lacks these detailed timings.
Its exact cause remains unconfirmed; these data do not justify attributing every earlier outlier to the same cause.

Both new runs use the same optimized executable and fresh generated database homes on this Mac.
Each run sends 500 creates and 500 updates with 64-byte values and the same three schema fields.
Every ack still follows the existing native log's disk completion and the memory update.
The normal dependency high water stays at 4,096. The other settings match the previous measurement.
No build or lint command runs during either latency measurement.
The primary continues its normal work and stays unchanged.

Both runs return durable receipts for all 1,000 writes.
All 501 latest expected values match after a forced stop and restart.
The next author clock advances. Both final shutdowns return exit code 0 and `flush_ok: true`.
At the forced stop, the diagnostic run has 18 pending native batches and the control has five.
This exercises the existing journal recovery path. No physical power-loss check occurs.

## Next change

The next change should shorten or remove this shared pause.
It must keep the disk-before-ack guarantee.
One design is to build the normal disk files from a fixed complete log boundary while later log writes continue.
That design must retain later writes, their quota, and their memory protection when the new disk state becomes current.
It also must coordinate the journal stream change with those later writes.
Simply removing the existing lock can lose the later accepted view or break the generation-bound reservations.

This investigation adds only numeric diagnostics, not that concurrency change.
It adds no mutation log, disk format, or recovery implementation.
The candidate stays unmerged.

## Receipts and checks

`traced/` and `control/` contain all client samples, executable hashes, and recovery receipts.
`attribution/events.json` retains only the numeric diagnostic fields and their stage names.
`attribution/requests.json` joins every request to its native scope, gate wait, and accepted sequence.
The join requires exactly one complete foreground scope and gate event for each client request.
`attribution/summary.json` contains the complete component statistics and the worst twelve samples.
The raw daemon log does not enter the repository.

The optimized product build, format check, and sensitive-log-field source check pass.
Clippy for laststore, core, and node passes with warnings denied.
The committed file-size, function-size, and new-allow checks pass.
The diagnostics stay off unless the `fold_db::native_latency` debug target is enabled.
They contain no keys, values, schema names, or identities.
After the measurements, a Clippy fix changes only the numeric timestamp's equivalent result conversion.
The executable hashes describe the measured binary before that style fix.
No tests, fixture suite, mutation probes, or coverage gate run.

Both owned daemons stop before cleanup, with final PIDs 97125 and 259.
The exact scratch realpath, owner, and process state are checked before removal.
The generated homes and raw scratch logs are removed after the numeric receipts are saved.
No primary data, archive, or other agent's data participates.

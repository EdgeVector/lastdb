# Concurrent compaction — 2026-10-21

Final product source: `d054bf837`.
The change stays local and unmerged. The primary and shared systems stay unchanged.
All operational runs use fresh generated homes. No primary data or archive is read or copied.
The new path stays behind the existing isolated-node experiment.

## Change and safety

The old maintenance path holds the global write gate through the file rewrite, disk barriers, and old-file deletion.
The new path captures one immutable disk root and retains its file lifetime hold.
The rewrite then runs outside the write gate, the journal mutex, and the directory publication owner.
Foreground writes and ordinary lookup publication continue during that rewrite.

Compaction next merges newer stored batches into its private output while ordinary lookup publication still continues.
The final join captures one complete queued prefix and merges the smaller remaining suffix.
That last merge owns the directory publication boundary, while the write gate and journal mutex remain free.
The final CURRENT disk publication and full device flush also run outside the write gate and journal mutex.
Only the final memory/root handoff holds the global write gate.

The source uses the existing journal reader and publisher for the merge.
It verifies complete batches, quotes metadata space, and forms exact replay windows under the unchanged batch and byte limits.
Each window passes the existing recovery checks for aggregate byte count, operation checksums, and final COMMIT digest.
No new journal, file format, disk overlay, or startup recovery system is added.

Later accepted values, path holds, and space owners survive the captured prefix handoff.
A private compaction refusal falls back to ordinary prefix publication; it does not strand accepted writes.
A separate task owns maintenance through completion, even when its HTTP caller disconnects.
A shared generation allocator prevents concurrent private outputs from using the same receipt identity.
The compaction part counters change under the same short global gate as their installed root.

Cleanup also runs outside the global write gate, the view lock, and the journal mutex.
It captures an installed generation floor. An older active call makes deletion defer.
New calls at or above that floor remain allowed during deletion.
Cleanup removes only validated obsolete paths below the captured boundary and preserves grace periods and durable references.
It ignores newer or unfinished generation output and skips new or incomplete part headers during a live orphan sweep.

Capacity settings stay unchanged. Existing memory and part pressure can still require lookup completion before a request returns.
The final suffix join serializes lookup publication; it does not serialize the long file rewrite.
Compaction still consumes CPU and disk bandwidth. This change removes the long global lock hold, rather than that resource cost.

Every logical ack still follows its complete journal write, ordered file barriers, full device flush, and correct memory publication.
An unsupported memory publication still waits for the canonical disk lookup boundary.
No full device barrier is removed from the durable ack path.

## Method

`capture.py` uses the real optimized daemon and Unix socket.
Each measured request requires a durable receipt. The client omits the durability parameter to check the default path.
Client latency includes JSON encode, socket connect, and response decode. No initial sample is excluded.
Creates and updates have equal counts; updates use 128 known keys.
Known-key batch reads compare all final values with their expected hashes, before and after SIGKILL.
The next write after restart must advance the author clock.

`--serial-compaction` selects the previous maintenance path with its long global gate hold.
All final comparisons use the same executable hash and verify the native status mode.
The 1,024-part runs force frequent real compactions. The longer run uses the normal 4,096-part threshold.
Automatic cleanup keeps its existing zero-age policy and 32-generation slices.
No owned compiler or lint runs during a measurement.

`analyze.py` retains only numeric native trace events and client intervals.
It counts complete client calls inside the compaction rewrite and cleanup phases.
It also counts normal disk publications that complete during the compaction rewrite.
Disk-sync counters are process-wide; concurrent phase deltas are not exclusive costs.

The `draft` run covers source `6b3e9db78` and its separate executable hash.
It precedes the extra catch-up pass and the final part-counter handoff fix.
It is retained as development evidence and is excluded from the final comparison.

## Control failure and correction

The first serial control, under source `e6a68235d`, failed after 836 complete foreground native write scopes.
Its client received a non-JSON failure and the daemon could not prove its shutdown flush.
A restart then refused the home with a missing-file error.
The CURRENT receipt and all eight of its dependency files were present.
The active journal stream directory was absent.

The old cleanup helper removed empty stream directories after compaction moved their last live payload into packed files.
The next journal append and the next checkpoint archive still need that directory.
Cleanup now preserves journal stream directories. Checkpoint archive creation also creates its required parent after recovery verifies the complete boundary.
Missing committed payload still causes a recovery refusal; this does not create replacement data.

`serial-before/failure.json` records the failed control and its native completed-call count.
It has no complete client latency distribution and is excluded from the final comparison.
The corrected binary is used for the subsequent recovery and matched controls.
That binary recovers all 500 final records from the failed control home.
Two new writes return durable receipts, the author clock advances, and the final shutdown flush succeeds.

## Final results

All final runs use executable SHA-256 `cf1c951928c5c3fc9c9feaaa1c6476a9f15fb178185787e6a87e8362e260fcb6`.
`matrix.json` contains the full results and receipt checks.
The five runs contain 6,600 measured writes. The new path accounts for 5,600 writes.
Every measured write returns a durable receipt. Every run reports zero request errors and zero maintenance failures.
All final values match before the forced stop and after restart.
Every restart permits a new durable write with a later author clock. Every final shutdown flush succeeds.

| Run | Writes | Value bytes | File threshold | Typical ms | Average ms | P99 ms | Maximum ms |
|---|---:|---:|---:|---:|---:|---:|---:|
| Previous compaction path | 1,000 | 64 | 1,024 | 12.50 | 15.44 | 38.97 | 948.38 |
| New compaction path | 1,000 | 64 | 1,024 | 12.17 | 13.11 | 23.99 | 40.73 |
| Longer run | 3,000 | 64 | 4,096 | 17.13 | 26.02 | 79.39 | 553.38 |
| Larger values | 1,000 | 4,096 | 1,024 | 11.94 | 12.66 | 23.22 | 33.14 |
| 2 GiB disk cap | 600 | 64 | 512 | 16.06 | 17.47 | 37.95 | 51.66 |

Typical means the median. All client samples remain in these results.
The matched 1,000-write runs use the same threshold and executable.
The old maintenance path holds the global gate for as much as 906 ms and admits no write during those phases.
The new final compaction memory update holds the gate for at most 0.176 ms across eight measured compactions.
That interval includes the move to the blocking worker after lock acquisition.
The brief capture steps also use the gate. The file rewrite, disk publication, and file deletion do not use it.

| New path run | Complete calls during file rewrite | Normal disk publications during file rewrite | Complete calls during cleanup | Obsolete files removed |
|---|---:|---:|---:|---:|
| New compaction path | 164 | 8 | 59 | 2,366 |
| Longer run | 102 | 102 | 234 | 9,749 |
| Larger values | 159 | 7 | 55 | 2,315 |
| 2 GiB disk cap | 40 | 3 | 12 | 1,167 |

The file rewrite takes up to 2,616 ms in these runs. Writes continue through that work.
The final suffix merge takes up to 117 ms outside the global gate.
That merge still owns the directory publication boundary, so a request that requires canonical lookup completion can wait for it.
The final CURRENT publication takes up to 12.3 ms outside the global gate.

The larger-value run stops the daemon during a new file rewrite, after new packed data appears on disk.
The saved status shows `compaction_copying: true` at that point.
All 501 expected records, including the last acknowledged sentinel, match after restart.
The disk-cap run also completes an explicit durable flush before the forced stop.
That run checks active cap accounting; it does not check cap exhaustion.
The 16 MiB metadata cap stays unchanged. No final run reports a memory refusal.

## Remaining latency

This change does not establish a universal P99 below 24 ms.
The longer run reaches 79 ms P99 and a 553 ms maximum.
Its worst call waits 503 ms for another background logical write's journal append.
That append records 489 ms in an ordered disk barrier.
Other slow calls wait for canonical lookup completion after the journal ack boundary.
The numeric intervals in `longer-attribution/events.json.gz` and `requests.json` identify those phases.
Compaction does not hold the global gate during that file work.
Background logical mutations still use the same write sequence as foreground mutations.
Disk bandwidth, disk barriers, resource limits, and canonical lookup waits still affect latency.

## Checks

Use `checks.json` for the product build, format, Clippy, size, source-policy, and receipt syntax results.
No tests, fixtures, mutation probes, or coverage checks run.
The results contain process-stop recovery checks, not a physical power interruption.
All seven owned scratch homes and their raw logs are removed after the receipts are saved and all owned daemons stop.
`cleanup-ready.json` and `cleanup.json` record the exact path, owner, node check, and removal result.
No push, PR, merge, primary change, or shared-system change occurs.

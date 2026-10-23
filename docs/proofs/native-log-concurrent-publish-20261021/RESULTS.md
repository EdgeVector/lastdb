# Prepare disk lookup files outside the foreground write gate

Date: 2026-10-21. Product source: `8de86bfca`.
The candidate remains local and unmerged. The primary and shared systems stay unchanged.

## Result

The ordinary disk lookup-file preparation no longer holds the foreground write gate.
New writes can complete their own durable log append and memory update during that preparation.
The final disk-root and memory switch still takes the gate for a shorter interval.

Two matched 1,000-write comparisons reduce complete client P99 from **65-70 ms** to **22-29 ms**.
The 4,096-byte run also reaches **21.917 ms** P99.
The longer workload still has a **171.347 ms** P99 and a **2,382.878 ms** maximum.
This change removes the ordinary lookup-file block. It does not establish a general low-latency bound.

| Run | Value bytes | Writes | Median ms | Average ms | P95 ms | P99 ms | Maximum ms |
|---|---:|---:|---:|---:|---:|---:|---:|
| Serial control before | 64 | 1,000 | 11.780 | 13.848 | 19.112 | 65.140 | 70.091 |
| Concurrent first | 64 | 1,000 | 11.951 | 12.389 | 18.931 | 21.870 | 25.520 |
| Serial control after | 64 | 1,000 | 12.920 | 15.701 | 38.656 | 69.964 | 76.122 |
| Concurrent repeat | 64 | 1,000 | 14.111 | 15.744 | 25.996 | 29.198 | 37.264 |
| Concurrent longer | 64 | 3,000 | 16.420 | 30.874 | 56.817 | 171.347 | 2,382.878 |
| Concurrent larger value | 4,096 | 1,000 | 12.907 | 13.500 | 20.085 | 21.917 | 24.651 |

All six final runs use the same executable hash. `matrix.json` preserves that comparison.
Every measured write succeeds with a durable receipt. No slow sample or warm-up request is removed.
The host is shared. These measurements do not promise a latency for another host or workload.

## Why the old path blocks

The native log contains complete durable recovery bytes before the ack.
Ordinary reads from disk still need the keyed lookup files that name those bytes.
The old queue worker holds the write gate through the complete lookup-file update and journal-stream checkpoint.
It also assumes that the completed prefix contains the whole accepted memory view.
That assumption prevents a safe unlock during preparation.

## Change

The native writer now captures an exact CURRENT and complete TAIL under the existing gate.
A non-cloneable publication plan retains the process writer lease and that fixed boundary.
The plan verifies and prepares immutable lookup files outside the journal mutex and foreground gate.
It makes those private files durable before the final switch.
New complete log appends can proceed against the same stream during this work.

The worker then takes the foreground gate for installation.
It checks that the prior CURRENT and writer identity still match the captured boundary.
It publishes CURRENT first, then trims the existing TAIL interval through the completed prefix.
Later appended batches retain their original stream, batch identities, final marker digest, and exact remaining byte count.
The existing ordered file and directory barriers precede the final full device flush.
The full device flush for each foreground log append remains unchanged.

The TAIL trim uses the existing format and reader rules.
No second log, format, recovery implementation, or persistent storage cache is added.
Startup still uses the existing journal recovery and stream checkpoint.
The final switch retains later accepted memory and clears dirty state only through the captured logical sequence.
Later source changes also retain their current memory and schema-key state.

The queue retains the space reservations for both the captured prefix and later writes.
The existing request, operation, byte, dependency, and journal-age limits still include the captured work.
Only successful delivery releases that prefix's reservations.
A lost or failed publication requires recovery and retains the accepted queue's ownership.
Explicit fences and unsupported hidden writes still exclude later preparation when their complete disk state is required.
Capacity can still stop admission. The change does not raise any cap.

The experiment remains isolated behind `LASTDB_NATIVE_LOG_ACK_EXPERIMENT` and `LASTDB_ISOLATED_COPY`.
An isolated diagnostic flag, `LASTDB_SERIAL_LOG_PUBLICATION_DIAGNOSTIC`, selects the prior serial path for the matched control.
Native status records which publication path actually runs.
The 250 ms disk-file deadline and capacity triggers stay unchanged.
The software checksum backend also stays unchanged, so this comparison does not include a hardware-acceleration change.

## Memory lifetime across a partial completion

The old dirty model clears its root only when the whole root's latest sequence completes.
With continuous later writes, that rule retains earlier completed model entries.
An initial draft reaches two metadata refusals and preserves them in `first-draft/`.
That run has a 63.984 ms P99 and is not the final performance result.

Each prepared slot and definition now receives its acceptance sequence before publication.
A completed prefix removes only model entries at or below its sequence.
Later values and read-held versions retain their original owners and charges.
The replacement uses the same metadata byte allowance.
If a read-held version leaves no temporary allowance, cleanup retains the old root and retries at a later flush.
It does not discard a newer write or bypass a memory charge.

All six final runs report zero model metadata refusals.
Their maximum reported model metadata peak is 13,350,112 bytes, below the unchanged 16 MiB allowance.
The warm-set limits and raw-memory reservation policy stay unchanged.
The change adds no permission to purge a log-only write before its lookup files become readable.

## Direct evidence of concurrent progress

| Run | Write-gate wait P99 ms | Final disk switch median ms | Acks inside lookup preparation |
|---|---:|---:|---:|
| Serial control before | 53.428 | 46.799 | 0 |
| Concurrent first | 8.505 | 7.840 | 192 |
| Serial control after | 56.878 | 48.900 | 0 |
| Concurrent repeat | 12.427 | 9.884 | 203 |
| Concurrent larger value | 8.668 | 8.030 | 183 |

Each counted ack has a later sequence than the captured prefix.
Its complete client interval lies inside the lookup-preparation interval.
This establishes progress through the former block, rather than only a shorter background duration.
The first concurrent run prepares lookup files for a 60.474 ms median outside the gate.
Its final disk switch takes a 7.840 ms median.
Memory installation and short ownership work also use the gate and are included in the request's measured wait.

Private preparation and final installation each retain a full durability boundary.
This adds a background device flush relative to one combined serial publication.
Concurrent preparation also competes for CPU and disk resources.
The matched results measure those costs with the same executable and workload.
Process-wide barrier counter deltas can overlap these concurrent phases, so they are not exclusive phase costs.
The request intervals and elapsed gate waits remain separate measurements.

## Longer-run limits

The 3,000-write run contains 1,500 creates and 1,500 updates.
It performs three automatic maintenance runs, with zero reported maintenance failures.
The last reported run takes 3,531 ms across its maintenance work.
That existing maintenance path still drains the queue and holds the write gate.

The worst request spends 2,370.599 ms in the native gate wait.
Only 10.214 ms of that wait overlaps the ordinary final disk switch.
The trace does not separately time every other gate owner, so it cannot assign that whole pause to one maintenance step.

Of 35 requests above 80 ms, the largest measured component is:

- Work outside the native write scope for 24 requests.
- The gate wait for nine requests.
- Work after log acceptance for one request.
- Log acceptance for one request.

The existing memory refusal path can still require complete disk publication before success.
The trace does not record every admission-refusal reason.
These remaining paths prevent a general P99 claim and require separate work before a merge decision.

## Recovery and validation

Every run uses the real optimized daemon, a fresh generated home, and the real Unix socket.
No primary database copy or archive access occurs.
The workload and measurement driver retain the prior three-field schema, create/update mix, and 128 update keys.
The client timer includes JSON encode, connection, response, and JSON decode.
The dependency high water stays at 4,096. No build or lint command runs during a measurement.

All 500 expected values match before the forced stop in each final 1,000-write run.
All 501 values match after recovery, including the final acknowledged sentinel.
The longer run retains 1,500 values before the stop and 1,501 after recovery.
Every next author clock advances.
The first concurrent forced stop leaves 22 logged batches beyond CURRENT; recovery retains their values.
The stopped CURRENT and TAIL checksums are valid and their stream matches.
Every final shutdown returns exit code 0 and `flush_ok: true`.
The initial draft also stops cleanly after its recovery check.
No physical power-loss check occurs.

The optimized daemon build, formatting, and package-scoped Clippy pass.
File size, function size, new-allow, atom-counter writer, write-path, byte-slice, redaction, and URL checks pass.
The two receipt helpers pass Python syntax checks.
No tests, fixture suite, mutation probes, or coverage gates run.

The numeric intervals, complete client receipts, mode flags, executable hashes, and recovery results remain in this directory.
All seven owned generated daemons stop before cleanup.
The exact scratch realpath, owner, and process state are checked before removal.
Generated homes and raw logs are removed after receipt preservation; `cleanup.json` records the result.

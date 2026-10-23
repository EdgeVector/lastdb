# Existing native log: the 10 ms target remains unmet

Date: 2026-10-21. Branch: `kanban/write-under-10ms-20261021`.
Source base: `c70e6fdc7810bb6291a2564ad8157d7938de0494`.
No push, PR, merge, upgrade, or primary change occurs.

The candidate completes the existing local recovery log and the memory update before the ack.
It adds no second disk log, disk format, or recovery implementation.
The small-write medians are **12.449 ms** and **11.954 ms** in two 1,000-write runs.
The target remains below 10 ms with disk completion before the ack.
These measurements use the median for typical latency.

## Results

| Run | Value bytes | Writes | Median ms | Average ms | p95 ms | p99 ms | Below 10 ms |
|---|---:|---:|---:|---:|---:|---:|---:|
| Initial pilot | 64 | 100 | 26.464 | 47.796 | 180.104 | 493.175 | 0 |
| Small write | 64 | 1,000 | 12.449 | 36.741 | 121.172 | 563.964 | 3 |
| Small write repeat | 64 | 1,000 | 11.954 | 17.262 | 22.078 | 127.204 | 58 |
| Larger write | 4,096 | 1,000 | 13.250 | 20.744 | 93.132 | 125.459 | 14 |

Every measured write succeeds and returns a durable receipt.
Each run uses a fresh generated database, with 500 creates and 500 updates for the 1,000-write runs.
Updates address 128 existing keys. Each mutation supplies the three schema fields.
The pilot uses 50 creates and 50 updates.
The minimum small-repeat sample is 8.424 ms; that minimum does not meet the typical target.

The small-repeat create median is 11.063 ms. Its update median is 12.079 ms.
The larger-write create median is 14.535 ms. Its update median is 12.795 ms.
No samples or delays are removed from these results.
The different run averages show substantial variation on this host.
These runs do not establish a hardware latency floor or performance under sustained production load.

## Measurement

The optimized `lastdbd` runs on this Mac with fresh generated data and no cloud setup.
Each client request uses the real Unix socket and one connection.
The clock covers client JSON encode, connection, request, response, and response JSON decode.
The request omits the durability parameter. Every response must still report durable completion.
The write samples exclude boot, schema setup, later reads, recovery, and shutdown.
No build or lint command runs during a latency measurement.
The primary stays unchanged and continues its normal work.

Each run retains the normal dependency high water of 4,096.
The settings use two socket workers, no keychain, no recovery phrase output, and zero automatic-maintenance start delay.
The daemon uses a 64 MiB Rust stack setting, as in the earlier measurements.
The final candidate gives its bounded file-sync workers explicit 256 KiB stacks.
The pilot precedes that worker-stack change and the extra file-set time counters.
The different workloads and host conditions prevent a causal claim about that change alone.

Each run directory contains `inputs.json`, `writes.json`, and the recovery receipts.
`inputs.json` records the executable SHA-256, generated home, payload size, and experiment mode.
`writes.json` retains every raw latency, response receipt, and the native status before and after the samples.

## Disk completion and recovery

The existing native journal stores complete encrypted stored operations, including atom, molecule, counter, and author-clock changes.
Its existing TAIL file records the complete batch boundary.
The candidate uses these files before the ack. It does not promote or duplicate the cloud mutation log.
The cloud mutation log and its cloud purge rules stay unchanged.

On macOS, the native commit flushes every dependency before the TAIL publication barrier.
F_BARRIERFSYNC orders those writes before the TAIL rename.
The parent directory then flushes, and F_FULLFSYNC completes before memory publication and success.
An error poisons the writer and prevents success. There is no weaker fallback.
Other platforms retain the existing per-file durability barriers.
Apple documents these operations in its [fcntl manual source](https://github.com/apple-oss-distributions/xnu/blob/main/bsd/man/man2/fcntl.2).

After the samples, the driver checks all latest values by key and SHA-256.
It sends a final write and sends SIGKILL immediately after that write's ack.
It then reopens the same generated home and checks all expected values again.

| Run | Pending native batches at forced stop | Expected values after restart | Result |
|---|---:|---:|---|
| Pilot | 8 | 51 | All match; the next author clock advances |
| Small write | 1 | 501 | All match; the next author clock advances |
| Small write repeat | 17 | 501 | All match; the next author clock advances |
| Larger write | 13 | 501 | All match; the next author clock advances |

`stopped-boundaries.json` records valid CURRENT and TAIL checksums and their different batch boundaries.
The pending batches confirm that restart exercises the existing log recovery path.
All final shutdowns return exit code 0 and `flush_ok: true`.
No physical power-loss check occurs. Process-stop recovery alone does not establish power-loss behavior.

## Remaining costs and limits

The small-repeat native counters record 1,091 full device flushes over the sample interval.
Those flushes take 6.106 seconds in total.
The 1,226 order barriers take 1.432 seconds. The bounded file-set flushes take 0.171 seconds.
These totals include background work; they are not individual request timings.
The request receipts report a 2.085 ms median for preparation in that run.
The normal disk-file publication can also delay the next write through the shared write gate.

The ack has no collection timer. The normal disk files can finish after the ack.
That later publication uses the existing write queue, with a 250 ms deadline and capacity triggers.
The existing journal keeps its byte, batch, dependency, and age limits.
The queue admits at most 128 requests and refuses work that exceeds its complete operation capacity.
Disk-space reservations stay owned until complete publication or recovery.

The existing journal rotates after complete disk-file publication.
Its old payload files can remain because the normal disk records still reference those bytes.
Existing compaction and safe file reclaim remove files after all required references permit removal.
This candidate adds no separate log-retention policy.

The memory protection path still applies until the normal disk files finish.
Ordinary reads do not resolve an arbitrary key directly through the recovery log.
Thus, a durable ack alone does not yet permit the purge of every write record.
Unknown or incomplete typed-memory coverage waits for normal disk-file publication before the ack.
The change-feed worker also waits for that publication before it advances its tip or wakes subscribers.

The experiment requires both `LASTDB_NATIVE_LOG_ACK_EXPERIMENT=1` and isolated-node mode.
It refuses an attempt to enable the experiment without isolated-node mode.
Initialization and unsupported write shapes keep the existing complete disk path.
The measurements cover this schema and workload only. They do not establish all write-shape or concurrency behavior.
The candidate remains unmerged because it misses the latency target and retains these limits.

## Code checks and cleanup

The optimized product build and format check pass.
Atom-counter, write-path store, byte-slice, URL, and sensitive-log-field source checks pass.
Clippy for laststore, core, and node passes with warnings denied.
The committed file-size, function-size, and new-allow checks pass.
After the measurements, a Clippy fix changes only the error helper's borrowed argument.
It does not change the successful write path.
The executable hashes describe the measured binaries before that fix.
Two retired mutation-probe scripts are removed under the repository's no-tests rule.
No tests, mutation probes, fixture suite, or coverage gate run.

All four owned daemons stop before cleanup. Their shutdown receipts remain in this folder.
The exact scratch path, realpath, owner, and process state are checked before removal.
All generated database homes and owned scratch logs are removed after the receipts are saved.
No primary data, archive, shared service, or other agent's data participates.

# LastDB durable write latency: target not met

Date: 2026-10-21. Base source: `c70e6fdc7810bb6291a2564ad8157d7938de0494`.
Branch: `kanban/write-under-10ms-20261021`. No push, PR, merge, or primary change.

The final 1,000-request measurement averaged **13.627 ms**. The target is an average below 10 ms.
All 1,000 writes succeeded with durable receipts. The candidate remains disabled by default.

## Final measured result

| Requests | Count | Average, ms |
|---|---:|---:|
| Create | 500 | 12.595 |
| Update | 500 | 14.660 |
| Combined | 1,000 | **13.627** |

| Combined statistic | Milliseconds |
|---|---:|
| Minimum | 7.427 |
| Median | 13.671 |
| 95th percentile | 20.879 |
| 99th percentile | 27.047 |
| Maximum | 40.410 |

202 of 1,000 requests finished below 10 ms. The arithmetic average includes every request and every outlier.
`tuned-mixed-4096.json` contains the raw samples, receipts, expected value hashes, and native status before and after the run.
`summary.json` contains the short result. `iterations-summary.json` lists the earlier measurements.

## Method

The measurement uses the real optimized `lastdbd`, through its owner Unix socket, on this Mac.
Each request contains one mutation. This measures the ack latency for one write.
The client sends 500 creates, then 500 updates, with no pause between the two sets.
Each value contains 4 KiB of generated text. The record also contains its hash and range fields.
Updates address 128 existing keys under the same hash. The schema has three fields.

The clock starts before client JSON encode and stops after response JSON decode.
Each request includes a new Unix socket connection. The server closes each connection after its response.
The node boot and the local schema setup precede the measurement. The run excludes no write samples.
No other build or lint command runs during this final latency measurement.

The source build uses `dev-release`: optimization level 3, 16 code units, and no link-time optimization.
`build.txt` records the measured executable hash and the Rust version.
The final source also contains equivalent lint edits after the measurement.
`verified-build.txt` records the executable hash after those edits.

The candidate uses these settings:

- `LASTDB_FAST_DURABLE_EXPERIMENT=1`
- `LASTDB_ISOLATED_COPY=1`
- `LASTDB_DEPENDENCY_HIGH_WATER=128`
- `LASTDB_AUTO_MAINTENANCE_START_DELAY_SECS=0`
- `LASTDB_UDS_WORKERS=2`
- `RUST_MIN_STACK=67108864`
- Keychain and recovery phrase output disabled for the generated home.

The node uses a new generated home under the temporary directory. It never opens a primary copy or the archive.
The primary, brain, situations, and kanban do not participate.

## Candidate change

The candidate appends one complete stored mutation to a bounded disk log.
It starts the required disk sync and publishes the complete memory update.
The ack waits for both operations. The foreground path has no collection timer.
The stored record includes the author clock, atom counters, and other staged native changes.
Stored field bodies retain the existing encryption.

On this Rust build, `File::sync_data` calls macOS `F_FULLFSYNC`.
The candidate uses that call directly, with no weaker fallback.
Native checkpoints run after log acceptance. They retain newer memory records while an older prefix completes.
A checkpoint starts at 128 queued scopes or after an idle period. It does not authorize an earlier foreground ack.

The run records 1,006 disk log syncs, with 7.888 seconds of total sync time.
The mean per sync is 7.841 ms. This count includes background work.
The reported mutation preparation phase averages 2.169 ms per request.
The complete client request still exceeds the target. These component totals overlap and do not form a serial sum.

The final status reports automatic native maintenance active, with zero completed maintenance runs at the last measured ack.
This run does not establish steady-state compaction throughput. The maintenance work can wait for accepted writes to drain.

## Recovery result

The live query before the stop returned all 500 expected latest values.
A final default write omitted the durability parameter and returned a durable receipt.
The client sent `SIGKILL` immediately after that ack, without a flush request or a graceful stop.

The same generated home reopened with a crash restart cause.
A real keyed query returned all 501 expected records and matching value hashes.
The next write used a larger author clock revision.
The final graceful stop returned exit code 0 and wrote a successful native flush receipt.

Receipts: `tuned-memory-read.json`, `tuned-stop.json`, `tuned-recovery.json`, and `tuned-shutdown.json`.
The generated homes and owned scratch files are removed. No owned daemon remains.
A process stop checks recovery from the disk log. No physical power-loss check occurred.

## Earlier observations

The raw device measurement used 256 distinct 4 KiB writes, each followed by `F_FULLFSYNC`.
Its average was 7.918 ms. `physical-durability.json` retains those samples.
That device measurement excludes LastDB work and does not establish the complete request target.

The first candidate averaged 13.275 ms for 500 creates and 25.611 ms for 500 updates.
It retained too much memory metadata after prefix checkpoints.
Later changes release completed memory owners, reduce record encode costs, and avoid redundant syncs from old background timers.
Earlier samples use different candidate versions and generated homes. The final 1,000-request run is the result for this attempt.

The rejected full-checkpoint approach waited for multiple file and directory syncs before each ack.
Its earlier debug-build measurements are not a controlled comparison with this optimized candidate.

## Validation and limits

The optimized product build, package Clippy checks, format check, and relevant source lints pass.
The checks include atom counter writers, byte slices, URLs, sensitive log fields, and the write path store rule.
The source size and new-allow checks also pass. No tests or fixture suite runs.

This is an isolated opt-in prototype with a fixed 64 MiB log and no log reuse.
It refuses a configured disk cap because the log does not yet join that quota ledger.
A different node format marker prevents an older node from opening the experimental home.
A torn or corrupt log tail causes an explicit recovery refusal. Tail repair needs more work.
Cloud behavior, log reuse, power-loss faults, and the other write forms lack operational proof here.
The candidate is not ready for production.

The next work must reduce the remaining write preparation and native read costs, then repeat this same measurement.
The under-10 ms average remains unproven.

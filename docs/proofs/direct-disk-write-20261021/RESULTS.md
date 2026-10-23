# Direct disk write: target not met

Date: 2026-10-21. Source base: `c70e6fdc7810bb6291a2564ad8157d7938de0494`.
Branch: `kanban/write-under-10ms-20261021`. No push, PR, merge, or primary change.

The candidate removes the separate 64 MiB experimental log.
It completes the normal native disk files and the memory update before each ack.
The final 1,000-write average is **68.565 ms**. The average target remains below 10 ms.

## Result

| Run | Requests | Dependency high water | Average, ms |
|---|---:|---:|---:|
| Earlier binary, normal path, explicit durable request | 20 | 128 | 376.211 |
| First direct candidate, one order command per file | 1,000 | 128 | 149.966 |
| Final direct candidate, combined file flushes | 1,000 | 4,096 | **68.565** |

The final run uses the normal dependency high water of 4,096.
The first direct run uses the earlier experiment's value of 128.
These settings cause different maintenance costs. The table does not isolate a code-only speedup.
The earlier binary contains the disabled experimental path; its flag stays unset during the baseline run.

| Final statistic | Milliseconds |
|---|---:|
| Create average, 500 requests | 55.883 |
| Update average, 500 requests | 81.247 |
| Combined average | 68.565 |
| Median | 41.847 |
| 95th percentile | 212.603 |
| 99th percentile | 320.718 |
| Minimum | 20.317 |
| Maximum | 1,025.812 |

All 1,000 writes succeed with durable receipts. None finishes below 10 ms.
The final status records two complete native maintenance runs and no maintenance error.
The last maintenance run takes 8,177 ms. The measurement includes any delay that affects a request.
The final status reports zero dirty warm records and zero accepted batches.
This final observation does not establish a maximum memory size throughout the run.

## Method and receipts

The real optimized `lastdbd` runs on this Mac with a fresh generated home.
The run uses 500 creates, then 500 updates, with 4 KiB values and three schema fields.
Updates address 128 keys. Each request uses one owner Unix socket connection.
The clock includes client JSON encode, connection, request, response, and response JSON decode.
The run omits the durability parameter. Every response must still report durable completion.
No build or lint command runs during the latency measurement.
Boot, local schema setup, and later recovery checks stay outside the write samples.

`grouped/writes.json` contains every raw sample and the before/after native status.
`grouped/inputs.json` records the measured executable SHA-256 and generated home path.
`grouped/memory-read.json` confirms all 500 latest values by their SHA-256.
The driver sends a final default write, then sends SIGKILL immediately after its ack.
`grouped/forced-stop.json` records that receipt and signal.
The same generated home reopens. All 501 expected values match and the next author clock advances.
`grouped/recovery.json` records the result. `grouped/shutdown.json` records final exit code 0.
No physical power-loss check occurs.

The first direct candidate also passes the value and forced-stop recovery checks.
Its separate receipts remain in `direct/`. The small baseline receipts remain in `baseline/`.

The settings retain isolated-node mode, no keychain, no recovery phrase output, two socket workers,
zero automatic-maintenance start delay, and a 64 MiB Rust stack setting from the earlier measurement.
The final dependency high water changes to the product default of 4,096, as noted above.
No primary, archived database, shared schema, or cloud setup participates.
All generated homes and owned scratch files are removed after the receipts are saved.

## Candidate changes

Native request scopes no longer collect writes for 500 ms or return a memory-only ack.
They complete the existing native publication before success.
The periodic native flush task stays disabled because native writes finish synchronously.
Plain create/update requests put their author clock in the same complete native write.
Other queued author-clock writes merge the durable maximum under the native write gate.

The native disk format, its normal recovery files, its quota ledger, and its read route stay in use.
The candidate adds no second disk log, format marker, log size cap, or log replay route.
The previous prototype remains available in local commit `75261082d` and its earlier proof folder.

On macOS, one synchronous native commit combines independent file flushes.
It holds at most 32 transient file handles and uses at most four file-sync workers per flush.
Before each visible CURRENT, TAIL, or archive rename, it flushes the private dependencies.
It then uses F_BARRIERFSYNC to order those files before the rename.
At the end, F_FULLFSYNC completes before the native commit returns.
An unsupported barrier or a final-sync error poisons the writer and refuses success.
Other mounts and other platforms keep their existing full file barriers.
Native maintenance retains its existing durability path; it is not part of this optimization.

Apple documents the order and final device-drain guarantees in its
[fcntl manual source](https://github.com/apple-oss-distributions/xnu/blob/main/bsd/man/man2/fcntl.2).
The relevant operations are F_BARRIERFSYNC and F_FULLFSYNC.
An order barrier alone does not authorize the ack.

## Existing mutation log

The existing `MutationEnvelope` retains full field values, keys, mutation identities, and signed author clocks.
`strip_sot_field_values` is now a compatibility no-op. Older comments that claim body removal are stale.
The existing capture path currently runs after the local write and requires a sync engine.
Its purge rule uses confirmed cloud frontiers.
The log itself writes through the existing namespaced disk store.

That log is a possible place to reuse the recovery record instead of a second log.
It is not yet the local write-before-ack boundary.
Reuse needs a local append before the ack, recovery before normal reads, complete accepted-write replay,
and retention until both local disk completion and required cloud delivery permit removal.
An upload receipt alone cannot authorize removal of local recovery work.
The direct candidate does not implement those changes or enable cloud sync.

## Validation and limit

The optimized product build, Clippy for laststore/core/node, and the format check pass.
Atom-counter, write-path store, byte-slice, URL, and sensitive-log-field source checks pass.
The committed source size and new-allow checks also pass.
No tests or fixture suite run.

The direct path passes the measured process-stop recovery case and misses the latency target.
The earlier 13.627 ms result belongs to the removed second-log prototype.
Neither result establishes the latency of reuse of the existing mutation log.
The existing-log option needs its own implementation and operational measurement before a merge.

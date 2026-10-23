# Durable write lock scope — 2026-10-21

Product source: `2a200f30260eee403e273bb00d582d988af75599`.
The branch stays local and unmerged. The primary and shared systems stay unchanged.
All database homes contain fresh generated data. No primary data or archive enters these runs.

## Change

The previous captured-prefix path prepares lookup files outside the logical write gate.
It then holds that gate and the journal mutex through the final CURRENT and TAIL disk publications.
One full device flush follows both publications before the gate opens.

The new path publishes CURRENT outside both locks, after the immutable lookup files reach disk.
An opaque token retains the captured prefix and the writer lease through that publication.
Later writes can append to the existing journal during this disk flush.
They still finish their own full device flush before the memory update and ack.

The final gate hold joins the durable CURRENT to the writer and the logical memory state.
It completes only the captured prefix. Later writes keep their values, quotas, and path protection.
This step performs no disk I/O.

The writer trims its TAIL interval in memory at that step.
The next ordinary append publishes that trimmed interval through the existing durable append path.
The disk TAIL can retain an older published prefix until that append.
Existing recovery starts after CURRENT and already accepts such a retained prefix.
No file format, journal, or recovery implementation is added.

## Safety boundaries

- The captured publication remains the sole CURRENT owner. Other publication and maintenance APIs refuse an active token.
- A lost token marks the active owner as abandoned. Admission then requires recovery.
- Journal appends retain their former boundary until CURRENT completes its full device flush.
- The final join holds the logical gate and journal mutex. A generation-bound reservation cannot cross it unconsumed.
- A trimmed TAIL reaches disk only after its preceding CURRENT is durable.
- Existing suffix, dependency, disk, and memory caps stay unchanged. The queue retains its complete prefix owners until delivery.
- Foreground read/modify/write requests still share the logical write gate. Compaction and file reclaim retain their existing exclusive path.

No claim here removes the remaining foreground or maintenance serialization.
No physical power interruption forms part of these operational checks.

## Measurement method

The control and candidate use the same optimized `lastdbd` executable and fresh generated homes.
`--serial-install` selects the prior final-switch path with the disk publications inside the gate.
The reported native status must match the requested mode.

Each client request uses the real Unix socket and requires a durable receipt.
The durability parameter stays absent, which checks the default ack path.
Latency includes client JSON encode, socket connect, and response decode.
Each run contains equal create and update counts. Updates use 128 known keys.
No sample or initial write is excluded. Native automatic maintenance stays enabled at 4,096 parts.
No owned build or lint process runs during the measurements.

The host is shared. Other `lastdbd` processes and a native disk-maintenance command were active during these runs.
Their resource use is uncontrolled. Total latency cannot support a clean before/after causal claim on this host.
The trace separately records the disk-publication phase, gate admission, and final memory-switch interval.
The new disk-publication phase includes its worker dispatch and disk barriers.
The gate hold runs from the end of `directory_join_wait` to the end of `directory_install`.
It includes the dispatch to the blocking worker. The trace calls exclude their small final release overhead.
The `directory_install` elapsed value alone measures only the worker body and excludes dispatch.
Process-wide disk-sync counter deltas can overlap concurrent phases; they are not exclusive phase costs.

`analyze.py` retains only the numeric native trace. It reports foreground gate admissions inside the CURRENT publication phase.
The raw daemon logs stay outside the receipts and are removed after the owned homes stop.

## Lock measurements

All values below are milliseconds. The gate hold includes worker dispatch.

| Run | Prefix count | Gate hold median | Gate hold P99 | Gate hold maximum | Memory-switch body median |
|---|---:|---:|---:|---:|---:|
| Serial control before | 123 | 24.673 | 54.535 | 54.664 | 24.651 |
| Narrow gate one | 131 | 0.204 | 5.189 | 6.287 | 0.172 |
| Serial control after | 167 | 40.246 | 101.197 | 142.700 | 40.103 |
| Narrow gate two | 104 | 0.182 | 2.033 | 5.463 | 0.1545 |
| Narrow gate, 3,000 writes | 1,280 | 0.167 | 5.269 | 7.000 | 0.133 |
| Narrow gate, 4 KiB values | 111 | 0.173 | 4.989 | 5.161 | 0.149 |

The earlier chat numbers of 0.15–0.17 ms measure only the memory-switch body.
The complete measured median hold is 0.17–0.20 ms, with dispatch included.

Across the four narrow runs, 136 foreground scopes acquire the gate inside a CURRENT publication phase.
There are 625 foreground `journal_accept` intervals that overlap those phases.
Both serial controls report zero for these measures.
All 1,626 narrow memory-switch phases report zero full-device-sync time.
The source also places every disk operation before the final gate acquisition.

This demonstrates narrower exclusion during the background disk switch.
It does not demonstrate concurrent preparation of two foreground logical writes.

## Request latency and remaining delays

All values below are milliseconds. All six runs use the same executable hash, recorded in `matrix.json`.

| Run | Writes | Value bytes | Median | Average | P99 | Maximum |
|---|---:|---:|---:|---:|---:|---:|
| Serial control before | 1,000 | 64 | 37.766 | 41.417 | 109.907 | 391.432 |
| Narrow gate one | 1,000 | 64 | 40.252 | 43.998 | 123.248 | 347.326 |
| Serial control after | 1,000 | 64 | 54.996 | 75.782 | 190.254 | 7,236.780 |
| Narrow gate two | 1,000 | 64 | 31.153 | 33.039 | 80.392 | 138.760 |
| Narrow gate, longer | 3,000 | 64 | 47.615 | 95.725 | 524.259 | 14,332.602 |
| Narrow gate, large values | 1,000 | 4,096 | 33.477 | 34.769 | 91.268 | 292.229 |

The mixed first comparison and the shared disk load prevent an end-to-end P99 improvement claim.
The large-value run also briefly overlaps the preceding numeric trace analysis; it is a correctness and scope check.

The longer run exposes the remaining delays:

- Its gate-wait P99 is 117.982 ms. Its maximum gate wait is 14,286.440 ms.
- The worst request takes 14,332.602 ms. Its own journal phase takes 34.280 ms.
- Three automatic compactions finish without failure. The last complete maintenance run takes 27,179 ms.
- Compaction and reclaim still use the exclusive gate. This trace does not identify every gate owner or its exact overlap.
- The post-journal wait P99 is 244.678 ms. Existing requests without a complete memory publication still await lookup publication and delivery.
- Of 764 requests above 80 ms, the largest component is the post-journal wait for 616 requests.
- The journal phase is the largest component for 46 requests; gate wait accounts for 91, and work outside the native scope accounts for 11.

These delays remain unresolved. Do not infer an end-to-end latency guarantee from the shorter background gate hold.

## Durable recovery and memory

Every one of the 8,000 measured client writes receives a durable success receipt.
All six known-key reads match the last acknowledged values before the forced stop.
The five short runs recover all 501 expected values, including the final acknowledged sentinel.
The longer run recovers all 1,501 expected values. The next write advances the author clock in every run.
All six graceful shutdowns return zero and record `flush_ok: true`.

`narrow-one` stops with six complete journal batches beyond CURRENT. Recovery includes them all.
`narrow-two` completes an explicit flush before SIGKILL, then restarts with no pending batch.
Its disk TAIL still starts at batch 1,104; CURRENT and TAIL both finish at batch 1,113.
That retained old prefix passes the existing decoder and startup recovery without a new recovery path.

All six runs report zero core-memory or model-metadata refusals.
Peak model metadata is 13,248,736 bytes against the unchanged 16,777,216-byte cap.
The longer run also completes three compactions without a maintenance failure.
The receipts and stopped-boundary checksums remain in the run folders and `matrix.json`.

## Code checks

The optimized product build, format check, and package-scoped Clippy check pass.
The file-size, function-size, new-allow, atom-counter-writer, write-path, byte-slice, redaction, and URL checks pass.
The receipt helper syntax check passes. Details are in `checks.json`.

No tests, fixtures, mutation probes, or coverage checks run.

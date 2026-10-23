# Independent-schema write investigation — 2026-10-21

The work stays local and unmerged. The primary, archive, and shared systems stay unchanged.
All calls use fresh generated homes and the real optimized daemon over its Unix socket.
The mutation log and disk-before-ack guarantee stay unchanged.

## Scope

Tom requested an investigation of narrower locks and a P99 workload across different schemas.
This change adds numeric diagnostics. It does not replace the write locks or claim a speed improvement.
No test suite, fixture, mutation probe, primary copy, or primary control occurs.

## Workload

Each run declares 64 schemas with distinct names, descriptive names, and field descriptions.
Every schema has the same three field names and a hash plus range key.
Each client owns a disjoint set of schemas and rotates through that set.
Consecutive calls from one client use different schemas. Concurrent clients never use the same schema.
All requests use the real `/api/mutations/batch` route and require a durable receipt.
The client omits the durability parameter, so the default path must supply that guarantee.
Each eight-record batch stays in one schema and changes eight known keys.
The first call per schema creates the records. Later calls update them.
No initial client sample is excluded.

The one-record run supplies the small-write comparison. Its requests still use all 64 schemas.
Latency measures the whole batch call, from client JSON encode through response decode.
P99 is the nearest-rank 99th percentile across all 1,000 client samples in each run.
No batch percentile is divided by its record count.
The default 4,096-file compaction threshold and all memory caps stay unchanged.

Every run compares the final values by exact keys in batch reads.
It then writes one sentinel, receives its durable ack, and stops the owned daemon with SIGKILL.
After restart, every expected value must match and the next write must advance the author clock.
The final graceful shutdown must report a successful disk flush.
These are process-stop checks, not a physical power interruption.

## Final measurements

Product source: `b06480448c9f99a7c68e91a594413d3d824eb72b`.
Executable SHA-256: `c7af7cba376e90fc6546fe7a86a4e8eaf5fbf3aa7ec8e8d5fc9c58caea50e4ed`.
All three final runs use eight request workers and the same executable.
The schema metadata confirms 64 distinct schema identities, 192 distinct field identities, and 192 distinct molecules.
The client intervals show no same-schema overlap.

| Current implementation | Records per batch | Concurrent clients | Calls | Typical ms | Average ms | P99 ms | Maximum ms | Batches/second |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Small writes across 64 schemas | 1 | 1 | 1,000 | 14.04 | 14.94 | 24.18 | 26.98 | 66.87 |
| Batches across 64 schemas | 8 | 1 | 1,000 | 23.76 | 27.56 | 111.30 | 805.94 | 36.25 |
| Batches across 64 schemas | 8 | 4 | 1,000 | 82.63 | 105.50 | 309.32 | 393.57 | 37.80 |

Typical means the median. These are results for the current locks, not for a replacement lock design.
The final calls contain 17,000 record mutations. Every call returns a durable receipt.
All 1,091 final records match after forced-stop recovery across those three homes.
Every next write advances the author clock, and every final shutdown flush succeeds.
The five preliminary runs add 5,000 calls and 33,000 record mutations. All of their receipt and recovery checks also pass.
No final or preliminary run reports a refused client request or a maintenance failure.

### Exact wait split for four clients

| Native phase | Median ms | P99 ms | Maximum ms |
|---|---:|---:|---:|
| Write admission, including both wait types | 59.81 | 285.31 | 367.33 |
| Actual write mutex wait, summed across admission attempts | 59.19 | 96.69 | 129.94 |
| Sealed-prefix completion wait with the mutex free | 0.00 | 204.70 | 237.37 |
| Preparation while the global mutex stays held | 11.13 | 12.35 | 21.37 |
| Journal acceptance, including its durable write | 8.86 | 12.83 | 19.81 |
| Whole global hold through journal and memory acceptance | 19.96 | 24.64 | 35.45 |
| Wait after journal acceptance | 0.02 | 183.38 | 237.39 |

The percentiles describe different distributions. They must not be added.
The native admission interval also includes a small amount of control and trace overhead.

The actual mutex still serializes preparation across independent schemas.
About 11 ms of the protected step is preparation. This identifies an opportunity, not proof that all that work can move.
The queue also seals when accepted work requires complete lookup publication or reaches a resource boundary.
That wait remains after the caller releases the mutex.
See `fold_db/crates/core/src/storage/disk_pack/queue.rs:43`.

### Memory pressure and disk waits

The eight-record workloads reach the existing 16 MiB dirty-model metadata cap.
The one-client run records nine internal model reservation refusals; the four-client run records 33.
The peak stays below the cap. The caller receives no resource error.
The existing fallback requires complete disk lookup publication before the ack.
See `transaction/request.rs:249` and `write/native_paths.rs:33`.
Ordinary lookup preparation takes up to 362 ms in the one-client run.
The request can wait for that work even though the file work does not hold the global mutex.

The worst one-client call takes 806 ms, with only 12 ms in write admission.
Its journal acceptance takes 780 ms. The process-wide counters also record a long ordered disk barrier in that interval.
Two other slow one-client calls wait 317 ms and 233 ms after journal acceptance.
Narrower data locks do not remove either disk cost.

The small-write run records no sealed-prefix wait and no model reservation refusal.
Its P99 is 24 ms across the same 64 schemas.
The large difference between these workloads cannot be described as contention on one schema.

## Source findings

The outer request gate covers the first read, all private preparation, the journal append, and the memory update.
See `fold_db/crates/core/src/storage/disk_pack/transaction/request.rs:42` and its `log_scope` owner.
Existing locks per tip and molecule sit inside that broader boundary.
They cannot permit parallel request preparation while the outer gate remains exclusive.

The atom counter planner reads old tips, old counters, and body state before it writes the complete batch.
See `fold_db/crates/core/src/db_operations/atom_store/counter/commit.rs:165`.
Molecule headers also contain shared reference counts, read and changed in the same request.
See `fold_db/crates/core/src/db_operations/atom_store/counter/molecule.rs:122`.
Proteins can make different schemas change the same molecules and atom counters.
Every native batch also writes the node's author-clock metadata.
Separate schema locks therefore do not establish independent storage changes.

The private read view captures the current disk root, accepted writes, and dirty memory.
It relies on the request gate to keep that state valid through preparation.
See `fold_db/crates/core/src/storage/disk_pack/read.rs:71` and `transaction/publication.rs:122`.
The existing source certificates do not constitute a complete conflict-validation protocol.
They cover optional, bounded call-local source facts, not every absent key, range read, shared counter, or metadata dependency.
The request scope also accepts a future that runs once; a stale preparation cannot simply run that future again.

Physical file identity is not a stable logical lock key. Compaction changes file placement.
One mutation log and one durable CURRENT boundary still order storage publication across schemas.
Separate file locks cannot replace that order while the existing recovery format stays unchanged.

## Diagnostic correction

The original `gate_wait` event measures write admission. It includes two different waits:

1. Acquisition of the global write mutex.
2. Completion of a sealed write prefix, while that mutex is free.

The new opt-in events separate `write_mutex_wait` and `sealed_prefix_wait`.
A numeric admission identity links every retry to its parent admission event.
The trace contains no schema, key, value, or user identity.
The normal path keeps its existing order, queue limits, disk barriers, and ack rules.

The receipt field `resident_stages_us.gate_wait` is explicitly set to zero by the current product path.
That value does not prove that the global mutex has no wait.
The investigation uses the numeric native events instead.

The analyzer pairs each journal append with its last preceding request gate acquisition.
The global mutex makes that pair exclusive. Its journal sequence identifies the complete native scope.
Separate admission identities identify the mutex and sealed-prefix components, even when client calls overlap.
The analyzer requires one complete foreground native scope for every client call.
It does not claim a one-to-one match between an overlapping HTTP interval and a native scope.
Disk-sync counters are process-wide; concurrent counter deltas are not exclusive costs.

## Preliminary runs

The first five runs use product source `d054bf837` without the extra admission events.
Their executable SHA-256 is `cf1c951928c5c3fc9c9feaaa1c6476a9f15fb178185787e6a87e8362e260fcb6`.
The inherited two-worker setup appears in `one-client` and `four-clients`.
The eight-worker controls appear in the folders with that prefix.
Those controls show why the final comparison must keep the worker count fixed.
No difference between those controls is attributed to a product speed fix.

The first requested scratch name exceeds the macOS Unix socket path limit.
The daemon refuses that path before startup. `setup-path-refusal` preserves the refusal.
The capture helper now checks that explicit path before it starts a daemon.

## Safe direction for a later lock change

Parallel preparation remains useful for independent batches.
The safe boundary uses actual tip, molecule, atom-counter, blob-counter, protein, and metadata dependencies.
A schema name alone does not describe that set.

The next implementation needs these parts together:

1. Capture an immutable request view and all relevant dependencies.
2. Prepare independent work outside the global gate.
3. Take the required dependency locks in a fixed order.
4. Validate every state dependency against the current state before publication.
5. Rebuild a stale plan from its original request, with no external side effects from the discarded plan.
6. Keep shared counter changes and author-clock state correct under the final commit order.
7. Retain the batch's memory and disk capacity owners through the durable disk write and memory update.
8. Return the ack only after that complete boundary.

A request without complete dependency coverage must retain the current serialized path.
This investigation does not substitute a schema mutex for those guarantees.

## Checks and disposition

The optimized daemon build, Clippy, format, size, new-allow, byte-slice, redaction, URL, and write-path checks pass.
The atom-counter writer boundary check and all four receipt-helper syntax checks pass.
`checks.json` contains the commands. `matrix.json` contains the complete run summaries and receipt checks.
Only the numeric opt-in diagnostics change in product code. Lock order and durable ack behavior stay unchanged.
No push, PR, merge, primary change, or shared-system change occurs.
Cleanup receipts record the owned scratch paths, node checks, and final removal.

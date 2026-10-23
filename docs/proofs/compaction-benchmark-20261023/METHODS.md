# Paired compaction benchmark

The comparison isolates PR #275. The baseline is its parent, `c6b4f7ed9`.
The candidate is its merge, `c94ec7760`. Later main changes remain outside the comparison.
CAUTION: The write modes below describe those two historical revisions.
Later commit `0deeafc5e` requires disk completion before every native write ack on main.
The normal-write rows do not describe the current default behavior.
Both executables use the same Rust toolchain and optimized `dev-release` profile.
The build receipts retain the complete revisions and executable hashes.
Each build first removes the cached `laststore`, `fold_db`, and `lastdb_node` artifacts.
The build log must show a fresh compile of all three crates.
The executable must contain the expected compaction markers.
Each actual node must expose the expected compaction status before the seed phase.

## Workload

Three pairs use the order before/after, after/before, and before/after.
Each run starts a fresh generated home with 100,000 records, four fields, and a 1,024-byte body.
One HashRange schema uses one hash and 100,000 range keys.
Seed requests contain 128 records and require durable acknowledgements.
The homes use identical logical data and request recipes; their identities and physical bytes differ.
No primary data or archive data enters a run. No primary program or shared configuration changes.

Each home receives this sequence:

1. One full compaction without a writer.
2. A normal-write baseline: 100 batches at 20 batches per second.
3. A normal-write window: 400 batches at 20 batches per second, with one full copy and one cleanup caller.
4. One quiet full copy to normalize the next phase.
5. A durable-write baseline: 100 batches at 10 batches per second.
6. A durable-write window: 200 batches at 10 batches per second, with one full copy and one cleanup caller.
7. Two simultaneous full-compaction callers, without ordinary writes.
8. Exact reads of every seeded key and the complete product atom-counter diagnostic.

Each write batch updates 16 of 64 hot records. Both versions receive the same batch count, schedule, keys, and revisions.
The copy starts after five acknowledgements. Cleanup starts after the node reports its copy phase.
The two-copy phase uses a shared start barrier. The baseline may serve those callers in sequence.
The candidate may serve both at once. The measurement includes either behavior.

Normal writes request a memory acknowledgement. Durable writes request completion on disk.
The benchmark uses the normal flush interval. It does not force a flush inside a measured write window.
An explicit flush separates phases and precedes the final diagnostics.
Automatic maintenance remains off in both versions so it cannot introduce an unmatched extra copy.

## Measures

The main comparison uses caller-visible compaction time, cleanup response time, and write-response latency.
Each writer has a fixed offered rate. This is a latency benchmark, not a maximum-throughput benchmark.
Late dispatches remain visible; a slow version cannot reduce its work to improve its latency result.
The report separates all writer responses from responses whose call interval overlaps the compaction request.
Each run retains p50, p95, p99, maximum latency, and the largest acknowledgement gap.
Separate percentiles retain client dispatch delay plus RPC duration for all scheduled writes.
That sum excludes client payload construction and receipt bookkeeping.
The fixed write window can end before publication. It does not force a writer through every publication boundary.

The two-copy measure covers both requests from the first request start to the last response.
It also retains each request time and every capacity refusal.
A refused copy never counts as a fast successful copy.

The native lock timer has different scope across the revisions.
The old timer excludes the gate wait, queue drain, and read-view handoff.
The new timer includes the complete acquired-gate hold for successful publication.
It does not sum the holds from unsuccessful publication attempts.
Reported native timer values remain supplementary. Caller-visible response times supply the common comparison.

The resource sampler records the owned daemon once per second.
It records memory size and numeric file-handle counts, plus sample errors.
Those samples do not establish strict peaks between samples.
Seed and final whole-home diagnostic work remain outside the measured resource window.

## Controls and limits

All three pairs share one window under the existing host build lock.
No heavy build can share that measurement window.
Other host activity and filesystem cache remain uncontrolled. The pair order alternates to reduce order bias.
Every run requires at least 20 GiB of free temporary-volume space before it starts.
Three pairs support a local comparison, with per-pair results and ranges. They establish no general latency guarantee.
The benchmark includes no external file blobs, disk-cap transition, power-loss event, or path-budget exhaustion.
No tests run. The scripts issue actual product requests against generated homes.

The raw responses retain source and executable hashes, request hashes, timing samples, and complete diagnostics.
Each owned node stops. The supported cleanup command verifies ownership and removes each exact home.
Receipt compression occurs only after that cleanup, with a byte-equality check and retained hashes.

## Rejected attempts

An initializer path-type error stopped the first attempt before a node start.
The next attempt exposed an invalid shared Cargo-cache reuse.
Its candidate node reported the merge revision but still contained the baseline storage library.
The executable lacked all four new compaction markers and the new API status field.
The series stopped. Its files remain under `cache-reuse-invalid`, outside the accepted comparison.
The next series forces fresh crate builds and checks both executable and actual-node identity.

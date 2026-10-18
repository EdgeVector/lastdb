# Core-owned writes: proof and measurement plan

Status: final V6 source `7876211b…` and executable `c19a25ea…` pass all seven strict ordinary API workflows.
All eight matched cases finish. The [result report](RESULTS.md) retains the mixed measurements and explicit source-only route limits.
The V5 metadata allowance failure stays in its separate unchanged comparison.
The steps below retain the original proof method and the controls for a future reproduction.
The [route matrix](ROUTES.md) separates source coverage, ordinary API results, and remaining gaps.

## Reference program

Before is merged main `011b29f3a74eac3eb3a977678630c40345157ed4`, executable SHA `a0ad619076570fc472569712d5a40743a5a32823e909d45d4e33991b7c536285`.
[before-identity.json](before-identity.json) checks all 1,908 accepted product inputs against that immutable Git tree.
It also retains the accepted compiler, build settings, and preceding manifest identity.
[baseline-root.json](baseline-root.json) identifies a private executable in a new owned scratch directory.
The directory contains only that executable and its owner marker. It contains no database home and has no node sessions.

The [after identity](candidate-v6-identity.json) names all 1,931 final product inputs and the actual release executable.
Capture the final source before the build. Verify the same source after the build and after the workloads.
Do not reuse a historical source label for a newly built executable.
Keep the preserved before executable until the final result and source review accept the comparison.

## Required behavior

Logical create, update, delete, and scoped mutation routes publish their typed effects into the core model at the accepted write barrier.
A read must not use a separate `NativeWriteGraph` body or key lookup to find an accepted write.
The disk queue can retain private output bytes and reservation owners until flush. Those bytes do not become another read-visible state.
Both queued and durable writes use the common publication rule, with their existing ack and disk guarantees.
Reads that capture an earlier state must keep a complete earlier view and its charged owners until release.

Raw administrative and replay operations that cannot translate their bytes into a logical mutation remain private until their disk commit.
They publish the required core invalidation and complete-key changes before success. Their next value read can use normal disk fetch.
Inactive-store imports do not become active logical write-through routes merely because the source inventory includes their functions.
No route uses the removed raw buffer as a second read authority.

The source review must name each writer, its publication call, its reservation owner, and its invalidation or deletion rule.
It must distinguish core publication from a later read that refills the core from disk.
Record every fallback. A route is not complete merely because its final disk values read correctly.
Delete states and schema controls require the same visibility rule as positive values.
The supported query forms remain Hash and HashRange; this work adds no standalone Range query.

## Route proof matrix

Use new generated schemas and known keys. Batch each set of independent mutations and reads.
Retain every request, response, request ID, reported file cost, relevant status, and source identity.
The source agents will attach exact writer functions and ordinary API triggers to this matrix before the final run.

| Writer or behavior | Ordinary product action | Required result |
|---|---|---|
| New Hash record | One mutation batch with complete generated fields | The acked values read correctly before explicit flush and after reopen. |
| New HashRange record | One batch under known hashes and range keys | Exact and bounded membership agree; no unknown key enters the answer. |
| Warm update | Read known values, then update one batch | New values replace old values; untouched fields and keys remain correct. |
| No-prior-read update | Update known disk records after normal reopen | The core receives the result through the write route, before the first value read. |
| Durable write | Request durable mode on new keys and updates | Commit precedes its ack; the core receives the committed state without read refill. |
| Queued write | Request queued mode within the resource allowance | The acked state reads correctly before explicit flush; status identifies accepted work when it exists. |
| Delete | Warm a key, delete it, and query exact and bounded forms | Confirmed absence replaces the value; unrelated warm keys remain reusable. |
| Delete and recreate | Reuse one HashRange key for repeated batches | Values and membership agree; retained key capacity does not gain a charge each cycle. |
| Mixed batch | Create, update, and delete independent known keys in one batch | Read results contain a permitted complete committed state; no mixed field revision appears. |
| Protein propagation | Declare same-product schemas with different keys through the normal schema path | A shared-field change reaches each related molecule at its documented barrier; shared atom data stays correct. |
| Protein key move | Change a field used by a related schema's key | The old key disappears and the new key appears; shared fields remain correct after reopen. |
| Schema lifecycle | Create, read, change supported schema state, drop, and reinstall | Definitions, key bindings, and values follow the product's documented state. |
| Molecule-map change | Use the existing owner repair operation on independent generated molecules | The new mapping becomes visible; the old mapping cannot retain authority. |
| Scoped or shared write | Use the normal isolated app or scope API identified by the source inventory | The same core publication rule applies with correct scope and source precedence. |
| Replay | Export generated mutation logs, then replay through the existing owner route | The receiving node returns the new state immediately after the replay barrier and after reopen. |
| Import, retention, backfill, or administrative write | Use each existing product command named by the source inventory | A prior warm answer cannot hide a changed value, deleted key, or changed definition. |
| Raw native control write | Trigger the existing product operation that owns that control | Source changes reach the common core boundary; the read does not use the old raw buffer. |

The existing schema-map repair proof is not a substitute for protein propagation proof.
The source separates mixed-batch erasures from creates and updates. Same-key delete/recreate uses two separate acknowledged calls in this workload.
Hash and HashRange have independent delete membership under `mutation_manager/cas.rs::protein_delete_peers`.
Driver v5 therefore retains the HashRange value after a source Hash delete. Earlier drivers and receipts stay unchanged.
Shared-field updates and alternate-key moves retain their strict expected values and old-key removal gates.
The existing local replay route is `/api/db/replay-mutation-log`; export uses `/api/db/export-mutation-log`.
Both require a fresh guarded synthetic home and the local proof engine.
The reference native engine refuses activation at its backup/cloud gate before any replay call.
Use generated owner state through an existing product path. Do not derive either home from the primary or archive.
The source inventory must settle the exact protein, scoped, import, retention, and backfill commands before execution.
A route without an ordinary product trigger remains an explicit proof gap. Do not replace it with a fixture harness.
The discovery preserves that refusal without a bypass. Source review remains distinct from replay runtime proof.

## Read reuse and concurrent visibility

Retain the final completion workload as the first correctness pass:

- Repeated missing Hash and HashRange keys, then insert and delete.
- Unchanged keys after unrelated updates and deletes, before and after explicit flush.
- Equal-content records with shared UUIDs and alternating repeated reads.
- Constant membership after 24 same-key create/delete cycles.
- Schema lifecycle, molecule-map changes, normal reopen, and concurrent batched reads and writes.
- Singular and plural exact HashRange keys, bounded HashRange queries, and HashKey on a HashRange schema.

The after program must retain the existing zero-file-read gates where their complete-source conditions still hold.
New write-route checks capture status before the first value query, then record the query's values and reported file cost.
The source trace plus those observations distinguishes write publication from read refill.
The zero-file rules use a primed schema and complete keys within the admitted resource allowance.
Complete creates and replacements provide all requested fields. A partial update can still fetch an untouched field that no call loaded.
Do not require a zero-file-read gate from the before program for a capability this change adds.
Before and after must both return correct values for the same ordinary workload.

Use several concurrent readers and writers over independent known batches and over one shared revision cohort.
Accept only complete states permitted by the documented batch and capture boundary.
After a writer's ack, a newly started read must observe its accepted state at the same defined boundary.
An older read may finish with its earlier complete state. Its memory owner must remain charged until release.
Ordinary concurrency does not pause a call at an exact internal instruction. Record this limit instead of claiming deterministic old-view proof.

## Memory pressure and bounded writes

Run pressure separately from the timing comparison. Use one new generated home per resource configuration.
Use bounded mutation batches, repeated overwrites, deletes, and concurrent large keyed responses.
Repeated overwrites matter because a long read can retain older values after the writer flushes them.
Queue depth alone does not establish the bound on retained values.

Start with a small documented write allowance, if the existing product configuration supports it.
Use body sizes and batch widths within the normal request and response limits.
Increase concurrent calls in a fixed short sequence, then stop new writes and request an ordinary flush.
Record every accepted, completed, refused, and failed call. A refusal must match the product's documented resource response.
No refusal, timeout, or failed sample can disappear from the result.

Record the following observations before, during, and after each cohort:

- The write-memory charge, its limit, the peak charge, and any charge retained by older reads, when the product exposes them.
- Accepted and completed sequence values, accepted batch count, dirty and held paths, and writer failure state.
- Complete-key and definition charges, path count, purge count, and refusal count.
- The owned node's process-memory size as a separate observation.
- Ack duration, flush duration, final complete values, and the normal-reopen result.

Expected result: no accepted data becomes uncharged while a call still owns it.
At capacity, the product must wait, flush, or refuse before it publishes an unsupported allocation.
After readers finish and the queue flushes, retained charges must return to the documented steady state.
The writer must recover capacity and accept a final known batch.
Process memory includes allocator and runtime overhead. It is not identical to the charged allowance.
An unavailable reservation counter is a proof limit; process memory alone cannot establish correct reservation ownership.

The preserved v3 pressure uses 64 records with 32 KiB bodies. Its write quote can exceed the 16 MiB allowance.
The separately frozen v4 cohort uses 16 records, with the same bodies and allowance.
It records direct `core_model` metadata and body reservation counters at status and immediately after each overwrite ack.
Candidate acceptance requires a queued overwrite, a positive direct body reservation charge, and no retained body owner after all calls finish.
Clean current core slots can retain their charged owner after flush. Their current charge need not return to zero.
The direct peak must remain within the reported allowance. Published status samples can miss short intervals.

The old implementation uses a 1,024-request queue and a 128 MiB complete-prefix limit, with separate operation limits.
Those facts describe the before program. The final design must state its own bound and overhead before the after pressure run.

## Matched before and after measurements

Reuse the reviewed eight-case temperature driver without a change to its measured operations.
Each program runs with complete-key preload OFF and ON in two balanced rounds.
Use current-ON, before-ON, current-OFF, before-OFF, then the reverse order.
Every case uses a fresh generated home, 256 initial records per schema, four fields, and 1 KiB bodies.
The HashRange schema uses one known hash for comparability with the preceding reports.

Measure these conditions separately:

- First exact and bounded reads after normal process reopen.
- Repeated reads of the same keys and values.
- Different keys in the same process, after the first reads.
- New-key writes after reopen, updates without prior value reads, and updates after value reads.
- Ack time and total time through explicit flush, with the original status-call cost retained.

Hash widths 16 and 64 use batched independent queries. HashRange uses one query; width one uses the same endpoint.
Exact HashRange timing uses plural key pairs. The correctness workload also covers singular keys and HashKey on a HashRange schema.
First and disjoint groups have two observations each; repeated groups have ten.
Retain all medians, sample ranges, file-call counts, and material regressions.
Do not combine gains with the e10-to-v4 or v4-to-completion reports.

No timing starts until root confirms the frozen source, release identity, and a quiet Cargo window.
Hold the existing host lock across the full measurement run.
Do not run builds, source hashing, CPU profiles, or pressure workloads during that window.
Normal process reopen does not clear the OS file cache. File-call receipts do not count physical device reads.
A prior value read describes the caller's actions; it does not prove memory-only internal write preparation.
This small workload cannot establish maximum M5 throughput, a long-run memory bound, large-home performance, or cloud upload cost.

## Completion and cleanup

The final report must map every inventoried writer to its common core publication route and its ordinary product evidence.
Source review must find no remaining read-visible NativeWriteGraph lookup.
Any retained raw queue must state its private output purpose and charged lifetime.
All correctness failures and timing regressions remain visible until a corrected candidate receives a separate fresh run.
No final report can replace earlier samples or silently relax a gate.

Stop every owned node normally. Preserve exact request, source, executable, result, and cleanup receipts first.
Review exact paths, realpaths, owner markers, process state, open files, and sockets before supported reclaim.
Retain the before executable until the final report cross-review accepts the comparison.
Use the supported exact cleanup for each owned root; never remove primary data or another agent's root.
Keep concise reports, shared scripts, and identities in Git. Keep bulky raw originals local with exact hash and path records.

# LastDB logical warm set work plan

CAUTION: This plan records the October 18 implementation sequence and its earlier limits and write policy.
Use the [memory and disk routes](lastdb-rehydrate.md) and [native summary](lastdb-disk-native-node.md) for current behavior.
All native writes now wait for disk commit and clean memory publication. The warm set uses a byte limit.

Status: selected schema preload, shared core memory, and native storage merged in [PR 214](https://github.com/EdgeVector/lastdb/pull/214), 2026-10-18.
The [retention choice](lastdb-logical-warm-set-design.md#retention-choice) adds selected tips without values to the same logical warm set.
Calls that fetch values retain full records. Accepted writes remain complete through flush.
The [write report](proofs/core-owned-writes-20261018/RESULTS.md) records the earlier source, product checks, and matched measurements.
The [native cutover review](proofs/core-clarity-20261018/REVIEW.md) records the later default-engine change and source cleanup.
The [cutover validation](proofs/core-clarity-20261018/VALIDATION.md) identifies its separate source, release, and checks.
The [merge review](proofs/core-clarity-20261018/MERGE-REVIEW.md) records the final private-history cleanup and repeated fresh-data checks.
Native reads use the core model. The separate `NativeWriteGraph` is removed.
A bounded private queue retains prepared disk bytes until flush. Ordinary reads cannot use that queue.
Raw system writes finish on disk and clear affected core state before success.
The [write route inventory](proofs/core-owned-writes-20261018/ROUTES.md) names the earlier runtime evidence gaps and backend scope.
The package contracts below retain the wider targets; they do not establish proof for those gaps.
Tom selected complete range-key preload on the first query to selected schemas.
The [preload design](lastdb-schema-range-preload-design.md) defines that lifecycle.
The [logical design](lastdb-logical-warm-set-design.md) defines values, fields, and the common read boundary.
The [study](lastdb-schema-range-preload-study.md) preserves synthetic memory and native disk-component measurements.
The [fast range design](lastdb-fast-range-design.md) defines the disk-format target for first load and fallback.

## Target behavior

A selected HashRange schema loads all required membership keys before its first query continues.
Concurrent first queries share one load.
After readiness, ranges and sample select keys from the core logical set.
Tips and values remain partial and use the same warm paths and physical batches as exact reads.
No per-query saved answers, parallel warm graph, or new disk membership file is required.

The first exact query also waits when its schema explicitly selects this policy.
Its value lookup remains exact after the gate.
Hash-only exact schemas keep their current route.
Unselected schemas and schemas with a recorded preload refusal use the bounded disk selector.
A loading schema never supplies a falsely complete partial answer.

## Current state

The merged native source is `30c15b616ccdbd032e8e2d7d6c4c68d926e13aeb`.
The earlier branch `fix/core-owned-writes` includes native cutover source `a60b27d43f11fd6d1f3cf514d1579229d2256d5c`.
The later product commit `436cc86ad82c4ca2486e0b7f55e4a1b840dfeaed` corrects the public refusal's selector list.
The cutover results separate the full API workflows from that response-only check.
The earlier matched measurements use product source `7876211b6d64dff43ceddd602656017b74860fe9` and merged main `011b29f3a74eac3eb3a977678630c40345157ed4`.
Those measurements do not measure the later cutover.
The older reports below preserve the earlier preload and core-memory comparisons.
The candidate uses CoreWarmSet for partial molecules and full atoms.
Its least recently used list holds logical path references.
CoreKeyDomain now supplies complete candidate keys, with a separate byte owner and a shared first-query gate.
CoreWarmSet also retains current typed schema and molecule definitions, plus their required controls.
Queries share these objects and retain their memory charges through their full lifetime.
Accepted changes clear affected definitions before the new state becomes visible.
Confirmed absence uses the same dependency rules; read errors never enter memory as absence.
The [core memory report](proofs/core-memory-state-20261018/RESULTS.md) records fresh before-and-after measurements and the operational limits.
That report preserves the earlier v4 result. The [completion result](proofs/core-memory-completion-20261018/RESULTS.md) records the later fixes and final validation.
The final generated API checks pass. The comparison with v4 shows mixed timings and documented slower first and disjoint range reads.
It establishes the review fixes on a small generated home; it does not close the wider caller inventory or large-home proof.
Ready describes complete candidates; it does not promise an exact visible-record count.
Selected batches or current warm tips still resolve metadata, generation controls, and `rdel` delete barriers.

Each authorized warm path records every possible winning tip source, the required delete barriers, and its selected atom source.
An ordinary key change clears the paths that depend on that source, rather than all warm paths.
A structural replacement clears the affected molecule. An invalid source key still clears authority conservatively.
Confirmed missing keys enter the same logical set only after complete winner and barrier checks.
An incomplete receipt keeps the body available but requires the ordinary tip check.

Equal data can have different atom dates or source details.
Those full atom copies coexist at their selected keys; they do not remove each other's warm state.
Fully equal atoms still share memory. Equal content UUIDs alone do not establish full atom equality.

The preserved spike replay returns correct values for its measured workloads and respects its lowered path cap.
It does not establish every supported use case.
The earlier spike measurements found higher exact-read and write costs.
The core memory report gives the newer comparison against the preceding branch build.
The earlier candidate used `NativeWriteGraph` for accepted raw records.
The final native route removes that graph and its duplicate compatibility values.
The later cutover removes the legacy logical backend from normal startup and direct logical constructors.
Cloud stays disabled. An empty home initializes directly; no migration code is required.

The earlier preload candidate passes both 256-record generated Mini workloads, a selected 20,000-record workload, and separate key-budget and path-purge cases.
The [results report](lastdb-schema-range-preload-results.md) records the matched source identities, values, elapsed times, memory, disk size, and regressions.
Those larger receipts identify that earlier preload candidate. They are not rerun on the final completion executable `a0ad6190…`.
The completion report identifies its separate small generated workloads and all remaining scope limits.
The [before report](proofs/schema-preload-product-20261017/before-report.md) preserves the committed engine's 256-, 10,000-, and 20,000-record observations.
The earlier candidates remain diagnostic evidence.
Preload remains opt-in: lower range disk work does not establish a general elapsed-time improvement over the final disk selector.
The earlier memory prototype remains component evidence, not a complete product result.
No primary, archived home, or existing home supplies product data for this plan.

## Contract choices

- Ordinary reads support Hash and HashRange. Every normal range names a hash.
- Selected HashRange schemas use first-query preload across all required membership sources and hashes.
- Authorize the query scope and resolve schema bindings before the preload gate.
- One engine-owned load serves concurrent first queries and obeys explicit resource limits.
- Complete membership lives in the core logical model, with sparse tips and atom values.
- The full logical-record cap stays 10,000. Complete keys use a separate explicit byte allowance.
- The candidate defaults to a numeric 128 MiB key allowance per native runtime; `LASTDB_SCHEMA_KEY_BYTES` overrides it.
- Process-start `LASTDB_SCHEMA_PRELOAD_SCHEMAS` selects comma-separated schema names, identity hashes, or `*`; an empty value selects none.
- Known exact state can use current warm paths and typed definitions after the schema gate.
- Typed definitions use a separate 16 MiB allowance; retained older objects keep their charges until their last query ends.
- A definition that cannot fit this allowance is refused before any purge of other definitions.
- Active conflict histories retain their existing event reads. Unknown values and unselected range membership still require disk work.
- Complete source dependencies replace global tip invalidation for ordinary recognized writes.
- Confirmed missing states require complete winning-source and delete-barrier coverage, under the same captured read state.
- Different full atom copies with one content UUID coexist; each logical key retains its selected copy.
- A deleted key's later create reuses the retained key and tree allowance, rather than another charge for the same capacity.
- Ready range and sample calls do no disk key discovery.
- Fetch unknown tips, document-copy state, atoms, predicate fields, and sort fields in batches.
- Publish key changes with current logical writes before the ack.
- Preserve current protein propagation, source precedence, sample order, bounds, and preview cap.
- Physical flush and LastDB compaction preserve logical readiness.
- Logical replacement and incompatible definitions update or invalidate readiness before visibility.
- Reject history reads and unsupported key shapes at the common public boundary.
- Capacity or work refusal records a reason and selects disk fallback without a full retry on each query.
- Unknown never becomes absence or exhaustion through a budget stop.

The cold first-query load grows with all required schema keys.
This intentional lifecycle cost is separate from later exact lookup and fixed-hash range costs.
The existing bounded tip selector remains the first-load source and the fallback route.
The earlier optional 5,000-tip preload allowance is superseded.

## Execution packages

Packages 1–6 now have product code and bounded ordinary product evidence in the development branch.
The final route inventory records source coverage and each remaining runtime evidence gap.
An explicit public preload operation is not built.
Use separate reviewable commits for the packages; keep disk-format work separate from logical correctness.
The current branch and worktree remain the implementation venue.

| Package | Depends on | Current state | Remaining evidence |
|---|---|---|---|
| 0 Preserve the before versions | None | Complete: clean aa64f61 reference source, executable identity, and generated workload receipts. | The final comparison and immutable source receipts are complete. |
| 1 Add logical keys and their memory owner | 0 | Built: key-only domains, sparse values, lifetime references, byte owner, and native status. | Earlier preload candidate's 20,000-key and 128-path-purge cases pass; larger layouts remain unmeasured. |
| 2 Complete current boundaries and every affected writer | 1 | Built: typed core publication, captured roots, and disk-first system changes. | The final route inventory distinguishes source review from runtime proof for each route. |
| 3 Build one shared schema loader | 1 and 2 | Built: first-query gate, bounded pages, private load, concurrent changes, Ready, and DiskOnly. | Earlier preload candidate's concurrent first-call and byte-refusal cases pass. Request overlap passes; publication during Loading at ack remains unobserved. No explicit preload API exists. |
| 4 Route supported queries | 3 | Built: selected-schema query gate, memory candidate selection, and envelope R source. | Earlier preload candidate's ranges, sample, Hash-only, pages, bounds, filter, and envelope reads pass; the broader read-use-case inventory remains open. |
| 5 Accelerate first load and fallback | 3 and 4 | Built: batched selected Locations and packed ordinary keys. | Final costs and material regressions are recorded; cloud-part costs remain unmeasured. |
| 6 Complete cutover and final comparison | 2 through 5 | Native write cutover and matched comparison complete. Later cleanup makes native storage the sole logical engine. | The later cutover has separate validation. Broader runtime and size proof remain open. |

The earlier schema-preload comparison uses one executable with schema preload off and on.
The newer core memory comparison uses the preceding build and the final build, each with preload off and on.
The next action is review of the recorded performance regressions and the remaining runtime evidence gaps.
Keep the policy opt-in while the broader read-use-case evidence remains open.
The package details below retain the target contracts; their completion statements are not claims of completed proof.

### 0 Preserve the before versions

Record the earlier committed engine at aa64f61cd3d2d01a3f8953d4a1b3cf21f9940bab as one reference.
Preserve the current unfinished spike separately with its complete source state.
HEAD alone does not identify that spike: it includes modified files, untracked files, and deletions.
Use the existing source receipt (receipt not kept in this repository) and a complete code checkpoint.

Preserve each reference executable outside the candidate build output before a rebuild can replace it.
Verify its hash against its source/build receipt; rebuild from the exact source checkpoint if the match fails.
Record compiler, build settings, runtime settings, schema declarations, input seed, and workload version.
Keep source and executable identities separate from labels such as before and after.

Capture fresh synthetic Mini writes, reads, flush, and reopen for the earlier engine.
Keep the unfinished spike as diagnostic evidence, not a substitute for the earlier working reference.
The final report compares that engine with the final candidate.
It also compares the same final executable with schema preload off and on.
The second comparison isolates preload from the other core-model and disk-format changes.

Completion: the before executable survives candidate rebuilds, and its workload can be repeated from the recorded inputs.

### 1 Add logical keys and their memory owner

The preload-on-first-query policy and selected schema identities are built.
Inventory the existing membership sources for ranges, sample, and known-empty fallback.
Name each source's scope, molecule, layout, encoding, visibility rules, and required hashes.
Share domains only when those facts match; do not derive record existence from arbitrary field unions.

Extend MoleculeHashRange and CoreWarmSet with key-only membership and sparse tips and atoms.
A key can exist without a fabricated AtomEntry or atom UUID.
Keep shared key identities where practical and retain the direct exact lookup route.
Give ready calls a complete-domain lifetime reference across all candidate chunks.

Add Unloaded, Loading, Ready, DiskOnly with reason, and Failed with reason.
The separate byte owner uses a configurable 128 MiB allowance by default.
Charge allocation capacity, tree nodes, domain definitions, load buffers, write changes, and retained retired domains.
Track the highest live key count and key byte size separately.
Deletes reduce the current key bytes; later inserts reuse the retained allowance until either highest value grows.
Retained hash capacity remains charged until the domain ends.
One repeated delete-and-create key must not consume a new allowance on every cycle.
One shared semaphore permits one concurrent load per native runtime.
Each load reserves 64 MiB of temporary work; its I/O contexts retain the reservation until their work ends.
Each page permits 256 records and 16 MiB of read work.
Keep the 10,000 full-path cap and existing memory guard.

Separate full-path purge from whole-domain key purge.
Tip and atom purge preserves complete membership.
Whole-domain purge clears readiness under the publication lock before removal.
Capacity-refused schemas wait for an explicit retry or relevant resource change before another full load.

Add per-call phase costs and operational load state without key values.
Record load count, keys loaded, accounted bytes, wait time, membership I/O, tip I/O, atom I/O, retries, and refusal reason.
Keep process memory separate from the key allowance's accounted bytes.

Primary sources: [molecule](../fold_db/crates/core/src/atom/molecule_hash_range/mod.rs), [owner](../fold_db/crates/core/src/resident/core_set.rs), [purge](../fold_db/crates/core/src/resident/core_set/lifecycle.rs), [status](../fold_db/crates/core/src/storage/disk_pack/view.rs).

Completion: format, lint, and build checks pass for the new key and resource-owner types.
Their source contract requires no full atom or tip for each key.
The selected-schema policy remains opt-in at process start.
Package 3 supplies the runtime evidence for several hashes and more than 10,000 keys within the byte cap.

### 2 Complete current boundaries and every affected writer

Publish the disk root with its covered logical change number atomically.
Keep that number separate from file frontiers and queue delivery completion.
Capture definitions, membership controls, accepted changes, and logical state together.
Retain immutable selected entries and required file-root lifetime references during I/O.

The bounded source supplies complete candidate-key pages with explicit exhaustion.
Ready does not establish exact current visibility for every candidate.
The selected batch or current warm tip resolves stored forms, active generations, metadata, and delete barriers.
An error or budget stop cannot establish empty membership.
Keep pending changes that the captured disk root does not contain, including through a concurrent flush.

Complete the writer inventory for every domain that an opted-in schema can reference.
Cover queued and durable writes, shared aliases, proteins, imports, retention, backfill, generation activation, and administrative writes.
Publish inserts, deletes, eligibility changes, and both sides of key moves with current values before the ack.
Preserve the current protein propagation schedule.

Prepare and reserve complete changes before publication.
Define the later-change subscription that a loader uses and preserve its references through flush.
Prepare logical bulk replacement on disk, then publish its root, covered number, definitions, and readiness transition together.
Physical flush and LastDB compaction preserve Ready when logical membership stays the same.

The native raw-write publication route now maintains candidate completeness across its accepted changes.
Ordinary `mk`, `mgd`, and `mgr` changes update candidates without a whole-domain reload.
An `mgp` logical replacement invalidates the affected domain and requires a later load.
The captured generation flag handles physical tip deletion without loss of a retained generation candidate.

A related schema can change the same molecules as the first generated example.
Source coverage at the native boundary does not prove every caller's end-to-end behavior.
The final route inventory covers the native writer boundary. Its source-only routes still require separate runtime evidence.

Primary sources: [view](../fold_db/crates/core/src/storage/disk_pack/view.rs), [queue](../fold_db/crates/core/src/storage/disk_pack/queue.rs), [prepared paths](../fold_db/crates/core/src/fold_db_core/mutation_manager/write/native_paths.rs), [membership pages](../fold_db/crates/core/src/db_operations/atom_store/filter/partition_page.rs).

Completion: every affected writer has a named publication route and the required product receipt.
The final candidate has the common route and bounded product receipts. The final route report names the remaining broader proof gaps.
The current disk reader must retain one consistent captured boundary through the whole query batch.

### 3 Build one shared schema loader

The candidate exposes the first-query gate after scope and definition resolution.
It does not expose an explicit public preload operation.
Register one engine-owned load for each required domain.
Reserve its memory, disk-work allowance, deadline, and temporary change allowance first.
Concurrent first-query callers join that load instead of starting duplicate loads for the same domain.
Different domain loads share the single native-runtime load slot.

Build all required membership sources and hashes privately from bounded pages on the current disk format.
Keep resolved keys and essential flags; release unnecessary tip bodies, page objects, and file handles.
Retain accepted changes after the initial capture, including deletes and both sides of moves.
A concurrent flush cannot discard those required changes.

Merge changes outside the publication lock in bounded chunks.
Merge only a bounded final tail under the lock, verify coverage and compatible definitions, then publish Ready atomically.
An ordinary write does not restart the full load.
The waiting query keeps its original common root and pending references.
If a concurrent write makes the Ready domain newer, that older query can use its captured disk fallback.
Do not take a fresh boundary inside one query batch.
A later query can use Ready at its own common boundary.

Define one caller timeout independently from the shared load's lifetime.
A capacity or change-buffer refusal cancels the optional load before the next write becomes visible.
The write continues, and the schema records DiskOnly with a reason.
An I/O error remains an error; an incomplete source never becomes a ready empty schema.
Avoid a failed full preload on every later query.

Primary sources: [logical owner](../fold_db/crates/core/src/resident/core_set.rs), [current view](../fold_db/crates/core/src/storage/disk_pack/view.rs), [loader pages](../fold_db/crates/core/src/db_operations/atom_store/filter/partition_page.rs).

Completion: a fresh selected schema with several hashes and over 10,000 keys reaches Ready under its byte allowance.
Concurrent first-query product calls share each required domain load.
Concurrent inserts, deletes, moves, and flush do not lose current membership.
Refusal, cancellation, empty membership, and restart follow the declared states.
Package 4 supplies first-query and first-exact-query evidence through the common automatic gate.

### 4 Route all supported queries

Place the first-query gate after authorization and schema binding resolution.
An exact query to a selected HashRange schema also starts or joins that gate once.
After it, exact value resolution uses only supplied keys.
A ready range seeks within its named hash and selects ordered memory keys.

Check known warm tips before a missing-tip batch, then resolve missing atoms in a dependent batch.
Collect paths across fields and keys; do not await one physical request for each key.
Keep unknown, confirmed absence, selected tip, and full value distinct in the shared record lookup.
Known membership stays in the separate complete-key part; it does not establish a selected tip.
Retain only selected verified tips. Add a value after its fetch, without another path-budget slot.
Preserve a matching retained full value after a metadata-only call.
Keep dirty writes complete and retain their memory owners through older captured reads.
Use the same recency order, source invalidation, and whole-record purge for tips and full values.
Preserve full metadata and the slot-specific atom association.
Admit confirmed absence only after every possible winner and required delete barrier is checked.
Retain those source dependencies exactly as for a present tip.
If a candidate receipt is incomplete, keep the value for body reuse but do not authorize direct logical lookup.
Keep full atom copies separate when their dates or source details differ, even when their content UUID is equal.

Resolve document-copy precedence before predicates, order, limits, and projection.
Fetch unprojected predicate and sort fields in batches.
Preserve one tie comparator, explicit bounds, public cursor behavior, and start-inclusive/end-exclusive ranges.
Do not compare incompatible OPE domains or construct prefix bounds with a NUL suffix.

For N qualifying records, continue to N+1 or authoritative exhaustion.
Keep examined continuation separate from the last-returned-key cursor.
Keep key count separate from count after value predicates.
Use complete ready sources for ordered sample and whole-schema preview.
Preserve primary-source fallback, range-major preview order, stored-hash ties, content exclusions, and the maximum of 10.
Use the complete record-envelope R source when the public response needs its membership.
An empty R domain avoids a separate missing-envelope read for each selected key.
A nonempty R source needs complete candidates before memory selection replaces its disk discovery.
The final program includes that source and passes the ordinary record-envelope requests.

The first query intentionally loads all required schema keys before even a small answer.
Later narrow ranges must neither reload the schema nor enumerate unrelated hashes.
Unselected or recorded DiskOnly schemas keep bounded disk selection plus captured pending changes.
Tip and value purge causes only missing-path fetches while the complete keys remain ready.

Primary sources: [query entry](../fold_db/crates/core/src/fold_db_core/query/query_executor/run.rs), [key filter](../fold_db/crates/core/src/storage/disk_pack/logical_paths/core_filter.rs), [resolver](../fold_db/crates/core/src/schema/types/field/variant/read/core_model.rs), copy reads (file removed).

Completion: every supported read returns correct current keys, values, metadata, bounds, counts, and page flags.
Concurrent first queries share one automatic load, including when the first query is exact.
Ready range and sample calls perform zero disk key-discovery work.
Cold tips and values remain correct and use batches.
The selected policy is currently opt-in for the generated product runs.
Those runs do not establish the wider caller inventory or complete the cutover criteria.

### 5 Accelerate first load and fallback

First batch the Locations already selected by the cursor with the existing adjacent-frame block reader.
Do not repeat an exact tree lookup for a known selected Location.
Measure that change against the correct current-format loader before a disk-format change.

Then implement the packed-key locator pages in the [fast range design](lastdb-fast-range-design.md#disk-design).
Update accessors, byte-based splits, capacity quotes, validators, LastDB compaction, and dependency walks together.
Include backup readers and required format-feature rejection.
Keep encrypted tip bytes, shared atoms, file packing, and semantic cloud mutation order.
Retain explicit support and cost accounting for oversized OPE keys.

Use the existing compatible codec for compact OPE bytes in memory.
Do not change the encryption algorithm with the page format.
Cached tree-construction time is not complete first-query load time.

Primary sources: [range reader](../vendor/laststore/src/disk_pack/hash_range.rs), [batch reader](../vendor/laststore/src/disk_pack/batch.rs), [page output](../vendor/laststore/src/disk_pack/tree_output.rs), [page editor](../vendor/laststore/src/disk_pack/tree_edit.rs).

Completion: actual product calls quantify first-load and fallback time, key pages, payload reads, write ack, flush bytes, memory, and changed cloud parts.
Ordinary packed keys avoid their old per-key tail pages.
Report the remaining large-key cost and every material regression.
Keep this format work in separate reviewable changes from packages 1–4.

### 6 Complete cutover and final comparison

Close the complete reader/writer inventory and keep required product receipts beside each route.
The final native route removes `NativeWriteGraph` and duplicate native compatibility values after the caller review.
Legacy backend compatibility remains. The final route report states the deliberate disk-first routes and runtime proof limits.
Retain the intentional DiskOnly selector and required file-root lifetime controls.
Remove Single and standalone Range across declarations, callers, and clients together.
Reject as_of at every public entry point.

Run the matched comparison described below after the final source changes.
Keep first query, later key selection, tip resolution, atom fetch, and response time separate.
Report read, write ack, flush, memory, file, queue, and cloud-part costs with each material regression.
Preserve source, executable, configuration, input, result, and cleanup receipts.
Stop owned synthetic nodes and use supported exact-path cleanup after receipt preservation.

Completion: the report compares the earlier working engine, final disk fallback, and final preload under the same declared workloads.
The selected schema route has no required compatibility-value fallback and no unexplained current-state bypass.
No primary data, archived home, shared node, CI, deployment, or automation change is part of this local plan.

## Benchmark plan

Use fresh synthetic homes and ordinary Mini write/read calls.
Collect records into bounded mutation batches and collect keys into batch reads.
Size mutation batches by field-path count, bytes, and request limits, not record count alone.
Use the same generated inputs for each reference and candidate home.
Keep key count separate from atom and file-blob size.

| Synthetic cohort | Key shape and purpose |
|---|---|
| 256 records, four hashes | Current full lifecycle workload: four fields, 54-byte ranges, and 1 KiB bodies. |
| 10,000 records, four hashes | Before observation complete through exact/range requests; its later capped preview hits the read limit. |
| 20,000 records, four hashes | Current final off/on comparison; more keys than the 10,000 full-path cap. |
| 4,096-byte key allowance | Separate fallback case, with the ordinary 10,000 full-path cap. |
| 128 full-path cap | Separate value-purge case, with small batches and the normal key-byte allowance. |
| 100,000 keys and larger layouts | Extended measurement remains pending; do not infer it from the smaller cohorts. |
| Many small hashes and long keys | Extended overhead and overflow measurements remain pending. |

Choose representative combinations rather than an unbounded Cartesian product.
For selected calls, request 1, 16, 64, and 256 keys with one, four, and 24 fields where the path cap permits them.
Include warm values, complete keys with cold values, missing fields, copied records, and related schema writes.
Use the existing operational benchmark commands and public requests; add no tests or fixture harness.
Admit each large home through the actual disk space quote and the owned scratch allowance before its writes.
Run large homes sequentially and clean each owned home after its receipts no longer require it.

For each declared cohort, compare three configurations:

1. The earlier working engine from package 0.
2. The final candidate with schema preload off, using its bounded disk selector.
3. That same final candidate with the selected schema policy on.

Record first exact and first range queries separately, including their full preload time.
Record later ranges and sample, tip and atom misses, returned records, order, metadata, count meaning, and page flags.
The current comparison uses one fresh home for each configuration and cohort, with five repeated reads for each width.
Record first-query observations separately; one observation does not establish a first-load distribution.
Report the repeated-read median and observed p95; with five samples, that p95 is the maximum sample.
Report individual write acks as observations, not a write-latency distribution.
Report sampled process memory separately from accounted key bytes; sparse samples do not establish the actual memory peak.
Retain the full distribution and disclose the shared machine and uncontrolled operating-system file cache.

Separate process-cold runs from device-cold claims.
A fresh process can still read files from operating-system memory.
Record the actual cache conditions; report no physical-device result without a controlled method.
Keep receipt generation outside the measured interval where the existing product command permits it.

The driver covers concurrent first queries, post-load inserts/deletes/moves, flush, restart, and key-byte refusal.
The final pressure run covers value purge and preserves complete keys without a reload.
The extra ordinary run covers physical compaction and current membership after deletes.
Concurrent-load calls return coherent old and current results, but both write acks arrive after Ready.
Publication while Loading, whole-domain purge, and temporary-change refusal still need separate product evidence.
Measure later ready key selection explicitly: disk key-discovery work must be zero.
An overall read can still need batched disk fetches for unknown tips and atom values.
Cloud sync stays off in synthetic homes.
Changed cloud-part costs remain unmeasured; a source change or a local read benchmark does not establish them.

Before-and-after speed is measured, not inferred from the earlier representation prototype.
Require correct complete results, bounded resources, and no unexplained regression before completion.
Use format, syntax, lint, build, and real product operations for validation.

## Product evidence matrix

| Workload | Required result |
|---|---|
| First query to selected schema | One complete all-hash membership load before the query continues. |
| Concurrent first queries | One shared loader, bounded waits, and no duplicate full loads. |
| First exact query | Gate runs once; value resolution stays exact after readiness. |
| Later narrow range or page | No disk key discovery, no whole-schema reload, and no unrelated hash enumeration. |
| Schema with more than 10,000 keys | Complete keys can fit their byte allowance while full warm paths stay within 10,000. |
| Many small hashes | Complete all required domains and account for per-domain memory. |
| Empty schema then insert | Ready empty membership updates before the write ack. |
| Writes and flush during load | New inserts, deletes, and moves survive until atomic readiness publication. |
| Repeated overwrite | Correct new value without a complete-key rebuild when membership stays unchanged. |
| Path purge | Complete keys remain; later reads fetch only missing tips or values. |
| Whole-domain purge | Readiness clears before removal; the next query loads once. |
| Byte or change allowance refusal | Explicit DiskOnly reason, bounded disk fallback, and no failed full load per query. |
| I/O failure or unavailable cloud source | Explicit error or unknown state; no false completeness or empty answer. |
| Logical replacement or encoding change | Atomic update or invalidation; no use of incompatible keys. |
| Physical flush and LastDB compaction | Current membership and Ready remain intact. |
| Count predicates and ordering | Correct values, visibility, count meaning, bounds, and metadata under one boundary. |
| Sample and preview | Complete source, existing order and fallback, maximum of 10, independent of warm value recency. |
| Shared schemas and proteins | Compatible aliases share keys; different layouts remain separate; current propagation schedule remains correct. |
| Cancellation and deadline | One caller cannot corrupt shared load state; resources remain bounded. |
| Shutdown and reopen | Stored data remains correct; the next selected-schema query performs a fresh load. |

The matrix states the wider completion contract, not the list of completed product receipts.
The measured and planned final cases are the explicit cohorts above, with their status stated separately.
Mixed metadata, missing fields, document copies, broader proteins, and larger sizes still need separate product evidence.
Keep first-use cost separate from the later seek and selected-value costs.
Use format, syntax, lint, build, and product operational checks; add or run no tests or fixture harness.

## Reader and writer inventory

| Area | Current source and planned change |
|---|---|
| Core key model | [MoleculeHashRange](../fold_db/crates/core/src/atom/molecule_hash_range/mod.rs): separate ordered membership from required AtomEntry storage. |
| Logical owner | [CoreWarmSet](../fold_db/crates/core/src/resident/core_set.rs): own complete key domains, sparse values, and load state. |
| Capacity and purge | [Lifecycle](../fold_db/crates/core/src/resident/core_set/lifecycle.rs): separate full-path purge from whole-domain key purge and byte accounting. |
| Common query gate | [Query execution](../fold_db/crates/core/src/fold_db_core/query/query_executor/run.rs): start or join the schema load after scope and binding resolution. |
| Complete key selection | [Core filter](../fold_db/crates/core/src/storage/disk_pack/logical_paths/core_filter.rs): support ordered complete keys without full warm tips. |
| Tip and value batches | [Core resolver](../fold_db/crates/core/src/schema/types/field/variant/read/core_model.rs), [tip receipts](../fold_db/crates/core/src/schema/types/field/variant/read/core_receipts.rs): preserve unknown states and source metadata. |
| Current disk source | [Tip pages](../fold_db/crates/core/src/db_operations/atom_store/filter/partition_page.rs): expose bounded current membership and explicit exhaustion. |
| Publication and flush | [Current view](../fold_db/crates/core/src/storage/disk_pack/view.rs), [queue](../fold_db/crates/core/src/storage/disk_pack/queue.rs): publish key changes and retain loader changes through flush. |
| Native and shared writes | [Prepared paths](../fold_db/crates/core/src/fold_db_core/mutation_manager/write/native_paths.rs): complete typed scope and shared-molecule publication. |
| Sample | [HashRange loader](../fold_db/crates/core/src/db_operations/atom_store/filter/load/hash_range_pages.rs): use complete membership without all-value materialization. |
| Document copies | Copy reads (file removed): resolve precedence before predicates and pages. |
| Public response | [Query handler](../lastdb_host/src/handlers/query.rs), [response order](../lastdb_host/src/handlers/query/rows.rs): preserve public limits, cursors, metadata, and exact results. |
| Definitions and old key types | Registration, schema types, generated clients, retention, republish, imports, and bulk-root controls. |

## Completion criteria

- Every selected schema query starts or joins a bounded first-use gate before normal query execution.
- Ready covers every required source and hash under compatible logical definitions.
- Later ready range and sample calls use logical keys without disk key discovery.
- Full paths stay within 10,000; complete key memory stays within its separate byte allowance.
- Every supported write updates or invalidates membership before visibility and ack.
- Ordinary writes, flush, and physical LastDB compaction do not force complete-key reloads.
- Tip and atom purge preserves keys; whole-domain purge clears readiness before removal.
- Unknown tips, values, and predicate fields use batches and never become false absence.
- Counts, preview, document-copy precedence, sharing, and OPE bounds remain correct.
- Refusal, cancellation, restart, and incompatible controls have explicit bounded behavior.
- Compatibility removal leaves no required raw read fallback.
- Matched product measurements report first-use cost, later range gains, and all material regressions.

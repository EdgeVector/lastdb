# Schema range preload product results

Status: final candidate 5 matched small and 20,000-record product results complete, 2026-10-17.
This report does not mark the wider reader/writer cutover complete.

Review follow-up: two source fixes follow these measurements.
Captured reads now retain the batch owner for each memory reservation.
Repeated secondary reads now retain the selected atom location.
The first review executable is `ff832181fa11303c3c8675c2b4eb4158536d788459555ca9b6ba0b1c6d33dce9`.
The elapsed times below still describe candidate 5, before these two fixes.
The [first review receipts](proofs/schema-preload-review-fixes-20261017/review-result.json) record the checks, their limits, and a separate memory-cap problem.

The next review fix closes that memory-cap problem.
Writes without a sufficient memory reservation stay private until their disk commit.
The commit updates the warm set and range keys together.
The new executable is `739ab5e80e0cd915e4c42c2a13cd0e168f22410ceed0df4581aa0e2e1f92ffbb`.
The [durable-write review receipts](proofs/schema-preload-durable-review-fix-20261017/review-result.json) record the source review, build, and ordinary product checks.
No elapsed-time comparison for this executable appears in this report.

The final program returns correct values in the completed ordinary workloads.
Repeated ranges improve against the committed engine.
Small repeated exact reads and queued update/insert acks regress.
Preload reduces range disk work, but several ranges take more time with preload on than off.
The costs and limits below remain part of the result.
Keep preload opt-in.
Most of the range gain against before already appears with preload off.
The comparison does not split the packed-key gain from the logical warm-model gain.

## Programs and data

| Program | Source | Executable SHA256 |
|---|---|---|
| Before | Clean committed engine `aa64f61cd3d2d01a3f8953d4a1b3cf21f9940bab` | `3f591bae95065fd31a507dcbab12aa6e40e30b24ff346a55fa5a2b0d126b13cb` |
| Final off and on | [candidate5-source.json](proofs/schema-preload-product-20261017/candidate5-source.json) | `2c14d7d2f04865454ba9b1a41ba441ee7b69e2bad11a577f1056fe5b43c1324b` |

The reference used its own clean DEV checkout and target directory.
Each run creates a fresh private synthetic home.
No existing data home supplies the data.
Each candidate run copies its executable into its private root before node start.
Final off and on runs use the same executable hash.
The [final identity check](proofs/schema-preload-product-20261017/candidate5-final-identity-check.json) records 1,656 source hashes with zero mismatches.

Each generated record has four fields: a hash, a range, a title, and a body.
The range has 54 bytes; the body has 1 KiB.
Read requests return the title and body.
Seed writes use batches of at most 64 records, with an explicit flush after each batch.
The ordinary full-path cap is 10,000.

The small workload has 256 records across four hashes.
It covers exact reads, fixed-hash ranges, four concurrent first queries, and a ten-record schema preview.
It also covers update, insert, delete, key move, explicit flush, selected-value reads, and normal reopen.
The large workload has 20,000 records across four hashes and omits the later mutation phases.
The input receipts contain the full settings and generated key recipe.

The comparison against before includes the logical warm model and packed-key disk changes.
The final off/on comparison changes only the schema-preload policy.
Off sets an empty `LASTDB_SCHEMA_PRELOAD_SCHEMAS` value.
On selects `SchemaPreloadProduct`, with a separate 128 MiB key allowance.

## Repeated reads

Both final small configurations return the expected values and membership in all 54 read requests.
Range order, page flags, and field identities pass their checks.
The capped preview returns ten distinct generated keys with correct values.
All write, flush, and reopen phases pass.
All five owned nodes in each run stop normally with exit code zero.

The following times use five repeated requests per shape and width.
The read-operation count is the median product counter, not a physical device command count.

| 256-record workload | Keys | Before median / observed p95 ms | Final off median / observed p95 ms | Final on median / observed p95 ms | Read operations: before / off / on |
|---|---:|---:|---:|---:|---:|
| Exact | 16 | 0.955 / 1.082 | 2.574 / 2.758 | 2.535 / 2.763 | 0 / 12 / 12 |
| Exact | 64 | 2.860 / 2.893 | 3.885 / 3.928 | 3.896 / 4.001 | 0 / 12 / 12 |
| Range | 16 | 10.983 / 11.353 | 3.337 / 3.690 | 3.585 / 6.889 | 127 / 33 / 16 |
| Range | 64 | 31.971 / 32.252 | 5.914 / 6.041 | 6.919 / 7.184 | 307 / 45 / 16 |

The small 16-key and 64-key ranges take 7.4% and 17.0% more time with preload on than off.
Fewer read operations do not establish lower elapsed time.
Repeated exact requests use the warm values with zero core misses.
Their remaining twelve operations fetch schema metadata, with zero encrypted value bytes.
Those operations still cost more than the reference's zero-read exact route.

The candidate reads the schema definition and state from the same captured root as the query values.
A shared cache can already contain a later state, so it cannot safely replace that captured state.
The reference reuses cached metadata.
An immutable schema snapshot tied to the accepted state is a possible later change for this fixed cost.
This report does not introduce another answer cache.

### 20,000-record workload

The reference and final off runs each return 42 correct exact/range responses.
Their later ten-record previews receive HTTP 503 with `read_resource_limit`.
Those complete workloads fail; their valid earlier query measurements remain preserved.
All four owned nodes in each run stop normally with exit code zero.
The selected-schema final run completes all 44 requests, including the ten-record preview and normal reopen.
Its five owned nodes stop normally with exit code zero.

| Request | Keys | Before median / observed p95 ms | Final off median / observed p95 ms | Final on median / observed p95 ms | Read operations: before / off / on |
|---|---:|---:|---:|---:|---:|
| Exact | 16 | 0.935 / 0.985 | 2.688 / 9.392 | 2.708 / 8.868 | 0 / 12 / 12 |
| Exact | 64 | 2.692 / 2.799 | 3.653 / 3.787 | 3.665 / 3.749 | 0 / 12 / 12 |
| Exact | 256 | 10.137 / 10.178 | 9.132 / 9.362 | 9.267 / 10.343 | 0 / 12 / 12 |
| Range | 16 | 10.942 / 11.167 | 3.609 / 3.657 | 3.310 / 3.369 | 147 / 33 / 16 |
| Range | 64 | 32.247 / 32.364 | 5.746 / 5.862 | 6.608 / 6.709 | 332 / 48 / 16 |
| Range | 256 | 119.908 / 120.880 | 16.762 / 17.363 | 21.378 / 22.496 | 1173 / 133 / 16 |

The selected 256-key range takes 82.2% less time than before, a 5.61-fold ratio.
The fallback disk route takes 86.0% less time than before, a 7.15-fold ratio.
Preload adds 27.5% to the 256-key elapsed time versus final off, despite fewer disk operations.
It adds 15.0% at 64 keys and reduces elapsed time by 8.3% at 16 keys.
The current measurements do not isolate the cause of that memory-selection cost.

Each repeated selected range adds two memory selections and no key load.
The native core counter reports warm hits and zero misses throughout those repeated calls.
The query receipt reports 16 read operations, 3,651 payload bytes, and 60 decrypted bytes at each width.
Complete keys therefore remove repeated disk key discovery without removing every metadata or response cost.
The ten-record preview succeeds in 6.683 ms, with correct generated values, after earlier queries make the domains Ready.

The earlier 10,000-record reference run also returns 42 correct responses before the same preview refusal.
No final 100,000-record comparison runs in this report.

## First-query cost

Each number below is one observation after a process reopen.
The selected first query includes the complete key load for its required domains.
A process reopen does not clear the operating-system file cache.

| Workload | First request | Before ms | Final off ms | Final on ms |
|---|---|---:|---:|---:|
| 256 records | Exact, 16 keys | 17.912 | 17.288 | 30.210 |
| 256 records | Range, 16 keys | 12.951 | 13.642 | 27.082 |
| 20,000 records | Exact, 16 keys | 39.666 | 22.697 | 767.172 |
| 20,000 records | Range, 16 keys | 16.224 | 13.968 | 632.819 |

At 20,000 records, four concurrent first calls share two completed domain loads.
Their median elapsed time is 627.056 ms.
A later normal reopen reloads the keys and takes 638.632 ms for its first range.
The first-query elapsed time includes the loader; its query cost receipt does not measure the loader's full disk work.
Do not treat its reported read-operation count as the total preload I/O.

## Writes and flush

These values describe one batch of each kind per configuration.
They do not establish a write-latency distribution.
The returned ack mode matches across all three programs.
Queued mode acks the memory change; durable mode also waits for its local commit.

| Batch | Returned mode in all programs | Before ack ms | Final off ack ms | Final on ack ms |
|---|---|---:|---:|---:|
| Update four records | Queued | 179.934 | 280.667 | 281.252 |
| Insert four records | Queued | 12.463 | 131.704 | 134.797 |
| Delete two records | Durable | 264.003 | 135.431 | 130.521 |
| Move one key, two mutations | Durable | 266.484 | 255.240 | 257.461 |

The queued update and insert costs regress substantially.
They do not change to a durable ack mode.
The update and insert preparation counters remain about 4–6 ms in these observations.
The internal ack counter includes the full batch time; it does not identify a separate wait phase.
The receipts do not divide the remaining time into queue, storage, or control-record costs.
They do not establish the cause of the added time.

The seed uses four durable batches of 64 records.
The next figures sum request durations from one run; they are not seed wall time or a throughput distribution.

| Seed request cost | Before ms | Final off ms | Final on ms |
|---|---:|---:|---:|
| Sum of four mutation acks | 1394.752 | 1115.972 | 1078.405 |
| Sum of four explicit flush calls | 373.473 | 1.158 | 1.055 |

Seed ack totals improve in this observation, while the small queued batches regress.
An explicit flush can find that the automatic flush already completes the relevant work.
Its short duration does not measure the full disk write cost.

The large seed uses 313 durable mutation batches for 20,000 records.
Its request-duration sums also vary and do not form a throughput comparison.

| Large seed request cost | Before ms | Final off ms | Final on ms |
|---|---:|---:|---:|
| Sum of mutation acks | 166185.694 | 250948.868 | 160959.299 |
| Sum of explicit flush calls | 430.759 | 10954.196 | 490.049 |

The off-policy seed runs near separate operational work for part of its duration.
These sums preserve that variation instead of attributing it to the preload policy.

The final on key-move read observes pending work before and after the read.
The update, insert, and delete reads do not observe that condition.
These counters do not identify the pending mutation, so they do not establish a per-mutation preflush result.
Explicit final flush status reports zero dirty core paths, zero queued memory write records, and equal accepted and completed sequences.

## Complete keys, memory, and disk size

The final small selected run reports two Ready domains and 256 candidate keys after the first load.
Four concurrent first queries still report two loads in total.
Ordinary delete and key move keep that load count at two.
The end-of-run candidate count is 258 after inserts, deletes, and the move.
Candidate counts are not a general visible-record count; current metadata still decides visibility.

The initial complete keys use 150,064 charged bytes.
The 67,258,928-byte peak charge includes the temporary 64 MiB load reservation.
That charge is not measured process memory.
Sparse process samples do not establish the actual memory peak.

| Small process memory observation | Before bytes | Final off bytes | Final on bytes |
|---|---:|---:|---:|
| Before first exact query | 36,143,104 | 38,141,952 | 36,061,184 |
| After first exact query | 46,235,648 | 46,252,032 | 48,480,256 |
| Before first range query | 35,520,512 | 35,880,960 | 36,913,152 |
| After first range query | 44,580,864 | 43,745,280 | 48,873,472 |
| Highest sample, including seed | 177,995,776 | 155,418,624 | 155,795,456 |

At 20,000 records, Ready still has two domains with 20,000 candidate keys.
Its complete-key charge is 10,574,896 bytes; its peak charge is 77,683,760 bytes, including the temporary reservation.
The complete keys exceed the 10,000-path value cap without loading all field values.
The first exact query leaves only 48 logical value paths in memory.

| Large process memory observation | Before bytes | Final off bytes | Final on bytes |
|---|---:|---:|---:|
| Before first exact query | 164,986,880 | 38,780,928 | 37,011,456 |
| After first exact query | 170,016,768 | 48,152,576 | 65,929,216 |
| Before first range query | 172,851,200 | 37,650,432 | 38,486,016 |
| After first range query | 182,714,368 | 46,596,096 | 62,750,720 |
| Highest sample, including seed | 1,442,021,376 | 1,392,640,000 | 1,388,331,008 |

The high seed samples do not describe the retained complete-key size after process reopen.

The disk figures use allocated bytes from the exact generated data home.
They exclude executable copies and request receipts.
The earlier reference homes lack this exact measurement and are already reclaimed.
The 1 KiB body size does not describe the total database size.

| Data home | After seed bytes | After final stop bytes |
|---|---:|---:|
| Final off, 256 records | 29,507,584 | 42,815,488 |
| Final on, 256 records | 29,544,448 | 42,786,816 |
| Final off, 20,000 records | 6,821,421,056 | 6,943,989,760 |
| Final on, 20,000 records | 6,627,766,272 | 6,755,303,424 |

Database size includes encrypted fields, tips, control records, file pages, and old pages before LastDB compaction.
This report does not infer a disk-size improvement against before without its exact data-home measurement.

### Separate refusal and purge cases

Both cases use the final executable and fresh generated homes.
Their elapsed times are excluded because they overlap a large seed.

The 4,096-byte key allowance case completes all 54 reads and all later write/flush/reopen phases.
It reports two DiskOnly domains, no Ready domain, zero complete keys, and zero completed key loads.
Its charge remains 2,155 bytes with the same refusal reason across repeated calls.
It uses bounded disk selection instead of repeated whole-domain load attempts.

The separate 128-path-cap case uses smaller batches and four concurrent four-key first queries.
It completes 42 reads and the write/flush/reopen phases.
The selected-value walk reaches 128 paths and reports 673 purged paths.
The complete keys stay Ready with two loads and 258 candidates through the value purge.
The explicit flush reports zero dirty paths, zero queued memory write records, and accepted sequence equal to completed sequence.
The low-cap case is separate from the normal 10,000-path measurements.

## Additional ordinary product evidence

The [extra product report](proofs/schema-preload-product-20261017/EXTRA_PRODUCT.md) uses the same final executable.
Its Hash-only, record-envelope, page, bound, filter, sample, delete, flush, reopen, and physical LastDB compaction operations pass.
Hash-only calls start no key load, even when the policy names that schema.
Record-envelope reads add their required complete domains.
Deletes and physical compaction then preserve Ready and the load count.

The separate concurrent-load requests start while native status reports Loading.
Their write acks arrive after status becomes Ready.
The captured old queries return coherent old results; current and post-flush reads return the new values and membership.
Those receipts establish request overlap, not publication while the initial load remains active.
Reads before an explicit flush do not exclude an automatic flush.

The final read-source checks retain the chosen physical atom file through API projection.
They check that source before warm reuse.
A full optional receipt cache cannot silently remove a required file hint; the required-source refusal returns an explicit resource error.
These checks preserve the captured read boundary when another write publishes.

The [validation report](proofs/schema-preload-product-20261017/VALIDATION.md) records release build, Clippy, format, and source checks.
These source checks do not replace ordinary product evidence.

## Separate existing size limit

A separate 20,000-record, five-field operational run writes and flushes its generated data.
Its data home uses about 8.15 GiB.
Shutdown then rejects the saved key details for one schema above the 128 MiB batch limit.
The node exits with code one, and the run never starts a schema preload.
This failure is separate from the four-field matched benchmark and its normal shutdown observations.

The relevant limit exists in the reference source and stays unchanged.
No paired reference run of that five-field case occurs.
This work does not raise the guard.
The [size-limit audit](proofs/schema-preload-product-20261017/KEEP_SMALL_LIMIT.md) records the source linkage and failure receipts.
The per-schema record size limit remains.

## Limits and receipts

The machine has no exclusive reservation, and its operating-system file cache is uncontrolled.
Each configuration uses one fresh data home and five repeated reads per shape and width.
With five samples, the observed p95 is the maximum sample, not a latency guarantee.
First-query values and write batches are single observations.
Large seeds can overlap separate operational work; their durations do not establish comparative seed throughput.
Short read phases run after other owned drivers stop their product calls.

Cloud sync is off.
Cloud-part costs, broader writer routes, 100,000-key schemas, and larger layouts need separate evidence.
The old raw-write graph and remaining compatibility routes are not removed by this report.
Earlier candidates remain labelled diagnostic; their measurements do not enter the final comparison.
All final measurement nodes stop normally, and reviewed supported cleanup removes each final synthetic data root.
The reference source and executable identities remain in `reference.json` after the comparison.
The clean temporary reference checkout and executable are reclaimed through normal Git worktree removal without force.
The [receipt directory](proofs/schema-preload-product-20261017/README.md) contains commands, full request results, sources, identities, refusals, and cleanup results.

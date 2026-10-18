# Core memory completion: final result

The final generated-data API checks pass. The five review fixes remain in place.
The final build removes the twofold to threefold repeated-range delay found in the first three completion attempts.
This is a correctness completion with mixed timing results, not a general speed increase over the previous reviewed build.

This report compares the previous reviewed v4 executable with the final completion executable.
It does not compare the original engine with the packed engine.
The [earlier report](../core-memory-state-20261018/RESULTS.md) compares executable `e10f4593…` with reviewed v4 `bdb83b38…`.
That report also explicitly excludes the older `aa64f61` original engine. Its original results remain unchanged.
Do not combine the gains from the two reports.

## Exact programs and evidence

| Program | Executable SHA-256 | Source identity |
|---|---|---|
| Previous reviewed v4 | `bdb83b38545a2161217392f19f9e6f6c2e0fd2d35f8f449e2d6221cb29f2cbed` | [current-source-v4.json](../core-memory-state-20261018/current-source-v4.json) |
| Final completion | `a0ad619076570fc472569712d5a40743a5a32823e909d45d4e33991b7c536285` | [attempt4-final-identity.json](attempt4-final-identity.json) |

The final manifest identifies 1,908 product inputs, compiler details, build settings, and the executable.
The [final source check](final-source-verification.json) confirms the same inputs and program after all final workloads.
The [comparison](attempt4/comparison.json) records every measured group, sample, file-call count, and write-state observation.
The [run order](attempt4/run-order.json) records all eight completed cases and commands.
The [receipt manifest](attempt4-receipt-seal.json) retains exact hashes for the nine final run directories.
Raw file links require the local worktree; see the [evidence index note](EVIDENCE.md).

## What the final checks establish

The ordinary API workload uses one new generated home. It passes these checks:

- 219 keyed reads check complete fields, exact membership, update visibility, schema changes, molecule-map changes, flush, reopen, and concurrent calls.
- 58 warm-read gates require zero file-read calls. They include repeated missing keys and unchanged keys after unrelated writes.
- 30 equal-content reads check shared atom UUIDs. Twenty-four alternating repeated reads also require zero file-read calls.
- 24 cycles create and delete the same key. All observations retain 46,606 charged key bytes, 65 keys, two ready domains, and two loads.
- Seven supplemental HashKey queries check a HashRange schema. Six records use the selected hash; one record uses another hash and stays excluded.
- The supplemental calls check repeated reads, an accepted update, an accepted delete, and a normal reopen.

The two schema-absence refusals are expected. Other unexpected refusals fail the workload.
The final benchmark adds 696 keyed value checks and 112 measured write batches. All pass, with no refusal or failed batch item.
All 141 owned node sessions stop normally across the nine final run directories.
The proof uses no test suite, fixture harness, primary home, archive, or existing data home.

The API cannot set arbitrary Atom dates or expose each selected copy's creation date.
Source review therefore supports the full-metadata copy rule; the API proves shared UUIDs, repeated reuse, and correct returned values.
The API also does not expose direct admission of an impossible definition above 16 MiB.
Source review confirms refusal before purge for that branch. The report does not claim a direct API proof of that branch.

## Method and timing limits

The host is an M4 Max Mac with 36 GiB memory and 14 CPU cores, on macOS 26.5.2.
Each benchmark case uses a fresh home with independent Hash and HashRange schemas.
Each schema starts with 256 records, four fields, and generated 1 KiB bodies.
The HashRange schema uses one named hash. Bounded requests include the start and exclude the end.
The measured fields are title and body; the API also returns the key field.

Two balanced rounds compare preload OFF and ON for each program.
The first order is current-ON, before-ON, current-OFF, before-OFF. The second order is the reverse.
The eight cases take 228.235 seconds in total. No Cargo build or other heavy agent check runs during these measurements.
The host lock excludes competing agent Cargo work. It does not reserve the whole host.

Each first-read and disjoint-key group has two observations, one per fresh home.
Each repeated-read group has ten observations, five per home. All samples remain in the comparison.
A normal reopen clears process memory; it does not clear the OS file cache.
The read receipt counts file calls. It does not measure physical device reads.
Random per-home identities can change file placement and file-call counts.

Hash widths 16 and 64 use a batch of independent queries. HashRange uses one keyed query.
Width one uses the same `/api/query` endpoint for both types.
Timed exact HashRange reads use `HashRangeKeys`. The strict API workload also checks singular `HashRangeKey` calls.
HashKey on a HashRange schema receives the separate seven-call check. This report adds no standalone Range query.

## Repeated reads

All values below are median milliseconds. Lower is better.
OFF leaves complete-key preload disabled. ON selects the HashRange schema for preload.

| Read | v4 OFF | Final OFF | v4 ON | Final ON |
|---|---:|---:|---:|---:|
| Hash, one key | 0.323 | 0.301 | 0.294 | 0.299 |
| Hash, 64 keys | 4.461 | 4.910 | 4.597 | 4.876 |
| Exact HashRange, one key | 0.284 | 0.321 | 0.359 | 0.384 |
| Exact HashRange, 64 keys | 2.100 | 2.322 | 2.069 | 2.325 |
| Bounded HashRange, 64 keys | 3.942 | 4.354 | 4.997 | 4.902 |

The final bounded64 OFF median is 10.5% above v4. The ON median is 1.9% below v4.
OFF file calls are 32 and 27 per request in the two v4 homes, versus 33 and 30 in the final homes.
ON bounded64 reads report zero file calls in both programs. The listed exact repeated reads also report zero file calls.
These results remove the earlier twofold to threefold CPU delay. They do not establish equal latency or a general speed increase.

## First reads and disjoint keys

All values are median milliseconds. Each cell uses only two observations.
The first selected-schema read pays the complete-key preload cost when ON is selected.

| Read | Phase | v4 OFF | Final OFF | v4 ON | Final ON |
|---|---|---:|---:|---:|---:|
| Hash, one key | First after reopen | 4.712 | 4.568 | 4.796 | 4.906 |
| Hash, 64 keys | First after reopen | 35.069 | 38.991 | 36.367 | 36.073 |
| Exact HashRange, one key | First after reopen | 4.492 | 4.660 | 91.703 | 94.833 |
| Exact HashRange, one key | Other keys, same process | 1.996 | 2.301 | 1.837 | 2.169 |
| Exact HashRange, 64 keys | First after reopen | 41.666 | 48.672 | 116.605 | 132.030 |
| Exact HashRange, 64 keys | Other keys, same process | 34.346 | 41.530 | 33.622 | 42.824 |
| Bounded HashRange, 64 keys | First after reopen | 33.254 | 40.105 | 120.883 | 131.360 |
| Bounded HashRange, 64 keys | Other keys, same process | 27.804 | 35.893 | 33.048 | 45.395 |

Several first and disjoint reads are slower. Bounded64 disjoint reads rise by 29.1% OFF and 37.4% ON in this run.
The small observation count and per-home file layout limit the comparison. The report retains the changes without attributing them to noise.
Complete memory keys do not imply complete memory values. Disjoint keys can still require their tips and field values from disk.

## Writes

Each measured batch changes 64 records. The table shows ack and total time through an explicit flush, in median milliseconds.
The total includes a native-status call between the ack and the explicit flush.

| Geometry and caller condition | v4 OFF ack / total | Final OFF ack / total | v4 ON ack / total | Final ON ack / total |
|---|---:|---:|---:|---:|
| Hash, new keys after reopen | 182.238 / 425.960 | 174.323 / 418.084 | 182.946 / 435.155 | 160.072 / 404.803 |
| Hash, update with no prior value read | 178.655 / 460.629 | 162.397 / 455.482 | 182.017 / 475.034 | 169.943 / 505.300 |
| Hash, update after value reads | 80.343 / 401.024 | 96.183 / 429.896 | 85.174 / 397.889 | 88.157 / 446.668 |
| HashRange, new keys after reopen | 240.391 / 487.079 | 225.035 / 468.918 | 218.373 / 479.284 | 254.149 / 635.204 |
| HashRange, update with no prior value read | 250.408 / 739.295 | 250.981 / 561.726 | 252.437 / 585.153 | 283.889 / 610.355 |
| HashRange, update after value reads | 230.429 / 540.208 | 198.331 / 490.557 | 232.676 / 550.054 | 232.817 / 539.235 |

The first two caller conditions have two observations each. The condition after value reads has ten observations.
The final OFF Hash ack after value reads is 19.7% slower. The final ON Hash total is 12.3% slower in that condition.
The ON new-key HashRange total is 32.5% slower, from two observations.
Other groups improve or remain close. The data does not support a maximum write-throughput claim.

The caller conditions describe prior API reads. They do not establish memory-only internal write preparation.
A queued ack alone does not establish that all work stayed in memory.
Background flush can occur before the explicit flush or a status call.

## Earlier attempts and the final fix

All three earlier candidates pass the strict value and memory checks, but range CPU cost blocks their merge.
The reports preserve each result: [attempt 1](ATTEMPT1.md), [attempt 2](ATTEMPT2.md), and [attempt 3](ATTEMPT3.md).
Each source identity and local preserved Git object identifies that candidate. No slow sample is discarded or replaced.

The [CPU profile](profile-attempt3/off/stack-summary.json) identifies repeated OPE candidate work under `note_selected_query_tip`.
That work runs before the code discovers that body-only reuse has no physical source receipt to record.
The final code first retains the actual source location. It skips candidate encoding only when no physical source exists.
Real physical receipts still require the full candidate work. The change adds no invented source authority or saved query-answer list.
The final Hash materialization route receives the same source-presence check.

## Memory contract and remaining scope

The warm model keeps core schema state, molecule state, complete selected keys, and fetched logical values.
Precise dependencies retain unrelated warm paths. Confirmed missing paths require complete source and delete-barrier information.
Equal UUIDs with different full Atom metadata coexist. The selected physical Atom location survives the secondary read path.
Same-key create/delete cycles reuse the key allowance. An impossible definition refuses admission before purge.

The defaults are separate allowances: 128 MiB for complete keys, 16 MiB for shared definitions, and 10,000 logical paths.
They are not a total process-memory cap. Captured calls retain their data and reservation owners until release.
This workload observes charged bytes, not total process memory or a long-run memory bound.
Native pending writes still use their existing write structures; the compatibility write route remains in use.
This report does not claim a complete caller inventory or the removal of that route.

The workload does not measure a 16–20 GiB database, many large hashes, long-call write pressure, or cloud upload costs.
Concurrent API calls check valid batch visibility; they do not pause an old captured read at a chosen instruction.
Active conflicts and unknown values can still require file reads. Preload remains opt-in and can reload after loss of complete-key readiness.
Future scale work must use those bounds; the small synthetic cohort cannot establish them.

## Cleanup and validation

The final cleanup review covers nine exact owned synthetic roots, all with matching markers and no active process, open file, or socket.
The receipts remain in the worktree. The [cleanup result](attempt4-cleanup-final.json) confirms the original receipt hashes after supported exact reclaim.
The separate baseline executable container also uses a reviewed supported cleanup command; see [baseline-cleanup.json](baseline-cleanup.json).
The earlier attempt roots and the profile root also use supported exact cleanup, with retained receipt checks.

The final release, full Rust lint, separate fastembed lint, SDK checks, and product source checks pass.
See [validation.json](validation.json) for their exact scope and reported times.
At the local report freeze, final committed-tree checks and GitHub `ci-required` remain pending.

# Core-owned writes: V6 result

The final candidate passes all seven strict ordinary API workflows and all eight matched benchmark cases.
Native reads use captured typed core state and a disk root. They do not capture the private flush queue.
Logical writes publish full values, known absences, headers, and controls through the core model when the allowance permits them.
Untranslatable raw or system writes wait for disk and invalidate the required core state before success.
The [route report](ROUTES.md) separates source coverage from runtime gaps.
The private flush queue owns prepared disk output and its flush lifetimes. Ordinary reads use no queue lookup.
That queue is separate from the mutation-log export and replay contract.

Before is accepted completion `a0ad619076570fc472569712d5a40743a5a32823e909d45d4e33991b7c536285` from merged main `011b29f3…`.
After is `c19a25ea358b06dea5a0612e206524f4e282d5de9ff0eca46570c592b57b6564` from source commit `7876211b6d64dff43ceddd602656017b74860fe9`.
The final source identity contains 1,931 inputs. All inputs and the executable match before and after the workloads.
The [exact Git check](candidate-v6-committed-source-verification.json) matches those inputs against tree `ff994d7740590578d52e2ed85418896fb236db32`.
This comparison measures the core-owned write change. It does not repeat the original-engine, e10-to-v4, or v4-to-completion comparisons.

## Correctness

The [seven-workflow matrix](candidate-v6-matrix.json) and [receipt summary](v6-operational-summary.json) record these results:

- Hash and HashRange creates, partial updates, complete replacements, durable writes, independent mixed mutations, deletes, and separate acknowledged recreates pass.
- Named-database creates, updates, deletes, recreates, personal independence, and reopen pass.
- Protein partial updates and alternate-key moves pass. Repeated bucket-only moves retain shared field UUIDs and unrelated records after reopen.
- The prior completion workflow passes 219 keyed reads, 58 zero-file gates, 30 shared-copy reads, seven HashKey reads, and 24 membership cycles.
- The new logical, scoped, protein, and pressure workflows pass 58 further zero-file gates.
- Eight public Atom UUID reads return exact content while accepted writes remain pending. Their endpoint supplies no file-cost receipt.

The reference has two preserved value failures: stale protein fields after a partial update, and absent named-database values after create ACK.
The final candidate passes those value gates. Earlier failures and driver corrections remain in the [discovery report](DISCOVERY.md).
The first protein declarations create per-field records. An existing document envelope receives the typed write when one exists.
Hash deletion preserves the differently shaped HashRange membership under the existing protein contract.
Same-key delete/recreate uses separate ACKs because the existing mixed-batch entry executes erasures after creates and updates.

Native raw replay stops at its existing backup/cloud activation gate before health.
No replay request or owner provision occurs. Replay, inactive import, personal-share fanout, raw repair, retention, and compaction retain explicit runtime gaps.
Ordinary concurrent readers observe complete allowed states, but the workload does not pause a read at one exact internal instruction.
Source review therefore remains necessary for earlier captured state and reservation lifetimes.

## Matched measurements

The host is an Apple M4 Max with 36 GiB of memory.
Each case uses a fresh generated home with 256 records per schema, four fields, and 1 KiB bodies.
Two rounds use preload ON and OFF in balanced reverse order. Each case passes 87 reads and 14 writes.
The total is 696 read checks and 112 write checks, with no failed request or retry.
The [full comparison](benchmark-v6/TABLES.md) retains all 42 paired read groups and 12 paired write groups.
The [paired JSON](benchmark-v6/paired-tables.json) retains every sample, range, and reported file cost.

Repeated 64-key read medians, in milliseconds:

| Key form | Preload | Before | After | Change |
|---|---|---:|---:|---:|
| Hash | OFF | 4.892 | 4.727 | −3.4% |
| Hash | ON | 4.864 | 4.779 | −1.8% |
| HashRange exact | OFF | 2.103 | 2.327 | +10.7% |
| HashRange exact | ON | 2.356 | 2.344 | −0.5% |
| HashRange bounded | OFF | 4.150 | 4.535 | +9.3% |
| HashRange bounded | ON | 4.966 | 4.946 | −0.4% |

The largest repeated-read regression is exact HashRange width 16, preload OFF: 0.747 to 0.835 ms, or +11.9%.
Repeated Hash width 16 with preload ON rises 10.2%. The largest first-read regression is Hash width 16 ON, +4.3%.
All disjoint-key medians improve in this sample. The full comparison retains the small sample counts for those conditions.

Repeated exact Hash and HashRange reads report zero file calls in both programs.
Preloaded bounded reads also report zero file calls in both programs.
Without preload, bounded 64-key reads still decrypt 17,088 bytes per call in both programs.
Their file-call counts range from 24–32 before and 27–31 after. File layout and file-call counts differ across fresh homes.
Those counts describe software file calls, not physical device reads.

Each write batch contains 64 records. ACK medians and total time through explicit flush appear below, in milliseconds.
New-key and no-prior-value-read groups have two samples each. Read-primed groups have ten samples each.

| Key form | Preload | Caller condition | ACK before | ACK after | ACK change | Through flush change |
|---|---|---|---:|---:|---:|---:|
| Hash | OFF | New keys | 175.678 | 167.810 | −4.5% | −3.6% |
| Hash | OFF | No prior value read | 160.394 | 161.700 | +0.8% | −1.0% |
| Hash | OFF | Read-primed update | 86.066 | 97.796 | +13.6% | +1.2% |
| Hash | ON | New keys | 166.934 | 182.421 | +9.3% | −3.7% |
| Hash | ON | No prior value read | 163.505 | 175.733 | +7.5% | −2.3% |
| Hash | ON | Read-primed update | 86.700 | 130.812 | +50.9% | −0.6% |
| HashRange | OFF | New keys | 240.276 | 222.391 | −7.4% | −2.4% |
| HashRange | OFF | No prior value read | 251.354 | 251.933 | +0.2% | +1.5% |
| HashRange | OFF | Read-primed update | 231.938 | 243.302 | +4.9% | +2.4% |
| HashRange | ON | New keys | 214.873 | 216.983 | +1.0% | −2.8% |
| HashRange | ON | No prior value read | 247.708 | 241.423 | −2.5% | −5.9% |
| HashRange | ON | Read-primed update | 221.672 | 238.207 | +7.5% | −1.1% |

The +50.9% pooled Hash ACK result remains part of the result.
Its ten samples span 72.5–172.6 ms before and 73.9–183.6 ms after, with low and high values in both rounds.
The reference has six fast and four slow samples. The candidate has five fast and five slow samples.
This unresolved variability changes the pooled median. The through-flush median stays close, but that does not erase the ACK regression.
These results support no uniform speed improvement or causal preload explanation.

## Actual ACK observations

All 112 benchmark replies report `queued`.
The [frontier report](benchmark-v6/WRITE-FRONTIERS.md) records the actual state after each reply.
All 56 candidate writes show a pending batch and dirty logical paths after ACK. Every candidate model-refusal delta is zero.
The reference also shows pending work after all 56 ACKs. Its new model counters are unavailable, not zero.
These status samples follow the reply. They do not prove that all write preparation uses memory alone.

The preserved [V5 comparison](benchmark-v5/STATUS.md) exposes a metadata allowance failure.
Sixteen warm HashRange updates hit the metadata cap, then wait for disk despite a queued reply mode.
Source review finds duplicate source-string charges and temporary source-list copies in molecule preparation.
V6 removes those duplicates. It keeps the same 16 MiB metadata cap and reservation ownership.
The complete V5 timings and failures remain unchanged.

## Memory and startup costs

The separate pressure cohort uses sixteen 32 KiB records, six overwrite batches, four readers, and a 16 MiB write allowance.
Its charged body-reservation peak is 10,062,448 bytes, including quote overhead. Its separate metadata peak is 3,203,056 bytes.
Both stay below their separate 16 MiB caps. The matched benchmark instead uses a 128 MiB body allowance.
Its body-owner peak is 23,956,184 bytes, or 22.85 MiB.
After pressure calls finish, retained body and metadata counters are zero. Current clean core values retain charged owners after flush.

No pressure observation captures a positive retained-read interval. Private preparation can also contribute to retained counters.
The runtime samples alone do not establish a deterministic held-read lifetime bound.
The normal configuration separates a 10,000-path warm limit, 16 MiB definitions, 16 MiB typed metadata, and 128 MiB selected-schema keys.
These limits cover different owners. They are not a whole-process memory cap.
Allocation, runtime, active-call, and private preparation costs remain outside a simple sum of retained current values.

The first-query receiver-control read moves to native startup through the existing typed definition path.
The preserved V3 query measures one 16 KiB format read for that control. That measurement is not a final startup total.
Final startup reads the actual receiver registry; its cost depends on registry size.
This benchmark excludes process startup and does not measure that total.

## Limits and local status

Process reopen does not clear the OS file cache. The host has no exclusive reservation.
The small cohort does not establish maximum throughput, M5 performance, large-home performance, cloud cost, or a whole-process memory bound.
Hash and HashRange remain the supported key forms. This work adds no standalone Range query.
The final [validation report](VALIDATION.md) separates root-reported non-test checks, operational receipts, and final report-commit checks.
This task ends with a local report commit. It creates no new PR or merge.

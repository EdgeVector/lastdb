# Data and change notices in one disk commit

The data and its change notices now share one native commit. The reply waits for that commit.
The separate Mini notice queue is removed. The core prepares notices from the resolved schema and key.
The batch reserves memory for the notice bytes before acceptance.

One native commit still uses several file writes and disk-sync calls.
A separate clock reservation can require another commit before the data commit.
A mixed request can also require several data phases. Each phase saves its data and notices together.
The change does not add a migration or activate cloud sync.

The baseline is the previous durable version, with identical native disk counters added.
Both versions require disk completion before each reply. Both use fresh synthetic data.
The original primary and its archive are not used.

The tables measure the pre-integration candidate from `after-v2/source-identity.json`.
Its binary SHA-256 is `68f8943d8d1914aa7acae6cbf5a612c82c273f98385efc5da76da9b29816c5b1`.
The later integration of `origin/main` at `c70e6fdc` changes the source.
The final integrated build uses separate build checks and the `operations-main-r1` workload.
[integration.json](integration.json) records their current status and exact source identity.
The tables do not measure the final integrated build.

The integrated release passes full workspace Clippy, off-lane Clippy, and the release build.
Its API workload passes another 74 changes across 38 calls, four expected refusals, and normal restart checks.
Its binary SHA-256 is `26454b6eb19a555031903be8f85e8ab1ce121b331a29420728f3398fe587da55`.
Its two node sessions stop normally. Its temporary database is removed after an independent receipt and ownership review.

Each final operation read checks the values during the call. Some saved expectation maps contain later values.
The independent review reconstructs all 57 read request hashes and uses the recorded check results.

The Mac uses an Apple M4 Max, 14 CPU cores, and 36 GiB of memory.
Tom permitted concurrent jobs for this comparison. No owned build, archive, or cleanup job overlaps the timed writes.
Other database jobs cause disk traffic in all 3,342 host sample intervals that overlap the write windows.

The order is before, after, after, before. Each run uses a fresh database.
The workload uses four fields, a 1 KiB body, one caller, and batches of 1, 32, or 128 records.
Each category below contains 64 calls per version. It combines Create and Update calls from both runs.
The rate divides the total record count by the total HTTP time. It is not an average of rates.

| Key type | Batch | Before, records/s | After, records/s | Observed change |
|---|---:|---:|---:|---:|
| Hash | 1 | 0.983 | 0.993 | +1.1% |
| Hash | 32 | 9.705 | 10.669 | +9.9% |
| Hash | 128 | 13.257 | 13.968 | +5.4% |
| HashRange | 1 | 0.610 | 0.759 | +24.4% |
| HashRange | 32 | 7.634 | 10.466 | +37.1% |
| HashRange | 128 | 16.127 | 13.973 | -13.4% |

The combined rate is 11.609 before and 12.057 after: an observed increase of 3.9%.
Host load prevents an isolated speed claim. The unchanged baseline records 16.23 and 9.03 records/s across its two runs.
The candidate records 15.02 and 10.07 records/s. These results establish no maximum throughput or M5 result.

The counters below cover each write window through notice completion. Each version contains 384 HTTP calls.

| Counter | Before | After | Reduction |
|---|---:|---:|---:|
| Native accepted requests | 1,131 | 740 | 34.6% |
| Data-file sync calls | 30,356 | 22,842 | 24.8% |
| Directory sync calls | 42,372 | 30,646 | 27.7% |
| Sidecar sync calls | 0 | 0 | — |

The file-sync duration sum is 235.690 seconds before and 177.243 seconds after.
The directory-sync duration sum is 282.445 seconds before and 203.849 seconds after.
These process-wide counters include clock and background work. Duration sums can overlap; they are not request latency.
Native request counts do not count every direct storage commit. They do not prove exactly one total commit per HTTP call.

The pre-integration API checks pass for 768 timed calls and 41,216 mutations.
All four fields, key membership, notice IDs, notice coordinates, and normal reopen checks pass.
The pre-integration additional workload passes for 74 accepted mutations, 38 calls, four expected refusals, and two normal node sessions.
It covers single and batch routes, all requested durability modes, field-derived keys, Delete, conditions, and concurrent calls.
No tests, fixtures, existing database, crash, power-loss, or cloud check runs.

The captured preparation times remain small compared with the reply times.
The inner receipt clock includes the owning disk wait for these measured Create and Update routes.
It does not split disk work, gate waits, or other commit work.
The gate and publication clock fields contain unmeasured zeros. Detailed request phases remain unavailable in this isolated workload.
The optional status observation returns a cached startup snapshot; its report marks all missing write IDs explicitly.

The pre-integration source review confirms one journal commit for the combined data and notice batch.
Clock high-water reservations remain separate and precede the data gate.
The next candidate is bounded reuse of the journal stream. The measured pre-integration engine creates a new stream after every publication.
That rotation adds four file-sync calls and eight directory-sync calls on its normal path.
Those are source-derived counts, not a measured gain. A separate change must preserve recovery, receipts, space limits, and durable replies.

The PR remains a draft because the 128-record HashRange result declines under this shared load.
The change removes the separate notice commit. The timings do not resolve the remaining performance question.

The pre-integration release build, full workspace Clippy, off-lane Clippy, formatting, source identity, and local source checks pass.
The exact source inventories, request receipts, host observations, and node logs have portable byte-for-byte archives.
The archive parts contain all original IDs and times.
The original proof uses ten normal node sessions and six temporary roots. Its cleanup receipts cover those exact owned roots.
The final integration has separate validation and cleanup status in [integration.json](integration.json).

Evidence: [independent result review](independent-result-review.json), [operation review](operation-review.json),
[stage clocks](stages.json), [post-run checks](post-run-checks.json), and [exact archives](archives.json).
The exact baseline patch is [instrumentation.patch.gz](before/instrumentation.patch.gz). Decompress it before `git apply`.
The build uses the 3,600-second host-lock command in [execution.json](execution.json).
The imported inventory helper retains a historical 600-second command template; it is not the executed wrapper command.

# LastDB benchmarks in GitHub Actions

The `LastDB benchmarks` workflow checks each PR update and merge group.
Product changes require the `benchmark-complete` check before merge.
It requires completed measurements and verified artifact uploads. Slowdowns remain advisory.
Build failures, rejected requests, incomplete measurements, and upload failures block completion.
Documentation-only PRs pass the path check without measurements. Unknown paths require measurements.
The workflow also measures each main push. The larger nightly run starts at 09:27 UTC.
PR edits to titles or descriptions do not repeat a benchmark.

Use the CI summary and run link in the PR. Do not commit a duplicate report or receipt.
The required `generated-evidence` check keeps raw output outside Git.

Use the PR process for builds and benchmarks. Do not run them on Tom's machine.
Lightweight source, syntax, and format checks can run locally. Tests remain retired.

## Recipes

| Measure | Standard | Nightly |
|---|---:|---:|
| Seed records | 10,000 | 100,000 |
| Read and concurrency requests per case | 256 | 2,048 |
| Single and batch write requests per case | 32 | 128 |
| Complete base/candidate pairs | 2 | 3 |
| Fields per record | 4 | 4 |
| Body bytes per record | 1,024 | 1,024 |

The manual comparison also offers `batch-insert` for a focused insert review.
It keeps the standard 10,000 records and the same cold and hot read preparation.
It measures 256 insert requests at each batch size, with three alternating pairs.
Each version supplies 768 requests per batch size. Updates and concurrency are excluded.
The raw write samples retain `resident_stages_us` from each response.
The ordinary standard and nightly speed recipes stay unchanged.

Each case uses actual product requests through the daemon's Unix socket:

- Cold reads use exact keys once after the owned daemon restarts.
- Hot reads use 256 keys after an explicit warm-up.
- Read batches contain one record or 32 records.
- Write batches contain one record or 128 records. Each request requires a durable ack.
- Insert cases use new keys. Update cases change existing values on the hot keys.
- Concurrent reads, writes, and mixed work use 1, 4, 16, and 32 clients.
- Mixed work has equal read and write request counts. Its report separates their response times.
- Mixed readers and writers use the same hot keys.

Each read requires a complete result with valid generated values and a storage receipt.
The cold boundary purges clean logical records after the daemon restarts.
Each cold read uses fresh keys and requires disk-read operations plus payload bytes in its receipt.
Each hot read requires at least one warm hit per requested field and key.
The native unknown-path fallback does not always increment the logical miss counter.
The cold control therefore uses the purge boundary and actual disk-read receipts.
These checks reject invalid measurements. They do not form a test suite.

## Base and candidate

A PR run compares its exact merge revision with that merge's first parent.
The event's base value can lag the base used for the candidate merge tree.
The plan records that value and uses the actual merge parent for the comparison.
The candidate includes current base changes, so the comparison measures the proposed merge result.
An older branch cannot turn unrelated changes on main into a claimed PR speed change.
A main push uses its new tip and the previous tip from the push event.
A nightly run uses the selected commit and its first parent.
The workflow records complete SHAs and executable hashes.
The running daemon must report the expected revision.

The driver uses the workflow's exact source revision, separate from both product checkouts.
For a PR, this is the event's merge tree, which includes the current base driver.
An older PR head need not contain any benchmark scripts. The summary records the driver revision.

The separate `LastDB historical benchmark comparison` manual workflow accepts exact baseline
and candidate SHAs. An empty candidate uses the selected workflow revision.
An empty baseline uses the candidate's first parent. All three recipes are available.
The two revisions must differ. The plan rejects identical revisions before builds or cache restores.
This prevents a baseline cache restore from replacing the fresh candidate at the same source path.
Select the current main workflow to measure old product revisions with the current driver.
Manual comparisons do not emit `benchmark-complete` and cannot supply the PR completion gate.

Both builds use one Rust toolchain and the optimized `dev-release` profile.
The workflow caches dependency downloads and exact compiled baseline programs.
Main pushes build and save the accepted candidate before baseline work or measurements.
Only trusted main pushes publish reusable programs.
The cache key includes the full source SHA, Rust compiler, architecture, build flags, and build recipe.
It also includes the Ubuntu release, native compiler/linker versions, and native library package versions.
The hosted runner image label stays in the build record, outside the compatibility key.
Unrelated image software updates therefore do not discard a compatible program.
An exact cache hit must also pass metadata, executable SHA-256, and executable revision checks.
The candidate always builds from source. Each actual build uses a fresh target directory.
PR candidate compilation overlaps the main cache producer before the baseline restore.
On a miss, a PR waits up to ten minutes only while the exact baseline's main run can publish its cache.
A missing producer, incompatible environment, failed lookup, expired wait, or rejected cache uses a fresh baseline build.
No compiled target directory enters the cache.
The summary states the build mode, preparation time, producer run, source, and executable hash.
The first main run fills the new cache. Native toolchain, compiler, source, or recipe changes require another build.
Cache expiry or upload failure affects speed only; the source build remains available.
Builds finish before measurements start. The two daemons never run together.
Each pair reverses the prior pair's base/candidate order.
Every run starts with independent generated data. No primary data or archive data enters a run.
Old benchmark numbers and generated database homes never supply a new measurement.
Both versions run on the current GitHub machine, even when the baseline program comes from the cache.
The daemon uses its default storage, memory limits, and automatic maintenance settings.
Cloud stays off because the generated home has no cloud configuration.

## Disk and memory growth

Each measured PR update, merge group, and main push also uses the short resource recipe.
Nightly runs use the larger resource recipe. Documentation-only PRs still skip measurements.
The resource run uses the same verified programs and new generated homes after the speed run.
It adds no build. It requires a GitHub-hosted runner and cannot run on Tom's machine.

| Resource recipe | Standard | Nightly |
|---|---:|---:|
| Seed records | 512 | 2,048 |
| Rounds per case | 3 | 8 |
| Complete base/candidate pairs | 2 | 3 |
| Idle seconds per checkpoint | 1 | 3 |
| Final records after inserts | 2,048 | 18,432 |

Each run uses four fields and a 1,024-byte body per record.
The cases run in this order on each fresh generated home:

- `fixed-read` reads the same seeded keys each round. The record count stays fixed.
- `fixed-update` replaces the title and body at every seeded key each round. The record count stays fixed.
- `insert` adds one seed-sized set of new keys each round.

Writes use durable batches of at most 128 records. Reads use at most 64 exact keys per call.
Each write requires its complete durable ack and a keyed read of the new values.
Each case records an initial checkpoint and one checkpoint after each round.
A checkpoint requires a durable flush and an explicit full LastDB compaction with zero reclaim grace.
The compaction receipt must confirm complete reclaim. These settings affect only the generated home.
After compaction, keyed reads verify all known records and their latest values before the idle period.
The normal daemon memory limits and automatic maintenance stay enabled.
This recipe measures retained space after explicit compaction. It does not measure the default retention policy.

The artifact retains disk bytes before and after compaction, bytes per live record, and every compaction receipt.
It also retains daemon memory samples at 200 ms and memory samples through each idle period.
Idle memory uses the median after compaction. The peak includes the case, compaction, and idle period.
Seed and startup stay outside the peak. The seeded warm records remain part of daemon memory.
The summary reports growth from the initial checkpoint and the slope through all checkpoints.
Baseline and candidate results use medians across alternating pairs. Per-pair changes remain in the artifact.

Resource regressions stay advisory. A notice requires both a 20% increase and an absolute excess:

- Peak memory, final idle memory, or memory growth: at least 16 MiB more than the baseline.
- Final compacted disk size or disk growth: at least 1 MiB more than the baseline.

Zero or negative baseline growth uses zero for the percentage reference. The absolute excess still applies.
Every change appears in the summary, even below the notice threshold.
Resource regressions do not fail `benchmark-complete`. Missing samples, incomplete compaction, and failed uploads remain errors.
The resource step has a 40-minute limit. The complete job has a 150-minute limit for larger nightly work.
Raw results stay under `resource-growth/` in the existing CI artifact and expire after 14 days.
The manual comparison accepts `resource_only` to measure either resource recipe without the ordinary speed cases.
Automatic PR, merge group, main, and nightly runs always require both speed and resource measurements.

CAUTION: A positive slope alone does not establish a memory leak or unwanted disk growth.
Inserts increase the record count. Memory allocators can retain space, and automatic maintenance can change disk use.
Disk size counts file lengths, excludes short-lived disk peaks, and does not measure allocated blocks.

## Request phases

Each measured call carries a unique request ID.
After each write case, the driver requests the current request records from the explicit diagnostic status route.
It matches records by the exact request ID and the `github-benchmark` client name.
The samples retain each match. The `request-phases/` directory retains the full diagnostic response and the match count.
The diagnostic call starts after the timed calls, the memory samples, and the final disk-size observation.
It does not enter those measurements. It can give maintenance extra time between cases.
Compare base and candidate runs from the same driver. Do not reuse old measurements for this comparison.

The request ring holds 256 records. A standard case has 256 calls; a nightly case has 2,048 calls.
Other calls can displace measured records. An older baseline can also return a cached request list with no matches.
The artifact states the exact coverage. An absent record or phase is unreported, not zero.
The report lists the five largest reported phases for each slow call.
Socket queue wait precedes server duration. Parent and child phases can overlap, so their values must not be added.
These phases locate time in the request code. They do not prove a physical disk flush or storage correctness.

## PR 317 native diagnostics and read repeat

PR 317 uses four ordinary base/candidate pairs with the standard request sizes and case order.
This repeat retains the mixed-read P99 for each pair. The ordinary daemon has native diagnostics off.
The shared workflow defaults stay unchanged for other PRs, main pushes, and nightly runs.

A separate diagnostic pair uses fresh generated homes and enables the existing `LASTDB_SNAPSHOT_TIMING` output.
This mode requires a GitHub-hosted Actions runner. It follows the ordinary measurements and uses the same exact programs.
The diagnostic results stay in `native-diagnostics/` within the run artifact.
They supply phase evidence, not the ordinary speed comparison.

Each timed case records its outer UTC bounds outside the response-time measurement.
Each native scope must match one physical commit through its sequence and enclosed time interval.
The scope count must equal the actual successful write-call count for that case.
Missing or ambiguous evidence fails the diagnostic capture.
Each physical commit enters the totals once, even when it serves several requests.
The artifact retains request counts, phase coverage, physical counts, and the slowest native records.
Missing phase fields stay unreported. Parent and child phase totals can overlap.
The physical request count can include background work. It is not an exclusive cost for each client call.

## Results and limits

Open the workflow summary from the PR checks. Download its `lastdb-benchmarks` artifact for detail.
The artifact retains raw samples, read receipts, p50/p95/p99 response times, and requests per second.
It also retains records per second, sampled daemon memory size, disk growth, and build logs.
Per-pair changes and each run's metrics remain available beside the combined result.
Artifacts expire after 14 days. Raw results do not enter the source repository.
Each run keeps raw output in its unique runner temporary directory.
The workflow verifies the archive ID, nonzero size, SHA-256 digest, and expiration through the GitHub API.
The job summary records the download URL, digest, source revision, and expiration.
A separate compact receipt artifact retains those fields as `artifact.json`.
The driver stops each owned daemon and removes its generated home before it completes.
The artifact contains no generated identity or database home.

CAUTION: Shared GitHub machines vary. A small change does not establish a product slowdown.
The report shows every change. A p95 increase or request-rate decrease of 20% emits an advisory notice.
The notice does not change the exit status. No speed limit enters branch protection.

Cold means a LastDB warm-set miss. The operating system file cache stays uncontrolled.
The workflow does not claim a physical-device cold read.
Response time starts at socket request dispatch and ends after the full response arrives.
Payload preparation and response validation stay outside that timer.
Throughput includes client validation and scheduler costs. It does not prove maximum daemon throughput.
The workload uses a closed loop. It sends each client's next request after the prior response.
Memory samples occur every 200 ms during a case. They exclude seed and startup.
Disk size counts file lengths, not allocated blocks. Background maintenance can change disk size during a sample.
Files and directories removed before their size read contribute no bytes to that sample.
The standard batch-write p99 uses only 32 samples per run. Read the raw samples before a speed decision.
The workload has no cloud, external file blobs, or crash-recovery phase.

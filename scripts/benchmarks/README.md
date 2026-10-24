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

A PR run compares its exact event base with the event's merge revision.
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
An empty baseline uses the candidate's first parent. Either recipe is available.
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

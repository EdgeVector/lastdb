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

A PR run uses its exact head commit and the base SHA from the PR event.
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
The workflow caches dependency downloads only. Each version uses a fresh target directory.
This prevents an old storage library from entering a candidate through an artifact cache.
Builds finish before measurements start. The two daemons never run together.
Each pair reverses the prior pair's base/candidate order.
Every run starts with independent generated data. No primary data or archive data enters a run.
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

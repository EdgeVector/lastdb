# Latency controls

This manual CI operation investigates the concurrent-write advisory in run 40464008501.
The same workflow also runs when a PR changes these control files.
It does not provide benchmark-complete or replace a normal merge gate.

The product and historical measurement driver both use commit
688222b04d18d717da913007cb41e7174e48f395. The controller has its own recorded source commit.
The workflow accepts no alternate runner, product revision, or paid resource input.
It requires the public EdgeVector/lastdb repository and a standard GitHub-hosted Ubuntu 24.04 runner.
All builds and measurements run in CI. The primary and its archive stay outside this operation.
Tests remain retired. The operation uses actual product requests and generated homes.

## Build controls

Both fresh builds finish before measurements start. Each build uses the same product directory,
Rust toolchain, optimized dev-release profile, four build jobs, version string, and target directory.
The controller removes its one owned target after each build. It keeps the two executable pairs in separate directories.
Only dependency downloads enter the cache. No compiled program or compiled target enters it.
Both compiler wrappers are explicitly disabled.
The records include source revisions, build context, executable hashes, CPU details, and runner image.
Equal hashes from the two builds are a valid result.

The build budget requires 24 GiB of free space. The measurement budget requires 16 GiB.
CI removes installed runner tools only when the initial build budget needs that space.
The job limit is 120 minutes. Each build has a 15-minute limit; each comparison has a 40-minute limit.

## Measurement order

1. The same-binary comparison uses executable A for both labels.
2. The fresh-builds comparison uses executable A and separately compiled executable B.

Each comparison uses four complete pairs with orders AB, BA, AB, BA.
The historical standard recipe retains its 10,000 seed records and full case order.
Each concurrency case has 256 requests. Mixed work at 32 clients has 128 reads and 128 writes.
Each client alternates reads and writes. The controller keeps every request in its case.
The product keeps its default workers, memory limits, encryption, storage, and maintenance settings.
Each run has a new generated home. Normal identity and data-layout variation remain part of the measurement.

The controller requires complete request records, validated durable write acknowledgements,
successful shutdown receipts, and removal of every generated home before it marks the controls complete.
The summary retains per-pair values, mixed read and write results, request phases, and coverage.
These observations do not establish a source-code cause or a fixed speed guarantee.

## Evidence

Raw output, build logs, and samples stay in RUNNER_TEMP. Compiled programs are removed after the comparisons.
CI uploads the result files for 14 days and verifies the artifact ID, SHA-256 digest, and expiration.
The receipt names the controller revision. The result manifest separately names the pinned product and driver revisions.
Use the CI summary and run link as the validation record. Do not commit a duplicate report or raw output.

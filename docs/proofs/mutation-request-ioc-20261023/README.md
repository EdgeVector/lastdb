# Single mutation: explicit services and latency comparison

The single mutation handler mixed request rules, request order, and host work.
The refactor gives the request coordinator explicit services and puts pure rules
in a separate module. The host adapter retains access checks, write admission,
durability, error responses, and convergence. One boxed core future prevents a
debug worker stack overflow after the split. No dependency container is added.

The local build rule also changes. Ordinary builds and checks run directly.
The host lock is optional for performance measurements. Its default acquire
wait is 60 seconds. The performance guard builds before it takes that lock.
The change does not alter GitHub checks or the primary node.

The local comparison shows small median differences and variable tail latency.
It does not show a consistent latency regression or establish a speed improvement.
All eight runs return the expected checked values before and after restart.

| Operation | Before median, ms | After median, ms | Change | Before p95, ms | After p95, ms | Before p99, ms | After p99, ms |
|---|---:|---:|---:|---:|---:|---:|---:|
| Create | 12.506 | 13.028 | +4.2% | 19.263 | 19.058 | 28.207 | 26.984 |
| Update | 13.159 | 13.052 | -0.8% | 20.523 | 19.529 | 33.620 | 38.139 |
| Conditional update | 14.192 | 14.002 | -1.3% | 22.203 | 19.501 | 39.650 | 23.975 |
| Update, four callers | 49.093 | 47.867 | -2.5% | 132.248 | 127.845 | 182.237 | 185.764 |
| Read by key, control | 0.111 | 0.113 | +1.6% | 0.127 | 0.132 | 0.153 | 0.178 |

Each cell is the median of four run percentiles. Update p99 increases by
4.519 ms, or 13.4%. The read control p99 also increases, and the per-run tails
vary substantially. This shared Mac result does not isolate small tail changes.
The raw data retains those values; no outlier is removed.

Four-caller throughput is 65.38 requests/s before and 65.56 requests/s after.
Median memory size at the end is 124.99 MiB before and 121.11 MiB after.
Those memory samples are not peak memory measurements.

The before source is `9cb89c6776bd654aa8ac93b553db278e5938c33a` on main.
The after source is `f4abfaaed22265ed7e4d60c2562c091724644084`.
Both builds use Rust 1.96.1 and `cargo build --profile dev-release -p lastdb_node
--bin lastdbd --locked`, with two build jobs. The profile uses optimization
level 3, no LTO, and 16 codegen units.

The protocol uses four balanced pairs: before/after, after/before,
before/after, after/before. Each run starts a fresh empty home and loads one
published schema from schema service. The primary and its archive supply no data.
Encryption uses the normal product path; cloud sync stays off.
Each node uses its default worker stack.

Each run has 128 warm-up writes, then 300 creates, 300 updates, 300 conditional
updates, 400 updates from four callers, and 300 reads by key. The value is
1 KiB. Every mutation requests a durable receipt. Single-request latency is
the purpose of the serial mutation phases; ordinary application batching rules
do not change. Per side, the comparison measures 5,200 mutations and 1,200 reads.

The timer covers a new Unix socket HTTP connection, the request, and the response
bytes. JSON serialization and response parsing stay outside the timer.
Schema setup, warm-up writes, batch value checks, and restart checks are untimed.
The final batch reads verify 64 updated keys. Every node exits normally, and
the driver removes each owned data home after the result is saved.

Host processes remain outside this experiment's control. The driver saves
host load and the highest CPU processes before and after each run.
No compiler runs as part of the measurement. No host lock delays other work.

Sequential builds reuse one target directory. The baseline retains the
candidate's cached CLI version stamp. Source revisions come from the clean
worktrees and compiler source paths. The binaries have distinct SHA-256 hashes.
The baseline has no refactor adapter symbols; the candidate has five.
`builds.json` records this version-stamp limitation and the symbol evidence.

Evidence:

- `measure.py`: the product benchmark driver; no test suite or fixture harness.
- `builds.json`: source revisions, build options, hashes, and adapter symbols.
- `results/inputs.json`: settings and binary identities.
- `results/runs.json`: eight complete runs with value and restart receipts.
- `results/comparison.json`: before/after percentile values and changes.
- `results/*-samples.json`: all warm-up and measured latency samples.
- `results/*.log`: owned node startup and shutdown records.

Local format, source-size, redaction, byte-slice, URL, and shell syntax checks
pass. Both optimized builds and strict compiler lint pass. GitHub's
`ci-required` check remains required for merge. No tests run.

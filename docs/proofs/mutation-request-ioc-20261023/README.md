# Single mutation latency comparison

[PR #286](https://github.com/EdgeVector/lastdb/pull/286) separates mutation request
rules, request order, and host work. The local comparison shows small median
differences and variable tail latency. It does not establish a speed improvement.
All eight runs return the expected checked values before and after restart.

| Operation | Before median, ms | After median, ms | Change | Before p95, ms | After p95, ms | Before p99, ms | After p99, ms |
|---|---:|---:|---:|---:|---:|---:|---:|
| Create | 12.506 | 13.028 | +4.2% | 19.263 | 19.058 | 28.207 | 26.984 |
| Update | 13.159 | 13.052 | -0.8% | 20.523 | 19.529 | 33.620 | 38.139 |
| Conditional update | 14.192 | 14.002 | -1.3% | 22.203 | 19.501 | 39.650 | 23.975 |
| Update, four callers | 49.093 | 47.867 | -2.5% | 132.248 | 127.845 | 182.237 | 185.764 |
| Read by key, control | 0.111 | 0.113 | +1.6% | 0.127 | 0.132 | 0.153 | 0.178 |

Each cell is the median of four run percentiles. Update p99 increases by
4.519 ms, or 13.4%. The shared Mac results do not isolate small tail changes.
Four-caller throughput is 65.38 requests/s before and 65.56 requests/s after.
Median memory size at the end is 124.99 MiB before and 121.11 MiB after.
These memory samples do not measure the peak.

The before source is `9cb89c6776bd654aa8ac93b553db278e5938c33a`.
The after source is `f4abfaaed22265ed7e4d60c2562c091724644084`.
Both builds use Rust 1.96.1 and the `dev-release` profile: optimization level 3,
no LTO, and 16 codegen units. The original PR retains the historical raw data.

| Binary | SHA-256 |
|---|---|
| Before | `7672157699cf44b494aa26389722cd8eed507bcac2e623c44f0686ea565ee864` |
| After | `258ecc97368cab4b39e12ba3d86f6f0799241eb9228ab2c50426e2278a686e0f` |

The baseline retains a cached candidate CLI version stamp after sequential
target reuse. Clean source worktrees, compiler source paths, distinct binary
hashes, and adapter symbols identify the builds. Future comparisons should use
a separate target directory for each source worktree.

The protocol uses four balanced pairs with 1 KiB values, normal encryption,
durable receipts, cloud sync off, and the default worker stack. Each run starts
an empty home and loads one published schema. No primary data is used or copied.
Per side, it measures 5,200 mutations and 1,200 reads after 128 warm-up writes
per run. Final batch reads check 64 updated keys before and after restart.
All owned nodes stop normally, and all temporary node data is removed.

The timer covers a new Unix socket HTTP connection, the request, and response
bytes. JSON conversion, schema setup, warm-up writes, and value checks are untimed.
Other host processes remain outside the experiment's control.

`measure.py` retains the benchmark procedure. Set its `--output` path outside
the repository. Keep generated samples, logs, and run details outside Git.
The scoped ignore rule excludes the historical `results/` output path.

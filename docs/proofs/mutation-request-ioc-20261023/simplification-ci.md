# Mutation flow simplification: CI latency comparison

The four pairs show no consistent latency penalty from the simplification.
The median write changes range from +0.5% to +1.5%. Individual pairs change in both directions.
The largest serial median increase is 0.201 ms for an update.
Conditional update p99 increases by 0.545 ms; four-caller update p99 decreases by 6.908 ms.
These measurements do not establish a small general speed change.
All eight runs complete. All acknowledged-value checks pass before and after restart.

Before: `12fb0890b396e6adcbe62d1db257bbe814160239`. After: `fa83333bc31640c574827eda5685afea14aa95cc`.

Each value is the median of four run percentiles. All values are milliseconds.

| Operation | Before p50 | After p50 | Change | Before p95 | After p95 | Before p99 | After p99 |
|---|---:|---:|---:|---:|---:|---:|---:|
| create | 12.118 | 12.180 | +0.5% | 13.427 | 13.795 | 14.606 | 14.843 |
| update | 13.243 | 13.444 | +1.5% | 14.860 | 14.913 | 16.426 | 16.249 |
| conditional_update | 13.962 | 14.059 | +0.7% | 15.409 | 15.605 | 16.780 | 17.325 |
| concurrent4_update | 43.595 | 43.977 | +0.9% | 120.216 | 117.540 | 168.487 | 161.580 |
| read_control | 0.399 | 0.395 | -0.9% | 0.436 | 0.436 | 0.450 | 0.461 |

Method: four alternating pairs on one GitHub runner, with separate optimized builds.
Each run starts with generated data, 1 KiB values, durable writes, and 128 warm-up writes.
Each side measures 5,200 mutations and 1,200 control reads in total.
The write cases use `/api/mutation`. The driver checks the final values before and after restart.
The timer covers a new Unix socket connection through the response bytes.
Schema setup, JSON conversion, and value checks remain outside the timer.
Limits: one shared GitHub machine, four pairs, cloud off, one schema, and no prefix or aggregate case.
Small differences and variable tail latency do not establish a general speed change.

Driver: `python3 docs/proofs/mutation-request-ioc-20261023/measure.py` with `--rounds 4`.
Build: `cargo build --locked --profile dev-release -p lastdb_node --bin lastdbd`.
Driver SHA-256: `a62c890cc43fb42b1cff34dca71b07e460fac942b5a7948b63009cd6ef6f6007`.

Before executable SHA-256: `23d318ab6422a7bab17e0069349375075dca399f8812eba2045273827d6e9033`.
After executable SHA-256: `94533596e36411861100c92d81abbc2d463681a8f03cfb5c25428ab7459c7b72`.

Per-pair p50 changes:

| Pair | Create | Update | Conditional update | Four callers | Control read |
|---|---:|---:|---:|---:|---:|
| 1 | -4.8% | -3.2% | -2.6% | -2.7% | -1.0% |
| 2 | -0.8% | +5.2% | +3.7% | +0.8% | -0.9% |
| 3 | -0.5% | -0.0% | -0.9% | -1.6% | -5.7% |
| 4 | +4.7% | +2.9% | +0.3% | +3.1% | -0.6% |

[Successful CI run](https://github.com/EdgeVector/lastdb/actions/runs/40014783513). [Raw evidence archive](https://github.com/EdgeVector/lastdb/actions/runs/40014783513/artifacts/12527684605).
Archive SHA-256: `ad25916a312bc10e0c29096f4a272b2dfe70e603980c5fd54d0d14138bef5a8b`.
Retention: 14 days. Expiration: `2026-11-06T20:09:41Z`.
CI driver revision: `6a2e089f0a3280b3b3c27f1e74bd6da78e6b68c0`.
The archive download matches the GitHub digest and contains eight complete run records.
Raw output stays outside Git. The temporary runner workflow does not enter main.

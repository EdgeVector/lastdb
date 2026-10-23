# Longer durable-write follow-up

The primary matrix had mixed durable-write results, including a +21.0% plain
single-write p50 and +6.0% preloaded durable batch32 p50. Plain single-write pairs
were -6.7%, +37.2%, -11.6%, +39.2%, with the large penalties in candidate-first pairs.
This triggered a longer comparison before making the merge decision.

The same verified binaries and CPU/sampler controls are used. Fresh byte-identical
baseline fixtures per configuration. Four alternating adjacent pairs per configuration
with initial version order reversed from primary. Warm all 512 affected keys first.
Measure 512 durable single Hash writes (plain) or 128 durable batch32 HashRange
writes (preload), plus 64 forensic status requests/run. Eight write blocks retained.
Validate all 512 affected records again after flush/reopen and the same telemetry
sample/aggregate invariants. All 16 runs and 6,144 timed requests passed; 34 graceful
unforced zero-exit stops including fixtures. All owned synthetic homes cleaned.

This measures longer writes with warmed keys, rather than repeating the primary's
short 64-single-write block after reopening (which warmed only range records).
Do not pool the two experiments or claim this resolves every cold-write condition.

## Results (ms/request)

### plain

| Workload | Before p50 | After p50 | Change | Before p95 | After p95 |
|---|---:|---:|---:|---:|---:|
| hash_long_durable_write1 | 17.979 | 17.461 | -2.9% | 23.281 | 24.874 |
| telemetry_forensics | 7.121 | 7.113 | -0.1% | 9.008 | 9.325 |

hash_long_durable_write1 paired p50 changes: -8.8%, -0.4%, -13.0%, -2.2%.

telemetry_forensics paired p50 changes: -1.1%, +4.2%, -1.2%, -6.5%.

### preload

| Workload | Before p50 | After p50 | Change | Before p95 | After p95 |
|---|---:|---:|---:|---:|---:|
| range_long_durable_write32 | 65.555 | 63.470 | -3.2% | 97.978 | 96.905 |
| telemetry_forensics | 6.104 | 6.113 | +0.1% | 7.323 | 7.054 |

range_long_durable_write32 paired p50 changes: -1.9%, -3.9%, +10.5%, -2.4%.

telemetry_forensics paired p50 changes: -1.7%, -0.7%, -2.0%, +1.9%.

## Limits and reproduction

One host/client, OS caches and neighboring load uncontrolled, four workers on
one server CPU; no production capacity or statistical significance established.
The full source equivalence review and primary evidence remain applicable.

```bash
python3 docs/proofs/phase-telemetry-benchmark-20261023/focus.py --binaries /absolute/binaries --output /new/focused --scratch /new/owned-homes --provenance docs/proofs/phase-telemetry-benchmark-20261023/provenance.json
python3 docs/proofs/native-routing-benchmark-20261023/summarize.py /new/focused
```

CPU 0 and CPU 1 must be allowed on this host. Binary hashes must match provenance.

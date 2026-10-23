# Longer multi-get/write follow-up

The primary matrix measured structured-JSON p50 improvements of 4.8–8.6%, but
flat multi-get32 p50 was +9.6% plain and +27.3% preload. Encrypted durable batch32
was +9.5%, and encrypted durable singles +3.8% (all four pairs positive). Those
unfavorable results prompted this follow-up before the merge decision.

Same verified binaries, matching baseline-created fixture clones, separate fixed
client/server CPUs, four configured workers, and the host performance lock.
Four adjacent alternating pairs/configuration, with initial version order reversed
from primary. All three configurations retained. Repeat the ten fingerprint probes
and the exact primary structured-read prelude: 128 range16, 64 range64, 32 range256
queries (untimed here). Then time eight blocks of 128 identical flat multi-get32
requests, recording every block during the run. Encrypted runs additionally
warm all Hash keys and time 256 durable singles plus 64 durable batch32 requests.
Read back all flat/nested values after flush/reopen. All metadata and complete
pre-write fingerprint checks apply. No Delete/Purge probe in this follow-up.

All 24 runs and 27,136 timed requests passed; 51 graceful unforced zero-exit stops
including fixtures. All synthetic homes were ownership/process-checked and cleaned.
The primary short-window results remain documented, and these differently sized
workloads are not pooled with them.

## Results (ms/request)

### encrypted

| Workload | Before p50 | After p50 | Change | Before p95 | After p95 |
|---|---:|---:|---:|---:|---:|
| focused_multiget32 | 3.778 | 3.727 | -1.3% | 4.280 | 4.272 |
| hash_durable_write1 | 16.990 | 17.257 | +1.6% | 20.507 | 20.565 |
| range_durable_write32 | 71.224 | 72.631 | +2.0% | 82.062 | 84.443 |

focused_multiget32 paired p50 changes: +0.4%, -1.8%, -0.9%, -1.8%.

hash_durable_write1 paired p50 changes: +6.5%, +8.0%, -1.3%, -10.9%.

range_durable_write32 paired p50 changes: -4.4%, +5.6%, +13.6%, -5.7%.

### plain

| Workload | Before p50 | After p50 | Change | Before p95 | After p95 |
|---|---:|---:|---:|---:|---:|
| focused_multiget32 | 3.290 | 3.280 | -0.3% | 3.944 | 3.835 |

focused_multiget32 paired p50 changes: +0.7%, +1.8%, -1.9%, -2.5%.

### preload

| Workload | Before p50 | After p50 | Change | Before p95 | After p95 |
|---|---:|---:|---:|---:|---:|
| focused_multiget32 | 3.787 | 3.736 | -1.3% | 4.258 | 4.229 |

focused_multiget32 paired p50 changes: +3.2%, -0.6%, -1.9%, -1.5%.

## Limits and reproduction

One host/client, small warm data, OS caches and neighboring load uncontrolled;
four workers share one server CPU. No significance/production capacity claim.
Longer measurement and different later write placement do not rule out every
mixed-workload transient in the primary matrix. Retain both experiments.

```bash
python3 docs/proofs/owned-query-format-benchmark-20261023/focus.py --binaries /absolute/binaries --output /new/focused --scratch /new/owned-homes --provenance docs/proofs/owned-query-format-benchmark-20261023/provenance.json
python3 docs/proofs/native-routing-benchmark-20261023/summarize.py /new/focused
```

Select two allowed CPUs with --client-cpu / --server-cpu if necessary.

## First-block check and assessment

The first 128-request block repeats the primary short measurement window after
the same structured-read prelude. Preload paired p50 changes were -2.7%, -1.7%,
-2.1%, -1.5%; the prior +27.3% median penalty did not reproduce even in this first
block. All eight blocks were checked before the raw records were removed from the
current tree; see RESULTS.md for the artifact retention note. Longer multi-get
medians were -0.3% plain, -1.3% encrypted and -1.3% preload. Encrypted durable single/batch
medians were +1.6%/+2.0%, with positive and negative pairs. This is reassuring,
but does not explain the original spikes or establish zero performance risk.
Recommendation: merge the simpler ownership path for its observed structured-read
benefit, without a write improvement claim.

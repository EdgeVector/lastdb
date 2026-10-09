# LastDB ops telemetry

`lastdb ops` reads the live daemon's in-memory request telemetry from
`/api/status`. The CLI itself never writes metrics, so inspecting load does
not amplify the load being measured. Durable history is the sampler's job and
is bounded by design: its default sink is a JSONL log under the node home
(`logs/self-metrics.jsonl`), and setting `LASTDB_SELF_METRICS_TO_DB=1` on the
daemon additionally persists per-minute *aggregate deltas* as
`lastdb_telemetry/RequestOpsRollup` rows — never per-request samples. See
[Durable request-ops rollups](#durable-request-ops-rollups---since) below.

For the app/verb latency dashboard, run:

```bash
lastdb ops --by-app
```

Each row groups requests by the self-reported app label from
`X-LastDB-Client` (or the legacy `X-App-Id` fallback) and the Mini request
verb. `avg` is computed from the live aggregate counters. `p95` is computed
from samples still present in the recent in-memory ring, so it is a recent
shape signal rather than durable history. Empty output after a daemon restart is
expected until clients generate traffic.

Use the full offender view when a dashboard row looks suspicious:

```bash
lastdb ops
```

That view keeps the schema-level total-time table and slowest recent samples,
including request IDs when clients provide `X-LastDB-Request-Id`.

It also ranks **request body bytes by caller** (`Top by request body bytes`):
for each `(client, kind, schema)` key the node sums the HTTP request body sizes
it already records on every sample. That is the cheap answer to "who is stuffing
the node" — write volume by caller — complementary to wall-time and cold-load
rankings. It is **not** net disk growth (tips, indexes, and history amplify
beyond the request body). The table is omitted when every work key has a zero
body sum. Compact `lastdb ops --by-app` appends `body_sum=` / `body_max=` on
app/verb rows that recorded any body bytes.

## Durable request-ops rollups (`--since`)

The in-memory ring and aggregate table reset on daemon restart. For history
that survives restarts, the self-metrics sampler (60s cadence) writes bounded
`lastdb_telemetry/RequestOpsRollup` rows when the daemon runs with
`LASTDB_SELF_METRICS_TO_DB=1`. This is opt-in and off by default; without it
the sampler only appends to the JSONL log. Read the durable window with:

```bash
lastdb ops --since 6h
```

`--since` queries the rollup rows in the window and merges them into the same
offender tables the live view uses.

Each row is one `(client, kind, schema)` bucket for one sample interval and
carries interval **deltas** for `count`, `sum_ms`, `error_count`,
`sum_cold_shard_loads`, `sum_body_bytes` (write volume), the per-phase mutation timing sums
(`sum_queue_wait_us`, `sum_admission_wait_us`, `sum_parse_us`,
`sum_schema_resolve_us`, `sum_validate_us`, `sum_apply_us`, `sum_persist_us`,
`sum_flush_us`, `sum_index_wait_us`) and `phase_count` (how many requests in
the interval reported a phase breakdown). `max_ms`, `max_body_bytes`, and
`last_ts_ms` are copied cumulatively (running max / latest, not deltas). Phase fields are sums in microseconds precisely so intervals
stay delta-able and merge by addition.

Compatibility is append-only in both directions: the sampler upgrades the
rollup schema in place when it sees missing fields, and rows written before a
field shipped read back as 0 — for phase fields that means "no phases
reported", not a measurement of zero.

Known limitation: the sampler's delta baselines are kept for the life of the
process and never pruned. If an aggregate bucket is evicted from the bounded
in-memory table and later recreated, its cumulative counters restart at zero
while the stale baseline persists, so that bucket under-reports in rollup rows
until its counters pass the old baseline.

## Request phases (slow-write triage)

Both the live view and `--since` end with a `Request phases` table when any
key in the window reported per-phase timings: queue wait, QoS admission wait,
parse, schema resolve, validate, apply, persist, flush, and background index
wait, as microsecond sums per `(client, kind, schema)` key.
`phased=<reported>/<total>` is the denominator — how many of the key's
requests carried a phase breakdown at all.

Zero-hide applies on both axes: phases that measured zero are omitted from
each row, and the table is absent entirely when nothing reported phases.
Flush in particular stays near zero on default builds (background flusher;
sync only under `LASTDB_MUTATION_SYNC_FLUSH=1`), so a visible `flush=0` would
read as broken instrumentation rather than a measurement.

Proof sequence for "where did my slow write spend its time":

```bash
lastdb ops                # live: Request phases table + phases[...] on Slowest recent rows
lastdb ops --since 1h     # durable: merged rollup phase sums (daemon runs LASTDB_SELF_METRICS_TO_DB=1)
```

`Slowest recent` rows combine `req=<id>` (when the client sent
`X-LastDB-Request-Id`) with the same `phases[...]` breakdown, so one row
answers both "which request" and "where did its time go".

## Mini QoS and Busy-Node Triage

Mini's heavy-query admission gate lives in `lastdb_host/src/qos.rs`. It splits
work into two lanes:

- `Interactive`: small bounded reads and writes, such as point reads and normal
  brain/kanban mutations.
- `Bulk`: large writes, raw blob fetches, unbounded scans, and full-cap page
  reads.

The default colocated-Mini budget is 64 total permits and 8 bulk permits. That
keeps most slots reachable by interactive work while still allowing LastGit pack
IO and other bulk work to make progress. Operators can tune the gate at daemon
start with:

```bash
LASTDB_QOS_TOTAL=64
LASTDB_QOS_BULK=8
LASTDB_QOS_DISABLE=1
```

Use `LASTDB_QOS_DISABLE=1` only for an explicit A/B measurement or emergency
bypass. Normal busy-node handling should treat QoS rejections as backpressure,
not as daemon failure.

`GET /api/status` exposes the live gate under `status.qos`:

| Field | Meaning |
| --- | --- |
| `total_permits` | Current global DB-operation budget. |
| `bulk_permits` | Current heavy-operation lane budget. |
| `total_in_use` | Global permits currently held by both lanes. |
| `bulk_in_use` | Bulk-lane permits currently held. |
| `interactive_sheds` | Cumulative interactive acquisitions that timed out and returned 503. |
| `bulk_sheds` | Cumulative bulk acquisitions that timed out and returned 503. |

`lastdb status` prints the same counters as:

```text
QoS: total=<in_use>/<permits> bulk=<in_use>/<permits> sheds_i=<count> sheds_b=<count>
```

When a client sees `503`, `service_timeout`, `node did not respond`, or
`too many concurrent reads` during a hot window, check `lastdb status` and
`lastdb ops` before escalating. `lastdb status` exits 0 only when the owner
socket answers `/health`; exit 1 means the daemon is not serving (line 1
is `lastdbd: not reachable — <reason>`). A reachable-but-degraded node
still exits 0 and keeps the gauge lines. Rising `sheds_b` usually means bulk work is
being throttled as intended. Rising `sheds_i` means the interactive reservation
was still saturated; reduce concurrent clients, postpone bulk jobs, or lower
`LASTDB_QOS_BULK` on the next planned daemon restart. Do not run `doctor`,
`init`, or restart the primary node just because QoS counters are moving.

## Resident graph telemetry

`GET /api/status` exposes the process-local resident graph under
`status.resident`, and `lastdb status` prints a `Logical keys:` line plus a
compact `Resident:` line:

```text
Logical keys: 7 / 10000 (memory limit for logical keys (tips and atoms); RESIDENT_KEY_CAP=10000)
Resident: entries=7 bytes=4.00 KiB graph_budget=16.00 KiB hits schema/molecule/atom/blob/protein=1/3/5/7/9 rehydrates=2/4/6/8/10 persist enqueued/flushed/failed=11/12/13 evicted=14 dirty_refused=15
```

The `Logical keys:` line is the memory limit for tips and atoms
(`resident.resident_key_count` / `resident.resident_key_budget`). Do not tune a
byte setting to size that set.

`LASTDB_HASH_GROUP_WARM_BYTES` (`hash_group_warm=` on the `Memory budget:`
line) bounds the hash-group warm set for non-logical collections (indexes,
schema_index, atom_ref_edges_v2, keep_small, metadata, cas_blobs).
`LASTDB_RESIDENT_BYTES` (`resident_graph=` / `graph_budget=`) bounds the
ResidentGraph (`LASTDB_RESIDENT_MODE=write`).

The `hits` and `rehydrates` buckets come from `ResidentMetrics::snapshot()` and
are process-lifetime counters. They tell whether reads are being served from T0
memory or repopulating it from durable storage. `persist_enqueued`,
`persist_flushed`, and `deferred_persist_failed` track the deferred write drain:
a rising failure count means the keys remain dirty for retry. `evicted` counts
clean ResidentGraph entries dropped by the graph byte budget, while
`dirty_refused` counts budget passes that could not get under budget because
every candidate was dirty.

`entries`, `bytes`, and `graph_budget` are instantaneous ResidentGraph occupancy.
`graph_budget=unbounded` means the ResidentGraph byte cap is disabled for that
process.

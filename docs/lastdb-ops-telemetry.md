# LastDB ops telemetry

## Panic and daemon diagnostic privacy source change, 2026-10-28

The proposed source replaces raw panic reports with fixed fields.
New reports contain a fixed panic marker, numeric source line and column, numeric package version, timestamp, OS, and architecture.
They contain no panic payload, source path, backtrace, port, or log tail.
The daemon installs a fixed stderr marker before its setup and adds the report hook after it resolves the home.
The hooks do not call the previous hook, which can print raw panic details to captured stderr.
The CLI keeps exit status 141 for the existing broken-pipe condition and uses the fixed marker for other panics.
The daemon startup message omits the home path, socket path, and account ID.

Existing report retention stays at 20 files, with the existing startup purge.
This work does not access historical reports on Tom's machine.
Historical reports and captured logs can contain raw details. Their inventory and cleanup require separate confirmation.

New daemon tracing output uses a timestamp, severity, and a fixed category.
Its only numeric event fields are `workers`, `queue_capacity`, `max_field_value_bytes`, `default`, and `absolute_max`.
Only the daemon main target can emit these numeric fields.
It drops free-text messages, errors, other event fields, and span context before stderr output.
New terminal daemon errors use the fixed `daemon_failed` marker and retain exit status 1.
Invalid daemon arguments use the fixed `invalid_arguments` marker and retain exit status 2.
Requested help and version output stay unchanged.

New session exit marks retain finite categories instead of raw error or reason text.
The exit dispositions, product build identity, PID, timestamps, and canary rules stay unchanged.
Existing records retain their previous field values; the source does not sanitize historical entries.
Requested CLI product output, service-home paths, local user data, and diagnostic records outside these boundaries remain separate.
This change does not establish that all local files or logs contain no personal information.

This is a source review. CI and approved product checks must establish the behavior of a built candidate.
The source change does not authorize a merge, nightly release, deployment, or primary upgrade.

`lastdb ops` reads the live daemon's in-memory request telemetry from
`/api/status`. The CLI itself never writes metrics, so inspecting load does
not amplify the load being measured. Durable history is the sampler's job and
is bounded by design: its default sink is a JSONL log under the node home
(`logs/self-metrics.jsonl`), and setting `LASTDB_SELF_METRICS_TO_DB=1` on the
daemon additionally persists per-minute *aggregate deltas* as
`lastdb_telemetry/RequestOpsRollup` records — never per-request samples. See
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
the node" — write volume by caller — complementary to the wall-time
ranking. It is **not** net disk growth (alternate entries, field history and
blob counters add bytes beyond the request body). The table is omitted when every work key has a zero
body sum. Compact `lastdb ops --by-app` appends `body_sum=` / `body_max=` on
app/verb rows that recorded any body bytes.

## Durable request-ops rollups (`--since`)

The in-memory ring and aggregate table reset on daemon restart. For history
that survives restarts, the self-metrics sampler (60s cadence) writes bounded
`lastdb_telemetry/RequestOpsRollup` records when the daemon runs with
`LASTDB_SELF_METRICS_TO_DB=1`. This is opt-in and off by default; without it
the sampler only appends to the JSONL log. Read the durable window with:

```bash
lastdb ops --since 6h
```

`--since` queries the rollup records in the window and merges them into the same
offender tables the live view uses.

Each rollup record is one `(client, kind, schema)` bucket for one sample interval and
carries interval **deltas** for `count`, `sum_ms`, `error_count`,
`sum_body_bytes` (write volume), the per-phase mutation timing sums
(`sum_queue_wait_us`, `sum_admission_wait_us`, `sum_parse_us`,
`sum_schema_resolve_us`, `sum_validate_us`, `sum_apply_us`, `sum_persist_us`,
`sum_flush_us`, `sum_index_wait_us`) and `phase_count` (how many requests in
the interval reported a phase breakdown). `max_ms`, `max_body_bytes`, and
`last_ts_ms` are copied cumulatively (running max / latest, not deltas). Phase fields are sums in microseconds precisely so intervals
stay delta-able and merge by addition.

Compatibility is append-only in both directions, except for the removed
`sum_cold_shard_loads` field: the sampler upgrades the
rollup schema in place when it sees missing fields, and records written before a
field shipped read back as 0 — for phase fields that means "no phases
reported", not a measurement of zero.

The rollup schema no longer lists `sum_cold_shard_loads`. Its value was always
`0` in disk format v2. An extra field does not start a schema upgrade, so a
store that registered the field keeps it until a missing field starts the next
upgrade. The sampler does not write the field, and `--since` does not read it.

Known limitation: the sampler's delta baselines are kept for the life of the
process and never pruned. If an aggregate bucket is removed from the bounded
in-memory table and later recreated, its cumulative counters restart at zero
while the stale baseline persists, so that bucket under-reports in rollup records
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
The flush phase covers explicit store barriers. Native writes complete their disk
commit before acknowledgment and report that work in their native commit phases.
`LASTDB_MUTATION_SYNC_FLUSH` and `LASTDB_BACKGROUND_FLUSH_MS` are retired;
neither setting changes native durability.

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

## Native core memory and write telemetry

The native engine is the normal engine. It has no resident-mode choice.
Use `GET /api/db/native-status` for the direct `native_disk` counters.
This read does not wake the flush worker.

| Field | Meaning |
| --- | --- |
| `warm_records`, `core_warm_records` | Current count of decoded records in the warm set. |
| `core_warm_bytes` | The byte charge, the byte limit, the peak, and the purge totals of the warm set. |
| `core_warm_held` | Records held by running calls. |
| `core_warm_hits`, `core_warm_misses`, `core_warm_purged` | Core lookup and purge counters. |
| `accepted_sequence`, `completed_sequence` | Accepted write frontier and completed disk frontier. |
| `accepted_batches` | Accepted batches that still await complete disk publication. |
| `writer_failed` | Native writer failure state. A failed writer cannot acknowledge another accepted view. |
| `core_model.metadata_*` | Current, retained, used, peak, and limit bytes for core model metadata. |
| `core_model.body_reserved_*` | Current and retained charged write-body owners, including captured older values. |
| `core_memory` | Bounded definition memory, with hits, misses, refused admissions, and byte charges. |

A queued reply alone does not establish a memory-only ACK.
Sample the accepted and completed sequences and the pending batches immediately after the reply.
Explicit durable requests wait for disk.
A queued request can also wait for disk when its reservation or typed coverage is insufficient.

`lastdb status` prints `Logical keys:`, `Background purge work:`, and `Memory admission:` lines.
The `Logical keys:` line reports the count of warm records. It has no count limit; the warm set byte limit bounds it.
The background purge counters describe queued purge tasks; they do not establish completion of all native writes.
The direct native accepted/completed frontiers describe that completion.

`status.resident` retains historical wire fields for existing readers.
Its retired graph bytes, entry count, and revision fields do not measure current native core occupancy.
A zero historical graph budget does not mean native memory is unbounded.
The sampled status can also lag a short operation. An unavailable field differs from a measured zero.

The warm set byte limit (`LASTDB_CORE_WARM_BYTES`) is the one conservative process-reserve input.
It does not size a second cache.
`LASTDB_RESIDENT_MAX_DEFERRED_BYTES` controls the deferred write-body allowance.
The native metadata limit stays separate.
None of these counters defines a whole-process memory maximum.

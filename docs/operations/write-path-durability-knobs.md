# Write-path durability knobs

Three environment variables decide how much durable work one LastDB mutation
does beyond the product write itself. All three came from the 2026-09-21
primary restart loop (brain: `papercut-lastdb-primary-transient-30gb-footprint-spike-guard-restart-loop-20260921`).

| Variable | Default | What it does |
|---|---|---|
| `LASTDB_KEEP_SMALL_PERSIST_SECS` | `30` | Minimum seconds between durable writes of the storage-meters snapshot (`keep_small` plane). `0` writes it inline on every put. Each persist writes a small header plus shards for schemas that changed; a whole-map rewrite of every schema filled the plane at ~1.2 MB/s. |
| `LASTDB_MAX_COLD_GROUP_LOAD_BYTES` | 2 GiB (1 GiB before 2026-09-25) | HARD cap. A cold hash group larger than this is refused with `ColdGroupTooLarge` instead of read, decrypted and parsed whole. Plain byte count; `0` or unset keeps the default. The largest legitimate group on the primary is under 100 MB. |
| `LASTDB_SOFT_COLD_GROUP_LOAD_BYTES` | 1 GiB | SOFT cap. A cold group over it (and under the hard cap) still loads and logs `LASTSTORE_COLD_GROUP_OVER_SOFT_CAP`; the plane compactors reclaim it under the backup publish-target lock. Added 2026-09-25 so a group of superseded copies cannot make a node unbootable or unupgradeable. |
| `LASTDB_KEEP_SMALL_COMPACT_PROBE_INTERVAL_SECS` | `120` | How often the residual sweep probes the `keep_small` plane (other residual planes: hourly). Dirty-schema shards still append superseded copies, so the plane needs a fast probe to stay near one live copy per key. |
| `LASTDB_KEEP_SMALL_COMPACT_MIN_OVERHANG_BYTES` | 32 MiB | Reclaim floor for the `keep_small` plane (other residual planes: 256 MiB). An explicit `LASTDB_RESIDUAL_PLANE_COMPACT_MIN_OVERHANG_BYTES=0` still disables keep_small rewrites too. |
| `LASTDB_ATTRIBUTION_SOURCE_EVENTS` | off | `1` turns on the attribution source boundary from #2134: a durable pending scope before each write batch, then an event append and scope clear in one durable batch. That is two flushes per write batch. The pre-batching safe-upgrade latency bar measured a hot `brain put` at 1,065 ms with it on against 210 ms with it off. With it off, attribution never calls a walk complete and copy reclaim stays fail-closed. |

## How to read a slow write

1. `lastdb ops` names the client, kind and schema with the most total time.
2. `lastdb status` shows `slowest_request_ms`; a value near a client's timeout (60 s) before a memory spike points at a cold group load, not at the client.
3. Count segments per hour in the suspect group: `for f in <group>/*.seg; do stat -f %Sm -t %Y-%m-%dT%H "$f"; done | sort | uniq -c`.

## Related

- `lastdb db reclaim-keep-small-legacy [--execute]` drops the legacy
  `metadata` group that held the snapshot before it moved to `keep_small`.
  It reads only the group's key sidecar and never loads the group.
- `lastdb db compact --collection keep_small` reclaims superseded snapshot
  versions; the residual sweep does this on its own above the overhang trigger.

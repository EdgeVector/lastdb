# Write-path durability knobs

Two environment variables decide how much durable work one LastDB mutation
does beyond the product write itself. Both came from the 2026-09-21
primary restart loop (brain: `papercut-lastdb-primary-transient-30gb-footprint-spike-guard-restart-loop-20260921`).

| Variable | Default | What it does |
|---|---|---|
| `LASTDB_KEEP_SMALL_PERSIST_SECS` | `30` | Minimum seconds between durable writes of the storage-meters snapshot (`keep_small` plane). `0` writes it inline on every put. Each persist writes a small header plus shards for schemas that changed; a whole-map rewrite of every schema filled the plane at ~1.2 MB/s. |
| `LASTDB_ATTRIBUTION_SOURCE_EVENTS` | off | `1` turns on the attribution source boundary from #2134: a durable pending scope before each write batch, then an event append and scope clear in one durable batch. That is two flushes per write batch. The pre-batching safe-upgrade latency bar measured a hot `brain put` at 1,065 ms with it on against 210 ms with it off. With it off, attribution never calls a walk complete and copy reclaim stays fail-closed. |

## How to read a slow write

1. `lastdb ops` names the client, kind and schema with the most total time.
2. `lastdb status` shows `slowest_request_ms`. A value near a client's timeout (60 s) points at the node, not at the client.

## Related

- The residual sweep that compacted the `keep_small` plane and its two
  `LASTDB_KEEP_SMALL_COMPACT_*` knobs were removed with the LastStore backend
  (2026-10-26). A disk format v2 home merges its run files automatically.
- The cold-group caps `LASTDB_MAX_COLD_GROUP_LOAD_BYTES` and
  `LASTDB_SOFT_COLD_GROUP_LOAD_BYTES` were removed with disk format v1
  (2026-10-26). A disk format v2 lookup reads index and data blocks of the
  run files. It has no cold group load.

# Memory proof

Use an explicit release pair and a stopped, isolated copy of the real database.
The runner creates a new child copy through the installed `lastdb-dev` helper.
It never boots the source snapshot or the primary.

Run the fixture tests first:

```sh
python3 -m unittest discover \
  -s scripts/feature-proof/lastdb-footprint-under-guard -p 'test_*.py'
```

Run the proof with an explicit workload:

```sh
bash scripts/feature-proof/lastdb-footprint-under-guard/run.sh \
  --candidate /absolute/release/lastdbd \
  --source-git-oid FULL_FOLD_COMMIT \
  --snapshot-home /tmp/stopped-real-data-copy \
  --workload /absolute/workload.json \
  --report-dir /absolute/new-report-directory \
  --duration-secs 86400
```

`--duration-secs 600` is the safe-upgrade gate. `86400` is the soak.
The sibling `lastdb` must report the same version as `lastdbd`.
Every sample must name the requested allocator. The default is `mimalloc`; use `--allocator system` for the comparison binary.
The snapshot must contain no cloud connection files or live socket listener.
The copy must report no error except an unsupported socket copy.
The report directory must remain outside the snapshot.
The report directory must be new. Each run retains its private copy for diagnosis.
The report names that copy in `retained-home.json`.
The manifest includes hashes of the candidate, workload, and harness source files.
It also records the snapshot's high-water metadata hash. This metadata hash is not a full content hash.
The source metadata must remain unchanged throughout the run.

The workload declares its provenance, cycle interval, and operations.
Each operation has a unique name, method, path, JSON body, and minimum row count.
Query operations require explicit fields, a key or partition filter, and a limit of at most 1,000 rows.
Each query must return at least one real row.
Mutation operations use `/api/mutation` and the normal mutation wire fields.
The runner replaces `${sequence}` in a body with the cycle number.
A 24-hour run requires both read and write operations.

```json
{
  "provenance": "Name the captured workload and its source date",
  "cycle_secs": 30,
  "operations": [
    {
      "name": "board-point",
      "method": "POST",
      "path": "/api/query",
      "body": {
        "schema_name": "REPLACE_WITH_CAPTURED_SCHEMA",
        "fields": ["title"],
        "filter": {"HashKey": "default"},
        "limit": 10
      },
      "min_rows": 1
    }
  ]
}
```

The example contains one read and therefore supports only a harness smoke test.
Use captured queries and explicit private-copy writes for the long run.
Do not use an empty answer as memory-load evidence.

The evaluator rejects incomplete duration, stale samples, missing gauges, process changes, counter resets, missing calls, and failed requests.
The physical p99 limit is 12 GiB. Every multiplier must stay below 1.3.
Those two lines are backstops. They are not the operating target.
`warm_bytes_freed` is the lifetime sum of warm bytes that eviction steps released.
Each increase between samples is one step.
A step must drop `phys_footprint` by at least 0.25 of the bytes it freed.
A step that frees nothing is not that test.
A flat `eviction_events` counter is not a failure.
A step that frees bytes and does not move `phys_footprint` fails. The proof does not skip that step.
Footprint slack is `phys_footprint_bytes` minus `footprint_net_bytes`. It must stay at or under 512 MiB.
Malloc held-free is not that bar. A warm budget other than 4 GiB is not a failure.
The file-byte hydration counter must stay zero.

Status evidence comes from the telemetry records for the exact issued request IDs.
A zero store-wide delta during a status request proves zero cold loads during that request.
A positive delta can include concurrent work. It cannot identify the status request as the cause.
The runner reports that case as unproven instead of asserting a cause.
The status phase follows the measured workload. It issues one fixed pair of status requests.
It does not repeat positive observations to select a passing pair.
The final report fails when the status cost remains unproven and retains the independent memory measurements.
Two identical cached process-counter snapshots do not prove a zero-load status request.

`harness-smoke` is a run under 600 seconds. It is not an upgrade gate.
`upgrade-gate` is a run of at least `UPGRADE_GATE_DURATION_SECS` (600) and under 86400 seconds.
That kind is the safe-upgrade gate.
`long-memory-candidate` is a run of at least 86400 seconds.
That kind is a soak. It is not the merge blocker and it is not the upgrade blocker.
It proves only the candidate interval and workload named in its report.
The baseline comparison and the separate guard-recovery case remain required for the full allocator proof.
The runner never sets `full_allocator_proof` to true by itself.

The proof still refuses a primary home. It does not boot `~/.lastdb`.
The runner stops its owned node on failure or interruption.
Helper commands use a separate process group. A failed helper cannot leave its copy process behind.
The final report fails if the owned node does not stop.

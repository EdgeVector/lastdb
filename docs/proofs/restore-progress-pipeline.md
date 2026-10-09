# Restore progress and transfer bounds

Use `lastdb restore --into <fresh-home> --json --progress-json` for a supervised restore.
`--progress-json` requires `--json`; the CLI rejects other combinations before restore.
The final report remains one JSON document on stdout.
The progress stream uses JSON Lines on stderr.
It emits at phase changes and every five seconds, including during synchronous file operations.
The final event classifies success or failure when the sink accepts it.
A fixed 32-event queue separates the restore from the stderr sink.
A stalled sink can lose events. CLI exit waits at most 100 ms for delivery.
The final stdout document remains the authoritative result.

The progress schema is `lastdb.restore.progress`, version `1`.
All fields contain fixed labels, enum values, counts, or durations.
No event contains paths, cloud object names, writer identities, URLs, keys, or error text.
Unknown totals use `null`.
The stream has no ETA.

- `phase` identifies preflight, latest pointer, manifest chain, chunk transfer, integrity, commit, database open, tail download, tail replay, or flush.
- `phase_elapsed_ms` reports current phase time. `phase_timings_ms` retains completed phase durations.
- `chunks_total` and `bytes_declared` come from the selected manifest.
- `response_body_bytes` counts complete downloaded manifest, chunk, and tail bodies. It includes bytes later excluded by validated prefix recovery.
- This byte count excludes HTTP overhead and partial bodies from failed requests. It is not a network-interface byte counter.
- `bytes_installed` reports successful S0 installations.
- `authorization_ms`, `download_ms`, and `install_ms` separate operation costs. Concurrent durations overlap and must not be added to obtain elapsed time.
- `queued_downloads` includes pending requests and completed bodies that await ordered installation.
- `reserved_bytes` includes those bodies until installation ends. `authorizations_active` and `transfers_active` show cloud operations in progress.
- `tail_objects_total` and `tail_objects_downloaded` describe tail transfer. Replay units can differ from objects because transaction groups can span objects.
- `replay_segments_total`, `replay_segments_applied`, and `replay_records_applied` describe decoded replay units and applied records.

## Download policy

The queue retains eight concurrent requests and a 128 MiB reservation budget.
Each reservation covers the greater of the declared size and the 16 MiB legacy retry allowance.
A declared chunk above the budget runs alone.
The queue starts each next eligible request after an ordered installation releases its reservation.
It does not wait for the entire previous group of eight.
An error or cancellation drops every queued future. No download task survives the restore future.
The original order, SHA-256 checks, size fences, source scope, and remote write interlock remain enforced.

A controlled loopback fixture uses sixteen chunks and two 400 ms responses at positions eight and nine.
It compares the old batch policy with the continuous queue.
The test requires the ninth request before the eighth installation, ordered final installations, and the original reservation caps.
A separate fixture introduces authorization, response, and installation delays to test their separate counters.
These fixtures measure scheduling overhead. They do not predict production cloud throughput.

Run the focused checks:

```sh
cargo test -p fold_db --features cloud-sync --lib sync::engine::backup_restore -- --nocapture
cargo test -p fold_db --features cloud-sync --lib sync::engine::restore_progress
cargo test -p lastdb_node --bin lastdb restore_read_only_tests -- --test-threads=1
```

For the executable process check, build the CLI and run:

```sh
cargo build -p lastdb_node --bin lastdb
python3 scripts/test-restore-progress-cli.py --binary target/debug/lastdb
```

The pipe test fills a stderr pipe and leaves it unread.
The actual CLI must still exit and supply its final stdout JSON.
It uses an absent temporary source, so no cloud request occurs.

With `--reuse-chunks-from`, progress also reports `chunks_reused`, `bytes_reused`,
and `cache_read_ms`. These measure verified local prefixes and their read cost.
See [the cache proof](restore-verified-local-chunk-cache.md) for the command and safety checks.

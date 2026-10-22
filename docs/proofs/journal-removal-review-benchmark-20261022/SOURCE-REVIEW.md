Source review receipt — 2026-10-22

This receipt describes source changes in the candidate worktree. The build receipts identify the final source commit and binary hashes. Source review does not establish a product result.

**Cloud repair removal**

- `fold_db/crates/core/src/sync/cloud_purge/` retains the protocol and inventory definitions. The job, checkpoint, retained-source, upload, download, restore, and org replacement implementations are removed.
- `fold_db/crates/core/src/sync/cloud_purge_peer/` is removed. Its adoption handlers, tokens, peer records, control files, retained homes, and replacement import paths no longer exist.
- The repair-specific `fold_db/crates/core/src/sync/engine/cloud_purge_*.rs` files are removed. `CloudPurgeJob`, `CloudPurgeDriver`, `CloudPurgeWriteFence`, queue detachment, and fresh replacement frontier installation are removed.
- `fold_db/crates/core/src/sync/auth/ops/cloud_purge.rs`, `fold_db/crates/core/src/sync/auth/continuation.rs`, and the repair upload helpers are removed. `fold_db/crates/core/src/sync/auth/ops/cloud_control.rs` retains authenticated control reads.
- `lastdb_node/src/cloud_purge_job.rs`, `lastdb_node/src/cloud_purge_runtime.rs`, and `lastdb_node/src/cloud_purge_adoption.rs`, with their child modules, are removed. Their CLI and HTTP routes are removed.
- `fold_db/crates/core/src/sync/engine/backup_atom_rewrite.rs` and its node command are removed. Normal backup restore retains its chunk verification and immutable backup identity checks.

**Sync records and guards that remain**

`fold_db/crates/core/src/sync/engine/pin_log.rs` and `fold_db/crates/core/src/sync/snapshot_log/` retain cloud sync mutation records, sequence numbers, frontiers, and authenticated replay. These records serve cloud sync. They do not provide the native local disk commit or its restart recovery.

`fold_db/crates/core/src/sync/engine/pin_log/retired_source.rs` retains exact authenticated source objects for retired sync mutations. `fold_db/crates/core/src/sync/scope_alias.rs` retains the existing target-bound translation for those records. Its replacement installer is removed.

`fold_db/crates/core/src/sync/auth/cloud_generation.rs` retains a control read for each target. A generation that requires removed local adoption receives an explicit error. `fold_db/crates/core/src/sync/auth/observed_generation.rs` retains a memory-only monotonic control floor shared by client clones. It creates no disk records.

`fold_db/crates/core/src/sync/auth/recipient_delivery.rs` retains exact sender and recipient generation checks. The sender control read omits `db_hash`. Authority calls retain explicit account scope in `fold_db/crates/core/src/sync/auth/ops/org_authority.rs`.

`fold_db/crates/core/src/sync/engine/foreign_share.rs` retains ordinary share bootstrap, download frontiers, and replay. `fold_db/crates/core/src/sync/engine/cloud_share_refresh.rs` permits the exact legacy generation-zero receiver. A replacement requires an existing matching binding; this build does not create one.

**Compatibility refusals**

`fold_db/crates/core/src/sync/retired_cloud_repair.rs` checks exact legacy paths without reading their records. It refuses `home/cloud-purge-resync` or the stable sibling `.lastdb-cloud-purge/<canonical-home-sha256>/peer` path. It does not delete or replay that state. `lastdb_node/src/host/boot.rs` calls this check before the database opens.

Cloud activation and restore refuse generations that require the removed adoption workflow. The checks remain in `lastdb_node/src/cloud_activation.rs` and `lastdb_node/src/bin/lastdb/restore_phases/cloud_generation.rs`.

**Erase meter source review**

`fold_db/crates/core/src/db_operations/atom_store/hard_erase_journal.rs` and its commit module are removed. `fold_db/crates/core/src/db_operations/atom_store/erase_meters.rs` stages the final aggregate and audit fact with the native erase.

`EraseAggregate` retains both the cumulative private snapshot and the union of dirty schema names across chunks. This fixes the review concern that a later schema refresh could overwrite an earlier chunk's meter slice. The aggregate copy quote and the retained-effect cap precede their copies.

`fold_db/crates/core/src/storage/disk_pack/transaction.rs` marks this request durable. `fold_db/crates/core/src/storage/disk_pack/transaction/request.rs` prevents an early memory ack for that request. `fold_db/crates/core/src/storage/disk_pack/queue.rs` publishes meter effects after the disk root and before durable completion notification.

Cancellation before admission drops the private erase and its meter candidate. Runtime meter snapshots retain an unclean verdict. `fold_db/crates/core/src/db_operations/atom_store/keep_small.rs` imports older versions as incomplete state, without journal replay. `fold_db/crates/core/src/db_operations/delete_ledger.rs` retains completed audit facts; those facts never drive recovery or another erase.

**Product receipts and limits**

This receipt does not report a successful build, restart, crash interruption, cloud request, meter result, or performance result. Those results require separate receipts from the final candidate binary.

The round-two benchmark retains the original six measured phases. A separate fixed probe sweep follows them. `benchmark.py`, `focused.py`, `run_pairs.py`, and `summarize.py` in this folder define the procedure and provenance checks.

The procedure uses generated data on fresh owned homes. Automatic maintenance stays off for the matched comparison. Operating system caches remain uncontrolled. Process I/O counters do not measure physical SSD writes. Request overlap does not identify the internal write-gate interval. Three pairs do not establish statistical significance or default-maintenance behavior.

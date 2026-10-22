Source review receipt — 2026-10-22

This review covers frozen product revision `5919abbfbf2263993ea99700b73bd9c8d43d689b`.
The comparison baseline is `4118c7baa12539c8c6bcf4391c497159f2a1529a`.
The build receipts identify the final binary hashes. Source review does not establish a product result.

The product-source comparison changes 210 files, with 1,383 inserted lines and 17,391 deleted lines.
The net reduction is 16,008 lines. These counts exclude documentation and `scripts/lints/atom-counter-writers.allow`.

**Native commit and restart boundary**

Source inspection finds no active native redo writer, native restart replay, or journal-based cloud repair path.
`vendor/laststore/src/disk_pack/snapshot/writer.rs` writes payload through `PackWriter`, then publishes the changed pages, one generation receipt, and one root.
`vendor/laststore/src/disk_pack/publish.rs` writes and syncs a private root, renames it to `CURRENT`, and syncs the directory.
`complete_native_commit` completes the required durability barriers before the writer reports durable success.

`SnapshotWriter::open_locked` reads the selected root and verifies its existing generation receipt.
It does not rebuild a receipt from a journal or accept private unfinished output as restart input.
`fold_db/crates/core/src/storage/disk_pack/mod.rs` opens `HomeSnapshot` and starts with an empty accepted queue.
Memory writes that lack a completed flush do not form a second disk recovery source.

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

`FoldDB::for_restore` alone enables `preserve_retired_restore_intents`; normal initialization leaves it false.
`fold_db/crates/core/src/fold_db_core/fold_db/sync/engine_attach.rs` installs the preserver only for that explicit restore.
`fold_db/crates/core/src/db_operations/retired_intents.rs` records the exact incoming sync outcome and authenticated source objects before cursor advance.
These records are final sync facts, not native crash redo or cloud repair instructions.
Their exact readers validate the saved facts and source bytes; no boot path replays them or restores retired schema authority.
Some retained comments and proof fields still use the word `journal` for these sync facts.

`fold_db/crates/core/src/sync/auth/cloud_generation.rs` retains a control read for each target. A generation that requires removed local adoption receives an explicit error. `fold_db/crates/core/src/sync/auth/observed_generation.rs` retains a memory-only monotonic control floor shared by client clones. It creates no disk records.

`fold_db/crates/core/src/sync/auth/recipient_delivery.rs` retains exact sender and recipient generation checks. The sender control read omits `db_hash`. Authority calls retain explicit account scope in `fold_db/crates/core/src/sync/auth/ops/org_authority.rs`.

`fold_db/crates/core/src/sync/engine/foreign_share.rs` retains ordinary share bootstrap, download frontiers, and replay. `fold_db/crates/core/src/sync/engine/cloud_share_refresh.rs` permits the exact legacy generation-zero receiver. A replacement requires an existing matching binding; this build does not create one.

**Compatibility refusals**

`fold_db/crates/core/src/sync/retired_cloud_repair.rs` checks exact legacy paths without reading their records. It refuses `home/cloud-purge-resync` or the stable sibling `.lastdb-cloud-purge/<canonical-home-sha256>/peer` path. It does not delete or replay that state. `lastdb_node/src/host/boot.rs` calls this check before the database opens.

Cloud activation and restore refuse generations that require the removed adoption workflow. The checks remain in `lastdb_node/src/cloud_activation.rs` and `lastdb_node/src/bin/lastdb/restore_phases/cloud_generation.rs`.

`vendor/laststore/src/disk_pack/current.rs` requires snapshot feature `256` and refuses the retired internal-journal format. `vendor/laststore/src/bin/lastdb-disk-recovery.rs` removes generic `convert` and `reverse` commands. `vendor/laststore/src/disk_pack/conversion.rs` removes reverse conversion. Its remaining `finish_conversion` validates an existing forward fence and snapshot root before any completion marker changes. It opens no legacy source store.

`lastdb_node/src/bin/lastdb/restore_guards.rs` and `lastdb_node/src/bin/lastdb/restore_phases/destination.rs` refuse SegmentLog restore before destination file changes. `vendor/laststore/src/bin/lastdb-maintenance.rs` refuses SegmentLog maintenance before the source opens. `vendor/laststore/src/store/open.rs` refuses pending conversion fences; its pending legacy opener is removed.

This closure does not remove all HashGroup writers. HashGroup restore and maintenance remain supported. `vendor/laststore/src/store/append.rs` can still emit legacy data files. Those files contain authoritative stored data; they are separate from the removed native recovery journal.

General LastStore library APIs also remain, including explicit legacy layout options in `vendor/laststore/src/options.rs`.
The change closes the listed product commands. It does not remove the full legacy engine or all library writer APIs.

**Erase meter source review**

`fold_db/crates/core/src/db_operations/atom_store/hard_erase_journal.rs` and its commit module are removed. `fold_db/crates/core/src/db_operations/atom_store/erase_meters.rs` stages the final aggregate and audit fact with the native erase.

`EraseAggregate` retains both the cumulative private snapshot and the union of dirty schema names across chunks. This fixes the review concern that a later schema refresh could overwrite an earlier chunk's meter slice. The aggregate copy quote and the retained-effect cap precede their copies.

`fold_db/crates/core/src/storage/disk_pack/transaction.rs` marks this request durable. `fold_db/crates/core/src/storage/disk_pack/transaction/request.rs` prevents an early memory ack for that request. `fold_db/crates/core/src/storage/disk_pack/queue.rs` publishes meter effects after the disk root and before durable completion notification.

Cancellation before admission drops the private erase and its meter candidate. Runtime meter snapshots retain an unclean verdict. `fold_db/crates/core/src/db_operations/atom_store/keep_small.rs` imports older versions as incomplete state, without journal replay. `fold_db/crates/core/src/db_operations/delete_ledger.rs` retains completed audit facts; those facts never drive recovery or another erase.

Ordinary atom counters now retain their source schema. The erase debit uses that source, even when a protein peer removes the final reference. Older counters without a source reduce known global totals and mark per-schema meter trust incomplete. No extra body read is required.

**Product receipts and limits**

This receipt does not report a successful build, restart, crash interruption, cloud request, meter result, or performance result. Those results require separate receipts from the final candidate binary.

The round-two benchmark retains the original six measured phases.
A separate compaction workload sends 64 warm reads and 64 durable writes on fixed schedules.
A separate erase workload sends 64 single durable erase calls and one durable batch of 129 records.
The erase comparison measures the combined change from the original baseline; it does not isolate the meter-journal removal.
`benchmark.py`, `focused.py`, `erase_benchmark.py`, `run_pairs.py`, and `summarize.py` define the procedure and provenance checks.

The procedure uses generated data on fresh owned homes. Automatic maintenance stays off for the matched comparison. Operating system caches remain uncontrolled. Process I/O counters do not measure physical SSD writes. Request overlap does not identify the internal write-gate interval. Three pairs do not establish statistical significance or default-maintenance behavior.

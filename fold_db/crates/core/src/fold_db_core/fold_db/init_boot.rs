//! Boot-time helpers for [`FoldDB::initialize_from_init`].

use std::sync::Arc;

use tracing::info;

use crate::db_operations::DbOperations;
use crate::storage::StorageError;

use super::FoldDB;
use crate::fold_db_core::mutation_manager::MutationManager;
use crate::fold_db_core::sync_coordinator::SyncCoordinator;

impl FoldDB {
    /// Point-load the device clock once at boot. A mutation ACK must not
    /// read this metadata row or flush its namespace on every request.
    pub(super) async fn load_author_clock_state(
        db_ops: &Arc<DbOperations>,
        signer: &Arc<crate::security::Ed25519KeyPair>,
    ) -> Result<crate::schema::types::MutationAuthorClockState, StorageError> {
        let author_clock_state_key = crate::schema::types::author_clock::mutation_author_clock_key(
            &signer.public_key_base64(),
        );
        Ok(db_ops
            .metadata()
            .get_typed::<crate::schema::types::MutationAuthorClockState>(&author_clock_state_key)
            .await
            .map_err(|error| {
                StorageError::BackendError(format!(
                    "failed to load mutation author clock at boot: {error}"
                ))
            })?
            .unwrap_or_default())
    }

    /// Memory-first mutations: skip sync fsync on finalize by default;
    /// periodic flusher + shutdown cover durability (sled-like cadence).
    pub(super) fn start_background_flush(
        db_ops: &Arc<DbOperations>,
    ) -> super::super::mutation_flush::BackgroundFlushTask {
        let flush_policy = super::super::mutation_flush::MutationFlushPolicy::from_env();
        let background_ms = flush_policy
            .background_interval
            .map(|d| d.as_millis() as u64);
        info!(
            target: "fold_node::database",
            sync_on_finalize = flush_policy.sync_on_finalize,
            background_flush_ms = ?background_ms,
            sync_flush_env = super::super::mutation_flush::MUTATION_SYNC_FLUSH_ENV,
            background_env = super::super::mutation_flush::BACKGROUND_FLUSH_MS_ENV,
            "mutation flush policy (memory-first by default; flush later)"
        );
        match flush_policy.background_interval {
            Some(interval) => super::super::mutation_flush::BackgroundFlushTask::spawn(
                Arc::clone(db_ops),
                interval,
            ),
            None => super::super::mutation_flush::BackgroundFlushTask::disabled(),
        }
    }

    /// Resident-primary policy controls memory and admission. Schema persist
    /// lanes own mutation atoms and tips, so no second writer drains the
    /// same dirty graph in production.
    pub(super) fn log_resident_policy() -> crate::resident::BackgroundPersistTask {
        let resident_policy = crate::resident::ResidentPolicy::from_env();
        info!(
            target: "fold_node::database",
            mode = resident_policy.mode.as_str(),
            budget_bytes = resident_policy.budget_bytes,
            persist_ms = ?resident_policy.persist_interval.map(|d| d.as_millis() as u64),
            "resident policy (T0 graph budget + persist cadence)"
        );
        // The resident graph budget above is one of several this process
        // chooses; nothing used to compute their sum. Log the whole accounted
        // number against the RSS guard here, at boot, so a configuration that
        // cannot fit is visible BEFORE the guard kills the node rather than
        // after (2026-07-29 restart loop, brain
        // `lastdb-resident-write-hard-dirty-bytes-cap`).
        crate::memory_budget::process_memory_budget().log_at_boot();
        crate::resident::BackgroundPersistTask::disabled()
    }

    /// Opt-in bounded reaper for the unbounded `protein:` class — see
    /// `super::super::protein_reaper` for why a manual verb alone leaves
    /// future leaks unreaped.
    pub(super) fn start_protein_reaper(
        db_ops: &Arc<DbOperations>,
    ) -> super::super::protein_reaper::BackgroundProteinReaperTask {
        let protein_reaper_policy = super::super::protein_reaper::ProteinReaperPolicy::from_env();
        info!(
            target: "fold_node::database",
            reaper_ms = ?protein_reaper_policy.interval.map(|d| d.as_millis() as u64),
            "protein reaper policy (opt-in bounded background gc-proteins cadence)"
        );
        match protein_reaper_policy.interval {
            Some(interval) => super::super::protein_reaper::BackgroundProteinReaperTask::spawn(
                Arc::clone(db_ops),
                interval,
            ),
            None => super::super::protein_reaper::BackgroundProteinReaperTask::disabled(),
        }
    }

    /// Opt-in bounded reclaim for legacy tip-version chains left after tip
    /// history became write-opt-in. See `super::super::tip_history_drain`.
    pub(super) fn start_tip_history_drain(
        db_ops: &Arc<DbOperations>,
    ) -> super::super::tip_history_drain::BackgroundTipHistoryDrainTask {
        let tip_history_drain_policy =
            super::super::tip_history_drain::TipHistoryDrainPolicy::from_env();
        info!(
            target: "fold_node::database",
            drain_ms = ?tip_history_drain_policy.interval.map(|d| d.as_millis() as u64),
            max_keys = tip_history_drain_policy.max_keys,
            "tip-history drain policy (opt-in bounded background tv-chain reclaim)"
        );
        match tip_history_drain_policy.interval {
            Some(interval) => {
                super::super::tip_history_drain::BackgroundTipHistoryDrainTask::spawn(
                    Arc::clone(db_ops),
                    interval,
                    tip_history_drain_policy.max_keys,
                )
            }
            None => super::super::tip_history_drain::BackgroundTipHistoryDrainTask::disabled(),
        }
    }

    /// Opt-in bounded janitor that frees `atom:` bodies live Delete
    /// converge leaves unreferenced. See `super::super::atom_reclaim_janitor`
    /// — local-only, no exclusive barrier, does not gate ACK. Distinct from
    /// the cloud-sync `sync_coordinator.start_automatic_gc_atoms` cadence
    /// started at boot, which is coupled to the backup publish lock and
    /// byte-budget gated (cloud keep-set shrink, out of scope here).
    pub(super) fn start_atom_reclaim_janitor(
        db_ops: &Arc<DbOperations>,
        mutation_manager: &Arc<MutationManager>,
    ) -> super::super::atom_reclaim_janitor::BackgroundAtomReclaimJanitorTask {
        let atom_reclaim_janitor_policy =
            super::super::atom_reclaim_janitor::AtomReclaimJanitorPolicy::from_env();
        info!(
            target: "fold_node::database",
            reclaim_ms = ?atom_reclaim_janitor_policy.interval.map(|d| d.as_millis() as u64),
            "atom reclaim janitor policy (opt-in bounded background gc-atoms cadence after tip converge)"
        );
        match atom_reclaim_janitor_policy.interval {
            Some(interval) => {
                super::super::atom_reclaim_janitor::BackgroundAtomReclaimJanitorTask::spawn(
                    Arc::clone(db_ops),
                    Arc::clone(mutation_manager),
                    interval,
                )
            }
            None => {
                super::super::atom_reclaim_janitor::BackgroundAtomReclaimJanitorTask::disabled()
            }
        }
    }

    pub(super) fn start_sync_coordinator(
        db_ops: &Arc<DbOperations>,
        packing_slots: Option<(
            crate::sync::capture::plane_compactor::BackupPublishTargetSlot,
            Arc<tokio::sync::Mutex<Option<u64>>>,
        )>,
    ) -> SyncCoordinator {
        let coordinator = match packing_slots {
            Some((backup, pause)) => SyncCoordinator::new_with_packing(backup, pause),
            None => SyncCoordinator::new(),
        };
        coordinator.start_automatic_gc_atoms(Arc::clone(db_ops));
        // Plane self-compaction is not a cloud feature. Start it here,
        // beside gc-atoms, so a node booted without `cloud_sync.json`
        // reclaims `tips` / `atoms` / the capture-free planes on the same
        // expected trigger a cloud-synced node does. When an engine is set
        // later the cadence steps that engine's compactor instead, on the
        // same packing lock.
        coordinator.start_automatic_plane_compaction(db_ops);
        coordinator
    }
}

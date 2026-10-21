//! Boot-time helpers for [`FoldDB::initialize_from_init`].

use std::sync::Arc;

use tracing::info;

use crate::db_operations::DbOperations;
use crate::storage::StorageError;

use super::FoldDB;
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

    /// Log native memory admission and flush delay.
    pub(super) fn log_resident_policy() {
        let resident_policy = crate::resident::ResidentPolicy::from_env();
        info!(
            target: "fold_node::database",
            budget_bytes = resident_policy.budget_bytes,
            persist_ms = ?resident_policy.persist_interval.map(|d| d.as_millis() as u64),
            "native memory policy and flush delay"
        );
        // This remains conservative process headroom after the graph cache
        // removal. Actual native pool charges have their own status counters.
        crate::memory_budget::process_memory_budget().log_at_boot();
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

    /// The janitor that releases kept conflict records at the end of their
    /// retention window. See `super::super::conflict_janitor`.
    pub(super) fn start_conflict_janitor(
        db_ops: &Arc<DbOperations>,
    ) -> super::super::conflict_janitor::BackgroundConflictJanitorTask {
        let policy = super::super::conflict_janitor_policy::ConflictJanitorPolicy::from_env();
        info!(
            target: "fold_node::database",
            janitor_ms = ?policy.interval.map(|d| d.as_millis() as u64),
            duty_pct = policy.duty_pct,
            net_secs = ?policy.net_every.map(|d| d.as_secs()),
            "conflict retention janitor policy"
        );
        match policy.interval {
            Some(interval) => super::super::conflict_janitor::BackgroundConflictJanitorTask::spawn(
                Arc::clone(db_ops),
                interval,
                policy.duty_pct,
                policy.net_every,
            ),
            None => super::super::conflict_janitor::BackgroundConflictJanitorTask::disabled(),
        }
    }

    pub(super) fn start_sync_coordinator(db_ops: &Arc<DbOperations>) -> SyncCoordinator {
        let coordinator = SyncCoordinator::new();
        // Plane self-compaction is not a cloud feature. Start it here,
        // so a node booted without `cloud_sync.json`
        // reclaims `tips` / `atoms` / the capture-free planes on the same
        // expected trigger a cloud-synced node does. When an engine is set
        // later the cadence steps that engine's compactor instead, on the
        // same packing lock.
        coordinator.start_automatic_plane_compaction(db_ops);
        coordinator
    }
}

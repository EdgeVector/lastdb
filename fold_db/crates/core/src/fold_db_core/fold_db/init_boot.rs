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

    /// Log native memory admission.
    pub(super) fn log_resident_policy() {
        let resident_policy = crate::resident::ResidentPolicy::from_env();
        info!(
            target: "fold_node::database",
            budget_bytes = resident_policy.budget_bytes,
            "native memory admission policy"
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
        offline_maintenance: bool,
    ) -> super::super::protein_reaper::BackgroundProteinReaperTask {
        if offline_maintenance {
            return super::super::protein_reaper::BackgroundProteinReaperTask::disabled();
        }
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
        offline_maintenance: bool,
    ) -> super::super::conflict_janitor::BackgroundConflictJanitorTask {
        if offline_maintenance {
            return super::super::conflict_janitor::BackgroundConflictJanitorTask::disabled();
        }
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

    /// The task that drains dead molecules. See `super::super::molecule_drain`.
    pub(super) fn start_molecule_drain(
        db_ops: &Arc<DbOperations>,
        offline_maintenance: bool,
    ) -> super::super::molecule_drain::BackgroundMoleculeDrainTask {
        if offline_maintenance {
            return super::super::molecule_drain::BackgroundMoleculeDrainTask::disabled();
        }
        let policy = super::super::molecule_drain::MoleculeDrainPolicy::from_env();
        info!(
            target: "fold_node::database",
            drain_ms = ?policy.interval.map(|d| d.as_millis() as u64),
            duty_pct = policy.duty_pct,
            tips = policy.tips,
            batches = policy.batches,
            "molecule drain policy"
        );
        match policy.interval {
            Some(interval) => super::super::molecule_drain::BackgroundMoleculeDrainTask::spawn(
                Arc::clone(db_ops),
                policy,
                interval,
            ),
            None => super::super::molecule_drain::BackgroundMoleculeDrainTask::disabled(),
        }
    }

    pub(super) fn start_sync_coordinator(
        db_ops: &Arc<DbOperations>,
        offline_maintenance: bool,
    ) -> SyncCoordinator {
        let coordinator = SyncCoordinator::new();
        // Local compaction starts independently of credentials or a cloud service.
        if !offline_maintenance {
            coordinator.start_automatic_plane_compaction(db_ops);
        }
        coordinator
    }
}

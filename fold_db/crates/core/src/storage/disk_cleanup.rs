//! Explicit resumable local compaction, with an honest completion receipt.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use crate::storage::disk_pack::{map, physical, run_blocking, transaction, PackRuntime};
use crate::storage::error::{StorageError, StorageResult};

mod operation;

const SCOPE: &str = "already_deleted_records_in_active_main_and_cas_stores";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NativeCleanupPhase {
    Idle,
    Pending,
    Running,
    WaitingForReads,
    Retrying,
    RecoveryRequired,
    Complete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeCleanupBoundary {
    pub main_frontier: u64,
    pub cas_frontier: u64,
    pub main_generation: u64,
    pub cas_generation: u64,
}

impl NativeCleanupBoundary {
    fn capture(v2: &physical::V2Home) -> Self {
        let main = v2.home.status();
        let cas = v2.cas.status();
        Self {
            main_frontier: main.frontier,
            cas_frontier: cas.frontier,
            main_generation: main.generation,
            cas_generation: cas.generation,
        }
    }

    fn covers(self, earlier: Self) -> bool {
        self.main_frontier >= earlier.main_frontier
            && self.cas_frontier >= earlier.cas_frontier
            && self.main_generation >= earlier.main_generation
            && self.cas_generation >= earlier.cas_generation
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeCleanupHome {
    pub tiered_runs: usize,
    pub merging_runs: usize,
    pub held_retired_bytes: u64,
    pub removed_files: u64,
}

/// A completion receipt covers a past local boundary, never future writes or
/// retained backup copies. Live CAS rows are not an orphan-GC candidate here.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeCleanupStatus {
    pub version: u32,
    pub request_id: Option<String>,
    pub phase: NativeCleanupPhase,
    pub complete: bool,
    pub scope: String,
    pub main_home: String,
    pub cas_home: String,
    pub requested_at: Option<u64>,
    pub target: Option<NativeCleanupBoundary>,
    pub completed_at: Option<u64>,
    pub completed_through: Option<NativeCleanupBoundary>,
    pub merge_steps_since_resume: u64,
    pub main: NativeCleanupHome,
    pub cas: NativeCleanupHome,
    pub last_error: Option<String>,
    pub backup_copies_excluded: bool,
    pub live_cas_orphan_repair_included: bool,
}

impl NativeCleanupStatus {
    fn idle(v2: &physical::V2Home) -> Self {
        Self {
            version: 1,
            request_id: None,
            phase: NativeCleanupPhase::Idle,
            complete: false,
            scope: SCOPE.into(),
            main_home: v2.home.identity().to_string(),
            cas_home: v2.cas.identity().to_string(),
            requested_at: None,
            target: None,
            completed_at: None,
            completed_through: None,
            merge_steps_since_resume: 0,
            main: NativeCleanupHome::default(),
            cas: NativeCleanupHome::default(),
            last_error: None,
            backup_copies_excluded: true,
            live_cas_orphan_repair_included: false,
        }
    }

    fn homes(&self) -> impl Iterator<Item = &NativeCleanupHome> {
        [&self.main, &self.cas].into_iter()
    }

    fn needs_merge(&self) -> bool {
        self.homes()
            .any(|home| home.tiered_runs != 0 || home.merging_runs != 0)
    }

    fn active(&self) -> bool {
        self.request_id.is_some()
            && !matches!(
                self.phase,
                NativeCleanupPhase::Complete | NativeCleanupPhase::RecoveryRequired
            )
    }
}

pub(super) struct CleanupJob {
    pub(super) gate: Arc<tokio::sync::Mutex<()>>,
    pub(super) worker: AtomicBool,
    status: Mutex<NativeCleanupStatus>,
}

impl CleanupJob {
    pub(super) fn open(v2: &physical::V2Home) -> Self {
        let status = operation::load(v2).unwrap_or_else(|error| {
            let mut status = NativeCleanupStatus::idle(v2);
            status.phase = NativeCleanupPhase::RecoveryRequired;
            status.last_error = Some(message(&error));
            status
        });
        Self {
            gate: Arc::new(tokio::sync::Mutex::new(())),
            worker: AtomicBool::new(false),
            status: Mutex::new(status),
        }
    }

    pub(super) fn get(&self) -> NativeCleanupStatus {
        self.status
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub(super) fn set(&self, status: NativeCleanupStatus) {
        *self
            .status
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = status;
    }
}

impl PackRuntime {
    /// Read cached progress without disk IO or a wait for the merge worker.
    pub fn status_cleanup(&self) -> NativeCleanupStatus {
        self.cleanup_job.get()
    }

    /// Save an explicit cleanup request. Serving nodes start the worker; an
    /// offline owner can resume it after reopening with `resume_native_cleanup`.
    pub async fn request_native_cleanup(
        self: &Arc<Self>,
        request_id: Option<uuid::Uuid>,
    ) -> StorageResult<NativeCleanupStatus> {
        if transaction::PendingBatch::current(self).is_some() {
            return Err(invalid("native cleanup requires its own request scope"));
        }
        let request_id = request_id.map(|id| id.to_string());
        let owner = Arc::clone(&self.cleanup_job.gate).lock_owned().await;
        let prior = self.cleanup_job.get();
        if prior.active() || (request_id.is_some() && prior.request_id == request_id) {
            if request_id.is_some() && prior.request_id != request_id {
                return Err(invalid("another cleanup request is active"));
            }
            drop(owner);
            self.resume_native_cleanup();
            return Ok(prior);
        }
        let gate = self.enter_write().await;
        let runtime = Arc::clone(self);
        run_blocking(move || {
            let (_owner, _gate) = (owner, gate);
            runtime.require_cleanup_safe()?;
            let mut status = NativeCleanupStatus::idle(&runtime.physical);
            status.request_id =
                Some(request_id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string()));
            status.requested_at = Some(crate::clock::unix_secs());
            status.target = Some(runtime.cleanup_boundary());
            status.phase = NativeCleanupPhase::Pending;
            runtime.save_cleanup(&status)?;
            runtime.cleanup_job.set(status.clone());
            if runtime.serving.load(Ordering::Acquire) {
                runtime.resume_native_cleanup();
            }
            Ok(status)
        })
        .await
    }

    fn cleanup_boundary(&self) -> NativeCleanupBoundary {
        NativeCleanupBoundary::capture(&self.physical)
    }

    fn require_cleanup_safe(&self) -> StorageResult<()> {
        self.require_initialized()?;
        if self.requires_recovery()
            || self.physical.home.is_poisoned()
            || self.physical.cas.is_poisoned()
        {
            return Err(invalid("native cleanup requires a successful home reopen"));
        }
        Ok(())
    }
}

fn invalid(message: &str) -> StorageError {
    StorageError::InvalidOperation(message.into())
}

fn message(error: &StorageError) -> String {
    error.to_string().chars().take(1024).collect()
}

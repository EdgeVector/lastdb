//! Explicit resumable local compaction, with an honest completion receipt.
//!
//! Accepted writes before the request are durable before its target is saved.
//! New writes continue between steps and can prolong cleanup. Completion is
//! recorded under the ordinary write gate only after both homes drain, the
//! readable roots advance, and no held/failed obsolete file remains. Backups
//! and logically live CAS orphans have separate owners and are not deleted.

use super::*;
use std::sync::atomic::AtomicBool;

mod checkpoint;
mod model;
mod operation;
mod worker;

pub use model::{
    NativeCleanupBoundary, NativeCleanupHome, NativeCleanupPhase, NativeCleanupStatus,
};

pub(super) struct CleanupJob {
    pub(super) gate: Arc<tokio::sync::Mutex<()>>,
    pub(super) worker: AtomicBool,
    status: Mutex<NativeCleanupStatus>,
}

impl CleanupJob {
    pub(super) fn open(v2: &physical::V2Home) -> Self {
        let status = checkpoint::load(v2).unwrap_or_else(|error| {
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
    /// An active request is reused; a supplied different id is refused.
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
            // The blocking owner survives cancellation of the HTTP caller.
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
        self.require_restore_validated()?;
        if self.restore_validation_pending() {
            return Err(invalid("native cleanup requires verified restore counters"));
        }
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

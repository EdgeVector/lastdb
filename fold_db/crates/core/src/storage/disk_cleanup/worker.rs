//! Background compaction worker and `PackRuntime` cleanup coordination.

use super::{
    truncate_error, CleanupJob, NativeCleanupBoundary, NativeCleanupHome, NativeCleanupPhase,
    NativeCleanupStatus, NODE_CLEANUP_CHECKPOINT,
};
use crate::platform::clock::unix_secs;
use crate::storage::disk_pack::{
    cas_delete, creation_refusal, map, run_blocking, transaction, PackRuntime,
};
use crate::storage::error::{StorageError, StorageResult};
use laststore::space::{reserve_space, SpacePolicy};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};
use std::time::Duration;
use tracing::Instrument;
use uuid::Uuid;
use NativeCleanupPhase::*;

impl PackRuntime {
    pub fn status_cleanup(&self) -> NativeCleanupStatus {
        self.cleanup_job.get()
    }

    pub async fn request_native_cleanup(
        self: &Arc<Self>,
        req_id: Option<Uuid>,
    ) -> StorageResult<NativeCleanupStatus> {
        if transaction::PendingBatch::current(self).is_some() {
            return Err(StorageError::invalid(
                "native cleanup requires its own request scope",
            ));
        }
        let requested_id = req_id.as_ref().map(Uuid::to_string);
        let owner = Arc::clone(&self.cleanup_job.owner_lock).lock_owned().await;
        let prior = self.cleanup_job.get();
        if prior.active() || (requested_id.is_some() && prior.request_id == requested_id) {
            if requested_id.is_some() && prior.request_id != requested_id {
                return Err(StorageError::invalid("another cleanup request is active"));
            }
            drop(owner);
            self.resume_cleanup();
            return Ok(prior);
        }
        let (gate, runtime) = (self.enter_write().await, Arc::clone(self));
        run_blocking(move || {
            let (_owner, _gate) = (owner, gate);
            runtime.require_cleanup_safe()?;
            let status = NativeCleanupStatus {
                request_id: Some(requested_id.unwrap_or_else(|| Uuid::new_v4().to_string())),
                requested_at: Some(unix_secs()),
                target: Some(NativeCleanupBoundary::capture(&runtime.physical)),
                phase: Pending,
                ..NativeCleanupStatus::idle(&runtime.physical)
            };
            runtime.save_cleanup(&status)?;
            runtime.cleanup_job.set(status.clone());
            if runtime.serving.load(Ordering::Acquire) {
                runtime.resume_cleanup();
            }
            Ok(status)
        })
        .await
    }

    pub(crate) fn begin_serving(self: &Arc<Self>) {
        self.serving.store(true, Ordering::Release);
        self.resume_cleanup();
    }

    fn resume_cleanup(self: &Arc<Self>) {
        if self.cleanup_job.get().active() && !self.cleanup_job.running.swap(true, Ordering::AcqRel)
        {
            let job = Arc::clone(&self.cleanup_job);
            tokio::spawn(worker_loop(Arc::downgrade(self), job).in_current_span());
        }
    }

    async fn cleanup_tick(self: &Arc<Self>) -> StorageResult<NativeCleanupStatus> {
        let owner = Arc::clone(&self.cleanup_job.owner_lock).lock_owned().await;
        let runtime = Arc::clone(self);
        let (status, owner) = run_blocking(move || {
            runtime.require_cleanup_safe()?;
            let mut status = runtime.cleanup_job.get();
            if !status.active() {
                return Ok((status, owner));
            }
            (status.phase, status.last_error) = (Running, None);
            runtime.cleanup_job.set(status.clone());
            let cas_policy = SpacePolicy::default();
            for (home, policy) in [
                (&runtime.physical.home, &runtime.space_policy),
                (&runtime.physical.cas, &cas_policy),
            ] {
                runtime.adopt_merged_root()?;
                if home.retired_bytes() > 0 {
                    status.phase = WaitingForReads;
                } else if home.cleanup_step(policy).map_err(map)? {
                    status.merge_steps_since_resume =
                        status.merge_steps_since_resume.saturating_add(1);
                    runtime.cleanup_job.set(status.clone());
                    runtime.adopt_merged_root()?;
                }
            }
            runtime.refresh(&mut status, 0, 0);
            Ok((status, owner))
        })
        .await?;
        if !status.active() || status.needs_merge() {
            return Ok(status);
        }
        self.finalize_cleanup(owner).await
    }

    async fn finalize_cleanup(
        self: &Arc<Self>,
        owner: tokio::sync::OwnedMutexGuard<()>,
    ) -> StorageResult<NativeCleanupStatus> {
        let (gate, runtime) = (self.enter_write().await, Arc::clone(self));
        run_blocking(move || {
            let (_owner, _gate) = (owner, gate);
            runtime.require_cleanup_safe()?;
            cas_delete::require_clear(&runtime.physical)?;
            runtime.adopt_merged_root()?;
            let mut status = runtime.cleanup_job.get();
            runtime.refresh(&mut status, 0, 0);
            if status.needs_merge() {
                return Ok(status);
            }
            let (main_held, main_removed) = runtime.physical.home.cleanup_files().map_err(map)?;
            let (cas_held, cas_removed) = runtime.physical.cas.cleanup_files().map_err(map)?;
            runtime.adopt_merged_root()?;
            runtime.refresh(&mut status, main_removed, cas_removed);
            if (status.main.held_retired_bytes
                | status.cas.held_retired_bytes
                | main_held
                | cas_held)
                != 0
            {
                status.phase = WaitingForReads;
            } else {
                let boundary = NativeCleanupBoundary::capture(&runtime.physical);
                if !status.target.is_some_and(|target| boundary.covers(target)) {
                    return Err(StorageError::invalid(
                        "cleanup target or frontier regressed",
                    ));
                }
                (
                    status.phase,
                    status.complete,
                    status.completed_at,
                    status.completed_through,
                ) = (Complete, true, Some(unix_secs()), Some(boundary));
                runtime.save_cleanup(&status)?;
            }
            runtime.cleanup_job.set(status.clone());
            Ok(status)
        })
        .await
    }

    fn refresh(&self, status: &mut NativeCleanupStatus, main_added: u64, cas_added: u64) {
        for (dst, home, added) in [
            (&mut status.main, &self.physical.home, main_added),
            (&mut status.cas, &self.physical.cas, cas_added),
        ] {
            let (tiered_runs, merging_runs, _) = home.run_totals();
            *dst = NativeCleanupHome {
                tiered_runs,
                merging_runs,
                held_retired_bytes: home.retired_bytes(),
                removed_files: dst.removed_files.saturating_add(added),
            };
        }
        self.cleanup_job.set(status.clone());
    }

    fn save_cleanup(&self, status: &NativeCleanupStatus) -> StorageResult<()> {
        let bytes =
            serde_json::to_vec(status).map_err(|_| StorageError::invalid("cleanup encode"))?;
        let path = self.physical.home.path();
        let _space =
            reserve_space(path, &self.space_policy, bytes.len() as u64 + 4096).map_err(map)?;
        self.physical
            .home
            .save_checkpoint(NODE_CLEANUP_CHECKPOINT, &bytes)
            .map_err(map)
    }

    fn is_healthy(&self) -> bool {
        let physical = &self.physical;
        !(self.requires_recovery() || physical.home.is_poisoned() || physical.cas.is_poisoned())
    }

    fn require_cleanup_safe(&self) -> StorageResult<()> {
        if self.fresh_initialization_pending() {
            return Err(creation_refusal(
                "creation requires durable key proofs first",
            ));
        }
        self.is_healthy().then_some(()).ok_or_else(|| {
            StorageError::invalid("native cleanup requires a successful home reopen")
        })
    }
}

async fn worker_loop(weak: Weak<PackRuntime>, job: Arc<CleanupJob>) {
    while let Some(runtime) = weak.upgrade() {
        let delay = match runtime.cleanup_tick().await {
            Ok(status) if !status.active() => break,
            Ok(status) if status.phase == WaitingForReads => Duration::from_millis(250),
            Ok(_) => Duration::from_millis(25),
            Err(error) => {
                let (healthy, mut status) = (runtime.is_healthy(), job.get());
                (status.complete, status.last_error) = (false, Some(truncate_error(&error)));
                status.phase = if healthy { Retrying } else { RecoveryRequired };
                job.set(status);
                if !healthy {
                    break;
                }
                Duration::from_secs(30)
            }
        };
        drop(runtime);
        tokio::time::sleep(delay).await;
    }
    job.running.store(false, Ordering::Release);
    if let Some(runtime) = weak.upgrade().filter(|_| job.get().active()) {
        runtime.resume_cleanup();
    }
}

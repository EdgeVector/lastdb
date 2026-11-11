//! Cleanup checkpoint persistence, bounded merge rounds, and background worker.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use tracing::Instrument;

use super::{
    invalid, map, message, physical, run_blocking, CleanupJob, NativeCleanupBoundary,
    NativeCleanupHome, NativeCleanupPhase, NativeCleanupStatus, PackRuntime, StorageResult,
};

const STEP_PAUSE: std::time::Duration = std::time::Duration::from_millis(25);
const HELD_PAUSE: std::time::Duration = std::time::Duration::from_millis(250);
const RETRY_PAUSE: std::time::Duration = std::time::Duration::from_secs(30);

pub(super) fn load(v2: &physical::V2Home) -> StorageResult<NativeCleanupStatus> {
    let Some(bytes) = v2.home.load_cleanup_checkpoint().map_err(map)? else {
        return Ok(NativeCleanupStatus::idle(v2));
    };
    let status: NativeCleanupStatus = serde_json::from_slice(&bytes)
        .map_err(|_| invalid("native cleanup checkpoint encoding is invalid"))?;
    let current = NativeCleanupBoundary::capture(v2);
    let target_ok = status.target.is_some_and(|target| current.covers(target));
    let finish_ok = status.completed_through.is_some_and(|finish| {
        status.target.is_some_and(|target| finish.covers(target)) && current.covers(finish)
    });
    let expected = NativeCleanupStatus::idle(v2);
    if status.version != expected.version
        || status.scope != expected.scope
        || status.main_home != v2.home.identity().to_string()
        || status.cas_home != v2.cas.identity().to_string()
        || status.private_home != expected.private_home
        || status.private.is_some() != expected.private.is_some()
        || !status.backup_copies_excluded
        || status.live_cas_orphan_repair_included
        || status
            .request_id
            .as_deref()
            .and_then(|id| uuid::Uuid::parse_str(id).ok())
            .is_none()
        || !target_ok
        || status.requested_at.is_none()
        || status.complete != (status.phase == NativeCleanupPhase::Complete)
        || (status.complete && (!finish_ok || status.completed_at.is_none()))
        || (!status.complete
            && (status.completed_at.is_some() || status.completed_through.is_some()))
        || status.phase == NativeCleanupPhase::Idle
    {
        return Err(invalid(
            "native cleanup checkpoint identity or boundary is invalid",
        ));
    }
    Ok(status)
}

impl PackRuntime {
    pub(super) fn save_cleanup(&self, status: &NativeCleanupStatus) -> StorageResult<()> {
        let bytes = serde_json::to_vec(status)
            .map_err(|_| invalid("native cleanup checkpoint encoding failed"))?;
        let _space = laststore::space::reserve_space(
            self.physical.home.path(),
            &self.space_policy,
            (bytes.len() as u64).saturating_add(4096),
        )
        .map_err(map)?;
        self.physical
            .home
            .save_cleanup_checkpoint(&bytes)
            .map_err(map)
    }

    /// Mark this runtime as the serving node and resume a saved cleanup request.
    pub(crate) fn begin_serving(self: &Arc<Self>) {
        self.serving.store(true, Ordering::Release);
        self.resume_native_cleanup();
    }

    /// Resume a saved request without creating work when no request is active.
    pub fn resume_native_cleanup(self: &Arc<Self>) {
        if !self.cleanup_job.get().active() || self.cleanup_job.worker.swap(true, Ordering::AcqRel)
        {
            return;
        }
        let weak = Arc::downgrade(self);
        let job = Arc::clone(&self.cleanup_job);
        tokio::spawn(
            async move {
                {
                    let _running = Running(Arc::clone(&job));
                    while let Some(runtime) = weak.upgrade() {
                        let delay = match runtime.cleanup_tick().await {
                            Ok(status) if !status.active() => break,
                            Ok(status) if status.phase == NativeCleanupPhase::WaitingForReads => {
                                HELD_PAUSE
                            }
                            Ok(_) => STEP_PAUSE,
                            Err(error) => {
                                let terminal = runtime.requires_recovery()
                                    || runtime.physical.home.is_poisoned()
                                    || runtime.physical.cas.is_poisoned()
                                    || runtime
                                        .physical
                                        .private_home()
                                        .is_some_and(|home| home.is_poisoned());
                                let mut status = job.get();
                                status.complete = false;
                                status.phase = if terminal {
                                    NativeCleanupPhase::RecoveryRequired
                                } else {
                                    NativeCleanupPhase::Retrying
                                };
                                status.last_error = Some(message(&error));
                                job.set(status);
                                if terminal {
                                    break;
                                }
                                RETRY_PAUSE
                            }
                        };
                        drop(runtime);
                        tokio::time::sleep(delay).await;
                    }
                }
                if job.get().active() {
                    if let Some(runtime) = weak.upgrade() {
                        runtime.resume_native_cleanup();
                    }
                }
            }
            .in_current_span(),
        );
    }

    async fn cleanup_tick(self: &Arc<Self>) -> StorageResult<NativeCleanupStatus> {
        let owner = Arc::clone(&self.cleanup_job.gate).lock_owned().await;
        let runtime = Arc::clone(self);
        let (status, owner) = run_blocking(move || {
            let status = runtime.cleanup_round()?;
            Ok((status, owner))
        })
        .await?;
        if !status.active() || status.needs_merge() {
            return Ok(status);
        }
        let gate = self.enter_write().await;
        let runtime = Arc::clone(self);
        run_blocking(move || {
            let (_owner, _gate) = (owner, gate);
            runtime.finish_cleanup()
        })
        .await
    }

    fn cleanup_round(&self) -> StorageResult<NativeCleanupStatus> {
        self.require_cleanup_safe()?;
        let mut status = self.cleanup_job.get();
        if !status.active() {
            return Ok(status);
        }
        status.phase = NativeCleanupPhase::Running;
        status.last_error = None;
        self.cleanup_job.set(status.clone());
        let cas_policy = laststore::space::SpacePolicy::default();
        let homes = [
            (&self.physical.home, &self.space_policy),
            (&self.physical.cas, &cas_policy),
        ];
        let private = self.physical.private_home().map(|home| (home, &cas_policy));
        for (home, policy) in homes.into_iter().chain(private) {
            self.adopt_merged_root()?;
            if home.retired_bytes() > 0 {
                status.phase = NativeCleanupPhase::WaitingForReads;
                continue;
            }
            if home.cleanup_step_with_space(policy).map_err(map)? {
                status.merge_steps_since_resume = status.merge_steps_since_resume.saturating_add(1);
                self.cleanup_job.set(status.clone());
                self.adopt_merged_root()?;
            }
        }
        self.update_cleanup_progress(&mut status);
        self.cleanup_job.set(status.clone());
        Ok(status)
    }

    fn finish_cleanup(&self) -> StorageResult<NativeCleanupStatus> {
        self.require_cleanup_safe()?;
        crate::storage::disk_pack::cas_delete::require_clear(&self.physical)?;
        self.adopt_merged_root()?;
        let mut status = self.cleanup_job.get();
        self.update_cleanup_progress(&mut status);
        if status.needs_merge() {
            self.cleanup_job.set(status.clone());
            return Ok(status);
        }
        let main = self.physical.home.cleanup_files().map_err(map)?;
        let cas = self.physical.cas.cleanup_files().map_err(map)?;
        let private = self
            .physical
            .private_home()
            .map(|home| home.cleanup_files().map_err(map))
            .transpose()?;
        self.adopt_merged_root()?;
        status.main = progress(
            &self.physical.home,
            status.main.removed_files.saturating_add(main.removed_files),
        );
        status.cas = progress(
            &self.physical.cas,
            status.cas.removed_files.saturating_add(cas.removed_files),
        );
        if let Some((home, removed)) = self.physical.private_home().zip(private.as_ref()) {
            let earlier = status.private.as_ref().map_or(0, |home| home.removed_files);
            status.private = Some(progress(
                home,
                earlier.saturating_add(removed.removed_files),
            ));
        }
        if status.homes().any(|home| home.held_retired_bytes > 0)
            || main.held_bytes > 0
            || cas.held_bytes > 0
            || private.is_some_and(|result| result.held_bytes > 0)
        {
            status.phase = NativeCleanupPhase::WaitingForReads;
        } else {
            let boundary = self.cleanup_boundary();
            let target = status
                .target
                .ok_or_else(|| invalid("cleanup request has no durable target"))?;
            if !boundary.covers(target) {
                return Err(invalid("cleanup frontier went backwards"));
            }
            status.phase = NativeCleanupPhase::Complete;
            status.complete = true;
            status.completed_at = Some(crate::clock::unix_secs());
            status.completed_through = Some(boundary);
            self.save_cleanup(&status)?;
        }
        self.cleanup_job.set(status.clone());
        Ok(status)
    }

    fn update_cleanup_progress(&self, status: &mut NativeCleanupStatus) {
        status.main = progress(&self.physical.home, status.main.removed_files);
        status.cas = progress(&self.physical.cas, status.cas.removed_files);
        if let Some((home, previous)) = self.physical.private_home().zip(status.private.as_mut()) {
            *previous = progress(home, previous.removed_files);
        }
    }
}

fn progress(home: &laststore::disk_run::home::RunHome, removed_files: u64) -> NativeCleanupHome {
    let status = home.status();
    NativeCleanupHome {
        tiered_runs: status.levels.iter().map(|level| level.0).sum(),
        merging_runs: status.merging.0,
        held_retired_bytes: home.retired_bytes(),
        removed_files,
    }
}

struct Running(Arc<CleanupJob>);
impl Drop for Running {
    fn drop(&mut self) {
        self.0.worker.store(false, Ordering::Release);
    }
}

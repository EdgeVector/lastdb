//! One bounded merge round, then one serialized completion check.

use super::*;

impl PackRuntime {
    pub(super) async fn cleanup_tick(self: &Arc<Self>) -> StorageResult<NativeCleanupStatus> {
        let owner = Arc::clone(&self.cleanup_job.gate).lock_owned().await;
        let runtime = Arc::clone(self);
        let (status, owner) = run_blocking(move || {
            let status = runtime.cleanup_round()?;
            Ok((status, owner))
        })
        .await?;
        if !status.active()
            || status.main.tiered_runs
                + status.main.merging_runs
                + status.cas.tiered_runs
                + status.cas.merging_runs
                > 0
        {
            return Ok(status);
        }
        // Complete against one stable accepted prefix, not two unrelated polls.
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
        for (home, policy) in [
            (&self.physical.home, &self.space_policy),
            (&self.physical.cas, &cas_policy),
        ] {
            self.adopt_merged_root();
            if home.retired_bytes() > 0 {
                status.phase = NativeCleanupPhase::WaitingForReads;
                continue;
            }
            if home.cleanup_step_with_space(policy).map_err(map)?.is_some() {
                status.merge_steps_since_resume = status.merge_steps_since_resume.saturating_add(1);
                self.cleanup_job.set(status.clone());
                self.adopt_merged_root();
            }
        }
        status.main = progress(&self.physical.home, status.main.removed_files);
        status.cas = progress(&self.physical.cas, status.cas.removed_files);
        self.cleanup_job.set(status.clone());
        Ok(status)
    }

    fn finish_cleanup(&self) -> StorageResult<NativeCleanupStatus> {
        self.require_cleanup_safe()?;
        super::super::cas_delete::require_clear(&self.physical)?;
        self.adopt_merged_root();
        let mut status = self.cleanup_job.get();
        status.main = progress(&self.physical.home, status.main.removed_files);
        status.cas = progress(&self.physical.cas, status.cas.removed_files);
        if status.main.tiered_runs
            + status.main.merging_runs
            + status.cas.tiered_runs
            + status.cas.merging_runs
            > 0
        {
            self.cleanup_job.set(status.clone());
            return Ok(status);
        }
        let main = self.physical.home.cleanup_files().map_err(map)?;
        let cas = self.physical.cas.cleanup_files().map_err(map)?;
        self.adopt_merged_root();
        status.main = progress(
            &self.physical.home,
            status.main.removed_files.saturating_add(main.removed_files),
        );
        status.cas = progress(
            &self.physical.cas,
            status.cas.removed_files.saturating_add(cas.removed_files),
        );
        if status.main.held_retired_bytes > 0
            || status.cas.held_retired_bytes > 0
            || main.held_bytes > 0
            || cas.held_bytes > 0
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

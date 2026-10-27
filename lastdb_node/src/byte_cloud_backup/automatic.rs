//! One durable owner policy drives the existing pipeline. Recovery never enables it.
use super::{
    model::Saved,
    store::{self, Store},
    BackupError, ByteBackupRequest, Result,
};
use crate::backup_runtime::{Disposition, PauseReason, PauseState, RunOrigin};
use cloud_backup_protocol::Id;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tracing::Instrument;

mod policy;
pub use policy::AutomaticBackupStatus;
use policy::Policy;

pub(super) use policy::{configure, status};

pub fn spawn_automatic(
    host: Arc<crate::host::Host>,
    shutdown: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(
        async move {
            let mut changes = host.byte_backup.subscribe();
            let mut last_error = None;
            while !shutdown.load(Ordering::Acquire) && !host.byte_backup.snapshot().shutting_down {
                let error = tick(&host).err().map(|error| error.to_string());
                if error != last_error {
                    if let Some(error) = &error {
                        tracing::warn!(target: "lastdbd::byte_backup", error = %error,
                        "automatic backup controller needs attention");
                    }
                    last_error = error;
                }
                tokio::select! {
                    result = changes.changed() => { if result.is_err() { break; } }
                    _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {}
                }
            }
        }
        .in_current_span(),
    )
}

fn tick(host: &crate::host::Host) -> Result<()> {
    let _transition = host.byte_backup.transition()?;
    host.byte_backup.check_admission()?;
    let Some(mut policy) = policy::load(host)? else {
        return Ok(());
    };
    if !policy.state.enabled || policy.state.paused.is_some() {
        return Ok(());
    }
    let result = advance(host, &mut policy);
    if let Err(error) = &result {
        let pause = error.pause_state();
        if pause.disposition == Disposition::Attention {
            policy.state.paused = Some(pause);
            policy.save(host)?;
        }
    }
    result
}

fn advance(host: &crate::host::Host, policy: &mut Policy) -> Result<()> {
    policy.require_connection(host)?;
    let now = fold_db::clock::unix_secs();
    if host.byte_backup.snapshot().active.is_some() {
        return Ok(());
    }
    if let Some(id) = policy.state.pending_request_id.clone() {
        return pending(host, policy, id, now);
    }
    if now < policy.state.next_run_at {
        return Ok(());
    }
    if let Some(id) = Store::active(&host.home)? {
        if !store::state(&host.home, &id)?
            .ok_or(BackupError::Invalid)?
            .terminal()
        {
            return Ok(()); // An owner manual job is never adopted or replaced.
        }
    }
    let frontier = current_frontier(host)?;
    if policy
        .state
        .last_backed_up_frontier
        .is_some_and(|old| frontier <= old)
    {
        policy.state.next_run_at = now.saturating_add(policy.state.interval_seconds);
        return policy.save(host);
    }
    let id = Id::try_from(uuid::Uuid::new_v4().to_string())?;
    policy.state.pending_request_id = Some(id.clone());
    policy.state.pending_frontier = Some(frontier);
    policy.state.attempts = 0;
    // Save the automatic request before admission. A crash retries this same identity.
    policy.save(host)?;
    invoke(host, policy, id, now)
}

fn pending(host: &crate::host::Host, policy: &mut Policy, id: Id, now: u64) -> Result<()> {
    let saved = store::state(&host.home, &id)?;
    if let Some(saved) = &saved {
        policy.validate_job(saved)?;
        if saved.terminal() {
            return complete(host, policy, saved, now);
        }
        let pause: Option<PauseState> = store::read::<Option<PauseState>>(
            &Store::base(&host.home)
                .join("jobs")
                .join(id.as_str())
                .join("pause.json"),
        )?
        .flatten();
        if pause.is_some_and(|pause| pause.disposition == Disposition::Attention) {
            policy.state.paused = pause;
            return policy.save(host);
        }
    }
    if now >= policy.state.next_run_at {
        invoke(host, policy, id, now)?;
    }
    Ok(())
}

fn complete(host: &crate::host::Host, policy: &mut Policy, saved: &Saved, now: u64) -> Result<()> {
    if saved.phase != super::Phase::Published || saved.accepted.is_none() {
        policy.state.pending_request_id = None;
        policy.state.pending_frontier = None;
        policy.state.paused = Some(PauseState {
            disposition: Disposition::Attention,
            reason: PauseReason::Closing,
        });
    } else {
        policy.state.last_completed_request_id = Some(saved.request_id.clone());
        policy.state.last_completed_at = Some(now);
        // Conservative: later writes during capture remain eligible for the next backup.
        policy.state.last_backed_up_frontier = policy.state.pending_frontier;
        policy.state.pending_request_id = None;
        policy.state.pending_frontier = None;
        policy.state.attempts = 0;
        policy.state.next_run_at = now.saturating_add(policy.state.interval_seconds);
    }
    policy.save(host)
}

fn invoke(host: &crate::host::Host, policy: &mut Policy, id: Id, now: u64) -> Result<()> {
    policy.state.attempts = policy.state.attempts.saturating_add(1);
    let backoff = 60u64.saturating_mul(1u64 << policy.state.attempts.min(6));
    policy.state.next_run_at = now.saturating_add(backoff.min(3600));
    policy.save(host)?;
    let origin = RunOrigin::Auto {
        consent_revision: policy.state.revision,
    };
    if let Err(error) =
        super::control::request_locked(host, &ByteBackupRequest::Start { request_id: id }, origin)
    {
        let pause = error.pause_state();
        if pause.disposition == Disposition::Attention {
            policy.state.paused = Some(pause);
            policy.save(host)?;
        }
        return Err(error);
    }
    Ok(())
}

fn current_frontier(host: &crate::host::Host) -> Result<u64> {
    let runtime = host
        .db
        .db_ops()
        .namespaced_store()
        .disk_pack_runtime()
        .ok_or(BackupError::Source)?;
    if runtime.requires_recovery() {
        return Err(BackupError::Source);
    }
    Ok(runtime.status_native().frontier)
}

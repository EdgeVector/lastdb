//! One owner, one saved pending request, separate log and snapshot schedules.
mod cleanup;
mod policy;
mod replacement;
use super::{
    model::Saved,
    recovery::RecoveryPoint,
    store::{self, Store},
    work::Work,
    BackupError, Phase, Result,
};
use crate::backup_runtime::{Disposition, PauseReason, PauseState};
use cloud_backup_protocol::Id;
pub use policy::AutomaticBackupStatus;
pub(super) use policy::{configure, status};
use policy::{Pending, Policy};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tracing::Instrument;

pub fn spawn_automatic(
    host: Arc<crate::host::Host>,
    shutdown: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut changes = host.byte_backup.subscribe();
        let mut last_error = None;
        while !shutdown.load(Ordering::Acquire) && !host.byte_backup.snapshot().shutting_down {
            let error = tick(&host).err().map(|error| error.to_string());
            if error != last_error {
                if let Some(error) = &error { tracing::warn!(target: "lastdbd::byte_backup", error = %error, "automatic backup needs attention"); }
                last_error = error;
            }
            tokio::select! {
                result = changes.changed() => { if result.is_err() { break; } }
                _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {}
            }
        }
    }.in_current_span())
}
fn tick(host: &crate::host::Host) -> Result<()> {
    let _transition = host.byte_backup.transition()?;
    host.byte_backup.check_admission()?;
    let Some(mut policy) = policy::load(host)? else {
        return Ok(());
    };
    let runtime = native(host)?;
    if runtime.mutation_log_status()?.enabled != policy.state.enabled {
        runtime.configure_mutation_log(policy.state.enabled)?;
    }
    if !policy.state.enabled
        || policy.state.paused.is_some()
        || host.byte_backup.snapshot().active.is_some()
    {
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
    if let Some(id) = policy.cleanup.clone() {
        if Store::active(&host.home)?.as_ref() != Some(&id) {
            cleanup::retire(host, &id)?;
            policy.cleanup = None;
            policy.save(host)?;
        }
    }
    if let Some(pending) = policy.pending.clone() {
        return pending_job(host, policy, &pending, now);
    }
    if now < policy.state.next_log_at && now < policy.state.next_snapshot_at {
        return Ok(());
    }
    if let Some(id) = Store::active(&host.home)? {
        if !store::state(&host.home, &id)?
            .ok_or(BackupError::Invalid)?
            .terminal()
        {
            return Ok(());
        }
    }
    let point: Option<RecoveryPoint> = store::read(&Store::base(&host.home).join("recovery.json"))?;
    if let Some(point) = &point {
        point.validate()?;
    }
    let journal = native(host)?.mutation_log_status()?;
    let reason = if now >= policy.state.next_snapshot_at {
        Some("snapshot interval")
    } else if journal.snapshot_required {
        Some("mutation log needs a snapshot")
    } else if point
        .as_ref()
        .is_none_or(|p| p.anchor != journal.anchor || p.through.is_none())
    {
        Some("new snapshot base")
    } else {
        None
    };
    let work = if let Some(reason) = reason {
        policy.state.snapshot_reason = Some(reason.to_owned());
        Work::Snapshot {
            minimum: point.as_ref().map(|p| p.frontiers),
        }
    } else {
        let context = point.as_ref().ok_or(BackupError::Source)?.log_context()?;
        let head = journal.head.ok_or(BackupError::Source)?;
        if head.sequence == context.from.sequence {
            if head != context.from {
                return Err(BackupError::Identity);
            }
            policy.state.next_log_at = now.saturating_add(policy.state.log_interval_seconds);
            return policy.save(host);
        }
        if head.sequence < context.from.sequence {
            return Err(BackupError::Identity);
        }
        Work::MutationLog {
            context: Box::new(context),
        }
    };
    let pending = Pending {
        id: request_id()?,
        revision: policy.state.revision,
        work,
    };
    policy.state.pending_request_id = Some(pending.id.clone());
    policy.pending = Some(pending.clone());
    policy.state.attempts = 0;
    policy.state.retry_at = 0;
    policy.save(host)?;
    invoke(host, policy, &pending, now)
}
fn pending_job(
    host: &crate::host::Host,
    policy: &mut Policy,
    pending: &Pending,
    now: u64,
) -> Result<()> {
    if replacement::retire(host, policy, pending)? {
        let saved = store::state(&host.home, &pending.id)?.ok_or(BackupError::Missing)?;
        return complete(host, policy, &saved, now);
    }
    if let Some(saved) = store::state(&host.home, &pending.id)? {
        policy.validate_job(&saved)?;
        if saved.terminal() {
            return complete(host, policy, &saved, now);
        }
        if let Some(completion) = host.byte_backup.snapshot().last_completion {
            if completion.invocation.request_id == pending.id {
                if let crate::backup_runtime::InvocationOutcome::Paused(pause) = completion.outcome
                {
                    if pause.disposition == Disposition::Attention {
                        policy.state.paused = Some(pause);
                        return policy.save(host);
                    }
                }
            }
        }
        let pause = store::read::<Option<PauseState>>(
            &Store::base(&host.home)
                .join("jobs")
                .join(pending.id.as_str())
                .join("pause.json"),
        )?
        .flatten();
        if pause.is_some_and(|p| p.disposition == Disposition::Attention) {
            policy.state.paused = pause;
            return policy.save(host);
        }
    }
    if now >= policy.state.retry_at {
        invoke(host, policy, pending, now)?;
    }
    Ok(())
}
fn complete(host: &crate::host::Host, policy: &mut Policy, saved: &Saved, now: u64) -> Result<()> {
    if saved.phase != Phase::Published {
        policy.cleanup = Some(saved.request_id.clone());
        policy.pending = None;
        policy.state.pending_request_id = None;
        policy.state.attempts = 0;
        policy.state.retry_at = 0;
        if saved.local_action.is_some() {
            policy.state.next_snapshot_at = 0;
            policy.state.next_log_at = 0;
            policy.state.paused = None;
        } else {
            policy.state.paused = Some(PauseState {
                disposition: Disposition::Attention,
                reason: PauseReason::Closing,
            });
        }
        return policy.save(host);
    }
    let point: RecoveryPoint = Store::open(&host.home, &saved.request_id)?
        .read("recovery.json")?
        .ok_or(BackupError::Missing)?;
    point.validate()?;
    if saved.accepted.as_ref() != Some(&point.accepted) {
        return Err(BackupError::Identity);
    }
    policy.cleanup = policy
        .state
        .last_completed_request_id
        .replace(saved.request_id.clone());
    policy.state.last_completed_at = Some(now);
    policy.state.last_backed_up_frontiers = Some(point.frontiers);
    policy.pending = None;
    policy.state.pending_request_id = None;
    policy.state.attempts = 0;
    policy.state.retry_at = 0;
    match saved.work {
        Work::Snapshot { .. } => {
            policy.state.last_snapshot_at = Some(now);
            policy.state.next_snapshot_at =
                now.saturating_add(policy.state.snapshot_interval_seconds);
        }
        Work::MutationLog { .. } => {
            policy.state.last_log_at = Some(now);
        }
    }
    let backlog = native(host)?
        .mutation_log_status()?
        .head
        .is_some_and(|head| {
            point
                .through
                .as_ref()
                .is_some_and(|p| head.epoch == p.epoch && head.sequence > p.sequence)
        });
    policy.state.next_log_at = if backlog {
        now
    } else {
        now.saturating_add(policy.state.log_interval_seconds)
    };
    policy.save(host)
}
fn invoke(
    host: &crate::host::Host,
    policy: &mut Policy,
    pending: &Pending,
    now: u64,
) -> Result<()> {
    policy.state.attempts = policy.state.attempts.saturating_add(1);
    policy.state.retry_at =
        now.saturating_add((15u64.saturating_mul(1u64 << policy.state.attempts.min(8))).min(3600));
    policy.save(host)?;
    super::control::request_automatic(host, pending.id.clone(), pending.revision, &pending.work)?;
    Ok(())
}
fn native(host: &crate::host::Host) -> Result<Arc<fold_db::storage::disk_pack::PackRuntime>> {
    let runtime = host
        .db
        .db_ops()
        .namespaced_store()
        .disk_pack_runtime()
        .ok_or(BackupError::Source)?;
    if runtime.requires_recovery() {
        return Err(BackupError::Source);
    }
    Ok(runtime)
}

pub(super) fn reserved(id: &Id) -> bool {
    id.as_str().starts_with("a17dbac2-")
}

fn request_id() -> Result<Id> {
    let mut bytes = *uuid::Uuid::new_v4().as_bytes();
    bytes[..4].copy_from_slice(&[0xa1, 0x7d, 0xba, 0xc2]);
    Ok(Id::try_from(uuid::Uuid::from_bytes(bytes).to_string())?)
}

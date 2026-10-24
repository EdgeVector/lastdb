use super::{
    model::{Binding, Expected, State},
    *,
};
use std::time::Duration;

pub(super) enum Next {
    Idle(Option<Duration>),
    Again,
    Invoke {
        revision: u64,
        id: Id,
        binding: Binding,
    },
}

/// All control-file changes share the same lock as worker admission and Off.
pub(super) fn next(host: &Host) -> Result<Next> {
    let _transition = host.native_backup.transition()?;
    let runtime = host.native_backup.snapshot();
    let mut state = State::load(&host.home)?;
    if !state.enabled || runtime.shutting_down || runtime.active.is_some() {
        return Ok(Next::Idle(None));
    }
    let binding = state.binding.clone().ok_or(BackupError::Invalid)?;
    if state
        .pause
        .is_some_and(|pause| pause.disposition == Disposition::Attention)
    {
        return Ok(Next::Idle(None));
    }
    if let Some(id) = state.scheduled.clone() {
        if reconcile(host, &mut state, &id)? {
            return Ok(Next::Again);
        }
    } else if select_existing(host, &mut state, &binding)? {
        return Ok(Next::Again);
    } else if !dirty(host, &binding)? {
        return Ok(Next::Idle(None));
    } else {
        state.selection_at_schedule = Store::active(&host.home)?;
        state.scheduled = Some(Id::try_from(uuid::Uuid::new_v4().to_string())?);
        state.save(&host.home)?;
        return Ok(Next::Again);
    }
    if state
        .pause
        .is_some_and(|pause| pause.disposition == Disposition::Attention)
    {
        return Ok(Next::Idle(None));
    }
    if let Some(deadline) = state.retry_at_ms {
        let now = model::now_ms()?;
        if deadline > now {
            let remaining = (deadline - now).min(30_000);
            if deadline - now > remaining {
                state.retry_at_ms = Some(now.checked_add(remaining).ok_or(BackupError::Limit)?);
                state.save(&host.home)?;
            }
            return Ok(Next::Idle(Some(Duration::from_millis(remaining))));
        }
    }
    let id = state.scheduled.clone().ok_or(BackupError::Invalid)?;
    if let Some(saved) = local::saved(&host.home, &id)? {
        local::compatible(&saved, &binding)?;
    }
    if prefer_active(host, &mut state, &binding, true)? {
        state.save(&host.home)?;
        return Ok(Next::Again);
    }
    state.awaiting_completion = true;
    state.expected = Some(Expected {
        runtime_id: Id::try_from(host.native_backup.instance_id().to_string())?,
        after_token: runtime
            .last_completion
            .map_or(0, |completion| completion.invocation.token),
        token: None,
        auto_revision: Some(state.revision),
    });
    state.pause = None;
    state.retry_at_ms = None;
    state.save(&host.home)?;
    Ok(Next::Invoke {
        revision: state.revision,
        id,
        binding,
    })
}

pub(super) fn prefer_active(
    host: &Host,
    state: &mut State,
    binding: &Binding,
    honor_pause: bool,
) -> Result<bool> {
    let Some(id) = state.scheduled.clone() else {
        return Ok(false);
    };
    let Some(active) = Store::active(&host.home)?.filter(|active| active != &id) else {
        return Ok(false);
    };
    let saved = local::saved(&host.home, &active)?.ok_or(BackupError::Invalid)?;
    let terminal = local::terminal(&host.home, &saved)?;
    if terminal && state.selection_at_schedule.as_ref() == Some(&active) {
        return Ok(false);
    }
    // No worker or cloud effect can precede its durable Saved record. Preserve
    // any admitted A; only an unused scheduled UUID may yield to manual B.
    if local::saved(&host.home, &id)?.is_some() {
        return if terminal {
            Ok(false)
        } else {
            Err(BackupError::Identity)
        };
    }
    local::compatible(&saved, binding)?;
    state.scheduled = Some(active.clone());
    state.selection_at_schedule = None;
    state.clear_pause();
    let completion = host.native_backup.snapshot().last_completion;
    let pause = completion
        .filter(|done| done.invocation.request_id == active)
        .and_then(|done| match done.outcome {
            super::super::InvocationOutcome::Paused(pause) => Some(pause),
            _ => None,
        })
        .or(saved.pause);
    if let Some(pause) = pause.filter(|_| honor_pause) {
        local::record_pause(state, pause)?;
    }
    Ok(true)
}

fn reconcile(host: &Host, state: &mut State, id: &Id) -> Result<bool> {
    let saved = local::saved(&host.home, id)?;
    if let Some(saved) = &saved {
        if local::terminal(&host.home, saved)? {
            state.scheduled = None;
            state.selection_at_schedule = None;
            state.clear_pause();
            if saved.phase == super::super::Phase::Closed {
                local::record_pause(
                    state,
                    PauseState {
                        disposition: Disposition::Attention,
                        reason: PauseReason::Closing,
                    },
                )?;
            }
            state.save(&host.home)?;
            return Ok(true);
        }
    }
    if state.awaiting_completion {
        let expected = state.expected.as_ref().ok_or(BackupError::Invalid)?;
        let completion = host
            .native_backup
            .snapshot()
            .last_completion
            .filter(|completion| {
                expected.runtime_id.as_str() == host.native_backup.instance_id().to_string()
                    && completion.invocation.request_id == *id
                    && expected.token.map_or(
                        completion.invocation.token > expected.after_token,
                        |token| token == completion.invocation.token,
                    )
                    && match completion.invocation.origin {
                        RunOrigin::Auto { consent_revision } => consent_revision == state.revision,
                        RunOrigin::Manual => {
                            expected.auto_revision.is_none() || expected.token.is_none()
                        }
                    }
            });
        let pause = completion
            .and_then(|completion| match completion.outcome {
                super::super::InvocationOutcome::Paused(pause) => Some(pause),
                _ => None,
            })
            .or_else(|| {
                saved.and_then(|saved| {
                    if saved.pause_consent_revision == Some(state.revision)
                        || (expected.token.is_some()
                            && expected.auto_revision.is_none()
                            && saved.pause_consent_revision.is_none())
                    {
                        saved.pause
                    } else {
                        None
                    }
                })
            })
            .unwrap_or(PauseState {
                disposition: Disposition::Retry,
                reason: PauseReason::Shutdown,
            });
        local::record_pause(state, pause)?;
        state.save(&host.home)?;
        return Ok(true);
    }
    Ok(false)
}

fn select_existing(host: &Host, state: &mut State, binding: &Binding) -> Result<bool> {
    let Some(id) = Store::active(&host.home)? else {
        return Ok(false);
    };
    let saved = local::saved(&host.home, &id)?.ok_or(BackupError::Invalid)?;
    if local::terminal(&host.home, &saved)? {
        return Ok(false);
    }
    local::compatible(&saved, binding)?;
    state.scheduled = Some(id);
    state.selection_at_schedule = None;
    if let Some(pause) = saved.pause {
        local::record_pause(state, pause)?;
    }
    state.save(&host.home)?;
    Ok(true)
}

/// Bind the exact worker after admission, including a manual invocation which
/// entered while automatic preflight was outside the transition lock.
pub(super) fn bind_started(host: &Host, revision: u64, id: &Id) -> Result<()> {
    let _transition = host.native_backup.transition()?;
    let mut state = State::load(&host.home)?;
    if !state.enabled || state.revision != revision || state.scheduled.as_ref() != Some(id) {
        return Ok(());
    }
    let snapshot = host.native_backup.snapshot();
    let invocation = snapshot
        .active
        .or_else(|| snapshot.last_completion.map(|done| done.invocation));
    let expected = state.expected.as_mut().ok_or(BackupError::Invalid)?;
    if let Some(invocation) =
        invocation.filter(|run| run.request_id == *id && run.token > expected.after_token)
    {
        expected.token = Some(invocation.token);
        expected.auto_revision = match invocation.origin {
            RunOrigin::Manual => None,
            RunOrigin::Auto { consent_revision } => Some(consent_revision),
        };
    }
    state.save(&host.home)
}

fn dirty(host: &Host, binding: &Binding) -> Result<bool> {
    let runtime = host
        .db
        .db_ops()
        .namespaced_store()
        .disk_pack_runtime()
        .ok_or(BackupError::Source)?;
    let current = runtime.native_backup_boundary()?;
    if current.source_home.to_string() != binding.source_home.as_str() {
        return Err(BackupError::Identity);
    }
    match local::coverage(&host.home, binding)? {
        Some(frontier) if current.frontier < frontier => Err(BackupError::Identity),
        Some(frontier) => Ok(current.frontier > frontier),
        None => Ok(true),
    }
}

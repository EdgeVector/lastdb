use super::{
    model::{Binding, State},
    *,
};
use cloud_backup_protocol::{
    jobs::{JobResult, JobStatus},
    publication::AcceptedBackup,
    Id,
};
use std::path::Path;

pub(super) fn saved(home: &Path, id: &Id) -> Result<Option<super::super::model::Saved>> {
    let value: Option<super::super::model::Saved> = store::read(
        &Store::base(home)
            .join("jobs")
            .join(id.as_str())
            .join("state.json"),
    )?;
    if let Some(value) = &value {
        value.validate()?;
        if value.request_id != *id {
            return Err(BackupError::Identity);
        }
    }
    Ok(value)
}

pub(super) fn compatible(saved: &super::super::model::Saved, binding: &Binding) -> Result<()> {
    if saved.account != binding.scope.account_id
        || saved.source_home != binding.source_home
        || saved.endpoint.as_ref() != Some(&binding.endpoint)
        || saved
            .intent
            .as_ref()
            .is_some_and(|intent| intent.job.stream.scope != binding.scope)
    {
        return Err(BackupError::Identity);
    }
    Ok(())
}

pub(super) fn accepted(home: &Path) -> Result<Option<AcceptedBackup>> {
    let backup: Option<AcceptedBackup> = store::read(&Store::base(home).join("accepted.json"))?;
    if let Some(backup) = &backup {
        backup.validate()?;
    }
    Ok(backup)
}

pub(super) fn terminal(home: &Path, saved: &super::super::model::Saved) -> Result<bool> {
    if saved.locally_closed() {
        let close: super::super::model::CloseIntent = store::read(
            &Store::base(home)
                .join("jobs")
                .join(saved.request_id.as_str())
                .join("close.json"),
        )?
        .ok_or(BackupError::Invalid)?;
        if close.request_id != saved.request_id {
            return Err(BackupError::Identity);
        }
        return Ok(true);
    }
    let result: Option<JobResult> = store::read(
        &Store::base(home)
            .join("jobs")
            .join(saved.request_id.as_str())
            .join("terminal.json"),
    )?;
    let Some(result) = result else {
        return Ok(false);
    };
    saved.validate_terminal(&result)?;
    if saved.retained_source_bytes != Some(0) {
        return Ok(false);
    }
    Ok(true)
}

pub(super) fn coverage(home: &Path, binding: &Binding) -> Result<Option<u64>> {
    let Some(backup) = accepted(home)? else {
        return Ok(None);
    };
    if backup.job.stream.scope != binding.scope
        || backup.job.stream.source_home != binding.source_home
    {
        return Err(BackupError::Identity);
    }
    let saved = saved(home, &backup.job.commit_id)?.ok_or(BackupError::Invalid)?;
    compatible(&saved, binding)?;
    if !accepted_complete(home, &saved, &backup)? {
        return Ok(None);
    }
    Ok(Some(backup.native_root.frontier.get()))
}

/// Called under the transition lock after an exact remote accepted-head check.
pub(super) fn bind_accepted_endpoint(
    host: &Host,
    binding: &Binding,
    backup: &AcceptedBackup,
) -> Result<()> {
    if accepted(&host.home)?.as_ref() != Some(backup) {
        return Err(BackupError::Busy);
    }
    let mut saved = saved(&host.home, &backup.job.commit_id)?.ok_or(BackupError::Invalid)?;
    if saved.account != binding.scope.account_id
        || saved.source_home != binding.source_home
        || backup.job.stream.scope != binding.scope
        || !accepted_complete(&host.home, &saved, backup)?
    {
        return Err(BackupError::Identity);
    }
    if let Some(endpoint) = &saved.endpoint {
        if endpoint != &binding.endpoint {
            return Err(BackupError::Identity);
        }
        return Ok(());
    }
    if host
        .native_backup
        .snapshot()
        .active
        .as_ref()
        .is_some_and(|active| active.request_id == saved.request_id)
    {
        return Err(BackupError::Busy);
    }
    saved.endpoint = Some(binding.endpoint.clone());
    store::write_completed_state(&host.home, &saved)
}

fn accepted_complete(
    home: &Path,
    saved: &super::super::model::Saved,
    backup: &AcceptedBackup,
) -> Result<bool> {
    let result: Option<JobResult> = store::read(
        &Store::base(home)
            .join("jobs")
            .join(saved.request_id.as_str())
            .join("terminal.json"),
    )?;
    let Some(result) = result else {
        return Err(BackupError::Invalid);
    };
    saved.validate_terminal(&result)?;
    match result.status {
        JobStatus::Published { backup: terminal } if terminal.as_ref() == backup => {
            Ok(saved.retained_source_bytes == Some(0))
        }
        _ => Err(BackupError::Invalid),
    }
}

/// Remote acceptance may precede the local terminal checkpoint. Recover only
/// the selected job whose complete frozen begin and commit validate that head.
pub(super) fn pending_head(
    home: &Path,
    binding: &Binding,
    backup: &AcceptedBackup,
    id: &Id,
) -> Result<()> {
    if &backup.job.commit_id != id {
        return Err(BackupError::Identity);
    }
    let saved = saved(home, id)?.ok_or(BackupError::Identity)?;
    compatible(&saved, binding)?;
    backup.validate_for(
        saved.begin.as_ref().ok_or(BackupError::Invalid)?,
        saved.commit.as_ref().ok_or(BackupError::Invalid)?,
    )?;
    if terminal(home, &saved)? {
        return Err(BackupError::Identity);
    }
    Ok(())
}

pub(super) fn record_pause(state: &mut State, pause: PauseState) -> Result<()> {
    pause.validate()?;
    state.awaiting_completion = false;
    state.expected = None;
    state.pause = Some(pause);
    if pause.disposition == Disposition::Retry {
        let seconds = [1u64, 2, 4, 8, 16, 30][usize::from(state.retry_attempt.min(5))];
        state.retry_attempt = state.retry_attempt.saturating_add(1);
        state.retry_at_ms = Some(
            model::now_ms()?
                .checked_add(seconds * 1000)
                .ok_or(BackupError::Limit)?,
        );
    } else {
        state.retry_at_ms = None;
    }
    Ok(())
}

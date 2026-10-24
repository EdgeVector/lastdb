use super::{
    identity::Source,
    model::{CloseIntent, Saved},
    store::Store,
    worker::Work,
    BackupError, Phase, Result,
};
use cloud_backup_protocol::jobs::{JobCloseRequest, JobResult, JobStatus};
use cloud_backup_protocol::Version;

pub(super) fn cancel_uncaptured(source: &Source, store: &Store, saved: &mut Saved) -> Result<bool> {
    let Some(close) = store.read::<CloseIntent>("close.json")? else {
        return Ok(false);
    };
    if close.request_id != saved.request_id {
        return Err(BackupError::Identity);
    }
    if saved.locally_closed() {
        return Ok(true);
    }
    if saved.begin.is_some() {
        return Ok(false);
    }
    if let Some(capture) = source.runtime.native_backup_capture_status()? {
        if saved
            .intent
            .as_ref()
            .is_some_and(|intent| intent.job == capture.begin.job)
        {
            return Ok(false);
        }
    }
    // No capture or job admission exists. The durable close intent fences all
    // later retries before a cut, credentials or cloud admission can occur.
    saved.phase = Phase::Closed;
    saved.retained_source_bytes = Some(0);
    saved.blocked_reason = None;
    saved.pause = None;
    saved.pause_consent_revision = None;
    store.save(saved)?;
    Ok(true)
}

pub(super) fn save(store: &Store, saved: &Saved, result: &JobResult) -> Result<()> {
    saved.validate_terminal(result)?;
    if store
        .read::<JobResult>("terminal.json")?
        .as_ref()
        .is_some_and(|prior| prior != result)
    {
        return Err(BackupError::Invalid);
    }
    store.write("terminal.json", result)
}
pub(super) async fn close(work: &mut Work<'_>) -> Result<()> {
    let intent: CloseIntent = work.store.read("close.json")?.ok_or(BackupError::Invalid)?;
    if intent.request_id != work.saved.request_id {
        return Err(BackupError::Identity);
    }
    work.checkpoint(Phase::Closing)?;
    let begin = work.saved.begin.as_ref().ok_or(BackupError::Invalid)?;
    let request = JobCloseRequest {
        version: Version,
        job: begin.job.clone(),
        job_input_sha256: begin.input_sha256()?,
        reason: intent.reason,
    };
    let result = work.client.close(&request).await?;
    finish(work, &result)
}
pub(super) fn finish(work: &mut Work<'_>, result: &JobResult) -> Result<()> {
    save(work.store, work.saved, result)?;
    if let JobStatus::Published { backup } = &result.status {
        work.store.write_base("accepted.json", backup)?;
        work.saved.phase = Phase::Published;
    } else {
        work.saved.phase = Phase::Closed;
    }
    work.saved.blocked_reason = None;
    work.saved.pause = None;
    work.saved.pause_consent_revision = None;
    // Authority reaches disk before source release or private metadata cleanup.
    work.store.save(work.saved)?;
    work.capture.confirm_terminal(result.clone())?;
    fold_db::storage::disk_pack::backup_manifest::cleanup_after_terminal(work.capture, result)?;
    work.saved.retained_source_bytes = Some(0);
    work.store.set_other_bytes(0);
    work.store.save(work.saved)
}

pub(super) async fn reconcile(
    source: &Source,
    store: &Store,
    saved: &mut Saved,
    result: JobResult,
) -> Result<()> {
    saved.validate_terminal(&result)?;
    save(store, saved, &result)?;
    // A terminal retry must never create a new source cut under an old identity.
    if let Some(previous) = source.runtime.native_backup_capture_status()? {
        if previous.begin.job == result.job {
            let capture = source.runtime.resume_native_backup_capture().await?;
            capture.confirm_terminal(result.clone())?;
            fold_db::storage::disk_pack::backup_manifest::cleanup_after_terminal(
                &capture, &result,
            )?;
        } else if saved.retained_source_bytes != Some(0) {
            return Err(BackupError::Source);
        }
    } else if saved.retained_source_bytes != Some(0) {
        return Err(BackupError::Source);
    }
    if let JobStatus::Published { backup } = &result.status {
        store.write_base("accepted.json", backup)?;
        saved.phase = Phase::Published;
    } else {
        saved.phase = Phase::Closed;
    }
    saved.retained_source_bytes = Some(0);
    saved.blocked_reason = None;
    saved.pause = None;
    saved.pause_consent_revision = None;
    store.set_other_bytes(0);
    store.save(saved)
}

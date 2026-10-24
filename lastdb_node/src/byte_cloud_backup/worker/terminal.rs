//! Replay exact durable terminal authority before any credential or cloud request.
use super::*;
use crate::native_cloud_backup::InvocationOutcome;
use std::fs::File;
use std::sync::Arc;

pub(in crate::byte_cloud_backup) fn validate_terminal(
    saved: &Saved,
    result: &JobResult,
) -> Result<()> {
    saved.validate()?;
    if !saved.begin_submitted {
        return Err(BackupError::Invalid);
    }
    let begin = saved.begin.as_ref().ok_or(BackupError::Invalid)?;
    result.validate_for_begin(begin)?;
    match &result.status {
        JobStatus::Published { backup } => {
            backup.validate_for(begin, saved.commit.as_ref().ok_or(BackupError::Invalid)?)?;
            if saved
                .accepted
                .as_ref()
                .is_some_and(|old| old != backup.as_ref())
            {
                return Err(BackupError::Identity);
            }
        }
        JobStatus::Closed { .. } => {
            if saved.accepted.is_some() || saved.phase == Phase::Published {
                return Err(BackupError::Identity);
            }
        }
        JobStatus::Active => return Err(BackupError::Invalid),
    }
    Ok(())
}

pub(super) async fn reconcile(source: &Source, store: &Store, saved: &mut Saved) -> Result<()> {
    source.validate(saved)?;
    let result: JobResult = store.read("terminal.json")?.ok_or(BackupError::Missing)?;
    validate_terminal(saved, &result)?;
    let capture = source
        .runtime
        .resume_byte_backup_capture(&source.key)
        .await?;
    if capture.begin()? != *saved.begin.as_ref().ok_or(BackupError::Invalid)? {
        return Err(BackupError::Identity);
    }
    // finish checks the exact client root before any accepted ledger write.
    // A locally cancelled, never-submitted job cannot supply a cloud terminal.
    super::finish(&capture, store, saved, &result)
}

/// The Store file lock and captured source stay owned until this returns.
/// There is deliberately no client parameter or credential construction here.
pub(in crate::byte_cloud_backup) fn terminal_invocation(
    source: Source,
    store: Store,
    mut saved: Saved,
    permit: File,
    pause: Arc<AtomicBool>,
) -> InvocationOutcome {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        transfer::check_pause(&pause)?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| BackupError::Source)?;
        runtime.block_on(reconcile(&source, &store, &mut saved))
    }))
    .unwrap_or(Err(BackupError::WorkerPanic));
    let outcome = match result {
        Ok(()) => InvocationOutcome::Complete,
        Err(error) => {
            saved.blocked_reason = Some(error.to_string());
            match Store::open(&source.home, &saved.request_id).and_then(|fresh| fresh.save(&saved))
            {
                Ok(()) => InvocationOutcome::Paused(error.pause_state()),
                Err(checkpoint) => InvocationOutcome::Paused(checkpoint.pause_state()),
            }
        }
    };
    drop(store);
    drop(source);
    drop(permit);
    drop(pause);
    outcome
}

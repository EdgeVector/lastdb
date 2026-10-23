use super::{
    identity::Source,
    model::Saved,
    store::{Store, CONTROL_RESERVE},
    BackupError, Result,
};
use cloud_backup_protocol::context::ContextReadRequest;
use cloud_backup_protocol::publication::{HeadReadRequest, HeadReadResult};
use cloud_backup_protocol::{BackupJob, BackupStream, ErrorCode, Id, Version};
use fold_db::storage::disk_pack::backup_capture::{
    BackupCapturePhase, NativeBackupCapture, NativeBackupCaptureRequest,
};
use fold_db::sync::auth::native_backup::{NativeBackupClient, NativeBackupError};

pub(super) async fn intent(
    source: &Source,
    store: &Store,
    saved: &mut Saved,
    client: &NativeBackupClient,
) -> Result<()> {
    source.validate(saved)?;
    if saved.intent.is_some() {
        return Ok(());
    }
    let context = client
        .context(&ContextReadRequest { version: Version }, &source.account)
        .await?;
    if !context.new_work_enabled || !context.open_for_writes {
        return Err(BackupError::Waiting);
    }
    let previous: Option<BackupStream> = store.read_base("stream.json")?;
    let stream = match previous {
        Some(stream)
            if stream.scope == context.scope && stream.source_home == source.source_home =>
        {
            stream
        }
        _ => BackupStream {
            scope: context.scope,
            source_home: source.source_home.clone(),
            stream_id: Id::try_from(uuid::Uuid::new_v4().to_string())?,
        },
    };
    stream.validate()?;
    store.write_base("stream.json", &stream)?;
    let response = match client
        .head(&HeadReadRequest {
            version: Version,
            stream: stream.clone(),
        })
        .await
    {
        Ok(HeadReadResult::Ok { response }) => Some(response),
        Ok(HeadReadResult::Error { error }) if error.code == ErrorCode::NotFound => None,
        Err(NativeBackupError::Protocol(error)) if error.code == ErrorCode::NotFound => None,
        Err(error) => return Err(error.into()),
        _ => return Err(BackupError::Source),
    };
    if response.as_ref().is_some_and(|head| {
        head.active_commit_id
            .as_ref()
            .is_some_and(|active| active != &saved.request_id)
    }) {
        return Err(BackupError::Busy);
    }
    saved.intent = Some(NativeBackupCaptureRequest {
        job: BackupJob {
            stream,
            commit_id: saved.request_id.clone(),
        },
        recovery: source.recovery.clone(),
        predecessor: response.and_then(|head| head.accepted.map(|backup| backup.head())),
    });
    store.save(saved)
}

pub(super) async fn capture(
    source: &Source,
    store: &Store,
    saved: &mut Saved,
) -> Result<NativeBackupCapture> {
    let intent = saved.intent.as_ref().ok_or(BackupError::Invalid)?;
    if let Some(previous) = source.runtime.native_backup_capture_status()? {
        if previous.begin.job == intent.job {
            let capture = source.runtime.resume_native_backup_capture().await?;
            bind(store, saved, &capture)?;
            return Ok(capture);
        }
        match previous.phase {
            BackupCapturePhase::Terminal { result, .. } => {
                let old = source.runtime.resume_native_backup_capture().await?;
                old.confirm_terminal((*result).clone())?;
                fold_db::storage::disk_pack::backup_manifest::cleanup_after_terminal(
                    &old, &result,
                )?;
                drop(old);
            }
            _ => return Err(BackupError::Busy),
        }
    }
    // Bound the initial catalogue before the capture can expose its coordinator allowance.
    let quote =
        2 * 1024 * 1024 + cloud_backup_protocol::limits::NATIVE_FILES_MAX as u64 * (512 + 1536);
    store.admit(quote + CONTROL_RESERVE)?;
    source.runtime.flush_native().await?;
    let capture = source
        .runtime
        .prepare_native_backup_capture(intent.clone())
        .await?;
    bind(store, saved, &capture)?;
    Ok(capture)
}
fn bind(store: &Store, saved: &mut Saved, capture: &NativeBackupCapture) -> Result<()> {
    let begin = capture.frozen_begin()?;
    if saved.begin.as_ref().is_some_and(|prior| prior != &begin) {
        return Err(BackupError::Invalid);
    }
    saved.begin = Some(begin);
    let status = capture.status()?;
    if !matches!(status.phase, BackupCapturePhase::Preparing) && status.native_files > 0 {
        if saved
            .native_files
            .is_some_and(|files| files != status.native_files)
            || saved
                .native_bytes
                .is_some_and(|bytes| bytes != status.native_bytes)
        {
            return Err(BackupError::Invalid);
        }
        saved.native_files = Some(status.native_files);
        saved.native_bytes = Some(status.native_bytes);
    }
    saved.retained_source_bytes = status.retained_source_bytes;
    store.save(saved)
}

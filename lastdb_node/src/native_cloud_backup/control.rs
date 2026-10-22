//! A saved owner request precedes the independent process-lifetime worker.
use super::{
    identity::Source,
    model::{CloseIntent, Saved},
    store::{self, Store},
    *,
};
use cloud_backup_protocol::jobs::{CloseReason, JobCloseRequest, JobResult, JobStatus};
use cloud_backup_protocol::publication::AcceptedBackup;
use cloud_backup_protocol::{Id, Version};
use std::fs::File;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

static RUNNING: AtomicBool = AtomicBool::new(false);
struct Permit {
    _lock: File,
}
impl Drop for Permit {
    fn drop(&mut self) {
        RUNNING.store(false, Ordering::Release);
    }
}

pub async fn request(
    host: &crate::host::Host,
    action: NativeBackupRequest,
) -> Result<NativeBackupStatus> {
    if matches!(action, NativeBackupRequest::Status) {
        return status(&host.home);
    }
    let terminal_resume =
        matches!(action, NativeBackupRequest::Resume) && active_terminal(&host.home)?;
    if !matches!(action, NativeBackupRequest::Close) && !terminal_resume {
        require_enabled()?;
    }
    let source = Source::from_host(host)?;
    if matches!(action, NativeBackupRequest::Close) {
        return close(source).await;
    }
    let id = match action {
        NativeBackupRequest::Start { request_id } => request_id,
        NativeBackupRequest::Resume => Store::active(&host.home)?.ok_or(BackupError::Missing)?,
        _ => return Err(BackupError::Invalid),
    };
    if let Some(value) = archived_terminal(&host.home, &id)? {
        return Ok(value);
    }
    if RUNNING.load(Ordering::Acquire) {
        return if Store::active(&host.home)?.as_ref() == Some(&id) {
            status(&host.home)
        } else {
            Err(BackupError::Busy)
        };
    }
    let permit = acquire(&source.home)?;
    ensure_previous(&source.home, &id)?;
    let store = Store::open(&source.home, &id)?;
    let saved = match store.state()? {
        Some(saved) => saved,
        None => source.saved(id.clone()),
    };
    source.validate(&saved)?;
    store.save(&saved)?;
    store.select(&id)?;
    // A released result also resumes uncertain local cleanup.
    launch(source, store, saved, permit)?;
    status(&host.home)
}

fn acquire(home: &Path) -> Result<Permit> {
    RUNNING
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .map_err(|_| BackupError::Busy)?;
    match Store::lock(home) {
        Ok(lock) => Ok(Permit { _lock: lock }),
        Err(error) => {
            RUNNING.store(false, Ordering::Release);
            Err(error)
        }
    }
}
fn ensure_previous(home: &Path, id: &Id) -> Result<()> {
    let Some(active) = Store::active(home)? else {
        return Ok(());
    };
    if &active == id {
        return Ok(());
    }
    let previous = Store::open(home, &active)?;
    let saved = previous.state()?.ok_or(BackupError::Invalid)?;
    if saved.locally_closed() {
        return validate_local_close(&previous, &saved);
    }
    let result: JobResult = previous.read("terminal.json")?.ok_or(BackupError::Busy)?;
    saved.validate_terminal(&result).and_then(|()| {
        if saved.retained_source_bytes == Some(0) {
            Ok(())
        } else {
            Err(BackupError::Busy)
        }
    })
}
fn launch(source: Source, store: Store, mut saved: Saved, permit: Permit) -> Result<()> {
    std::thread::Builder::new()
        .name("lastdb-native-backup".into())
        .spawn(move || {
            let _permit = permit;
            let result = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|_| BackupError::Source)
                .and_then(|runtime| {
                    runtime.block_on(super::worker::run(&source, &store, &mut saved))
                });
            if let Err(error) = result {
                saved.blocked_reason = Some(error.label().into());
                // Reconcile any uncertain previous write before a final error checkpoint.
                if let Ok(fresh) = Store::open(&source.home, &saved.request_id) {
                    let _ = fresh.save(&saved);
                }
            }
        })
        .map_err(|_| BackupError::Source)?;
    Ok(())
}

async fn close(source: Source) -> Result<NativeBackupStatus> {
    let home = source.home.clone();
    let id = Store::active(&source.home)?.ok_or(BackupError::Missing)?;
    let store = Store::open(&source.home, &id)?;
    let mut saved = store.state()?.ok_or(BackupError::Missing)?;
    source.validate(&saved)?;
    store.write(
        "close.json",
        &CloseIntent {
            request_id: id.clone(),
            reason: CloseReason::UserRequested,
        },
    )?;
    if let Some(begin) = &saved.begin {
        let request = JobCloseRequest {
            version: Version,
            job: begin.job.clone(),
            job_input_sha256: begin.input_sha256()?,
            reason: CloseReason::UserRequested,
        };
        let client = crate::native_backup_access::client_for_recovery(&source.home)
            .map_err(|_| BackupError::Identity)?;
        let result = client.close(&request).await?;
        saved = store.state()?.ok_or(BackupError::Invalid)?;
        source.validate(&saved)?;
        saved.validate_terminal(&result)?;
        super::terminal::save(&store, &saved, &result)?;
    }
    if !RUNNING.load(Ordering::Acquire) {
        let permit = acquire(&source.home)?;
        saved = store.state()?.ok_or(BackupError::Invalid)?;
        source.validate(&saved)?;
        launch(source, store, saved, permit)?;
    }
    status(&home)
}

// Status is a fixed set of keyed local reads. It does not open a capture,
// enumerate jobs, obtain credentials, audit dependencies or make cloud calls.
pub(super) fn status(home: &Path) -> Result<NativeBackupStatus> {
    status_for(home, Store::active(home)?)
}
fn active_terminal(home: &Path) -> Result<bool> {
    let Some(id) = Store::active(home)? else {
        return Ok(false);
    };
    let root = Store::base(home).join("jobs").join(id.as_str());
    if store::read::<JobResult>(&root.join("terminal.json"))?.is_some() {
        return Ok(true);
    }
    Ok(store::read::<Saved>(&root.join("state.json"))?.is_some_and(|saved| saved.locally_closed()))
}
fn archived_terminal(home: &Path, id: &Id) -> Result<Option<NativeBackupStatus>> {
    if Store::active(home)?.as_ref() == Some(id) {
        return Ok(None);
    }
    let path = Store::base(home).join("jobs").join(id.as_str());
    let Some(saved) = store::read::<Saved>(&path.join("state.json"))? else {
        return Ok(None);
    };
    if let Some(result) = store::read::<JobResult>(&path.join("terminal.json"))? {
        saved.validate_terminal(&result)?;
    } else if saved.locally_closed() {
        let close: CloseIntent =
            store::read(&path.join("close.json"))?.ok_or(BackupError::Invalid)?;
        if &close.request_id != id {
            return Err(BackupError::Identity);
        }
    } else {
        return Ok(None);
    }
    status_for(home, Some(id.clone())).map(Some)
}
fn validate_local_close(store: &Store, saved: &Saved) -> Result<()> {
    let close: CloseIntent = store.read("close.json")?.ok_or(BackupError::Invalid)?;
    if close.request_id != saved.request_id {
        return Err(BackupError::Identity);
    }
    Ok(())
}
fn status_for(home: &Path, selection: Option<Id>) -> Result<NativeBackupStatus> {
    let mut value = NativeBackupStatus {
        enabled: require_enabled().is_ok(),
        running: RUNNING.load(Ordering::Acquire),
        job: None,
        phase: None,
        native_objects_verified: 0,
        external_files_verified: 0,
        manifest_pages_verified: 0,
        retained_source_bytes: None,
        reserved_cloud_bytes: None,
        blocked_reason: None,
        last_accepted_backup: None,
    };
    let base = Store::base(home);
    value.last_accepted_backup = store::read::<AcceptedBackup>(&base.join("accepted.json"))?;
    if let Some(accepted) = &value.last_accepted_backup {
        accepted.validate()?;
    }
    let Some(id) = selection else {
        return Ok(value);
    };
    let root = base.join("jobs").join(id.as_str());
    let saved: Saved = store::read(&root.join("state.json"))?.ok_or(BackupError::Invalid)?;
    saved.validate()?;
    if saved.request_id != id {
        return Err(BackupError::Invalid);
    }
    value.job = saved.intent.as_ref().map(|intent| intent.job.clone());
    value.phase = Some(saved.phase);
    value.native_objects_verified = saved.native_objects_verified;
    value.external_files_verified = saved.external_files_verified;
    value.manifest_pages_verified = saved.manifest_pages_verified;
    value.retained_source_bytes = saved.retained_source_bytes;
    value.blocked_reason = saved.blocked_reason.clone();
    if let Some(result) = store::read::<JobResult>(&root.join("terminal.json"))? {
        saved.validate_terminal(&result)?;
        value.phase = Some(match result.status {
            JobStatus::Published { .. } => Phase::Published,
            _ => Phase::Closed,
        });
    } else if let Some(close) = store::read::<CloseIntent>(&root.join("close.json"))? {
        if close.request_id != id {
            return Err(BackupError::Invalid);
        }
        if !saved.locally_closed() {
            value.phase = Some(Phase::Closing);
        }
    }
    Ok(value)
}

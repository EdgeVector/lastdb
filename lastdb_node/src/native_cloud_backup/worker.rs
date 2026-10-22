use super::{
    identity::Source,
    model::{CloseIntent, Saved},
    store::{Store, CONTROL_RESERVE},
    *,
};
use cloud_backup_protocol::jobs::{CloseReason, JobResult, JobStatus, JobStatusRequest};
use cloud_backup_protocol::{ErrorCode, Version};
use fold_db::storage::disk_pack::backup_capture::NativeBackupCapture;
use fold_db::storage::disk_pack::backup_manifest::ManifestAssembly;
use fold_db::sync::auth::native_backup::{NativeBackupClient, NativeBackupError};

pub(super) struct Work<'a> {
    pub source: &'a Source,
    pub store: &'a Store,
    pub saved: &'a mut Saved,
    pub client: &'a NativeBackupClient,
    pub capture: &'a NativeBackupCapture,
}
impl Work<'_> {
    pub fn checkpoint(&mut self, phase: Phase) -> Result<()> {
        self.saved.phase = phase;
        self.saved.blocked_reason = None;
        self.saved.retained_source_bytes = self.capture.status()?.retained_source_bytes;
        self.store.save(self.saved)
    }
    pub fn check_close(&self) -> Result<()> {
        if let Some(intent) = self.store.read::<CloseIntent>("close.json")? {
            if intent.request_id != self.saved.request_id {
                return Err(BackupError::Identity);
            }
            return Err(BackupError::Closing);
        }
        Ok(())
    }
    pub fn budget(&self, assembly_bytes: u64) -> Result<()> {
        self.capture
            .set_coordinator_spool_bytes(self.source.coordinator_bytes(self.store)?)?;
        self.store.set_other_bytes(
            self.capture
                .metadata_spool_bytes()?
                .checked_add(assembly_bytes)
                .ok_or(BackupError::Limit)?,
        );
        self.store.admit(CONTROL_RESERVE)
    }
}

pub(super) async fn run(source: &Source, store: &Store, saved: &mut Saved) -> Result<()> {
    source.validate(saved)?;
    if let Some(result) = store.read::<JobResult>("terminal.json")? {
        return super::terminal::reconcile(source, store, saved, result).await;
    }
    if super::terminal::cancel_uncaptured(source, store, saved)? {
        return Ok(());
    }
    let client = if store.read::<CloseIntent>("close.json")?.is_some() {
        crate::native_backup_access::client_for_recovery(&source.home)
            .map_err(|_| BackupError::Identity)?
    } else {
        source.client()?
    };
    let _ = super::usage::refresh(store, &client, &source.account).await;
    super::setup::intent(source, store, saved, &client).await?;
    if super::terminal::cancel_uncaptured(source, store, saved)? {
        return Ok(());
    }
    let capture = super::setup::capture(source, store, saved).await?;
    let mut work = Work {
        source,
        store,
        saved,
        client: &client,
        capture: &capture,
    };
    if let Some(result) = store.read::<JobResult>("terminal.json")? {
        return super::terminal::finish(&mut work, &result);
    }
    match work.check_close() {
        Err(BackupError::Closing) => return super::terminal::close(&mut work).await,
        other => other?,
    }
    if !capture.status()?.source_available {
        store.write(
            "close.json",
            &CloseIntent {
                request_id: work.saved.request_id.clone(),
                reason: CloseReason::SourceUnavailable,
            },
        )?;
        return super::terminal::close(&mut work).await;
    }
    let result = active(&mut work).await;
    if matches!(result, Err(BackupError::Closing)) {
        if let Some(terminal) = store.read::<JobResult>("terminal.json")? {
            return super::terminal::finish(&mut work, &terminal);
        }
        return super::terminal::close(&mut work).await;
    }
    if result.is_err() {
        if let Some(terminal) = store.read::<JobResult>("terminal.json")? {
            return super::terminal::finish(&mut work, &terminal);
        }
    }
    result
}

async fn active(work: &mut Work<'_>) -> Result<()> {
    work.budget(0)?;
    work.check_close()?;
    let begin = work
        .saved
        .begin
        .as_ref()
        .ok_or(BackupError::Invalid)?
        .clone();
    let result = match work
        .client
        .job_status(&JobStatusRequest {
            version: Version,
            job: begin.job.clone(),
        })
        .await
    {
        Ok(result) => {
            result.validate_for_begin(&begin)?;
            Some(result)
        }
        Err(NativeBackupError::Protocol(error)) if error.code == ErrorCode::NotFound => None,
        Err(error) => return Err(error.into()),
    };
    if let Some(result) = result
        .as_ref()
        .filter(|result| !matches!(result.status, JobStatus::Active))
    {
        return super::terminal::finish(work, result);
    }
    work.checkpoint(Phase::Certifying)?;
    work.capture
        .certify_file_dependencies(&work.source.key)
        .await?;
    work.check_close()?;
    if result.is_none() {
        let result = work.client.begin(&begin).await?;
        if !matches!(result.status, JobStatus::Active) {
            return super::terminal::finish(work, &result);
        }
    }
    let mut assembly = ManifestAssembly::open(work.capture, &work.source.key)?;
    super::content::native(work, &mut assembly).await?;
    super::content::external(work, &mut assembly).await?;
    work.check_close()?;
    work.capture.audit_file_dependencies()?;
    work.checkpoint(Phase::AssemblingManifest)?;
    work.budget(assembly.spool_bytes())?;
    assembly.finish_content()?;
    super::publication::publish(work, &mut assembly).await
}

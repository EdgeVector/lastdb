use super::{transfer, worker::Work, BackupError, Phase, Result};
use cloud_backup_protocol::access::ReferencePutResult;
use cloud_backup_protocol::context::ContextReadRequest;
use cloud_backup_protocol::jobs::{JobResult, JobStatus};
use cloud_backup_protocol::publication::{CommitResponse, CommitStatusRequest};
use cloud_backup_protocol::{PendingReason, Version};
use fold_db::storage::disk_pack::backup_manifest::{FrozenUpload, ManifestAssembly};
use std::fs::OpenOptions;
use std::os::unix::fs::OpenOptionsExt;

pub(super) async fn publish(
    work: &mut Work<'_>,
    assembly: &mut ManifestAssembly<'_>,
) -> Result<()> {
    work.checkpoint(Phase::UploadingManifest)?;
    for index in 0..assembly.manifest_page_count() {
        work.check_close()?;
        work.budget(assembly.spool_bytes())?;
        let upload = assembly.manifest_upload(index)?;
        let view = upload_one(work, upload).await?;
        assembly.confirm_manifest_page(index, &view)?;
        work.saved.manifest_pages_verified = index as u64 + 1;
        work.store.save(work.saved)?;
    }
    work.checkpoint(Phase::PublishingReferences)?;
    work.budget(assembly.spool_bytes())?;
    assembly.freeze_references()?;
    for index in 0..assembly.reference_count() {
        work.check_close()?;
        if assembly.reference_confirmed(index)? {
            continue;
        }
        let request = assembly.reference_request(index)?;
        let result = work.client.put_reference(&request).await?;
        if let ReferencePutResult::Error { error } = &result {
            return Err(super::NativeBackupError::Protocol(error.clone()).into());
        }
        assembly.confirm_reference(index, &result)?;
    }
    work.check_close()?;
    work.budget(assembly.spool_bytes())?;
    let upload = assembly.freeze_manifest_index()?;
    let view = upload_one(work, upload).await?;
    assembly.confirm_manifest_index(&view)?;
    let commit = assembly.commit_input()?;
    if work
        .saved
        .commit
        .as_ref()
        .is_some_and(|prior| prior != &commit)
    {
        return Err(BackupError::Invalid);
    }
    work.saved.commit = Some(commit);
    work.checkpoint(Phase::Submitting)?;
    accept(work).await
}

async fn upload_one(
    work: &Work<'_>,
    upload: FrozenUpload,
) -> Result<cloud_backup_protocol::transfer::TransferView> {
    if let Some(receipt) = upload.receipt {
        return transfer::verified(&upload.input, receipt);
    }
    let result = transfer::batch(work, vec![upload.input.clone()], |_| {
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&upload.path)?;
        if !file.metadata()?.is_file() || file.metadata()?.len() != upload.input.object.bytes.get()
        {
            return Err(BackupError::Invalid);
        }
        Ok(Box::new(file))
    })
    .await?;
    result.into_iter().next().ok_or(BackupError::Invalid)
}

async fn accept(work: &mut Work<'_>) -> Result<()> {
    let begin = work
        .saved
        .begin
        .as_ref()
        .ok_or(BackupError::Invalid)?
        .clone();
    let commit = work
        .saved
        .commit
        .as_ref()
        .ok_or(BackupError::Invalid)?
        .clone();
    work.check_close()?;
    let mut result = work.client.commit(&begin, &commit).await?;
    let mut attempt = 0u32;
    loop {
        result.validate_for(&begin, &commit)?;
        match result {
            CommitResponse::Accepted { backup } => {
                return super::terminal::finish(
                    work,
                    &JobResult {
                        version: Version,
                        job: begin.job.clone(),
                        job_input_sha256: begin.input_sha256()?,
                        status: JobStatus::Published { backup },
                    },
                );
            }
            CommitResponse::Rejected { .. } => return Err(BackupError::Source),
            CommitResponse::Pending { reason, .. } => {
                work.checkpoint(Phase::AwaitingAcceptance)?;
                work.check_close()?;
                // A verifying status call advances one bounded saved service step.
                // Other pending outcomes use backoff; all retain the same commit.
                if reason == PendingReason::Verifying {
                    tokio::task::yield_now().await;
                } else {
                    transfer::pause(work, attempt).await?;
                }
                if attempt.is_multiple_of(16) {
                    let context = work
                        .client
                        .context(
                            &ContextReadRequest { version: Version },
                            &work.source.account,
                        )
                        .await?;
                    work.check_close()?;
                    if !context.new_work_enabled || !context.open_for_writes {
                        return Err(BackupError::Authority);
                    }
                }
                attempt = attempt.saturating_add(1);
            }
        }
        result = work
            .client
            .commit_status(&CommitStatusRequest {
                version: Version,
                job: begin.job.clone(),
            })
            .await?;
    }
}

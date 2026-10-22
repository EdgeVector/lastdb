use super::{BackupError, Result};
use cloud_backup_protocol::jobs::{CloseReason, JobBeginInput, JobResult, JobStatus};
use cloud_backup_protocol::publication::{AcceptedBackup, CommitInput};
use cloud_backup_protocol::{AccountId, BackupJob, CloudScope, DecimalU64, Id, RecoveryDescriptor};
use fold_db::storage::disk_pack::backup_capture::NativeBackupCaptureRequest;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum NativeBackupRequest {
    Start { request_id: Id },
    Resume,
    Close,
    Status,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    Preparing,
    Certifying,
    UploadingNative,
    CopyingFiles,
    AssemblingManifest,
    UploadingManifest,
    PublishingReferences,
    Submitting,
    AwaitingAcceptance,
    Closing,
    Published,
    Closed,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Saved {
    pub format: u8,
    pub request_id: Id,
    pub account: AccountId,
    pub source_home: Id,
    pub recovery: RecoveryDescriptor,
    pub intent: Option<NativeBackupCaptureRequest>,
    pub begin: Option<JobBeginInput>,
    pub commit: Option<CommitInput>,
    pub phase: Phase,
    pub native_objects_verified: u64,
    pub external_files_verified: u64,
    pub manifest_pages_verified: u64,
    pub retained_source_bytes: Option<u64>,
    pub blocked_reason: Option<String>,
}
impl Saved {
    pub fn locally_closed(&self) -> bool {
        self.phase == Phase::Closed
            && self.begin.is_none()
            && self.commit.is_none()
            && self.retained_source_bytes == Some(0)
    }
    pub fn validate(&self) -> Result<()> {
        if self.format != 1 {
            return Err(BackupError::Invalid);
        }
        self.recovery.validate_for(&CloudScope {
            account_id: self.account.clone(),
            account_generation: DecimalU64::new(0),
        })?;
        if let Some(intent) = &self.intent {
            intent.job.validate()?;
            if intent.job.commit_id != self.request_id
                || intent.job.stream.scope.account_id != self.account
                || intent.job.stream.source_home != self.source_home
                || intent.recovery != self.recovery
            {
                return Err(BackupError::Identity);
            }
        }
        if let Some(begin) = &self.begin {
            begin.validate()?;
            let intent = self.intent.as_ref().ok_or(BackupError::Invalid)?;
            if begin.job != intent.job
                || begin.recovery != intent.recovery
                || begin.predecessor != intent.predecessor
            {
                return Err(BackupError::Invalid);
            }
        }
        if let Some(commit) = &self.commit {
            commit.validate_for(self.begin.as_ref().ok_or(BackupError::Invalid)?)?;
        }
        Ok(())
    }
    pub fn validate_terminal(&self, result: &JobResult) -> Result<()> {
        result.validate_for_begin(self.begin.as_ref().ok_or(BackupError::Invalid)?)?;
        if let JobStatus::Published { backup } = &result.status {
            backup.validate_for(
                self.begin.as_ref().ok_or(BackupError::Invalid)?,
                self.commit.as_ref().ok_or(BackupError::Invalid)?,
            )?;
        }
        if matches!(result.status, JobStatus::Active) {
            return Err(BackupError::Invalid);
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CloseIntent {
    pub request_id: Id,
    pub reason: CloseReason,
}

#[derive(Serialize)]
pub struct NativeBackupStatus {
    pub enabled: bool,
    pub running: bool,
    pub job: Option<BackupJob>,
    pub phase: Option<Phase>,
    pub native_objects_verified: u64,
    pub external_files_verified: u64,
    pub manifest_pages_verified: u64,
    pub retained_source_bytes: Option<u64>,
    pub reserved_cloud_bytes: Option<u64>,
    pub blocked_reason: Option<String>,
    pub last_accepted_backup: Option<AcceptedBackup>,
}

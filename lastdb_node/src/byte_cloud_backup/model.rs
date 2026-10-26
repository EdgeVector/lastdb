use super::{BackupError, Result};
use cloud_backup_protocol::v2::{AcceptedBackup, Begin, Commit};
use cloud_backup_protocol::{AccountId, Id};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StreamRecord {
    pub source_home: Id,
    pub stream: cloud_backup_protocol::v2::Stream,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum ByteBackupRequest {
    Start { request_id: Id },
    Resume,
    Status,
    Close,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    Preparing,
    Capturing,
    Uploading,
    Inventory,
    Committing,
    Published,
    Closed,
    /// A disk format v1 job that a new start replaced. No program after
    /// decision D1 can resume or close it. This program sent nothing to the
    /// cloud for it and keeps the record for status only.
    AbandonedFormatV1,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Saved {
    pub format: u32,
    pub request_id: Id,
    pub account: AccountId,
    pub source_home: Id,
    pub endpoint: String,
    pub begin: Option<Begin>,
    pub begin_submitted: bool,
    pub commit: Option<Commit>,
    pub accepted: Option<AcceptedBackup>,
    pub phase: Phase,
    pub objects_verified: u64,
    pub metadata_pack_objects: u64,
    pub multi_file_pack_objects: u64,
    pub native_files: u64,
    pub native_bytes: u64,
    pub retained_source_bytes: Option<u64>,
    pub blocked_reason: Option<String>,
}

impl Saved {
    pub fn validate(&self) -> Result<()> {
        if self.format != 2 || self.account.as_str().len() != 32 || self.endpoint.len() > 4096 {
            return Err(BackupError::Invalid);
        }
        if (self.begin_submitted && self.begin.is_none())
            || (self.commit.is_some() && !self.begin_submitted)
        {
            return Err(BackupError::Invalid);
        }
        if self.multi_file_pack_objects > self.metadata_pack_objects {
            return Err(BackupError::Invalid);
        }
        if let Some(begin) = &self.begin {
            begin.validate()?;
            if begin.job.commit_id != self.request_id
                || begin.job.stream.scope.account_id != self.account
            {
                return Err(BackupError::Identity);
            }
        }
        if let Some(commit) = &self.commit {
            commit.validate_for(self.begin.as_ref().ok_or(BackupError::Invalid)?)?;
        }
        if let Some(accepted) = &self.accepted {
            accepted.validate_for(
                self.begin.as_ref().ok_or(BackupError::Invalid)?,
                self.commit.as_ref().ok_or(BackupError::Invalid)?,
            )?;
        }
        if self.phase == Phase::Published && self.accepted.is_none() {
            return Err(BackupError::Invalid);
        }
        if self.phase == Phase::AbandonedFormatV1 && !self.format_v1() {
            return Err(BackupError::Invalid);
        }
        Ok(())
    }
    pub fn terminal(&self) -> bool {
        self.phase == Phase::AbandonedFormatV1
            || (matches!(self.phase, Phase::Published | Phase::Closed)
                && self.retained_source_bytes == Some(0))
    }
    /// A disk format v1 home wrote a pack descriptor into the begin.
    pub fn format_v1(&self) -> bool {
        self.begin
            .as_ref()
            .is_some_and(|begin| !begin.descriptor.as_value().is_null())
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct ByteBackupStatus {
    pub enabled: bool,
    pub automatic_backup_enabled: bool,
    pub manual_connection: Option<super::ManualConnectionStatus>,
    pub running: bool,
    pub request_id: Option<Id>,
    pub phase: Option<Phase>,
    pub objects_verified: u64,
    pub metadata_pack_objects: u64,
    pub multi_file_pack_objects: u64,
    pub native_files: u64,
    pub native_bytes: u64,
    pub retained_source_bytes: Option<u64>,
    pub blocked_reason: Option<String>,
    pub last_accepted_backup: Option<AcceptedBackup>,
}

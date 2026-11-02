use super::{BackupError, Result};
pub use crate::backup_runtime::Phase;
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
    Start {
        request_id: Id,
    },
    Resume,
    Status,
    Close,
    EnableAutomatic {
        log_interval_seconds: u64,
        snapshot_interval_seconds: u64,
    },
    DisableAutomatic,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum LocalAction {
    ReplaceWithSnapshot,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Saved {
    pub format: u32,
    pub request_id: Id,
    #[serde(default, skip_serializing_if = "super::work::Work::is_default")]
    pub work: super::work::Work,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub automatic_revision: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_action: Option<LocalAction>,
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
        self.work.validate(self.begin.as_ref())?;
        if self.local_action.is_some()
            && (self.begin_submitted
                || self.commit.is_some()
                || self.accepted.is_some()
                || self.phase == Phase::Published
                || self.automatic_revision.is_none()
                || !matches!(self.work, super::work::Work::MutationLog { .. }))
        {
            return Err(BackupError::Invalid);
        }
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
        Ok(())
    }
    pub fn terminal(&self) -> bool {
        matches!(self.phase, Phase::Published | Phase::Closed)
            && self.retained_source_bytes == Some(0)
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct ByteBackupStatus {
    pub backup_kind: &'static str,
    pub enabled: bool,
    pub automatic_backup_enabled: bool,
    pub automatic_backup: Option<super::AutomaticBackupStatus>,
    pub mutation_log: fold_db::storage::disk_pack::mutation_log::MutationLogStatus,
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

/// The status model's durability view of one byte backup status.
impl From<ByteBackupStatus> for crate::self_metrics::DurabilityHealth {
    fn from(status: ByteBackupStatus) -> Self {
        let blocked = status.blocked_reason.is_some()
            || status
                .automatic_backup
                .as_ref()
                .is_some_and(|s| s.paused.is_some());
        let automatic = status.automatic_backup.as_ref();
        let journal = &status.mutation_log;
        Self {
            observed: true,
            automatic_enabled: status.automatic_backup_enabled,
            running: status.running,
            phase: status.phase,
            accepted_backup_recorded: status.last_accepted_backup.is_some(),
            degraded: blocked.then_some(true),
            reasons: Vec::new(),
            // The local head and the backed-up position come from this one status read,
            // so coverage is measured against them and stamped with this read's time.
            // An accepted receipt alone never becomes a coverage claim.
            checked_at: Some(fold_db::clock::unix_secs()),
            local_frontiers: journal.head.as_ref().map(|h| h.frontiers),
            backed_up_frontiers: automatic.and_then(|a| a.last_backed_up_frontiers),
            covered: None,
            snapshot_required: journal.snapshot_required,
            last_upload_at: automatic.and_then(|a| a.last_log_at),
            next_upload_at: automatic.map(|a| a.next_log_at),
            last_snapshot_at: automatic.and_then(|a| a.last_snapshot_at),
            next_snapshot_at: automatic.map(|a| a.next_snapshot_at),
        }
        .with_coverage()
    }
}

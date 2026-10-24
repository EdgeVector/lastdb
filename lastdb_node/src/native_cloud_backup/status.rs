//! Bounded local status; no source or credential admission.
use super::{
    model::{CloseIntent, Saved},
    runtime::BackupRuntime,
    store::{self, Store},
    *,
};
use cloud_backup_protocol::{
    jobs::{JobResult, JobStatus},
    publication::AcceptedBackup,
    Id,
};
use std::path::Path;
pub(super) fn status(home: &Path, runtime: &BackupRuntime) -> Result<NativeBackupStatus> {
    status_for(home, Store::active(home)?, runtime)
}
pub(super) fn status_for(
    home: &Path,
    selection: Option<Id>,
    runtime: &BackupRuntime,
) -> Result<NativeBackupStatus> {
    let mut value = NativeBackupStatus {
        enabled: require_enabled().is_ok(),
        running: runtime.snapshot().active.is_some(),
        job: None,
        phase: None,
        native_objects_verified: 0,
        external_files_verified: 0,
        manifest_pages_verified: 0,
        native_files: None,
        native_bytes: None,
        metadata_pack_objects_verified: None,
        multi_file_pack_objects_verified: None,
        retained_source_bytes: None,
        reserved_cloud_bytes: None,
        cloud_usage: None,
        cloud_usage_observed_at_ms: None,
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
    value.running &= runtime
        .snapshot()
        .active
        .as_ref()
        .is_some_and(|active| active.request_id == id);
    let root = base.join("jobs").join(id.as_str());
    let saved: Saved = store::read(&root.join("state.json"))?.ok_or(BackupError::Invalid)?;
    saved.validate()?;
    if saved.request_id != id {
        return Err(BackupError::Invalid);
    }
    if let Ok(Some(cached)) = store::read::<super::usage::CachedUsage>(&base.join("usage.json")) {
        if cached.validate(&saved.account).is_ok() {
            value.reserved_cloud_bytes = Some(cached.usage.reserved_bytes.get());
            value.cloud_usage_observed_at_ms = Some(cached.observed_at_ms);
            value.cloud_usage = Some(cached.usage);
        }
    }
    value.job = saved.intent.as_ref().map(|intent| intent.job.clone());
    value.phase = Some(saved.phase);
    value.native_objects_verified = saved.native_objects_verified;
    value.external_files_verified = saved.external_files_verified;
    value.manifest_pages_verified = saved.manifest_pages_verified;
    value.native_files = saved.native_files;
    value.native_bytes = saved.native_bytes;
    value.metadata_pack_objects_verified = saved.metadata_pack_objects_verified;
    value.multi_file_pack_objects_verified = saved.multi_file_pack_objects_verified;
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

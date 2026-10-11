//! Durable local job intent and exact replacement receipts.

use super::files;
use super::protocol::{CloudAccount, CloudPurgeControl, CloudPurgePhase, CloudReplacementManifest};
use super::RetainedSource;
use crate::sync::error::{SyncError, SyncResult};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CloudPurgeJobPhase {
    SourceReady,
    Accepted,
    Uploading,
    Publishing,
    Ready,
    Cancelled,
    Paused,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudPurgeJob {
    pub format_version: u32,
    pub operation_id: String,
    pub account: CloudAccount,
    pub source_id: String,
    pub source_manifest_sha256: String,
    pub expected_scope_version: u64,
    pub phase: CloudPurgeJobPhase,
    pub capability_locator: Option<String>,
    pub control: Option<CloudPurgeControl>,
    pub replacement: Option<CloudReplacementManifest>,
    pub uploaded_objects: BTreeSet<String>,
    pub last_error: Option<String>,
    pub created_at_unix_secs: u64,
    pub updated_at_unix_secs: u64,
    #[serde(skip)]
    pub directory: PathBuf,
}

impl CloudPurgeJob {
    pub fn create(
        directory: &Path,
        operation_id: &str,
        source: &RetainedSource,
        scope_version: u64,
    ) -> SyncResult<Self> {
        uuid::Uuid::parse_str(operation_id)
            .map_err(|_| SyncError::Storage("invalid operation ID".into()))?;
        if directory.join("job.json").exists() {
            return Err(SyncError::Storage(
                "purge job already exists; resume the saved source".into(),
            ));
        }
        source.verify()?;
        if source.directory != directory.join("source") {
            return Err(SyncError::Storage(
                "purge source lies outside its durable job directory".into(),
            ));
        }
        let now = crate::clock::unix_secs();
        let job = Self {
            format_version: 1,
            operation_id: operation_id.into(),
            account: source.account.clone(),
            source_id: source.prepared.source_id.clone(),
            source_manifest_sha256: source.prepared.manifest_sha256.clone(),
            expected_scope_version: scope_version,
            phase: CloudPurgeJobPhase::SourceReady,
            capability_locator: None,
            control: None,
            replacement: None,
            uploaded_objects: BTreeSet::new(),
            last_error: None,
            created_at_unix_secs: now,
            updated_at_unix_secs: now,
            directory: directory.to_path_buf(),
        };
        job.save()?;
        Ok(job)
    }

    pub fn reopen(directory: &Path) -> SyncResult<Self> {
        let path = directory.join("job.json");
        if std::fs::metadata(&path)?.len() > 64 * 1024 * 1024 {
            return Err(SyncError::Storage(
                "purge job receipt exceeds its size limit".into(),
            ));
        }
        let mut job: Self = serde_json::from_slice(&std::fs::read(path)?)
            .map_err(|_| SyncError::Storage("invalid purge job checkpoint".into()))?;
        job.directory = directory.to_path_buf();
        if job.format_version != 1 || uuid::Uuid::parse_str(&job.operation_id).is_err() {
            return Err(SyncError::Storage("invalid purge job identity".into()));
        }
        let source = job.source()?;
        if source.account != job.account
            || source.prepared.source_id != job.source_id
            || source.prepared.manifest_sha256 != job.source_manifest_sha256
        {
            return Err(SyncError::Storage(
                "purge job source identity changed".into(),
            ));
        }
        if let Some(control) = &job.control {
            job.require_control(control)?;
        }
        Ok(job)
    }

    pub fn source(&self) -> SyncResult<RetainedSource> {
        RetainedSource::reopen(&self.directory.join("source"))
    }

    pub fn save(&self) -> SyncResult<()> {
        files::atomic_json(&self.directory.join("job.json"), self)
    }

    pub fn require_control(&self, control: &CloudPurgeControl) -> SyncResult<()> {
        control.validate_identity().map_err(SyncError::Storage)?;
        if control.account != self.account
            || control.operation_id != self.operation_id
            || control.selected_writer.source_id != self.source_id
        {
            return Err(SyncError::Storage(
                "cloud control does not identify this retained source operation".into(),
            ));
        }
        if self.control.as_ref().is_some_and(|old| {
            old.generation != control.generation || old.revision > control.revision
        }) {
            return Err(SyncError::Storage(
                "cloud control generation or revision regressed".into(),
            ));
        }
        Ok(())
    }

    pub fn update_control(&mut self, control: CloudPurgeControl) -> SyncResult<()> {
        self.observe_control(control)?;
        self.save()
    }

    /// Refresh an in-memory status view without replacing the worker checkpoint.
    pub fn observe_control(&mut self, control: CloudPurgeControl) -> SyncResult<()> {
        self.require_control(&control)?;
        self.phase = match control.phase {
            CloudPurgePhase::Ready => CloudPurgeJobPhase::Ready,
            CloudPurgePhase::Cancelled => CloudPurgeJobPhase::Cancelled,
            CloudPurgePhase::Uploading => CloudPurgeJobPhase::Uploading,
            CloudPurgePhase::Publishing => CloudPurgeJobPhase::Publishing,
            CloudPurgePhase::Paused => CloudPurgeJobPhase::Paused,
            _ => CloudPurgeJobPhase::Accepted,
        };
        self.control = Some(control);
        self.last_error = None;
        self.updated_at_unix_secs = crate::clock::unix_secs();
        Ok(())
    }

    pub fn write_proof_receipt(&self, path: &Path) -> SyncResult<()> {
        self.source()?.verify()?;
        let control = self
            .control
            .as_ref()
            .ok_or_else(|| SyncError::Storage("cloud control absent".into()))?;
        self.require_control(control)?;
        let replacement = self
            .replacement
            .as_ref()
            .ok_or_else(|| SyncError::Storage("exact replacement manifest absent".into()))?;
        if control.phase != CloudPurgePhase::Ready || !control.deletion_is_complete() {
            return Err(SyncError::Storage(
                "purge proof requires deletion and exact replacement readiness".into(),
            ));
        }
        let expected_hash = replacement.sha256().map_err(SyncError::Storage)?;
        let published = control
            .replacement_manifest
            .as_ref()
            .ok_or_else(|| SyncError::Storage("published replacement absent".into()))?;
        let published_hash = published
            .get("sha256")
            .or_else(|| published.get("replacement_manifest_sha256"))
            .and_then(serde_json::Value::as_str);
        let exact = published_hash == Some(expected_hash.as_str())
            || serde_json::from_value::<CloudReplacementManifest>(published.clone())
                .ok()
                .is_some_and(|value| {
                    value.sha256().ok().as_deref() == Some(expected_hash.as_str())
                });
        if !exact {
            return Err(SyncError::Storage(
                "published replacement differs from the retained source".into(),
            ));
        }
        files::atomic_json(
            path,
            &serde_json::json!({
                "format_version":1,"operation_id":self.operation_id,"account":self.account,
                "generation":control.generation,"source_manifest_sha256":self.source_manifest_sha256,
                "replacement_manifest_sha256":expected_hash,"source_files_verified":true,
                "deletion_receipt":control.deletion_receipt,"control":control,
            }),
        )
    }
}

pub(super) fn object_identity(object: &super::protocol::CloudPurgeObject) -> String {
    format!(
        "{}/{}:{}:{}",
        object.logical_root, object.relative_key, object.sha256, object.bytes
    )
}

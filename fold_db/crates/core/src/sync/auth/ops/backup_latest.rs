use super::super::AuthClient;
use super::op_failed;
use crate::sync::error::{SyncError, SyncResult};
use base64::Engine;
use serde::{Deserialize, Serialize};

/// Format version a `backup/latest` pointer carries when it omits the field.
/// Every pointer written before the field existed is a v1 pointer, and every
/// writer in this build still omits it.
pub const BACKUP_LATEST_FORMAT_VERSION_V1: u32 = 1;

/// CAS conflict reason the storage service returns when a candidate would
/// move a scope's `backup/latest` from a newer `format_version` back to an
/// older one. Mirrors `backup_latest_transition_conflict` in the Lambda.
const BACKUP_LATEST_FORMAT_VERSION_DOWNGRADE: &str = "format_version_downgrade";
const BACKUP_LATEST_MISSING_ERROR: &str = "backup latest pointer missing";

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct BackupLatestPointer {
    pub store_uuid: String,
    pub epoch: u64,
    pub counter: u64,
    pub manifest_sha256: String,
    pub updated_at_unix_secs: u64,
    /// Backup object-format version the pointer's manifest chain uses.
    /// Absent on the wire means v1. Read-side only in this build: no writer
    /// emits it, so a pointer this client lands serializes exactly as before.
    /// An older server that does not know the field ignores it; an older
    /// client that does not know it ignores it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format_version: Option<u32>,
}

impl BackupLatestPointer {
    /// The pointer's format version, with the pre-field default applied.
    pub fn format_version(&self) -> u32 {
        self.format_version
            .unwrap_or(BACKUP_LATEST_FORMAT_VERSION_V1)
    }

    /// Refuse, by name, a pointer whose format this build's v1 readers do not
    /// understand. Call it before any manifest or chunk request so a v2 tip
    /// is a typed [`SyncError::UnsupportedBackupFormat`], not a decode
    /// failure deep in the chain walk.
    pub fn require_v1_format(&self) -> SyncResult<()> {
        let format_version = self.format_version();
        if format_version == BACKUP_LATEST_FORMAT_VERSION_V1 {
            Ok(())
        } else {
            Err(SyncError::UnsupportedBackupFormat { format_version })
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct BackupLatestCasResponse {
    pub key: String,
    pub latest: BackupLatestPointer,
    pub etag: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct BackupLatestGetResponse {
    pub key: String,
    pub latest: BackupLatestPointer,
    #[serde(default)]
    pub etag: String,
}

/// Map a CAS refusal to the typed error the caller can act on.
///
/// `format_version_downgrade` means cloud `latest` already names a format
/// this writer does not speak; surface it as
/// [`SyncError::UnsupportedBackupFormat`] carrying the cloud version. Every
/// other refusal keeps the legacy `backup_latest_cas: <reason>` storage text
/// that the publisher's `stale_counter` / `store_uuid_mismatch` matchers read.
fn backup_latest_cas_failure(value: &serde_json::Value) -> SyncError {
    let reason = value.get("reason").and_then(serde_json::Value::as_str);
    if reason == Some(BACKUP_LATEST_FORMAT_VERSION_DOWNGRADE) {
        let format_version = value
            .get("current")
            .and_then(|current| current.get("format_version"))
            .and_then(serde_json::Value::as_u64)
            .and_then(|version| u32::try_from(version).ok())
            .unwrap_or(BACKUP_LATEST_FORMAT_VERSION_V1);
        return SyncError::UnsupportedBackupFormat { format_version };
    }
    let detail = reason
        .or_else(|| value.get("error").and_then(serde_json::Value::as_str))
        .map(str::to_string)
        .or_else(|| Some("backup_latest_cas failed".to_string()));
    op_failed("backup_latest_cas", detail)
}

/// Treat only the storage service's exact absent-pointer response as empty.
fn backup_latest_get_optional_response(
    value: serde_json::Value,
) -> SyncResult<Option<BackupLatestGetResponse>> {
    if value == serde_json::json!({"ok": false, "error": BACKUP_LATEST_MISSING_ERROR}) {
        return Ok(None);
    }
    if value.get("ok").and_then(serde_json::Value::as_bool) != Some(true) {
        let detail = value
            .get("reason")
            .or_else(|| value.get("error"))
            .and_then(serde_json::Value::as_str)
            .map(str::to_string)
            .or_else(|| Some("backup_latest_get failed".to_string()));
        return Err(op_failed("backup_latest_get", detail));
    }
    serde_json::from_value(value).map(Some).map_err(|e| {
        SyncError::Serialization(format!("backup_latest_get response decode failed: {e}"))
    })
}

/// Exact metadata and encrypted bytes for one normal recovery descriptor.
pub struct BackupRecoveryDescriptorPut<'a> {
    pub db_hash: &'a str,
    pub store_uuid: &'a str,
    pub epoch: u64,
    pub counter: u64,
    pub manifest_sha256: &'a str,
    pub descriptor_name: &'a str,
    pub descriptor_sha256: &'a str,
    pub ciphertext: &'a [u8],
}

impl AuthClient {
    pub async fn require_backup_expected_absent_capability(&self) -> SyncResult<()> {
        let value = self
            .post(
                "/api/sync/presign",
                serde_json::json!({"action": "backup_latest_cas_capabilities"}),
            )
            .await?;
        if value.get("ok").and_then(serde_json::Value::as_bool) == Some(true)
            && value
                .get("backup_expected_absent")
                .and_then(serde_json::Value::as_bool)
                == Some(true)
        {
            Ok(())
        } else {
            Err(SyncError::Storage(
                "cloud service lacks fresh backup CAS support".into(),
            ))
        }
    }

    /// Store an encrypted normal-cut recovery descriptor at the account root.
    /// The service checks its name and hash and writes it only once.
    pub async fn backup_recovery_descriptor_put(
        &self,
        request: BackupRecoveryDescriptorPut<'_>,
    ) -> SyncResult<()> {
        let value = self
            .post_no_default_db_hash(
                "/api/sync/presign",
                serde_json::json!({
                    "action": "backup_recovery_descriptor_put",
                    "db_hash": request.db_hash,
                    "backup_store_uuid": request.store_uuid,
                    "backup_epoch": request.epoch,
                    "backup_counter": request.counter,
                    "manifest_sha256": request.manifest_sha256,
                    "descriptor_name": request.descriptor_name,
                    "descriptor_sha256": request.descriptor_sha256,
                    "descriptor_base64": base64::engine::general_purpose::STANDARD.encode(request.ciphertext),
                }),
            )
            .await?;
        if value.get("ok").and_then(serde_json::Value::as_bool) != Some(true) {
            let detail = value
                .get("reason")
                .or_else(|| value.get("error"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_string);
            return Err(op_failed("backup_recovery_descriptor_put", detail));
        }
        let expected_key = format!("snapshots/{}", request.descriptor_name);
        if value.get("key").and_then(serde_json::Value::as_str) != Some(expected_key.as_str()) {
            return Err(SyncError::Storage(
                "backup recovery descriptor put returned the wrong key".into(),
            ));
        }
        Ok(())
    }

    pub async fn backup_latest_get(&self) -> SyncResult<BackupLatestGetResponse> {
        self.backup_latest_get_optional().await?.ok_or_else(|| {
            op_failed(
                "backup_latest_get",
                Some(BACKUP_LATEST_MISSING_ERROR.into()),
            )
        })
    }

    /// Return `None` only when the service reports that `backup/latest` is absent.
    pub async fn backup_latest_get_optional(&self) -> SyncResult<Option<BackupLatestGetResponse>> {
        let value = self
            .post(
                "/api/sync/presign",
                serde_json::json!({
                    "action": "backup_latest_get",
                }),
            )
            .await?;
        backup_latest_get_optional_response(value)
    }

    pub async fn backup_latest_cas(
        &self,
        store_uuid: &str,
        epoch: u64,
        counter: u64,
        manifest_sha256: &str,
    ) -> SyncResult<BackupLatestCasResponse> {
        self.backup_latest_cas_with_condition(store_uuid, epoch, counter, manifest_sha256, false)
            .await
    }

    pub async fn backup_latest_cas_if_absent(
        &self,
        store_uuid: &str,
        epoch: u64,
        counter: u64,
        manifest_sha256: &str,
    ) -> SyncResult<BackupLatestCasResponse> {
        self.backup_latest_cas_with_condition(store_uuid, epoch, counter, manifest_sha256, true)
            .await
    }

    async fn backup_latest_cas_with_condition(
        &self,
        store_uuid: &str,
        epoch: u64,
        counter: u64,
        manifest_sha256: &str,
        backup_expected_absent: bool,
    ) -> SyncResult<BackupLatestCasResponse> {
        let value = self
            .post(
                "/api/sync/presign",
                serde_json::json!({
                    "action": "backup_latest_cas",
                    "backup_store_uuid": store_uuid,
                    "backup_epoch": epoch,
                    "backup_counter": counter,
                    "manifest_sha256": manifest_sha256,
                    "backup_expected_absent": backup_expected_absent,
                }),
            )
            .await?;
        if !value
            .get("ok")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
        {
            return Err(backup_latest_cas_failure(&value));
        }
        serde_json::from_value(value).map_err(|e| {
            SyncError::Serialization(format!("backup_latest_cas response decode failed: {e}"))
        })
    }
}

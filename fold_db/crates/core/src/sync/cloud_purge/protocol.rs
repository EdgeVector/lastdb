//! Versioned cloud control wire contract. Private writer capabilities stay out of receipts.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CloudAccountKind {
    Personal,
    Org,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct CloudAccount {
    pub kind: CloudAccountKind,
    pub id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CloudPurgePhase {
    Held,
    Draining,
    Deleting,
    Uploading,
    Publishing,
    Ready,
    Paused,
    Cancelled,
}

impl CloudPurgePhase {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Ready | Self::Cancelled)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CloudSelectedWriter {
    pub principal_hash: String,
    pub source_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CloudPurgeControl {
    pub format_version: u32,
    pub account: CloudAccount,
    pub operation_id: String,
    pub generation: u64,
    pub revision: u64,
    pub phase: CloudPurgePhase,
    pub selected_writer: CloudSelectedWriter,
    pub scope_manifest: Value,
    #[serde(default)]
    pub replacement_manifest: Option<Value>,
    #[serde(default)]
    pub replacement_manifest_ref: Option<super::inventory::CloudInventoryRef>,
    #[serde(default)]
    pub deletion_receipt: Option<Value>,
    #[serde(default)]
    pub last_error: Option<Value>,
    #[serde(default)]
    pub data_generation: Option<u64>,
    #[serde(default)]
    pub continuation: Option<Value>,
    #[serde(default)]
    pub continuation_sha256: Option<String>,
    #[serde(default)]
    pub previous_ready_control: Option<Value>,
    #[serde(default)]
    pub last_ready_generation: Option<u64>,
}

impl CloudPurgeControl {
    pub fn validate_identity(&self) -> Result<(), String> {
        if self.format_version != 1 || self.generation == 0 || self.account.id.is_empty() {
            return Err("invalid cloud purge control identity".into());
        }
        uuid::Uuid::parse_str(&self.operation_id)
            .map_err(|_| "invalid cloud purge operation identity".to_string())?;
        Ok(())
    }

    pub fn deletion_is_complete(&self) -> bool {
        self.deletion_receipt
            .as_ref()
            .and_then(|receipt| receipt.get("complete"))
            .and_then(Value::as_bool)
            == Some(true)
    }

    /// Cancellation changes protocol admission while preserving the exact
    /// prior physical root. Every other cancelled state remains held.
    pub fn unchanged_continuation(&self) -> Option<&Value> {
        if self.phase != CloudPurgePhase::Cancelled || self.deletion_is_complete() {
            return None;
        }
        let continuation = self.continuation.as_ref()?;
        let previous = continuation.get("previous_generation")?.as_u64()?;
        let data = continuation.get("data_generation")?.as_u64()?;
        let required = continuation.get("required_adopted_generation")?.as_u64()?;
        if continuation.get("format_version")?.as_u64()? != 1
            || continuation.get("kind")?.as_str()? != "unchanged_cloud"
            || !continuation.get("cancelled_before_delete")?.as_bool()?
            || previous.checked_add(1)? != self.generation
            || data > previous
            || required != data
            || self.data_generation != Some(data)
            || self.last_ready_generation != Some(required)
            || self.continuation_sha256.as_deref()
                != Some(crate::hex::sha256_hex(serde_json::to_vec(continuation).ok()?).as_str())
        {
            return None;
        }
        if required > 0 {
            let previous: Self =
                serde_json::from_value(self.previous_ready_control.clone()?).ok()?;
            if previous.account != self.account
                || previous.generation != required
                || previous.phase != CloudPurgePhase::Ready
                || !previous.deletion_is_complete()
            {
                return None;
            }
        }
        Some(continuation)
    }

    pub fn permits_local_adoption(&self) -> bool {
        (self.phase == CloudPurgePhase::Ready && self.deletion_is_complete())
            || self.unchanged_continuation().is_some()
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CloudControlGet {
    #[serde(default)]
    pub account: Option<CloudAccount>,
    #[serde(default)]
    pub protocol_required: bool,
    #[serde(default)]
    pub generation: u64,
    #[serde(default)]
    pub control: Option<CloudPurgeControl>,
    #[serde(default)]
    pub notification_only: bool,
    #[serde(default)]
    pub notification: Option<CloudGenerationNotification>,
    #[serde(default)]
    pub notification_scope: Option<String>,
    #[serde(default)]
    pub scope_root: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudGenerationNotification {
    pub account: CloudAccount,
    pub generation: u64,
    pub operation_id: Option<String>,
    pub phase: String,
    pub open_for_writes: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CloudPurgeObject {
    pub logical_root: String,
    pub relative_key: String,
    pub sha256: String,
    pub bytes: u64,
    pub role: String,
}

impl CloudPurgeObject {
    pub fn validate(&self) -> Result<(), String> {
        if !crate::hex::is_lower_hex_sha256(&self.sha256)
            || self.logical_root.is_empty()
            || self.logical_root.contains('/')
            || self.logical_root == "."
            || self.logical_root == ".."
            || self.relative_key.is_empty()
            || self.relative_key.starts_with('/')
            || self
                .relative_key
                .split('/')
                .any(|part| part.is_empty() || part == "." || part == "..")
            || self.relative_key.chars().any(char::is_control)
            || self.relative_key.contains('\\')
        {
            return Err("invalid retained cloud object identity".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreparedCloudSource {
    #[serde(default)]
    pub roots: Vec<CloudReplacementRoot>,
    pub source_id: String,
    pub manifest_sha256: String,
    pub layout_sha256: String,
    pub writer_frontier: BTreeMap<String, u64>,
    pub objects: Vec<CloudPurgeObject>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudReplacementRoot {
    #[serde(default = "default_root_format")]
    pub format: String,
    #[serde(default)]
    pub store_uuid: String,
    #[serde(default)]
    pub source_id: String,
    #[serde(default)]
    pub writer_frontier: BTreeMap<String, u64>,
    pub logical_root: String,
    pub manifest_sha256: String,
    pub manifest_key: String,
    pub layout_sha256: String,
    pub layout_key: String,
    pub objects: Vec<CloudPurgeObject>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudReplacementManifest {
    pub format_version: u32,
    pub account: CloudAccount,
    pub operation_id: String,
    pub generation: u64,
    pub source_manifest_sha256: String,
    pub roots: Vec<CloudReplacementRoot>,
    pub removed_roots: Vec<String>,
}

impl CloudReplacementManifest {
    pub fn sha256(&self) -> Result<String, String> {
        let value = serde_json::to_value(self).map_err(|error| error.to_string())?;
        let bytes = serde_json::to_vec(&value).map_err(|error| error.to_string())?;
        Ok(crate::hex::sha256_hex(&bytes))
    }
}

#[derive(Clone, Deserialize)]
pub struct CloudPurgeUpload {
    #[serde(flatten)]
    pub object: CloudPurgeObject,
    #[serde(default)]
    pub url: String,
    pub method: String,
    pub upload_id: String,
    #[serde(default)]
    pub part_number: u32,
    pub completion_ticket: String,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    #[serde(default)]
    pub parts: Vec<CloudPurgeUploadPart>,
    #[serde(default)]
    pub completed_parts: Vec<CloudPurgePartReceipt>,
}

#[derive(Clone, Deserialize)]
pub struct CloudPurgeUploadPart {
    pub part_number: u32,
    pub url: String,
    pub offset: u64,
    pub bytes: u64,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct CloudPurgePartReceipt {
    pub part_number: u32,
    pub etag: String,
}

#[derive(Serialize)]
pub struct CloudPurgeUploadReceipt {
    #[serde(flatten)]
    pub object: CloudPurgeObject,
    pub upload_id: String,
    pub etag: String,
    pub completion_ticket: String,
    pub parts: Vec<CloudPurgePartReceipt>,
}

#[derive(Deserialize)]
pub struct CloudPurgeUploadBatch {
    #[serde(default)]
    pub uploads: Vec<CloudPurgeUpload>,
    #[serde(default)]
    pub already_present: Vec<CloudPurgeObject>,
    #[serde(default)]
    pub verified: Vec<CloudPurgeObject>,
    #[serde(default)]
    pub verification_pending: Vec<CloudPurgeObject>,
}

#[derive(Deserialize)]
pub struct CloudPurgeBegin {
    pub control: CloudPurgeControl,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CloudPurgePreparation {
    pub account: CloudAccount,
    pub scope_version: u64,
    #[serde(default)]
    pub scope_manifest: Value,
    #[serde(default)]
    pub capabilities: Value,
    #[serde(default)]
    pub control: Option<Value>,
}

#[derive(Deserialize)]
pub struct CloudPurgeDownload {
    #[serde(flatten)]
    pub object: CloudPurgeObject,
    pub url: String,
    pub method: String,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
}

#[derive(Deserialize)]
pub struct CloudPurgeDownloadBatch {
    #[serde(default, alias = "download")]
    pub downloads: Vec<CloudPurgeDownload>,
}

fn default_root_format() -> String {
    "laststore_v1".into()
}

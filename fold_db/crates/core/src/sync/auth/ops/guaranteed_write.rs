//! Cloud CAS for a schema field that declares a guaranteed write.
//!
//! The cloud owns one version pointer per `(field_id, slot)`.  The payload is
//! opaque to the service: the caller supplies a sealed mutation envelope and
//! applies it locally only after this operation succeeds.  A matching retry is
//! successful, so a timeout after the cloud commit cannot create two grants.

use super::super::helpers::attach_target_scope;
use super::super::AuthClient;
use super::op_failed;
use crate::sync::error::{SyncError, SyncResult};
use crate::sync::org_sync::SyncTarget;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct GuaranteedWritePointer {
    pub model_version: u32,
    pub field_id: String,
    pub slot: String,
    /// Content-addressed file blobs that the mutation needs on the target.
    /// The storage service checks these before it advances the pointer.
    #[serde(default)]
    pub blob_ids: Vec<String>,
    /// Opaque, caller-minted mutation identity. It also makes a retry safe.
    pub version: String,
    /// A sealed mutation envelope. The storage service never interprets it.
    pub payload: String,
    pub updated_at_unix_secs: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct GuaranteedWriteGetResponse {
    pub key: String,
    pub latest: Option<GuaranteedWritePointer>,
    #[serde(default)]
    pub etag: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct GuaranteedWriteCasResponse {
    pub key: String,
    pub latest: GuaranteedWritePointer,
    #[serde(default)]
    pub etag: String,
}

/// One slot that an atomic guaranteed-write grant changes.
///
/// `expected_version = None` requires the slot to be absent. All members are
/// checked before the cloud head moves, so a stale member grants no slot.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct GuaranteedWriteSetSlot {
    pub field_id: String,
    pub slot: String,
    pub expected_version: Option<String>,
}

/// The complete mutation set that the cloud head records for crash recovery.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct GuaranteedWriteSetGrant {
    pub model_version: u32,
    pub version: String,
    pub payload: String,
    pub slots: Vec<GuaranteedWriteSetMember>,
    pub updated_at_unix_secs: u64,
}

/// A named member of a recorded grant. Expected versions are not persisted.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct GuaranteedWriteSetMember {
    pub field_id: String,
    pub slot: String,
}

/// The scoped cloud head. It holds current slot versions and the full most
/// recent grant, so a restart cannot infer a partial set from separate keys.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct GuaranteedWriteSetHead {
    pub model_version: u32,
    pub slots: Vec<GuaranteedWriteSetSlotState>,
    pub last_grant: GuaranteedWriteSetGrant,
    pub updated_at_unix_secs: u64,
}

/// Current version metadata for a slot in an atomic grant head.
///
/// The head keeps a SHA-256 binding to the sealed payload. It does not repeat
/// the payload per slot; `last_grant` carries the complete durable set.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct GuaranteedWriteSetSlotState {
    pub model_version: u32,
    pub field_id: String,
    pub slot: String,
    pub version: String,
    pub payload_sha256: String,
    pub updated_at_unix_secs: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct GuaranteedWriteSetGetResponse {
    pub key: String,
    pub latest: Option<GuaranteedWriteSetHead>,
    #[serde(default)]
    pub etag: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct GuaranteedWriteSetCasResponse {
    pub key: String,
    pub latest: GuaranteedWriteSetHead,
    #[serde(default)]
    pub etag: String,
}

impl AuthClient {
    /// Read the cloud version for one guaranteed field slot.
    pub async fn guaranteed_write_get_for_target(
        &self,
        target: &SyncTarget,
        field_id: &str,
        slot: &str,
    ) -> SyncResult<GuaranteedWriteGetResponse> {
        let mut body = serde_json::json!({
            "action": "guaranteed_write_get",
            "guaranteed_field_id": field_id,
            "guaranteed_slot": slot,
        });
        attach_target_scope(&mut body, target);
        let value = self.post("/api/sync/presign", body).await?;
        if !value
            .get("ok")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
        {
            return Err(op_failed(
                "guaranteed_write_get",
                value
                    .get("reason")
                    .or_else(|| value.get("error"))
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string),
            ));
        }
        serde_json::from_value(value).map_err(|error| {
            SyncError::Serialization(format!(
                "guaranteed_write_get response decode failed: {error}"
            ))
        })
    }
}

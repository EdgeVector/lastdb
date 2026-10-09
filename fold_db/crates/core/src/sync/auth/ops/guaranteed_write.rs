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
use std::collections::BTreeSet;

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

/// Accept only an acknowledgement that names every requested slot.
///
/// A successful HTTP response alone cannot prove that a multi-slot grant was
/// atomic. The durable `last_grant` must name exactly the requested members,
/// and the current head must carry the requested version for each member.
/// Never include a field or slot value in this error: both are user data.
fn validate_guaranteed_write_set_acknowledgement(
    response: GuaranteedWriteSetCasResponse,
    requested: &[GuaranteedWriteSetSlot],
    version: &str,
    payload: &str,
) -> SyncResult<GuaranteedWriteSetCasResponse> {
    let requested_slots: BTreeSet<(&str, &str)> = requested
        .iter()
        .map(|slot| (slot.field_id.as_str(), slot.slot.as_str()))
        .collect();
    let granted_slots: BTreeSet<(&str, &str)> = response
        .latest
        .last_grant
        .slots
        .iter()
        .map(|slot| (slot.field_id.as_str(), slot.slot.as_str()))
        .collect();
    let grant_is_complete = response.latest.last_grant.version == version
        && response.latest.last_grant.payload == payload
        && response.latest.last_grant.slots.len() == requested_slots.len()
        && granted_slots == requested_slots;
    let states_are_complete = requested_slots.iter().all(|(field_id, slot)| {
        response.latest.slots.iter().any(|state| {
            state.field_id == *field_id && state.slot == *slot && state.version == version
        })
    });

    if grant_is_complete && states_are_complete {
        Ok(response)
    } else {
        Err(SyncError::Storage(
            "guaranteed_write_set_cas: response did not acknowledge the complete requested set"
                .to_string(),
        ))
    }
}

impl AuthClient {
    /// Read the scoped head for atomic guaranteed-write grants.
    pub async fn guaranteed_write_set_get_for_target(
        &self,
        target: &SyncTarget,
    ) -> SyncResult<GuaranteedWriteSetGetResponse> {
        let mut body = serde_json::json!({
            "action": "guaranteed_write_set_get",
        });
        attach_target_scope(&mut body, target);
        let value = self.post("/api/sync/presign", body).await?;
        if !value
            .get("ok")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
        {
            return Err(op_failed(
                "guaranteed_write_set_get",
                value
                    .get("reason")
                    .or_else(|| value.get("error"))
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string),
            ));
        }
        serde_json::from_value(value).map_err(|error| {
            SyncError::Serialization(format!(
                "guaranteed_write_set_get response decode failed: {error}"
            ))
        })
    }

    /// Atomically grant every named slot, or grant none of them.
    ///
    /// The cloud stores the complete winner set in one scoped head. A matching
    /// retry succeeds after a lost response; callers can then apply the sealed
    /// mutation locally exactly once.
    pub async fn guaranteed_write_set_cas_for_target(
        &self,
        target: &SyncTarget,
        model_version: u32,
        version: &str,
        payload: &str,
        slots: &[GuaranteedWriteSetSlot],
    ) -> SyncResult<GuaranteedWriteSetCasResponse> {
        let mut body = serde_json::json!({
            "action": "guaranteed_write_set_cas",
            "model_version": model_version,
            "guaranteed_version": version,
            "guaranteed_payload": payload,
            "guaranteed_write_set": slots,
        });
        attach_target_scope(&mut body, target);
        let value = self.post("/api/sync/presign", body).await?;
        if !value
            .get("ok")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
        {
            return Err(op_failed(
                "guaranteed_write_set_cas",
                value
                    .get("reason")
                    .or_else(|| value.get("error"))
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string),
            ));
        }
        let response = serde_json::from_value(value).map_err(|error| {
            SyncError::Serialization(format!(
                "guaranteed_write_set_cas response decode failed: {error}"
            ))
        })?;
        validate_guaranteed_write_set_acknowledgement(response, slots, version, payload)
    }

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

    /// Compare the cloud version and, on success, install `candidate`.
    ///
    /// `expected_version = None` means create-only. Passing the version from
    /// [`Self::guaranteed_write_get_for_target`] gives update semantics.
    pub async fn guaranteed_write_cas_for_target(
        &self,
        target: &SyncTarget,
        expected_version: Option<&str>,
        candidate: &GuaranteedWritePointer,
    ) -> SyncResult<GuaranteedWriteCasResponse> {
        let mut body = serde_json::json!({
            "action": "guaranteed_write_cas",
            "model_version": candidate.model_version,
            "guaranteed_field_id": candidate.field_id,
            "guaranteed_slot": candidate.slot,
            "guaranteed_version": candidate.version,
            "guaranteed_payload": candidate.payload,
            "guaranteed_blob_ids": candidate.blob_ids,
        });
        if let Some(expected_version) = expected_version {
            body["guaranteed_expected_version"] =
                serde_json::Value::String(expected_version.to_string());
        }
        attach_target_scope(&mut body, target);
        let value = self.post("/api/sync/presign", body).await?;
        if !value
            .get("ok")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
        {
            return Err(op_failed(
                "guaranteed_write_cas",
                value
                    .get("reason")
                    .or_else(|| value.get("error"))
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string),
            ));
        }
        serde_json::from_value(value).map_err(|error| {
            SyncError::Serialization(format!(
                "guaranteed_write_cas response decode failed: {error}"
            ))
        })
    }
}

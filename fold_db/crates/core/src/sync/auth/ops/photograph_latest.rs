use super::super::helpers::attach_target_scope;
use super::super::AuthClient;
use super::op_failed;
use crate::sync::error::{SyncError, SyncResult};
use crate::sync::org_sync::SyncTarget;
use crate::sync::snapshot_log::{Frontier, LatestCasPayload};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct PhotographLatestPointer {
    pub model_version: u32,
    pub snapshot_id: String,
    pub frontier: Frontier,
    pub counter: u64,
    pub updated_at_unix_secs: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct PhotographLatestGetResponse {
    pub key: String,
    pub latest: Option<PhotographLatestPointer>,
    #[serde(default)]
    pub etag: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct PhotographLatestCasResponse {
    pub key: String,
    pub latest: PhotographLatestPointer,
    pub etag: String,
}

impl AuthClient {
    pub async fn photograph_latest_get_for_target(
        &self,
        target: &SyncTarget,
    ) -> SyncResult<PhotographLatestGetResponse> {
        let mut body = serde_json::json!({"action": "photograph_latest_get"});
        attach_target_scope(&mut body, target);
        let value = self.post("/api/sync/presign", body).await?;
        if !value
            .get("ok")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
        {
            let detail = value
                .get("reason")
                .or_else(|| value.get("error"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
                .or_else(|| Some("photograph_latest_get failed".to_string()));
            return Err(op_failed("photograph_latest_get", detail));
        }
        serde_json::from_value(value).map_err(|e| {
            SyncError::Serialization(format!("photograph_latest_get response decode failed: {e}"))
        })
    }

    pub async fn photograph_latest_cas_for_target(
        &self,
        target: &SyncTarget,
        payload: &LatestCasPayload,
    ) -> SyncResult<PhotographLatestCasResponse> {
        let mut body = serde_json::json!({
            "action": "photograph_latest_cas",
            "model_version": payload.model_version,
            "snapshot_id": payload.snapshot_id,
            "frontier": payload.frontier,
            "latest_counter": payload.counter,
        });
        attach_target_scope(&mut body, target);
        let value = self.post("/api/sync/presign", body).await?;
        if !value
            .get("ok")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
        {
            let detail = value
                .get("reason")
                .or_else(|| value.get("error"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
                .or_else(|| Some("photograph_latest_cas failed".to_string()));
            return Err(op_failed("photograph_latest_cas", detail));
        }
        serde_json::from_value(value).map_err(|e| {
            SyncError::Serialization(format!("photograph_latest_cas response decode failed: {e}"))
        })
    }
}

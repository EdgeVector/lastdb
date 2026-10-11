//! Account purge control calls deliberately bypass automatic database claims.

use super::super::AuthClient;
use crate::sync::cloud_purge::protocol::*;
use crate::sync::error::{SyncError, SyncResult};
use serde::de::DeserializeOwned;
use serde_json::{json, Value};

impl AuthClient {
    pub(super) async fn purge_call<T: DeserializeOwned>(
        &self,
        mut request: Value,
        action: &str,
    ) -> SyncResult<T> {
        request["action"] = Value::String(action.into());
        let response = self
            .post_no_default_db_hash("/api/sync/presign", request)
            .await?;
        if response.get("ok").and_then(Value::as_bool) != Some(true) {
            let reason = response
                .get("reason")
                .and_then(Value::as_str)
                .or_else(|| response.pointer("/error/code").and_then(Value::as_str))
                .unwrap_or("cloud_purge_request_failed");
            let safe = reason
                .chars()
                .filter(|c| c.is_ascii_alphanumeric() || *c == '_')
                .take(100)
                .collect::<String>();
            return Err(SyncError::Storage(format!("{action}: {safe}")));
        }
        serde_json::from_value(response)
            .map_err(|_| SyncError::Storage(format!("{action}: invalid response")))
    }

    pub async fn cloud_control_get(&self, account: &CloudAccount) -> SyncResult<CloudControlGet> {
        self.purge_call(json!({"account":account}), "cloud_control_get")
            .await
    }

    pub async fn cloud_control_adopt(
        &self,
        account: &CloudAccount,
        generation: u64,
        replacement_manifest_sha256: &str,
        source_id: &str,
    ) -> SyncResult<String> {
        let response: Value = self.purge_call(json!({
            "account":account, "cloud_generation":generation,
            "replacement_manifest_sha256":replacement_manifest_sha256, "source_id":source_id,
        }), "cloud_control_adopt").await?;
        response
            .get("adoption_token")
            .and_then(Value::as_str)
            .filter(|token| !token.is_empty())
            .map(str::to_string)
            .ok_or_else(|| SyncError::Storage("cloud adoption token absent".into()))
    }

    pub async fn cloud_purge_prepare(
        &self,
        account: &CloudAccount,
    ) -> SyncResult<CloudPurgePreparation> {
        self.purge_call(json!({"account":account}), "cloud_purge_prepare")
            .await
    }

    pub async fn cloud_control_download(
        &self,
        account: &CloudAccount,
        generation: u64,
        operation_id: &str,
        objects: &[CloudPurgeObject],
    ) -> SyncResult<CloudPurgeDownloadBatch> {
        self.purge_call(
            json!({"account":account,"cloud_generation":generation,
            "cloud_operation_id":operation_id,"objects":objects}),
            "cloud_control_download",
        )
        .await
    }

    pub async fn cloud_purge_begin(
        &self,
        account: &CloudAccount,
        operation_id: &str,
        expected_scope_version: u64,
        source: &PreparedCloudSource,
        capability: &str,
    ) -> SyncResult<CloudPurgeBegin> {
        let source_ref = self
            .stage_cloud_inventory(account, operation_id, capability, "source", source)
            .await?;
        self.purge_call(json!({"account":account,"operation_id":operation_id,"cloud_operation_id":operation_id,
            "expected_scope_version":expected_scope_version,"prepared_source_ref":source_ref,"cloud_capability":capability}), "cloud_purge_begin").await
    }

    pub async fn cloud_purge_status(
        &self,
        account: &CloudAccount,
        operation_id: &str,
    ) -> SyncResult<CloudPurgeControl> {
        let response: Value = self
            .purge_call(
                json!({"account":account,"cloud_operation_id":operation_id}),
                "cloud_purge_status",
            )
            .await?;
        decode_control(response)
    }

    pub async fn cloud_purge_resume(
        &self,
        account: &CloudAccount,
        operation_id: &str,
        generation: u64,
        capability: &str,
    ) -> SyncResult<CloudPurgeControl> {
        let response: Value = self
            .purge_call(
                purge_identity(account, operation_id, generation, capability),
                "cloud_purge_resume",
            )
            .await?;
        decode_control(response)
    }

    pub async fn cloud_purge_cancel(
        &self,
        account: &CloudAccount,
        operation_id: &str,
        generation: u64,
        capability: &str,
    ) -> SyncResult<CloudPurgeControl> {
        let response: Value = self
            .purge_call(
                purge_identity(account, operation_id, generation, capability),
                "cloud_purge_cancel",
            )
            .await?;
        decode_control(response)
    }

    pub async fn cloud_purge_upload_batch(
        &self,
        account: &CloudAccount,
        operation_id: &str,
        generation: u64,
        capability: &str,
        objects: &[CloudPurgeObject],
        receipts: &[CloudPurgeUploadReceipt],
    ) -> SyncResult<CloudPurgeUploadBatch> {
        let mut request = purge_identity(account, operation_id, generation, capability);
        request["objects"] = json!(objects);
        request["receipts"] = json!(receipts);
        self.purge_call(request, "cloud_purge_upload_batch").await
    }

    pub async fn cloud_purge_commit(
        &self,
        manifest: &CloudReplacementManifest,
        capability: &str,
    ) -> SyncResult<CloudPurgeControl> {
        let mut request = purge_identity(
            &manifest.account,
            &manifest.operation_id,
            manifest.generation,
            capability,
        );
        let reference = self
            .stage_cloud_inventory(
                &manifest.account,
                &manifest.operation_id,
                capability,
                "replacement",
                manifest,
            )
            .await?;
        request["replacement_manifest_ref"] = json!(reference);
        request["replacement_manifest_sha256"] =
            json!(manifest.sha256().map_err(SyncError::Storage)?);
        let response: Value = self.purge_call(request, "cloud_purge_commit").await?;
        decode_control(response)
    }
}

fn purge_identity(
    account: &CloudAccount,
    operation_id: &str,
    generation: u64,
    capability: &str,
) -> Value {
    json!({"account":account,"cloud_operation_id":operation_id,"cloud_generation":generation,"cloud_capability":capability})
}

fn decode_control(response: Value) -> SyncResult<CloudPurgeControl> {
    let value = response.get("control").cloned().unwrap_or(response);
    let control: CloudPurgeControl = serde_json::from_value(value)
        .map_err(|_| SyncError::Storage("invalid cloud control response".into()))?;
    control.validate_identity().map_err(SyncError::Storage)?;
    Ok(control)
}

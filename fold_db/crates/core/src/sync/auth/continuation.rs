//! Permanent-generation acknowledgement for a verified pre-delete cancellation.

use super::AuthClient;
use crate::sync::cloud_purge::protocol::{CloudAccount, CloudPurgeControl};
use crate::sync::error::{SyncError, SyncResult};
use serde_json::{json, Value};

impl AuthClient {
    pub(crate) async fn cloud_previous_ready_token(
        &self,
        control: &CloudPurgeControl,
        source_id: &str,
    ) -> SyncResult<Option<String>> {
        let continuation = control
            .unchanged_continuation()
            .ok_or_else(|| held("the unchanged-cloud continuation is invalid"))?;
        let floor = continuation["required_adopted_generation"]
            .as_u64()
            .ok_or_else(|| held("the continuation floor is absent"))?;
        if floor == 0 {
            return Ok(None);
        }
        let previous: CloudPurgeControl = serde_json::from_value(
            control
                .previous_ready_control
                .clone()
                .ok_or_else(|| held("the prior ready control is absent"))?,
        )?;
        let manifest = previous
            .replacement_manifest
            .ok_or_else(|| held("the prior ready replacement is absent"))?;
        let response = self.post_no_default_db_hash("/api/sync/presign", json!({
            "action":"cloud_control_adopt", "account":control.account,
                "cloud_generation":previous.generation, "cloud_operation_id":previous.operation_id,
            "replacement_manifest_sha256":crate::hex::sha256_hex(serde_json::to_vec(&manifest)?),
            "source_id":source_id,
        })).await?;
        token(&response).map(Some)
    }

    pub async fn cloud_control_continue(
        &self,
        account: &CloudAccount,
        generation: u64,
        continuation_sha256: &str,
        source_id: &str,
        previous_adoption_token: Option<&str>,
    ) -> SyncResult<String> {
        let mut request = json!({"action":"cloud_control_continue", "account":account,
            "cloud_generation":generation, "continuation_sha256":continuation_sha256,
            "source_id":source_id});
        if let Some(token) = previous_adoption_token {
            request["cloud_adoption_token"] = json!(token);
        }
        let response = self
            .post_no_default_db_hash("/api/sync/presign", request)
            .await?;
        token(&response)
    }
}

fn token(response: &Value) -> SyncResult<String> {
    if response.get("ok").and_then(Value::as_bool) != Some(true) {
        return Err(held("the continuation acknowledgement was refused"));
    }
    response
        .get("adoption_token")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .ok_or_else(|| held("the continuation adoption token is absent"))
}

fn held(reason: &str) -> SyncError {
    SyncError::CloudPurgeHeld(reason.into())
}

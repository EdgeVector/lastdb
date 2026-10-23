//! Account control reads retain ordinary authentication and exact authority.

use super::super::AuthClient;
use crate::sync::cloud_purge::protocol::{CloudAccount, CloudControlGet};
use crate::sync::{SyncError, SyncResult};
use serde::de::DeserializeOwned;
use serde_json::{json, Value};

impl AuthClient {
    pub(super) async fn control_call<T: DeserializeOwned>(
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
                .unwrap_or("cloud_control_request_failed");
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
        let response: CloudControlGet = self
            .control_call(json!({"account":account}), "cloud_control_get")
            .await?;
        if response
            .account
            .as_ref()
            .or_else(|| response.control.as_ref().map(|control| &control.account))
            != Some(account)
        {
            return Err(SyncError::CloudPurgeHeld(
                "cloud control resolved another account".into(),
            ));
        }
        self.observe_cloud_control(&response)?;
        Ok(response)
    }
}

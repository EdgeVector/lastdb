//! Explicit exact-key cloud delete under current service generation admission.

use super::super::{AuthClient, MAX_AUTH_REFRESH_RETRIES};
use crate::sync::cloud_purge::protocol::CloudAccount;
use crate::sync::error::{SyncError, SyncResult};

impl AuthClient {
    pub(crate) async fn complete_cloud_delete(
        &self,
        account: &CloudAccount,
        key: &str,
        ticket: &str,
    ) -> SyncResult<()> {
        if ticket.is_empty() || key.is_empty() || self.cloud_adoption_read.is_some() {
            return Err(SyncError::CloudPurgeHeld(
                "delete completion requires a captured ticket and writable client".into(),
            ));
        }
        let body = serde_json::json!({"action":"cloud_delete_complete", "account":account, "deletion_ticket":ticket});
        // The service-bound private ticket names the exact account/key and
        // generation. The service rechecks admission at the effect boundary.
        let send = || async {
            tokio::time::timeout(
                self.request_timeout,
                self.post_once_inner("/api/sync", body.clone()),
            )
            .await
            .map_err(|_| SyncError::Auth("cloud delete completion request timeout".into()))?
        };
        let mut result = send().await;
        if self.auth_refresh.is_some() {
            for attempt in 1..=MAX_AUTH_REFRESH_RETRIES {
                if !matches!(result, Err(SyncError::Auth(_))) {
                    break;
                }
                self.refresh_auth_once().await?;
                result = send().await;
                if attempt < MAX_AUTH_REFRESH_RETRIES && matches!(result, Err(SyncError::Auth(_))) {
                    tokio::time::sleep(std::time::Duration::from_millis(250 * u64::from(attempt)))
                        .await;
                }
            }
        }
        let result = result?;
        let resolved: CloudAccount = serde_json::from_value(
            result
                .get("account")
                .cloned()
                .ok_or_else(|| SyncError::Storage("cloud delete receipt lacks account".into()))?,
        )?;
        if result.get("ok").and_then(serde_json::Value::as_bool) != Some(true)
            || result
                .get("already_completed")
                .and_then(serde_json::Value::as_bool)
                != Some(true)
            || result.get("method").and_then(serde_json::Value::as_str) != Some("DELETE")
            || result.get("key").and_then(serde_json::Value::as_str) != Some(key)
            || &resolved != account
        {
            return Err(SyncError::Storage(
                "cloud delete did not return the exact completed receipt".into(),
            ));
        }
        Ok(())
    }
}

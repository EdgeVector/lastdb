//! Private server-bound multipart completion transport.

use super::{AuthClient, MAX_AUTH_REFRESH_RETRIES};
use crate::sync::error::{SyncError, SyncResult};
use std::time::Duration;

impl AuthClient {
    /// A private multipart ticket is the complete destination authority. The
    /// service rechecks its account/generation when it commits the uploaded
    /// part. Never infer local adoption from a foreign recipient's ticket.
    pub(super) async fn post_cloud_upload_completion_ticket(
        &self,
        account: &crate::sync::cloud_purge::protocol::CloudAccount,
        ticket: &str,
        etag: &str,
        expected_etag: Option<&str>,
        create_only: bool,
    ) -> SyncResult<serde_json::Value> {
        if ticket.is_empty() || etag.is_empty() {
            return Err(SyncError::CloudPurgeHeld(
                "multipart completion needs a private ticket and a writable client".into(),
            ));
        }
        let body = serde_json::json!({
            "action": "cloud_upload_complete", "account": account,
            "completion_ticket": ticket, "etag": etag,
            "expected_etag": expected_etag, "create_only": create_only,
        });
        let send = || async {
            tokio::time::timeout(
                self.request_timeout,
                self.post_once_inner("/api/sync/presign", body.clone()),
            )
            .await
            .map_err(|_| SyncError::Auth("multipart completion request timeout".into()))?
        };
        let mut result = send().await;
        if self.auth_refresh.is_none() {
            return result;
        }
        for attempt in 1..=MAX_AUTH_REFRESH_RETRIES {
            if !matches!(result, Err(SyncError::Auth(_))) {
                break;
            }
            self.refresh_auth_once().await?;
            result = send().await;
            if attempt < MAX_AUTH_REFRESH_RETRIES && matches!(result, Err(SyncError::Auth(_))) {
                tokio::time::sleep(Duration::from_millis(250 * u64::from(attempt))).await;
            }
        }
        result
    }
}

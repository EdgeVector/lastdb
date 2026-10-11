//! Only the storage service can complete a protocol multipart upload.

use super::super::AuthClient;
use crate::sync::error::{SyncError, SyncResult};

impl AuthClient {
    pub(crate) async fn complete_cloud_upload(
        &self,
        account: &crate::sync::cloud_purge::protocol::CloudAccount,
        ticket: &str,
        etag: &str,
        expected_etag: Option<&str>,
        create_only: bool,
        ticket_deadline: std::time::Instant,
    ) -> SyncResult<bool> {
        let deadline =
            ticket_deadline.min(std::time::Instant::now() + std::time::Duration::from_secs(900));
        loop {
            if std::time::Instant::now() >= deadline {
                return Err(SyncError::Storage("cloud upload verification remains pending; retry after the cloud job completes".into()));
            }
            let result = self
                .post_cloud_upload_completion_ticket(
                    account,
                    ticket,
                    etag,
                    expected_etag,
                    create_only,
                )
                .await?;
            if result
                .get("precondition_failed")
                .and_then(serde_json::Value::as_bool)
                == Some(true)
            {
                return Ok(false);
            }
            if result.get("ok").and_then(serde_json::Value::as_bool) != Some(true) {
                return Err(SyncError::Storage(
                    "cloud multipart finalization refused".into(),
                ));
            }
            if result.get("complete").and_then(serde_json::Value::as_bool) == Some(true) {
                return Ok(true);
            }
            if result
                .get("verification_pending")
                .and_then(serde_json::Value::as_bool)
                != Some(true)
            {
                return Err(SyncError::Storage(
                    "cloud multipart finalization lacks a completion receipt".into(),
                ));
            }
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }
    }
}

use super::super::{AuthClient, LockResponse};
use super::op_failed;
use crate::sync::error::{SyncError, SyncResult};

impl AuthClient {
    /// Acquire the device lock.
    pub async fn acquire_lock(&self, device_id: &str, ttl_secs: u64) -> SyncResult<bool> {
        let body = serde_json::json!({
            "action": "acquire_lock",
            "device_id": device_id,
            "ttl_secs": ttl_secs,
        });

        let resp = self.post("/api/storage-admin/lock", body).await?;
        let parsed: LockResponse = serde_json::from_value(resp)?;

        if !parsed.ok {
            if let Some(locked_by) = parsed.locked_by {
                return Err(SyncError::DeviceLocked {
                    device_id: locked_by,
                    expires_at: parsed.expires_at.unwrap_or_default(),
                });
            }
            return Err(op_failed("acquire_lock", parsed.error.or(parsed.reason)));
        }

        Ok(true)
    }

    /// Release the device lock.
    pub async fn release_lock(&self, device_id: &str) -> SyncResult<()> {
        self.lock_op("release_lock", device_id, None, "unlock failed")
            .await
    }

    /// Renew the device lock (extend TTL).
    pub async fn renew_lock(&self, device_id: &str, ttl_secs: u64) -> SyncResult<()> {
        self.lock_op("renew_lock", device_id, Some(ttl_secs), "renew failed")
            .await
    }

    async fn lock_op(
        &self,
        action: &str,
        device_id: &str,
        ttl_secs: Option<u64>,
        default_error: &str,
    ) -> SyncResult<()> {
        let mut body = serde_json::json!({
            "action": action,
            "device_id": device_id,
        });
        if let Some(ttl_secs) = ttl_secs {
            body["ttl_secs"] = serde_json::json!(ttl_secs);
        }

        let resp = self.post("/api/storage-admin/lock", body).await?;
        let parsed: LockResponse = serde_json::from_value(resp)?;

        if !parsed.ok {
            return Err(op_failed(
                action,
                parsed
                    .error
                    .or(parsed.reason)
                    .or_else(|| Some(default_error.to_string())),
            ));
        }

        Ok(())
    }
}

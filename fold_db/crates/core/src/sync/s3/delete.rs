//! Delete at the caller's explicit intent through a captured service ticket.

use super::{PresignedUrl, S3Client, SyncError, SyncResult};

impl S3Client {
    pub async fn delete(&self, presigned: &PresignedUrl) -> SyncResult<()> {
        if presigned.method != "DELETE" {
            return Err(SyncError::Storage(
                "delete response has the wrong method".into(),
            ));
        }
        if crate::sync::delete_completion::is_local_reference(&presigned.url) {
            return crate::sync::delete_completion::complete(&presigned.url).await;
        }
        if presigned.url.is_empty() || presigned.expires_in_secs == 0 {
            return Err(SyncError::Storage(
                "delete lacks a provider URL or captured ticket".into(),
            ));
        }
        let response = self
            .with_timeout(
                "delete",
                self.request_timeout,
                self.http.delete(&presigned.url).send(),
            )
            .await?;
        self.check_or_read_body(response, "delete").await?;
        Ok(())
    }
}

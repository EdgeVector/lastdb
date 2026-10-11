//! Dispatch received shares through their independent frontier.

use super::*;

impl SyncEngine {
    pub(crate) async fn download_with_auth_retry(&self, target: &SyncTarget) -> SyncResult<u64> {
        if target.read_binding.is_some() {
            return self.download_foreign_share(target).await;
        }
        match self.download_entries(target).await {
            Ok(n) => Ok(n),
            Err(ref e) if matches!(e, SyncError::Auth(_)) => {
                if let Some(ref refresh_cb) = self.auth_refresh {
                    tracing::info!(
                        "org download from '{}' auth failed, refreshing",
                        target.label
                    );
                    if let Ok(new_auth) = refresh_cb().await {
                        self.auth.update_auth(new_auth).await;
                        return self.download_entries(target).await;
                    }
                }
                Err(SyncError::Auth(format!(
                    "download from '{}' failed after auth refresh",
                    target.label
                )))
            }
            Err(e) => Err(e),
        }
    }
}

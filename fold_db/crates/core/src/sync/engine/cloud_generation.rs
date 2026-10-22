//! Per-target generation admission without repair or adoption.

use super::*;

impl SyncEngine {
    pub fn cloud_auth(&self) -> AuthClient {
        self.auth.clone()
    }

    pub(crate) async fn preflight_cloud_peer_targets(
        &self,
        targets: &[SyncTarget],
    ) -> SyncResult<Vec<SyncTarget>> {
        let outcomes = futures::future::join_all(targets.iter().map(|target| async {
            if target.read_binding.is_some() {
                crate::sync::cloud_share::validate_grant(target)?;
            }
            self.auth.cloud_target_preflight(target).await
        }))
        .await;
        self.refresh_foreign_share_targets(targets, outcomes).await
    }
}

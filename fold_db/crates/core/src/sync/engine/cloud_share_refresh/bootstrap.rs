//! Explicit bootstrap uses the same retained owner switch as reconnect.

use super::*;

impl SyncEngine {
    pub(in crate::sync::engine) async fn bootstrap_foreign_share(
        &self,
        target: &SyncTarget,
    ) -> SyncResult<BootstrapOutcome> {
        let (targets, _) = self.target_config_snapshot().await;
        let permitted = self.preflight_cloud_peer_targets(&targets).await?;
        let target = permitted
            .iter()
            .find(|allowed| allowed.prefix == target.prefix)
            .ok_or_else(|| held("this foreign share remains held with its prior receiver data"))?;
        let entries_replayed = self.download_with_auth_retry(target).await?;
        let last_seq = self
            .download_cursors
            .lock()
            .await
            .get(&target.cursor_key())
            .copied()
            .unwrap_or(0);
        Ok(BootstrapOutcome {
            last_seq,
            entries_replayed: usize::try_from(entries_replayed)
                .map_err(|_| held("the received share replay count exceeds this device limit"))?,
            schemas_replayed: false,
            embeddings_replayed: false,
        })
    }
}

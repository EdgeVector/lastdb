//! Host-facing peer adoption wiring and cycle admission.

use super::*;
use crate::sync::cloud_purge_peer::{AdoptionHandler, CloudPurgePeer, PeerAccountStatus};

impl SyncEngine {
    /// A stopped candidate's maintenance engine never starts a publisher.
    /// Arm the upload interlock before its queue archive/reset operations.
    pub async fn arm_stopped_cloud_purge_interlock(&self) {
        self.set_backup_only_mode().await;
    }

    pub fn cloud_purge_peer(&self) -> Option<Arc<CloudPurgePeer>> {
        self.auth.cloud_purge_peer()
    }

    pub fn set_cloud_purge_adoption_handler(&self, handler: AdoptionHandler) -> SyncResult<()> {
        self.cloud_purge_peer()
            .ok_or_else(|| {
                SyncError::CloudPurgeHeld("cloud peer has no durable receipt directory".into())
            })?
            .set_adoption_handler(handler)
    }

    pub fn cloud_purge_peer_status(&self) -> SyncResult<Vec<PeerAccountStatus>> {
        self.cloud_purge_peer()
            .map_or_else(|| Ok(Vec::new()), |peer| peer.statuses())
    }

    /// Independent reads are concurrent. No upload staging or peer replay
    /// starts before every configured target has a current generation proof.
    pub(crate) async fn preflight_cloud_peer_targets(
        &self,
        targets: &[SyncTarget],
    ) -> SyncResult<()> {
        let outcomes = futures::future::join_all(targets.iter().map(|target| {
            self.auth
                .cloud_peer_preflight((!target.prefix.is_empty()).then_some(target.prefix.as_str()))
        }))
        .await;
        for outcome in outcomes {
            outcome?;
        }
        Ok(())
    }
}

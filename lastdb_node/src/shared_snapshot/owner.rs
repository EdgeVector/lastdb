//! Device-authenticated transport for a prepared shared native snapshot.

use super::{PublishReply, SnapshotTransport};
use crate::byte_cloud_restore::{RestoreError, Result};
use cloud_backup_client::shared_objects::{
    BackupTransportError, SharedObjectClient, SnapshotPublication,
};
use cloud_backup_client::AuthClient;
use cloud_backup_protocol::shared::objects::{
    SharedObjectRequest, SharedObjectRole, SnapshotHeadRequest, SnapshotLogEndsRequest,
};
use cloud_backup_protocol::shared::{SealedObject, SnapshotManifest, SnapshotReceipt, StreamCover};
use cloud_backup_protocol::{AccountId, Id};
use fold_db::security::Ed25519KeyPair;
use std::io::Read;
use std::sync::Arc;

/// Signed transport for one explicitly configured registered device.
pub struct SignedSnapshotTransport {
    client: SharedObjectClient,
    signer: Arc<Ed25519KeyPair>,
}

impl SignedSnapshotTransport {
    pub fn from_auth(
        auth: AuthClient,
        account: AccountId,
        device: Id,
        signer: Arc<Ed25519KeyPair>,
    ) -> Result<Self> {
        Ok(Self {
            client: SharedObjectClient::from_auth(auth, account, device)
                .map_err(transport_error)?,
            signer,
        })
    }

    /// Resolve the predecessor to pin when the native publisher freezes.
    pub async fn current_snapshot(&self, database: &Id) -> Result<Option<SnapshotReceipt>> {
        self.client
            .snapshot_head(
                &SnapshotHeadRequest {
                    database_id: database.clone(),
                },
                &|message| Ok(self.signature(message)),
            )
            .await
            .map_err(transport_error)
    }

    pub(super) async fn accepted_log_ends(
        &self,
        request: &SnapshotLogEndsRequest,
    ) -> Result<Vec<StreamCover>> {
        self.client
            .snapshot_log_ends(request, &|message| Ok(self.signature(message)))
            .await
            .map_err(transport_error)
    }

    fn signature(&self, message: &[u8]) -> [u8; 64] {
        self.signer.sign(message).to_bytes()
    }
}

#[async_trait::async_trait]
impl SnapshotTransport for SignedSnapshotTransport {
    async fn upload(
        &self,
        database: &Id,
        snapshot: &Id,
        object: &SealedObject,
        bytes: Box<dyn Read + Send>,
        deadline_ms: u64,
    ) -> Result<SealedObject> {
        self.client
            .upload_exact(
                &SharedObjectRequest {
                    database_id: database.clone(),
                    object: object.clone(),
                    role: SharedObjectRole::Snapshot {
                        snapshot_id: snapshot.clone(),
                    },
                },
                bytes,
                deadline_ms,
                &|message| Ok(self.signature(message)),
            )
            .await
            .map_err(transport_error)
    }

    async fn stage_inventory(
        &self,
        manifest: &SnapshotManifest,
        inventory: &[SealedObject],
    ) -> Result<()> {
        self.client
            .stage_snapshot_inventory(manifest, inventory, &|message| Ok(self.signature(message)))
            .await
            .map_err(transport_error)
    }

    async fn publish(&self, manifest: &SnapshotManifest) -> Result<PublishReply> {
        match self
            .client
            .publish_snapshot(manifest, &|message| Ok(self.signature(message)))
            .await
            .map_err(transport_error)?
        {
            SnapshotPublication::Accepted(receipt) => Ok(PublishReply::Accepted(receipt)),
            SnapshotPublication::Refused => Ok(PublishReply::Refused),
        }
    }
}

fn transport_error(error: BackupTransportError) -> RestoreError {
    match error {
        BackupTransportError::Protocol(error) => RestoreError::Protocol(error),
        _ => RestoreError::Source("authenticated shared snapshot transport"),
    }
}

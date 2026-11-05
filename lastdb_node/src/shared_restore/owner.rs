//! Owner-authorized HTTPS transport for explicit shared recovery.

use super::recovery::{RecoveryObject, SharedRecoverySource};
use crate::byte_cloud_restore::{RecoveryKeys, RestoreError, Result};
use cloud_backup_client::shared_recovery::{BackupTransportError, SharedRecoveryClient};
use cloud_backup_client::AuthClient;
use cloud_backup_protocol::shared::{
    LogBatch, LogHead, RecoveryHoldRequest, RecoveryLogRequest, RecoveryView, SealedObject,
    SnapshotManifest,
};
use cloud_backup_protocol::{AccountId, DecimalU64, Id};
use fold_db::security::Ed25519KeyPair;
use folddb_profile::endpoints::{schema_service_url_for, Environment};
use schema_service_client::SchemaServiceClient;
use std::path::{Path, PathBuf};

struct OwnerSource {
    client: SharedRecoveryClient,
    signer: Ed25519KeyPair,
}

/// Restore with a registered device proof and credentials from the owner's
/// explicit connection. The destination gets a fresh stream and account key.
pub async fn restore_owner(
    auth: AuthClient,
    account: AccountId,
    device: Id,
    database: &Id,
    destination: &Path,
    recovery_phrase: &str,
    environment: Environment,
) -> Result<PathBuf> {
    let keys = RecoveryKeys::from_scope_phrase(recovery_phrase, &account)?;
    let source = OwnerSource {
        client: SharedRecoveryClient::from_auth(auth, account, device).map_err(transport_error)?,
        signer: Ed25519KeyPair::from_secret_key(keys.account_seed())
            .map_err(|_| RestoreError::Invalid("shared recovery signer"))?,
    };
    let schemas = SchemaServiceClient::new(schema_service_url_for(environment));
    super::restore_from_source(&source, database, destination, &keys, &schemas).await
}

#[async_trait::async_trait]
impl SharedRecoverySource for OwnerSource {
    async fn select(&self, database: &Id) -> Result<(RecoveryView, SnapshotManifest)> {
        let hold = Id::try_from(uuid::Uuid::new_v4().to_string())?;
        let request = RecoveryHoldRequest {
            database_id: database.clone(),
            hold_id: hold,
        };
        let view = self
            .client
            .open_view(&request, proof_expiry(now()), |message| {
                Ok(self.signature(message))
            })
            .await
            .map_err(transport_error)?;
        let manifest = self
            .client
            .manifest(&request, proof_expiry(now()), |message| {
                Ok(self.signature(message))
            })
            .await
            .map_err(transport_error)?;
        Ok((view, manifest))
    }

    async fn next_batch(
        &self,
        view: &RecoveryView,
        source_stream: &Id,
        previous: Option<&LogHead>,
    ) -> Result<LogBatch> {
        let request = RecoveryLogRequest {
            database_id: view.snapshot.database_id.clone(),
            hold_id: view.hold_id.clone(),
            source_stream: source_stream.clone(),
            after: previous.cloned(),
        };
        self.client
            .next_batch(&request, proof_expiry(now()), |message| {
                Ok(self.signature(message))
            })
            .await
            .map_err(transport_error)?
            .ok_or(RestoreError::Source("captured shared log continuation"))
    }

    async fn open_object(
        &self,
        view: &RecoveryView,
        object: &SealedObject,
    ) -> Result<RecoveryObject> {
        let current = now();
        self.client
            .open_object(view, object, current, proof_expiry(current), |message| {
                Ok(self.signature(message))
            })
            .await
            .map_err(transport_error)
    }
}

impl OwnerSource {
    fn signature(&self, message: &[u8]) -> [u8; 64] {
        self.signer.sign(message).to_bytes()
    }
}

fn now() -> DecimalU64 {
    DecimalU64::new(fold_db::clock::unix_millis())
}

fn proof_expiry(now: DecimalU64) -> DecimalU64 {
    DecimalU64::new(now.get().saturating_add(30_000))
}

fn transport_error(error: BackupTransportError) -> RestoreError {
    match error {
        BackupTransportError::Protocol(error) => RestoreError::Protocol(error),
        _ => RestoreError::Source("authenticated shared recovery transport"),
    }
}

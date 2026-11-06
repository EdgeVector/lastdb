//! Owner-authorized HTTPS transport for explicit shared recovery.

use super::recovery::{RecoveryObject, SharedRecoverySource};
use crate::byte_cloud_restore::{RecoveryKeys, RestoreError, Result};
use cloud_backup_client::shared_recovery::{BackupTransportError, SharedRecoveryClient};
use cloud_backup_client::AuthClient;
use cloud_backup_protocol::shared::{
    LogBatch, LogHead, RecoveryHoldRequest, RecoveryLogRequest, RecoveryObjectParent, RecoveryView,
    SealedObject, SnapshotManifest, WriterStream,
};
use cloud_backup_protocol::{DecimalU64, Id};
use fold_db::security::Ed25519KeyPair;
use folddb_profile::endpoints::{schema_service_url_for, Environment};
use schema_service_client::SchemaServiceClient;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use zeroize::Zeroizing;

struct OwnerSource {
    client: SharedRecoveryClient,
    signer: Arc<Ed25519KeyPair>,
    snapshot: Option<Id>,
    writer_head: Option<(Id, Option<LogHead>)>,
}

/// Database and optional exact snapshot required by the caller.
pub struct OwnerRestoreSelection<'a> {
    /// Database named by the protected recovery request.
    pub database: &'a Id,
    /// Require this snapshot ID in the service-selected recovery view.
    pub snapshot: Option<&'a Id>,
}

/// Restore with a registered device proof and credentials from the owner's
/// explicit connection. The destination gets a fresh stream and account key.
pub async fn restore_owner(
    auth: AuthClient,
    device: Id,
    signer: Ed25519KeyPair,
    selection: OwnerRestoreSelection<'_>,
    destination: &Path,
    recovery_phrase: &str,
    environment: Environment,
) -> Result<PathBuf> {
    let account = crate::backup_access::account_from_phrase(recovery_phrase)
        .map_err(|_| RestoreError::Invalid("shared recovery account phrase"))?;
    let keys = RecoveryKeys::from_scope_phrase(recovery_phrase, &account)?;
    let source = OwnerSource {
        client: SharedRecoveryClient::from_auth(auth, account, device.clone())
            .map_err(transport_error)?,
        signer: Arc::new(signer),
        snapshot: selection.snapshot.cloned(),
        writer_head: None,
    };
    let schemas = SchemaServiceClient::new(schema_service_url_for(environment));
    super::restore_from_source(
        &source,
        selection.database,
        &device,
        destination,
        &keys,
        &schemas,
    )
    .await
}

/// A stopped writer whose pending mutations have exact cloud receipts.
pub struct OwnerRejoin<'a> {
    pub auth: AuthClient,
    pub device: &'a Id,
    pub signer: Arc<Ed25519KeyPair>,
    pub stream: &'a WriterStream,
    pub confirmed_head: Option<&'a LogHead>,
    pub seed: Zeroizing<[u8; 32]>,
    pub source_data: &'a Path,
    pub source: &'a fold_db::FoldDB,
}

pub async fn prepare_owner_rejoin(request: OwnerRejoin<'_>) -> Result<super::PreparedRejoin> {
    let OwnerRejoin {
        auth,
        device,
        signer,
        stream,
        confirmed_head,
        seed,
        source_data,
        source,
    } = request;
    let keys = RecoveryKeys::from_seed(seed)?;
    let remote = OwnerSource {
        client: SharedRecoveryClient::from_auth(auth, keys.account_id().clone(), device.clone())
            .map_err(transport_error)?,
        signer,
        snapshot: None,
        writer_head: Some((stream.source_stream.clone(), confirmed_head.cloned())),
    };
    let schemas = SchemaServiceClient::new(schema_service_url_for(Environment::Dev));
    super::native::prepare_rejoin(
        &remote,
        &keys,
        &schemas,
        super::native::RejoinSource {
            device,
            stream,
            confirmed_head,
            source_data,
            source,
        },
    )
    .await
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
        if self
            .snapshot
            .as_ref()
            .is_some_and(|snapshot| snapshot != &view.snapshot.snapshot_id)
        {
            return Err(RestoreError::Invalid("selected shared snapshot ID"));
        }
        if let Some((stream, expected)) = &self.writer_head {
            let index = view
                .streams
                .binary_search_by(|cut| cut.source_stream.cmp(stream))
                .map_err(|_| {
                    RestoreError::Invalid("returning writer missing from recovery view")
                })?;
            if view.streams[index].head != *expected {
                return Err(RestoreError::Invalid(
                    "returning writer head differs from recovery view",
                ));
            }
        }
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
        parent: &RecoveryObjectParent,
    ) -> Result<RecoveryObject> {
        let current = now();
        self.client
            .open_object(
                view,
                object,
                parent,
                current,
                proof_expiry(current),
                |message| Ok(self.signature(message)),
            )
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
        BackupTransportError::Protocol(error)
            if error.code == cloud_backup_protocol::ErrorCode::Unauthorized
                && error.field == "shared.device.signature" =>
        {
            RestoreError::Source("shared recovery device signature refused")
        }
        BackupTransportError::Protocol(error) => RestoreError::Protocol(error),
        _ => RestoreError::Source("authenticated shared recovery transport"),
    }
}

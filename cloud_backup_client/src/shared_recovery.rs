//! Protected client for exact shared recovery cuts and sealed object reads.

use crate::backup_transport::{BackupTransport, ObjectStream, StoredObject, TemporaryGrant};
use crate::shared_authority::device_headers;
use crate::AuthClient;
use cloud_backup_protocol::shared::{
    LogBatch, RecoveryAuthorityRequest, RecoveryHoldRequest, RecoveryLogRequest,
    RecoveryObjectParent, RecoveryObjectRequest, RecoveryView, SealedObject, SharedReadGrant,
    SnapshotManifest,
};
use cloud_backup_protocol::{limits, AccountId, DecimalU64, Id};
use serde::de::DeserializeOwned;

pub use crate::backup_transport::BackupTransportError;

type Result<T> = std::result::Result<T, BackupTransportError>;

/// One registered device and its protected account connection.
pub struct SharedRecoveryClient {
    transport: BackupTransport,
    account: AccountId,
    device: Id,
}

impl SharedRecoveryClient {
    pub fn from_auth(auth: AuthClient, account: AccountId, device: Id) -> Result<Self> {
        Ok(Self {
            transport: BackupTransport::from_auth(auth)?,
            account,
            device,
        })
    }

    pub async fn open_view(
        &self,
        request: &RecoveryHoldRequest,
        proof_expires: DecimalU64,
        sign: impl FnOnce(&[u8]) -> Result<[u8; 64]>,
    ) -> Result<RecoveryView> {
        let view: RecoveryView = self
            .post(RecoveryAuthorityRequest::Open(request), proof_expires, sign)
            .await?;
        if view.hold_id != request.hold_id || view.snapshot.database_id != request.database_id {
            return Err(BackupTransportError::Integrity("shared recovery view"));
        }
        Ok(view)
    }

    pub async fn manifest(
        &self,
        request: &RecoveryHoldRequest,
        proof_expires: DecimalU64,
        sign: impl FnOnce(&[u8]) -> Result<[u8; 64]>,
    ) -> Result<SnapshotManifest> {
        let manifest: SnapshotManifest = self
            .post(
                RecoveryAuthorityRequest::Manifest(request),
                proof_expires,
                sign,
            )
            .await?;
        manifest.validate()?;
        if manifest.database_id != request.database_id {
            return Err(BackupTransportError::Integrity("shared snapshot scope"));
        }
        Ok(manifest)
    }

    pub async fn next_batch(
        &self,
        request: &RecoveryLogRequest,
        proof_expires: DecimalU64,
        sign: impl FnOnce(&[u8]) -> Result<[u8; 64]>,
    ) -> Result<Option<LogBatch>> {
        let batch: Option<LogBatch> = self
            .post(RecoveryAuthorityRequest::Log(request), proof_expires, sign)
            .await?;
        if let Some(batch) = &batch {
            batch.validate()?;
            if batch.stream.database_id != request.database_id
                || batch.stream.source_stream != request.source_stream
                || batch.after != request.after
            {
                return Err(BackupTransportError::Integrity("shared recovery batch"));
            }
        }
        Ok(batch)
    }

    pub async fn open_object(
        &self,
        view: &RecoveryView,
        object: &SealedObject,
        parent: &RecoveryObjectParent,
        now: DecimalU64,
        proof_expires: DecimalU64,
        sign: impl FnOnce(&[u8]) -> Result<[u8; 64]>,
    ) -> Result<ObjectStream> {
        let request = RecoveryObjectRequest {
            database_id: view.snapshot.database_id.clone(),
            hold_id: view.hold_id.clone(),
            object: object.clone(),
            parent: parent.clone(),
        };
        request.validate_for(view)?;
        let grant: SharedReadGrant = self
            .post(
                RecoveryAuthorityRequest::Object(&request),
                proof_expires,
                sign,
            )
            .await?;
        grant.validate_for(&request, view, now)?;
        self.transport
            .open(
                StoredObject {
                    sha256: &grant.object.sha256,
                    bytes: grant.object.bytes.get(),
                },
                TemporaryGrant {
                    url: grant.url.as_str(),
                    expires_ms: grant.expires_at_ms.get(),
                },
            )
            .await
    }

    async fn post<R: DeserializeOwned>(
        &self,
        request: RecoveryAuthorityRequest<'_>,
        proof_expires: DecimalU64,
        sign: impl FnOnce(&[u8]) -> Result<[u8; 64]>,
    ) -> Result<R> {
        let digest = request.authority_digest(&self.account, &self.device, proof_expires)?;
        let proof = sign(digest.as_str().as_bytes())?;
        let headers = device_headers(&self.device, proof_expires, proof)?;
        self.transport
            .post_bounded_with_headers(
                self.transport
                    .api_endpoint(request.path().trim_start_matches('/')),
                &request,
                limits::REQUEST_JSON_MAX_BYTES,
                headers,
            )
            .await
    }
}

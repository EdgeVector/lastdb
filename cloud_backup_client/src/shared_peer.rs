//! Protected reads of the retained accepted peer journal.

use crate::backup_transport::{BackupTransport, ObjectStream, StoredObject, TemporaryGrant};
use crate::shared_authority::device_headers;
use crate::AuthClient;
use cloud_backup_protocol::shared::peer::{
    validate_peer_grant, PeerAuthorityRequest, PeerFileRequest, PeerHeads, PeerHeadsRequest,
    PeerNextRequest, PeerSegmentRequest,
};
use cloud_backup_protocol::shared::{LogBatch, SharedReadGrant, WriterStream};
use cloud_backup_protocol::{limits, AccountId, DecimalU64, Id, ProtocolError};
use serde::de::DeserializeOwned;
use std::time::{SystemTime, UNIX_EPOCH};

pub use crate::backup_transport::BackupTransportError;
type Result<T> = std::result::Result<T, BackupTransportError>;

/// Exact shared peer transport scoped to one registered writer device.
pub struct SharedPeerClient {
    transport: BackupTransport,
    account: AccountId,
    device: Id,
}

impl SharedPeerClient {
    pub fn from_auth(auth: AuthClient, account: AccountId, device: Id) -> Result<Self> {
        Ok(Self {
            transport: BackupTransport::from_auth(auth)?,
            account,
            device,
        })
    }

    pub async fn heads(
        &self,
        request: &PeerHeadsRequest,
        expires: DecimalU64,
        sign: impl FnOnce(&[u8]) -> Result<[u8; 64]>,
    ) -> Result<PeerHeads> {
        let heads: PeerHeads = self
            .post(PeerAuthorityRequest::Heads(request), expires, sign)
            .await?;
        heads.validate_for(request)?;
        Ok(heads)
    }

    pub async fn next(
        &self,
        request: &PeerNextRequest,
        expires: DecimalU64,
        sign: impl FnOnce(&[u8]) -> Result<[u8; 64]>,
    ) -> Result<LogBatch> {
        let batch: LogBatch = self
            .post(PeerAuthorityRequest::Next(request), expires, sign)
            .await?;
        request.validate_batch(&batch)?;
        Ok(batch)
    }

    /// Open an exact accepted encrypted segment with a verified transport digest.
    pub async fn segment(
        &self,
        reader: &WriterStream,
        batch: &LogBatch,
        maximum: u64,
        expires: DecimalU64,
        sign: impl FnOnce(&[u8]) -> Result<[u8; 64]>,
    ) -> Result<ObjectStream> {
        if batch.stream.database_id != reader.database_id {
            return Err(BackupTransportError::Integrity("shared peer segment scope"));
        }
        let request = PeerSegmentRequest {
            reader: reader.clone(),
            source_stream: batch.stream.source_stream.clone(),
            head: batch.head()?,
        };
        let grant: SharedReadGrant = self
            .post(PeerAuthorityRequest::Segment(&request), expires, sign)
            .await?;
        if grant.object != batch.segment {
            return Err(BackupTransportError::Integrity("shared peer segment grant"));
        }
        self.open(&grant, maximum, expires).await
    }

    /// Open a file version authorized through a retained accepted mutation.
    /// The native writer separately checks the bytes against its file marker.
    pub async fn file(
        &self,
        request: &PeerFileRequest,
        maximum: u64,
        expires: DecimalU64,
        sign: impl FnOnce(&[u8]) -> Result<[u8; 64]>,
    ) -> Result<ObjectStream> {
        let grant: SharedReadGrant = self
            .post(PeerAuthorityRequest::File(request), expires, sign)
            .await?;
        self.open(&grant, maximum, expires).await
    }

    async fn open(
        &self,
        grant: &SharedReadGrant,
        maximum: u64,
        expires: DecimalU64,
    ) -> Result<ObjectStream> {
        validate_peer_grant(grant, now_ms()?, expires)?;
        if grant.object.bytes.get() > maximum {
            return Err(ProtocolError::limit("shared.peer.object_bytes").into());
        }
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
        request: PeerAuthorityRequest<'_>,
        expires: DecimalU64,
        sign: impl FnOnce(&[u8]) -> Result<[u8; 64]>,
    ) -> Result<R> {
        let digest = request.authority_digest(&self.account, &self.device, expires)?;
        let proof = sign(digest.as_str().as_bytes())?;
        let headers = device_headers(&self.device, expires, proof)?;
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

fn now_ms() -> Result<DecimalU64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|time| u64::try_from(time.as_millis()).ok())
        .map(DecimalU64::new)
        .ok_or(BackupTransportError::Configuration("clock"))
}

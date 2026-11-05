//! Explicit client for one signed shared writer append. Construction starts no work.
use crate::backup_transport::BackupTransport;
use crate::shared_authority::device_headers;
use crate::AuthClient;
use cloud_backup_protocol::shared::{append_authority_digest, BatchReceipt, LogBatch, LogHead};
use cloud_backup_protocol::{AccountId, DecimalU64, Id};
use reqwest::header::HeaderMap;

pub use crate::backup_transport::BackupTransportError;

/// The outcome of one submitted batch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AppendOutcome {
    /// A durable receipt matching the exact input batch.
    Accepted(BatchReceipt),
    /// Authoritative current head after an old receipt was reclaimed.
    /// A newer head alone does not confirm the saved batch's exact input.
    Reconcile(LogHead),
}

/// Ephemeral device authority for one exact batch. Keep signatures out of logs.
pub struct AppendAuthorization {
    headers: HeaderMap,
    head: LogHead,
}

impl AppendAuthorization {
    /// Sign a claim that binds the account, registered device, expiry and batch.
    /// The signer receives the canonical message and retains ownership of its key.
    pub fn sign(
        account: &AccountId,
        device: &Id,
        expires_at_ms: DecimalU64,
        batch: &LogBatch,
        sign: impl FnOnce(&[u8]) -> Result<[u8; 64], BackupTransportError>,
    ) -> Result<Self, BackupTransportError> {
        let head = batch.head()?;
        let digest = append_authority_digest(account, &batch.stream, device, expires_at_ms, &head)?;
        let signature = sign(digest.as_str().as_bytes())?;
        let headers = device_headers(device, expires_at_ms, signature)?;
        Ok(Self { headers, head })
    }
}

/// Shared-log control client using the existing protected account transport.
#[derive(Clone)]
pub struct SharedLogClient {
    transport: BackupTransport,
}

impl SharedLogClient {
    /// Use the owner's existing endpoint and account credentials.
    pub fn from_auth(auth: AuthClient) -> Result<Self, BackupTransportError> {
        Ok(Self {
            transport: BackupTransport::from_auth(auth)?,
        })
    }

    /// Submit the exact saved batch. Re-sign the same input to retry an uncertain outcome.
    pub async fn append(
        &self,
        batch: &LogBatch,
        proof: AppendAuthorization,
    ) -> Result<AppendOutcome, BackupTransportError> {
        if proof.head != batch.head()? {
            return Err(BackupTransportError::Configuration("shared append input"));
        }
        self.transport
            .post_shared_append(batch, proof.headers)
            .await
    }
}

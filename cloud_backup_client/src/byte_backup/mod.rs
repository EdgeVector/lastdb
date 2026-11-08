//! Separately versioned opaque byte transport. No worker, route or activation.
use super::backup_transport::{BackupTransport, StoredObject, TemporaryGrant};
use super::AuthClient;
use cloud_backup_protocol::v2::limits;
use serde::{de::DeserializeOwned, Serialize};

mod access;
mod blobs;
mod guest_permission;
mod jobs;
mod publication;
mod reader;
mod release;
mod transfer;

pub use super::backup_transport::{BackupTransportError, ObjectStream, UploadObservation};
pub use guest_permission::{GuestReadPermission, ShareEnvironment};
pub use reader::ByteBackupReader;
pub type Result<T> = std::result::Result<T, BackupTransportError>;

#[derive(Clone)]
pub struct ByteBackupClient {
    transport: BackupTransport,
}
impl ByteBackupClient {
    /// Construction reuses only configured credentials and bounded byte transport.
    /// It starts no enrollment, claim, legacy request, stream or background work.
    pub fn from_auth(auth: AuthClient) -> Result<Self> {
        Ok(Self {
            transport: BackupTransport::from_auth(auth)?,
        })
    }

    pub fn canonical_endpoint(&self) -> String {
        self.transport.endpoint("cloud/backups/v2/")
    }

    async fn post<Q: Serialize, R: DeserializeOwned>(
        &self,
        operation: &str,
        request: &Q,
    ) -> Result<R> {
        self.transport
            .post(
                &format!("cloud/backups/v2/{operation}"),
                request,
                limits::CONTROL_JSON_MAX_BYTES,
            )
            .await
    }
}

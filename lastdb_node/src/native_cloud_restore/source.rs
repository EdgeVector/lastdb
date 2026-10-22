use super::Result;
use cloud_backup_protocol::access::{ReadGrant, ReadGrantRequest, ReadGrantResponse};
use std::pin::Pin;

pub type ObjectStream = Pin<Box<dyn tokio::io::AsyncRead + Send>>;

/// Implementations authenticate the selected job and never substitute a newer head.
/// The importer validates responses, caps actual bytes, and verifies stored hashes.
/// No provider transport or service endpoint is activated by this trait.
#[async_trait::async_trait]
pub trait RestoreSource: Send + Sync {
    /// Exact immutable commits/status result, obtained with service read authority.
    /// A self-consistent request or local receipt does not establish acceptance.
    async fn accepted(
        &self,
        job: &cloud_backup_protocol::BackupJob,
    ) -> Result<cloud_backup_protocol::publication::AcceptedBackup>;
    async fn grants(&self, request: &ReadGrantRequest) -> Result<ReadGrantResponse>;
    async fn open(&self, grant: &ReadGrant) -> Result<ObjectStream>;
}

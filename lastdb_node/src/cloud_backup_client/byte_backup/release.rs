use super::*;
use cloud_backup_protocol::v2::blobs::{BlobReleaseRequest, BlobReleaseResponse};
use cloud_backup_protocol::ProtocolError;

impl ByteBackupClient {
    /// Record blob versions that an open job's records no longer use. The
    /// service deletes them when it accepts the job. Repeating a request is
    /// harmless; a path that was never uploaded is ignored.
    pub async fn release_blobs(&self, request: &BlobReleaseRequest) -> Result<BlobReleaseResponse> {
        request.validate()?;
        let response: BlobReleaseResponse = self.post("blobs/release", request).await?;
        if response.job != request.job || response.recorded.get() > request.paths.len() as u64 {
            return Err(ProtocolError::invalid("v2.blob_release.response").into());
        }
        Ok(response)
    }
}

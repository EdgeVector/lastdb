//! Record blob objects at client-derived paths (design
//! `design-lastdb-blob-files-on-demand-20261107`).
use super::*;
use cloud_backup_protocol::v2::blobs::{BlobReadItem, BlobReadRequest, BlobReadResponse};

impl ByteBackupClient {
    /// Ask for GET grants for blob paths of the caller's account. An absent
    /// object is a normal [`BlobReadItem::Absent`] result.
    pub async fn blob_read(&self, request: &BlobReadRequest) -> Result<BlobReadResponse> {
        request.validate()?;
        let response: BlobReadResponse = self.post("blobs/read", request).await?;
        response.validate_for(request)?;
        Ok(response)
    }

    /// Open one granted blob object. The read requires exactly the granted
    /// length. No SHA-256 of the stored bytes exists; the caller authenticates
    /// them (record files: AEAD decryption and the record's marker).
    pub async fn open_blob(&self, grant: &BlobReadItem) -> Result<ObjectStream> {
        let BlobReadItem::Grant {
            bytes,
            expires_ms,
            url,
            ..
        } = grant
        else {
            return Err(BackupTransportError::InvalidResponse("blob grant"));
        };
        self.transport
            .open_exact_length(
                bytes.get(),
                TemporaryGrant {
                    url: url.as_str(),
                    expires_ms: expires_ms.get(),
                },
            )
            .await
    }
}

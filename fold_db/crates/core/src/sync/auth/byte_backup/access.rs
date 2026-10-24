use super::*;
use cloud_backup_protocol::v2::access::*;
use cloud_backup_protocol::v2::context::{ContextReadRequest, ContextReadResponse};
use cloud_backup_protocol::v2::discovery::{StreamsListRequest, StreamsListResponse};
use cloud_backup_protocol::v2::usage::{UsageReadRequest, UsageReadResponse};
use cloud_backup_protocol::v2::AcceptedBackup;

impl ByteBackupClient {
    pub async fn context(&self, request: &ContextReadRequest) -> Result<ContextReadResponse> {
        request.validate()?;
        let response: ContextReadResponse = self.post("context/read", request).await?;
        response.validate_for(request)?;
        Ok(response)
    }

    pub async fn usage(&self, request: &UsageReadRequest) -> Result<UsageReadResponse> {
        request.validate()?;
        let response: UsageReadResponse = self.post("usage/read", request).await?;
        response.validate_for(request)?;
        Ok(response)
    }

    pub async fn list(&self, request: &StreamsListRequest) -> Result<StreamsListResponse> {
        request.validate()?;
        let response: StreamsListResponse = self.post("streams/list", request).await?;
        response.validate_for(request)?;
        Ok(response)
    }

    pub async fn put_inventory(&self, request: &InventoryPutRequest) -> Result<InventoryPutResult> {
        request.validate()?;
        let response: InventoryPutResult = self.post("inventory/put", request).await?;
        response.validate_for(request)?;
        Ok(response)
    }

    pub async fn read_grants(
        &self,
        request: &ReadGrantRequest,
        accepted: &AcceptedBackup,
    ) -> Result<ReadGrantResponse> {
        request.validate()?;
        accepted.validate()?;
        if &request.job != accepted.job() {
            return Err(NativeBackupError::Configuration("accepted selection"));
        }
        let response: ReadGrantResponse = self.post("objects/read", request).await?;
        response.validate_for(request, accepted)?;
        Ok(response)
    }

    /// The caller validates accepted membership through read_grants before open.
    /// EOF authenticates exact stored byte count and SHA256, not client plaintext.
    pub async fn open(&self, grant: &ReadGrant) -> Result<ObjectStream> {
        grant.validate()?;
        self.transport
            .open(
                StoredObject {
                    sha256: &grant.sha256,
                    bytes: grant.bytes.get(),
                },
                TemporaryGrant {
                    url: grant.url.as_str(),
                    expires_ms: grant.expires_ms.get(),
                },
            )
            .await
    }
}

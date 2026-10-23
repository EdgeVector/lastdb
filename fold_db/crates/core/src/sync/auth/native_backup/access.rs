use super::*;
use cloud_backup_protocol::access::*;
use cloud_backup_protocol::context::{ContextReadRequest, ContextReadResponse};
use cloud_backup_protocol::discovery::{StreamsListRequest, StreamsListResponse};
use cloud_backup_protocol::AccountId;

impl NativeBackupClient {
    pub async fn context(
        &self,
        request: &ContextReadRequest,
        expected: &AccountId,
    ) -> Result<ContextReadResponse> {
        request.validate()?;
        let response: ContextReadResponse = self.post("context/read", request).await?;
        response.validate_for(request, expected)?;
        Ok(response)
    }
    pub async fn list(&self, request: &StreamsListRequest) -> Result<StreamsListResponse> {
        request.validate()?;
        let response: StreamsListResponse = self.post("streams/list", request).await?;
        response.validate_for(request)?;
        Ok(response)
    }
    pub async fn read_grants(&self, request: &ReadGrantRequest) -> Result<ReadGrantResponse> {
        request.validate()?;
        let response: ReadGrantResponse = self.post("objects/read", request).await?;
        response.validate_for(request)?;
        Ok(response)
    }
    pub async fn put_reference(&self, request: &ReferencePutRequest) -> Result<ReferencePutResult> {
        request.validate()?;
        let response: ReferencePutResult = self.post("references/put", request).await?;
        response.validate_for(request)?;
        Ok(response)
    }
}

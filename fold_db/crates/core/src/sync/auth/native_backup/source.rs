//! Read-only source and account allocation observations use exact typed inputs.
use super::*;
use cloud_backup_protocol::source::{SourceReadRequest, SourceReadResponse};
use cloud_backup_protocol::usage::{UsageReadRequest, UsageReadResponse};

impl NativeBackupClient {
    pub async fn source_read(&self, request: &SourceReadRequest) -> Result<SourceReadResponse> {
        request.validate()?;
        let response: SourceReadResponse = self.post("source/read", request).await?;
        response.validate_for(request)?;
        Ok(response)
    }

    pub async fn usage_read(&self, request: &UsageReadRequest) -> Result<UsageReadResponse> {
        request.validate()?;
        let response: UsageReadResponse = self.post("usage/read", request).await?;
        response.validate_for(request)?;
        Ok(response)
    }
}

use super::*;
use cloud_backup_protocol::v2::transfer::*;
use cloud_backup_protocol::v2::Version;
use std::io::Read;

impl ByteBackupClient {
    pub async fn prepare(&self, request: &TransferPrepareRequest) -> Result<TransferBatchResponse> {
        request.validate()?;
        let response: TransferBatchResponse = self.post("transfers/prepare", request).await?;
        response.validate_for(&request.items)?;
        Ok(response)
    }

    pub async fn transfer_status(
        &self,
        request: &TransferStatusRequest,
        inputs: &[TransferInput],
    ) -> Result<TransferBatchResponse> {
        request.validate_inputs(inputs)?;
        let response: TransferBatchResponse = self.post("transfers/status", request).await?;
        response.validate_for(inputs)?;
        Ok(response)
    }

    pub async fn resume(&self, input: &TransferInput) -> Result<TransferResumeResult> {
        let request = identity(input)?;
        let response: TransferResumeResult = self.post("transfers/resume", &request).await?;
        response.validate_for(input)?;
        Ok(response)
    }

    pub async fn complete(&self, input: &TransferInput) -> Result<TransferItemResult> {
        let request = identity(input)?;
        let response: TransferItemResult = self.post("transfers/complete", &request).await?;
        response.validate_for(input)?;
        Ok(response)
    }

    /// The exact validated resume response binds its grant to the frozen input.
    /// An HTTP observation never supplies verified receipt or publication authority.
    pub async fn upload(
        &self,
        input: &TransferInput,
        response: &TransferResumeResponse,
        source: Box<dyn Read + Send>,
    ) -> Result<UploadObservation> {
        response.validate_for(input)?;
        self.transport
            .upload(
                StoredObject {
                    sha256: &input.object.sha256,
                    bytes: input.object.bytes.get(),
                },
                TemporaryGrant {
                    url: response.grant.url.as_str(),
                    expires_ms: response.grant.expires_ms.get(),
                },
                source,
            )
            .await
    }
}

fn identity(input: &TransferInput) -> Result<TransferIdentityRequest> {
    input.validate()?;
    Ok(TransferIdentityRequest {
        version: Version::V2,
        job: input.job.clone(),
        request_id: input.request_id.clone(),
        input_sha256: input.input_sha256()?,
    })
}

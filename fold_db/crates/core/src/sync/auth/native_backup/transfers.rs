use super::*;
use cloud_backup_protocol::transfer::*;

impl NativeBackupClient {
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
        request.validate()?;
        if inputs.len() != request.request_ids.len()
            || inputs
                .iter()
                .zip(&request.request_ids)
                .any(|(input, id)| input.job != request.job || &input.request_id != id)
        {
            return Err(NativeBackupError::Configuration("transfer status inputs"));
        }
        for input in inputs {
            input.validate()?;
        }
        let response: TransferBatchResponse = self.post("transfers/status", request).await?;
        response.validate_for(inputs)?;
        Ok(response)
    }
    pub async fn resume(&self, input: &TransferInput) -> Result<TransferResumeResult> {
        input.validate()?;
        let request = identity(input)?;
        let response: TransferResumeResult = self.post("transfers/resume", &request).await?;
        response.validate_for(input)?;
        Ok(response)
    }
    pub async fn complete(&self, input: &TransferInput) -> Result<TransferItemResult> {
        input.validate()?;
        let request = identity(input)?;
        let response: TransferItemResult = self.post("transfers/complete", &request).await?;
        response.validate_for(input)?;
        Ok(response)
    }
}

fn identity(input: &TransferInput) -> Result<TransferIdentityRequest> {
    Ok(TransferIdentityRequest {
        version: cloud_backup_protocol::Version,
        job: input.job.clone(),
        request_id: input.request_id.clone(),
        input_sha256: input.input_sha256()?,
    })
}

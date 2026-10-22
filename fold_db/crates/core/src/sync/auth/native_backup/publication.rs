use super::*;
use cloud_backup_protocol::jobs::JobBeginInput;
use cloud_backup_protocol::publication::*;

impl NativeBackupClient {
    pub async fn commit(
        &self,
        begin: &JobBeginInput,
        input: &CommitInput,
    ) -> Result<CommitResponse> {
        input.validate_for(begin)?;
        let response: CommitResponse = self.post("commits/prepare", input).await?;
        response.validate_for(begin, input)?;
        Ok(response)
    }
    pub async fn commit_status(&self, request: &CommitStatusRequest) -> Result<CommitResponse> {
        request.validate()?;
        let response: CommitResponse = self.post("commits/status", request).await?;
        response.validate_for_status(request)?;
        Ok(response)
    }
    pub async fn head(&self, request: &HeadReadRequest) -> Result<HeadReadResult> {
        request.validate()?;
        let response: HeadReadResult = self.post("heads/read", request).await?;
        response.validate_for(request)?;
        Ok(response)
    }
}

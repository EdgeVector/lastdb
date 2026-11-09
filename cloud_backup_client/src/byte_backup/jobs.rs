use super::*;
use cloud_backup_protocol::v2::jobs::*;

impl ByteBackupClient {
    pub async fn begin(&self, request: &JobBeginRequest) -> Result<JobResult> {
        request.validate()?;
        let response: JobResult = self.post("jobs/begin", request).await?;
        response.validate_for_begin(request)?;
        Ok(response)
    }

    pub async fn close(&self, request: &JobCloseRequest) -> Result<JobResult> {
        request.validate()?;
        let response: JobResult = self.post("jobs/close", request).await?;
        response.validate_for_close(request)?;
        Ok(response)
    }
}

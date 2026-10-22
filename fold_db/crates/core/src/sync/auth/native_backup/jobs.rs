use super::*;
use cloud_backup_protocol::jobs::*;

impl NativeBackupClient {
    pub async fn begin(&self, request: &JobBeginRequest) -> Result<JobBeginResult> {
        request.validate()?;
        let response: JobBeginResult = self.post("jobs/begin", request).await?;
        response.validate_for_begin(request)?;
        Ok(response)
    }
    pub async fn job_status(&self, request: &JobStatusRequest) -> Result<JobStatusResult> {
        request.validate()?;
        let response: JobStatusResult = self.post("jobs/status", request).await?;
        response.validate_for_status(request)?;
        Ok(response)
    }
    pub async fn close(&self, request: &JobCloseRequest) -> Result<JobCloseResult> {
        request.validate()?;
        let response: JobCloseResult = self.post("jobs/close", request).await?;
        response.validate_for_close(request)?;
        Ok(response)
    }
}

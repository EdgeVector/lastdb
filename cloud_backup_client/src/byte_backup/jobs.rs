use super::*;
use cloud_backup_protocol::v2::jobs::*;
use reqwest::header::{HeaderMap, HeaderValue};

impl ByteBackupClient {
    pub async fn begin(&self, request: &JobBeginRequest) -> Result<JobResult> {
        request.validate()?;
        // This client can reconcile a server-expired job and release its capture.
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-lastdb-backup-upload-lease",
            HeaderValue::from_static("1"),
        );
        let response: JobResult = self
            .transport
            .post_bounded_with_headers(
                self.transport.api_endpoint("cloud/backups/v2/jobs/begin"),
                request,
                limits::CONTROL_JSON_MAX_BYTES,
                headers,
            )
            .await?;
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

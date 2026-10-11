use super::*;

impl S3Client {
    pub(super) async fn complete_curl_upload(
        &self,
        presigned: &PresignedUrl,
        result: super::curl_transport::CurlUploadOutput,
    ) -> SyncResult<()> {
        super::check_curl_upload_output(&result.output)?;
        if !crate::sync::upload_completion::complete(
            &presigned.url,
            result.etag.as_deref(),
            None,
            false,
        )
        .await?
        {
            return Err(SyncError::Storage(
                "cloud upload completion precondition failed".into(),
            ));
        }
        Ok(())
    }
    pub(super) async fn complete_upload_response(
        &self,
        presigned: &PresignedUrl,
        response: reqwest::Response,
        expected_etag: Option<&str>,
        create_only: bool,
    ) -> SyncResult<bool> {
        let response = self.check_or_read_body(response, "upload").await?;
        let etag = response
            .headers()
            .get(reqwest::header::ETAG)
            .and_then(|value| value.to_str().ok());
        crate::sync::upload_completion::complete(&presigned.url, etag, expected_etag, create_only)
            .await
    }

    /// Conditional protocol uploads check the destination at service finalization.
    pub async fn upload_conditional(
        &self,
        presigned: &PresignedUrl,
        data: Vec<u8>,
        expected_etag: Option<&str>,
    ) -> SyncResult<bool> {
        let mut request = self
            .http
            .put(&presigned.url)
            .header("Content-Type", "application/octet-stream");
        if !crate::sync::upload_completion::is_multipart(&presigned.url) {
            request = if let Some(etag) = expected_etag {
                request.header(reqwest::header::IF_MATCH, etag)
            } else {
                request.header(reqwest::header::IF_NONE_MATCH, "*")
            };
        }
        let response = self
            .with_timeout(
                "conditional upload",
                self.request_timeout,
                request.body(data).send(),
            )
            .await?;
        if response.status() == reqwest::StatusCode::PRECONDITION_FAILED {
            return Ok(false);
        }
        self.complete_upload_response(presigned, response, expected_etag, expected_etag.is_none())
            .await
    }
}

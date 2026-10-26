use super::*;
use std::io::Read;

/// A provider response is an observation only. The service still verifies bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UploadObservation {
    HttpSuccess,
    AlreadyExists,
}

impl BackupTransport {
    pub(in crate::sync::auth) async fn upload(
        &self,
        object: StoredObject<'_>,
        grant: TemporaryGrant<'_>,
        source: Box<dyn Read + Send>,
    ) -> Result<UploadObservation> {
        self.upload_mode(object, grant, source, false).await
    }

    pub(in crate::sync::auth) async fn upload_part(
        &self,
        object: StoredObject<'_>,
        grant: TemporaryGrant<'_>,
        source: Box<dyn Read + Send>,
    ) -> Result<UploadObservation> {
        self.upload_mode(object, grant, source, true).await
    }

    async fn upload_mode(
        &self,
        object: StoredObject<'_>,
        grant: TemporaryGrant<'_>,
        source: Box<dyn Read + Send>,
        multipart_part: bool,
    ) -> Result<UploadObservation> {
        let url = urls::upload(grant.url, multipart_part)?;
        let timeout = self.transfer_timeout(object.bytes, grant.expires_ms)?;
        let deadline = tokio::time::Instant::now() + timeout;
        let permit = tokio::time::timeout_at(deadline, Arc::clone(&self.uploads).acquire_owned())
            .await
            .map_err(|_| BackupTransportError::Timeout)?
            .map_err(|_| BackupTransportError::Task)?;
        let (body, mut feeder, cancel) =
            feed::start(source, object.bytes, object.sha256.clone(), permit);
        // The deadline owns only the transport future. The feeder remains owned
        // here so every timeout/error cancels and joins it before source release.
        let mut request = self
            .http
            .put(url)
            .header(reqwest::header::CONTENT_LENGTH, object.bytes);
        if !multipart_part {
            request = request.header(reqwest::header::IF_NONE_MATCH, UPLOAD_IF_NONE_MATCH);
        }
        let response = tokio::time::timeout_at(deadline, request.body(body).send()).await;
        let response = match response {
            Ok(Ok(response)) => response,
            result => {
                cancel.stop();
                let _ = feeder.await;
                return Err(if result.is_err() {
                    BackupTransportError::Timeout
                } else {
                    BackupTransportError::Transport
                });
            }
        };
        let status = response.status();
        drop(response);
        if !status.is_success() {
            cancel.stop();
            let _ = feeder.await;
            return if status == reqwest::StatusCode::PRECONDITION_FAILED {
                Ok(UploadObservation::AlreadyExists)
            } else {
                Err(BackupTransportError::Http(status.as_u16()))
            };
        }
        if let Ok(result) = tokio::time::timeout_at(deadline, &mut feeder).await {
            result.map_err(|_| BackupTransportError::Task)??;
        } else {
            cancel.stop();
            let _ = feeder.await;
            return Err(BackupTransportError::Timeout);
        }
        Ok(UploadObservation::HttpSuccess)
    }
}

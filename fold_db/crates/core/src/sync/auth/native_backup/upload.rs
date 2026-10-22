use super::*;
use cloud_backup_protocol::transfer::{TransferInput, UploadGrant, UPLOAD_IF_NONE_MATCH};
use std::io::Read;

/// A provider response is an observation only. The service still verifies bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UploadObservation {
    HttpSuccess,
    AlreadyExists,
}

impl NativeBackupClient {
    pub async fn upload(
        &self,
        input: &TransferInput,
        grant: &UploadGrant,
        source: Box<dyn Read + Send>,
    ) -> Result<UploadObservation> {
        input.validate()?;
        grant.validate()?;
        let url = urls::signed(grant.url.as_str(), true)?;
        let timeout = self.transfer_timeout(input.object.bytes.get(), grant.expires_ms.get())?;
        tokio::time::timeout(timeout, self.put(input, url, source))
            .await
            .map_err(|_| NativeBackupError::Timeout)?
    }

    async fn put(
        &self,
        input: &TransferInput,
        url: Url,
        source: Box<dyn Read + Send>,
    ) -> Result<UploadObservation> {
        let permit = Arc::clone(&self.uploads)
            .acquire_owned()
            .await
            .map_err(|_| NativeBackupError::Task)?;
        let (body, feeder, cancel) = feed::start(source, input, permit);
        let response = self
            .http
            .put(url)
            .header(reqwest::header::CONTENT_LENGTH, input.object.bytes.get())
            .header(reqwest::header::IF_NONE_MATCH, UPLOAD_IF_NONE_MATCH)
            .body(body)
            .send()
            .await;
        let Ok(response) = response else {
            cancel.stop();
            let _ = feeder.await;
            return Err(NativeBackupError::Transport);
        };
        let status = response.status();
        drop(response);
        if !status.is_success() {
            cancel.stop();
            let _ = feeder.await;
            return if status == reqwest::StatusCode::PRECONDITION_FAILED {
                Ok(UploadObservation::AlreadyExists)
            } else {
                Err(NativeBackupError::Http(status.as_u16()))
            };
        }
        feeder.await.map_err(|_| NativeBackupError::Task)??;
        Ok(UploadObservation::HttpSuccess)
    }
}

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
        let deadline = tokio::time::Instant::now() + timeout;
        let permit = tokio::time::timeout_at(deadline, Arc::clone(&self.uploads).acquire_owned())
            .await
            .map_err(|_| NativeBackupError::Timeout)?
            .map_err(|_| NativeBackupError::Task)?;
        let (body, mut feeder, cancel) = feed::start(source, input, permit);
        // The deadline owns only the transport future. The feeder remains owned
        // here so every timeout/error cancels and joins it before source release.
        let response = tokio::time::timeout_at(
            deadline,
            self.http
                .put(url)
                .header(reqwest::header::CONTENT_LENGTH, input.object.bytes.get())
                .header(reqwest::header::IF_NONE_MATCH, UPLOAD_IF_NONE_MATCH)
                .body(body)
                .send(),
        )
        .await;
        let response = match response {
            Ok(Ok(response)) => response,
            result => {
                cancel.stop();
                let _ = feeder.await;
                return Err(if result.is_err() {
                    NativeBackupError::Timeout
                } else {
                    NativeBackupError::Transport
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
                Err(NativeBackupError::Http(status.as_u16()))
            };
        }
        if let Ok(result) = tokio::time::timeout_at(deadline, &mut feeder).await {
            result.map_err(|_| NativeBackupError::Task)??;
        } else {
            cancel.stop();
            let _ = feeder.await;
            return Err(NativeBackupError::Timeout);
        }
        Ok(UploadObservation::HttpSuccess)
    }
}

use super::super::backup_transport::{StoredObject, TemporaryGrant, UPLOAD_IF_NONE_MATCH};
use super::*;
use cloud_backup_protocol::transfer::{TransferInput, UploadGrant};
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
        self.upload_bytes(
            StoredObject {
                sha256: &input.object.sha256,
                bytes: input.object.bytes.get(),
            },
            TemporaryGrant {
                url: grant.url.as_str(),
                expires_ms: grant.expires_ms.get(),
            },
            source,
        )
        .await
    }

    pub(in crate::sync::auth) async fn upload_bytes(
        &self,
        object: StoredObject<'_>,
        grant: TemporaryGrant<'_>,
        source: Box<dyn Read + Send>,
    ) -> Result<UploadObservation> {
        let url = urls::signed(grant.url, true)?;
        let timeout = self.transfer_timeout(object.bytes, grant.expires_ms)?;
        let deadline = tokio::time::Instant::now() + timeout;
        let permit = tokio::time::timeout_at(deadline, Arc::clone(&self.uploads).acquire_owned())
            .await
            .map_err(|_| NativeBackupError::Timeout)?
            .map_err(|_| NativeBackupError::Task)?;
        let (body, mut feeder, cancel) =
            feed::start(source, object.bytes, object.sha256.clone(), permit);
        // The deadline owns only the transport future. The feeder remains owned
        // here so every timeout/error cancels and joins it before source release.
        let response = tokio::time::timeout_at(
            deadline,
            self.http
                .put(url)
                .header(reqwest::header::CONTENT_LENGTH, object.bytes)
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

//! The two existing unscoped GET-grant actions have no legacy client admission.
//! Owner-scoped main-file requests are deliberately unsupported: that service
//! action registers independent consumer copies and is not a read-only grant.
use super::*;
use crate::storage::disk_pack::{backup_capture::FileSource, file_dependencies::FileDependency};
use crate::storage::error::{StorageError, StorageResult};
use cloud_backup_protocol::{FileDependencyKind, Text};
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Clone)]
pub struct NativeAuthorizedFileSource {
    client: NativeBackupClient,
}

#[derive(Serialize)]
struct FileRequest<'a> {
    action: &'static str,
    file_hash: &'a str,
}
#[derive(Deserialize)]
struct FileResponse {
    ok: bool,
    #[serde(default)]
    urls: Vec<FileGrant>,
}
#[derive(Deserialize)]
struct FileGrant {
    url: Text<8192>,
    method: Text<8>,
    expires_in_secs: u64,
}

impl NativeAuthorizedFileSource {
    pub(super) fn new(client: NativeBackupClient) -> Self {
        Self { client }
    }

    /// The capture reserves disk capacity and authenticates the exact original
    /// encryption after this bounded download. An incomplete file stays private.
    pub async fn fetch_to_file(&self, dependency: &FileDependency, path: &Path) -> Result<()> {
        if dependency.access().owner_scope.is_some() {
            return Err(NativeBackupError::UnsupportedFileScope);
        }
        let action = match dependency.kind() {
            FileDependencyKind::Main => "presign_file_download",
            FileDependencyKind::Thumbnail | FileDependencyKind::Poster => "presign_thumb_download",
        };
        let request = FileRequest {
            action,
            file_hash: dependency.file_hash().as_str(),
        };
        let mut response: FileResponse = self
            .client
            .post_url(self.client.api_endpoint("sync/presign"), &request)
            .await?;
        if !response.ok || response.urls.len() != 1 {
            return Err(NativeBackupError::InvalidResponse("file source grant"));
        }
        let grant = response.urls.pop().ok_or(NativeBackupError::Source)?;
        if grant.method.as_str() != "GET" || grant.expires_in_secs == 0 {
            return Err(NativeBackupError::InvalidResponse("file source method"));
        }
        urls::signed(grant.url.as_str(), false)?;
        let grant = crate::sync::s3::PresignedUrl {
            url: grant.url.as_str().to_owned(),
            method: "GET".into(),
            expires_in_secs: grant.expires_in_secs,
        };
        crate::sync::s3::S3Client::with_request_timeout(
            Arc::clone(&self.client.http),
            self.client.timeout,
        )
        .download_to_file_limited(&grant, path, dependency.expected_stored_bytes())
        .await
        .map_err(|_| NativeBackupError::Source)?
        .ok_or(NativeBackupError::Source)
    }
}

#[async_trait::async_trait]
impl FileSource for NativeAuthorizedFileSource {
    async fn fetch_to_file(&self, dependency: &FileDependency, path: &Path) -> StorageResult<()> {
        Self::fetch_to_file(self, dependency, path)
            .await
            .map_err(|error| {
                let message = match error {
                    NativeBackupError::UnsupportedFileScope => {
                        "native file source owner scope needs a read-only grant"
                    }
                    _ => "native authorized file source failed",
                };
                StorageError::InvalidOperation(message.into())
            })
    }
}

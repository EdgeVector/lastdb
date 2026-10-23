//! Scoped main files read only an existing native-authorized consumer copy.
//! Unscoped GET actions bypass legacy client admission. No request registers
//! a consumer copy, adopts a generation, or substitutes a main file for a thumb.
use super::*;
use crate::storage::disk_pack::{backup_capture::FileSource, file_dependencies::FileDependency};
use crate::storage::error::{StorageError, StorageResult};
use cloud_backup_protocol::source::{SourceReadRequest, SourceReadResult, SourceReadSelector};
use cloud_backup_protocol::{CloudScope, DecimalU64, FileDependencyKind, Text, Version};
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Clone)]
pub struct NativeAuthorizedFileSource {
    client: NativeBackupClient,
    scope: CloudScope,
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
    pub(super) fn new(client: NativeBackupClient, scope: CloudScope) -> Self {
        Self { client, scope }
    }

    /// The capture reserves disk capacity and authenticates the exact original
    /// encryption after this bounded download. An incomplete file stays private.
    pub async fn fetch_to_file(&self, dependency: &FileDependency, path: &Path) -> Result<()> {
        self.scope.validate()?;
        if let Some(owner) = &dependency.access().owner_scope {
            return self.scoped_file(dependency, owner, path).await;
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

    async fn scoped_file(
        &self,
        dependency: &FileDependency,
        owner: &str,
        path: &Path,
    ) -> Result<()> {
        if dependency.kind() != FileDependencyKind::Main {
            return Err(NativeBackupError::UnsupportedFileScope);
        }
        let selector = SourceReadSelector {
            kind: dependency.kind(),
            owner_scope: Text::try_from(owner.to_owned())?,
            // The current pointer codec carries no app namespace. Never infer one.
            app_id: None,
            file_hash: dependency.file_hash().clone(),
            expected_ciphertext_bytes: DecimalU64::new(dependency.expected_stored_bytes()),
        };
        let request = SourceReadRequest {
            version: Version::V1,
            scope: self.scope.clone(),
            objects: vec![selector.clone()],
        };
        let response = self.client.source_read(&request).await?;
        match response
            .results
            .into_iter()
            .next()
            .ok_or(NativeBackupError::InvalidResponse("file source grant"))?
        {
            SourceReadResult::Grant { grant, .. } => {
                self.client.download_source(&selector, &grant, path).await
            }
            SourceReadResult::Error { error, .. } => Err(NativeBackupError::Protocol(error)),
        }
    }
}

#[async_trait::async_trait]
impl FileSource for NativeAuthorizedFileSource {
    async fn fetch_to_file(&self, dependency: &FileDependency, path: &Path) -> StorageResult<()> {
        Self::fetch_to_file(self, dependency, path)
            .await
            .map_err(|error| {
                let message = match error {
                    NativeBackupError::Protocol(error)
                        if error.code == cloud_backup_protocol::ErrorCode::SourceUnavailable =>
                    {
                        "native authorized file consumer copy is unavailable"
                    }
                    NativeBackupError::UnsupportedFileScope => {
                        "native file source owner scope needs a read-only grant"
                    }
                    _ => "native authorized file source failed",
                };
                StorageError::InvalidOperation(message.into())
            })
    }
}

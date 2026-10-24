//! Private byte transport facade. No client format or public wire value enters here.
use super::native_backup::{
    NativeBackupClient, NativeBackupError, NativeObjectStream, UploadObservation,
};
use super::AuthClient;
use cloud_backup_protocol::Digest;
use serde::{de::DeserializeOwned, Serialize};
use std::io::Read;

pub(super) const UPLOAD_IF_NONE_MATCH: &str = "*";

#[derive(Clone, Copy)]
pub(super) struct StoredObject<'a> {
    pub sha256: &'a Digest,
    pub bytes: u64,
}

#[derive(Clone, Copy)]
pub(super) struct TemporaryGrant<'a> {
    pub url: &'a str,
    pub expires_ms: u64,
}

/// Reuse the existing credential, HTTP, deadline and stream ownership machinery.
/// Generic entry points below never select or validate a V1 wire value.
#[derive(Clone)]
pub(super) struct BackupTransport {
    inner: NativeBackupClient,
}
impl BackupTransport {
    pub fn from_auth(auth: AuthClient) -> Result<Self, NativeBackupError> {
        Ok(Self {
            inner: NativeBackupClient::from_auth(auth)?,
        })
    }

    pub fn endpoint(&self, operation: &str) -> String {
        self.inner.api_endpoint(operation).to_string()
    }

    pub async fn post<Q: Serialize, R: DeserializeOwned>(
        &self,
        operation: &str,
        request: &Q,
        maximum: u64,
    ) -> Result<R, NativeBackupError> {
        self.inner
            .post_bounded(self.inner.api_endpoint(operation), request, maximum)
            .await
    }

    pub async fn upload(
        &self,
        object: StoredObject<'_>,
        grant: TemporaryGrant<'_>,
        source: Box<dyn Read + Send>,
    ) -> Result<UploadObservation, NativeBackupError> {
        self.inner.upload_bytes(object, grant, source).await
    }

    pub async fn open(
        &self,
        object: StoredObject<'_>,
        grant: TemporaryGrant<'_>,
    ) -> Result<NativeObjectStream, NativeBackupError> {
        self.inner.open_bytes(object, grant).await
    }
}

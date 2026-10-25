//! Format-neutral bounded byte transport. No legacy wire operation is admitted.
use super::AuthClient;
use cloud_backup_protocol::Digest;
use reqwest::{Client, Url};
use serde::{de::DeserializeOwned, Serialize};
use std::sync::Arc;
use std::time::Duration;

mod download;
mod error;
mod feed;
mod http;
mod upload;
mod urls;

pub use download::ObjectStream;
pub use error::BackupTransportError;
pub use upload::UploadObservation;
pub(super) type Result<T> = std::result::Result<T, BackupTransportError>;
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

#[derive(Clone)]
pub(super) struct BackupTransport {
    auth: AuthClient,
    http: Arc<Client>,
    base: Url,
    timeout: Duration,
    uploads: Arc<tokio::sync::Semaphore>,
}
impl BackupTransport {
    /// Use only explicit credentials, endpoint, refresh callback and deadlines.
    pub fn from_auth(auth: AuthClient) -> Result<Self> {
        let base = urls::https(&auth.base_url)?;
        if base.query().is_some() || auth.request_timeout.is_zero() {
            return Err(BackupTransportError::Configuration("endpoint or deadline"));
        }
        // trace-egress: propagate (control); skip-s3 (presigned object methods).
        let http = Client::builder()
            .https_only(true)
            .redirect(reqwest::redirect::Policy::none())
            .no_gzip()
            .no_brotli()
            .no_deflate()
            .no_zstd()
            .connect_timeout(auth.request_timeout)
            .build()
            .map_err(|_| BackupTransportError::Configuration("HTTP client"))?;
        Ok(Self {
            timeout: auth.request_timeout,
            auth,
            http: Arc::new(http),
            base,
            uploads: Arc::new(tokio::sync::Semaphore::new(
                lastdb_storage_policy::BACKUP_RESOURCES.concurrent_streams,
            )),
        })
    }

    pub fn endpoint(&self, operation: &str) -> String {
        self.api_endpoint(operation).to_string()
    }

    pub async fn post<Q: Serialize, R: DeserializeOwned>(
        &self,
        operation: &str,
        request: &Q,
        maximum: u64,
    ) -> Result<R> {
        self.post_bounded(self.api_endpoint(operation), request, maximum)
            .await
    }
}

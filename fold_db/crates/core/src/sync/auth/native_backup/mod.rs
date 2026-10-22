//! Native-only, bounded transport. Construction starts no work or enrollment.
//! Callers own runtime admission and durable retry identities before every effect.
use super::AuthClient;
use reqwest::{Client, Url};
use std::sync::Arc;
use std::time::Duration;

mod access;
mod download;
mod error;
mod feed;
mod files;
mod http;
mod jobs;
mod publication;
mod transfers;
mod upload;
mod urls;

pub use download::NativeObjectStream;
pub use error::NativeBackupError;
pub use files::NativeAuthorizedFileSource;
pub use upload::UploadObservation;
pub type Result<T> = std::result::Result<T, NativeBackupError>;

#[derive(Clone)]
pub struct NativeBackupClient {
    auth: AuthClient,
    http: Arc<Client>,
    base: Url,
    timeout: Duration,
    uploads: Arc<tokio::sync::Semaphore>,
}

impl NativeBackupClient {
    /// Reuse only the explicit endpoint, credentials, refresh callback and deadline.
    /// Legacy scope injection, adoption, automatic claims and workers are not called.
    pub fn from_auth(auth: AuthClient) -> Result<Self> {
        let base = urls::https(&auth.base_url)?;
        if base.query().is_some() || auth.request_timeout.is_zero() {
            return Err(NativeBackupError::Configuration("endpoint or deadline"));
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
            .map_err(|_| NativeBackupError::Configuration("HTTP client"))?;
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

    /// Construct only the bounded read-only source adapter. No request starts here.
    /// An owner-scoped pointer refuses until its source grant has a read-only API.
    pub fn authorized_file_source(&self) -> NativeAuthorizedFileSource {
        NativeAuthorizedFileSource::new(self.clone())
    }
}

//! Format-neutral bounded byte transport.
use super::{AuthClient, DEFAULT_AUTH_REQUEST_TIMEOUT};
use cloud_backup_protocol::{Digest, ProtocolError};
use futures::StreamExt;
use reqwest::header::HeaderMap;
use reqwest::{Client, Url};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::io::Read;
use std::sync::Arc;
use std::time::Duration;

mod download;
mod feed;

pub use download::ObjectStream;
pub(super) type Result<T> = std::result::Result<T, BackupTransportError>;
pub(super) const UPLOAD_IF_NONE_MATCH: &str = "*";

/// Transport failures never establish that a saved mutation failed remotely.
#[derive(Clone)]
pub enum BackupTransportError {
    Configuration(&'static str),
    Protocol(ProtocolError),
    Authentication,
    Timeout,
    Transport,
    Http(u16),
    InvalidResponse(&'static str),
    Integrity(&'static str),
    UnsupportedFileScope,
    Source,
    Task,
}
impl From<ProtocolError> for BackupTransportError {
    fn from(value: ProtocolError) -> Self {
        Self::Protocol(value)
    }
}
impl std::fmt::Display for BackupTransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Configuration(field) => write!(f, "native backup configuration: {field}"),
            Self::Protocol(error) => write!(f, "native backup service error: {:?}", error.code),
            Self::Authentication => f.write_str("native backup authentication refused"),
            Self::Timeout => f.write_str("native backup request deadline expired"),
            Self::Transport => f.write_str("native backup transport failed"),
            Self::Http(status) => write!(f, "native backup HTTP status {status}"),
            Self::InvalidResponse(field) => write!(f, "native backup invalid response: {field}"),
            Self::Integrity(field) => write!(f, "native backup byte integrity: {field}"),
            Self::UnsupportedFileScope => {
                f.write_str("native backup file owner scope needs a read-only source grant")
            }
            Self::Source => f.write_str("native backup source read failed"),
            Self::Task => f.write_str("native backup source task failed"),
        }
    }
}
impl std::fmt::Debug for BackupTransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl std::error::Error for BackupTransportError {}

/// A provider response is an observation only. The service still verifies bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UploadObservation {
    HttpSuccess,
    AlreadyExists,
}

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

#[derive(Deserialize)]
struct ErrorTag<'a> {
    result: Option<&'a str>,
}

#[derive(Clone)]
pub(super) struct BackupTransport {
    auth: Option<AuthClient>,
    http: Arc<Client>,
    base: Url,
    timeout: Duration,
    uploads: Arc<tokio::sync::Semaphore>,
}
impl BackupTransport {
    /// Use explicit account credentials, endpoint, and the fixed request deadline.
    pub fn from_auth(auth: AuthClient) -> Result<Self> {
        let base = download::https(&auth.base_url)?;
        Self::new(base, Some(auth))
    }

    pub(crate) fn for_guest(base_url: &str) -> Result<Self> {
        Self::new(download::https(base_url)?, None)
    }

    fn new(base: Url, auth: Option<AuthClient>) -> Result<Self> {
        if base.query().is_some() {
            return Err(BackupTransportError::Configuration("endpoint"));
        }
        // trace-egress: propagate (control); skip-s3 (presigned object methods).
        let http = Client::builder()
            .https_only(true)
            .redirect(reqwest::redirect::Policy::none())
            .no_gzip()
            .no_brotli()
            .no_deflate()
            .no_zstd()
            .connect_timeout(DEFAULT_AUTH_REQUEST_TIMEOUT)
            .build()
            .map_err(|_| BackupTransportError::Configuration("HTTP client"))?;
        Ok(Self {
            timeout: DEFAULT_AUTH_REQUEST_TIMEOUT,
            auth,
            http: Arc::new(http),
            base,
            uploads: Arc::new(tokio::sync::Semaphore::new(
                fold_db::storage_policy::BACKUP_RESOURCES.concurrent_streams,
            )),
        })
    }

    pub(super) fn apply_auth(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if let Some(auth) = &self.auth {
            auth.apply_auth(request)
        } else {
            request
        }
    }

    pub fn endpoint(&self, operation: &str) -> String {
        self.api_endpoint(operation).to_string()
    }

    /// True when the presigned URL is one multipart UploadPart.
    pub(crate) fn upload_is_multipart(
        url: &str,
    ) -> std::result::Result<bool, BackupTransportError> {
        download::is_multipart(url)
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

    pub(crate) async fn post_bounded<Q: Serialize, R: DeserializeOwned>(
        &self,
        url: Url,
        request: &Q,
        maximum: u64,
    ) -> Result<R> {
        self.post_bounded_with_headers(url, request, maximum, HeaderMap::new())
            .await
    }

    pub(crate) async fn post_bounded_with_headers<Q: Serialize, R: DeserializeOwned>(
        &self,
        url: Url,
        request: &Q,
        maximum: u64,
        headers: HeaderMap,
    ) -> Result<R> {
        let bytes = cloud_backup_protocol::canonical_bytes(request)?;
        if bytes.len() as u64 > maximum {
            return Err(BackupTransportError::Configuration("request bytes"));
        }
        tokio::time::timeout(self.timeout, self.once(&url, bytes, maximum, headers))
            .await
            .map_err(|_| BackupTransportError::Timeout)?
    }

    async fn once<R: DeserializeOwned>(
        &self,
        url: &Url,
        bytes: Vec<u8>,
        maximum: u64,
        headers: HeaderMap,
    ) -> Result<R> {
        let request = self
            .http
            .post(url.clone())
            .headers(headers)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(bytes);
        let request = self.apply_auth(request);
        let response = observability::propagation::inject_w3c(request)
            .send()
            .await
            .map_err(|_| BackupTransportError::Transport)?;
        let status = response.status();
        let body = bounded(response, maximum).await?;
        if status == reqwest::StatusCode::UNAUTHORIZED {
            return Err(BackupTransportError::Authentication);
        }
        if let Ok(error) = serde_json::from_slice::<ProtocolError>(&body) {
            return Err(BackupTransportError::Protocol(error));
        }
        if !status.is_success() {
            let tagged = serde_json::from_slice::<ErrorTag<'_>>(&body)
                .ok()
                .is_some_and(|tag| tag.result == Some("error"));
            if !tagged {
                return Err(BackupTransportError::Http(status.as_u16()));
            }
        }
        serde_json::from_slice(&body)
            .map_err(|_| BackupTransportError::InvalidResponse("JSON body"))
    }

    pub(crate) async fn upload(
        &self,
        object: StoredObject<'_>,
        grant: TemporaryGrant<'_>,
        source: Box<dyn Read + Send>,
    ) -> Result<UploadObservation> {
        self.upload_mode(object, grant, source, false).await
    }

    pub(crate) async fn upload_part(
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
        let (url, if_none_match) = download::upload_url(grant.url, multipart_part)?;
        let timeout = self.transfer_timeout(object.bytes, grant.expires_ms)?;
        let deadline = tokio::time::Instant::now() + timeout;
        let permit = tokio::time::timeout_at(deadline, Arc::clone(&self.uploads).acquire_owned())
            .await
            .map_err(|_| BackupTransportError::Timeout)?
            .map_err(|_| BackupTransportError::Task)?;
        let (body, mut feeder, cancel) =
            feed::start(source, object.bytes, object.sha256.clone(), permit);
        let mut request = self
            .http
            .put(url)
            .header(reqwest::header::CONTENT_LENGTH, object.bytes);
        if if_none_match {
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

pub(super) async fn bounded(response: reqwest::Response, maximum: u64) -> Result<Vec<u8>> {
    if response
        .content_length()
        .is_some_and(|bytes| bytes > maximum)
    {
        return Err(BackupTransportError::InvalidResponse("response bytes"));
    }
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| BackupTransportError::Transport)?;
        let next = body
            .len()
            .checked_add(chunk.len())
            .filter(|bytes| *bytes as u64 <= maximum)
            .ok_or(BackupTransportError::InvalidResponse("response bytes"))?;
        body.try_reserve_exact(next - body.len())
            .map_err(|_| BackupTransportError::InvalidResponse("response allocation"))?;
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

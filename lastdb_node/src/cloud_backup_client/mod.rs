//! HTTPS client, shared runtime connection pooling, and bounded storage transport.
use cloud_backup_protocol::{ErrorCode, ProtocolError};
use futures::StreamExt;
use reqwest::{Client, Url};
use std::{fmt, sync::Arc, sync::Mutex, time::Duration};

mod guest_permission;
mod signed_blob;
mod transport;

pub mod storage_transfer {
    pub use super::guest_permission::{GuestReadPermission, ShareEnvironment};
}

pub use signed_blob::{canonical_backup_endpoint, BlobUploadTarget, SignedBlobClient};
pub(crate) use transport::{cached_runtime_client, BackupTransport, PresignedPut};

pub(crate) const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
pub(crate) type TransportResult<T> = Result<T, BackupTransportError>;

static SHARED_HTTP_CLIENT: Mutex<Option<(tokio::runtime::Id, Client)>> = Mutex::new(None);

#[derive(Clone)]
pub struct AuthClient {
    pub(crate) base_url: String,
    pub(crate) api_key: Arc<str>,
}

impl AuthClient {
    pub fn new(base_url: String, api_key: String) -> Self {
        Self {
            base_url,
            api_key: api_key.into(),
        }
    }
}

pub fn build_shared_http_client() -> Result<Client, reqwest::Error> {
    cached_runtime_client(&SHARED_HTTP_CLIENT, || {
        // trace-egress: propagate — AuthClient and registration inject W3C on service requests.
        // trace-egress: skip-s3 — storage transfer preserves signed provider request headers.
        reqwest::Client::builder()
            .https_only(true)
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(15))
            .build()
    })
}

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
}

impl From<ProtocolError> for BackupTransportError {
    fn from(value: ProtocolError) -> Self {
        Self::Protocol(value)
    }
}

impl fmt::Display for BackupTransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Configuration(s) => write!(f, "native backup configuration: {s}"),
            Self::Protocol(err)
                if err.code == ErrorCode::Unauthorized && err.field == "payment.subscription" =>
            {
                f.write_str(
                    "active paid cloud subscription required — run 'lastdb cloud payment checkout' first",
                )
            }
            Self::Protocol(err) => write!(f, "native backup service error: {err}"),
            Self::Authentication => f.write_str("native backup authentication refused"),
            Self::Timeout => f.write_str("native backup request deadline expired"),
            Self::Transport => f.write_str("native backup transport failed"),
            Self::Http(s) => write!(f, "native backup HTTP status {s}"),
            Self::InvalidResponse(s) => write!(f, "native backup invalid response: {s}"),
            Self::Integrity(s) => write!(f, "native backup byte integrity: {s}"),
        }
    }
}

impl fmt::Debug for BackupTransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl std::error::Error for BackupTransportError {}

pub(crate) fn https(value: &str) -> TransportResult<Url> {
    let url = Url::parse(value).map_err(|_| BackupTransportError::Configuration("HTTPS URL"))?;
    (url.scheme() == "https"
        && url.host_str().is_some()
        && url.username().is_empty()
        && url.password().is_none()
        && url.fragment().is_none())
    .then_some(url)
    .ok_or(BackupTransportError::Configuration("HTTPS URL"))
}

pub(crate) async fn bounded(response: reqwest::Response, maximum: u64) -> TransportResult<Vec<u8>> {
    if response.content_length().is_some_and(|b| b > maximum) {
        return Err(BackupTransportError::InvalidResponse("response bytes"));
    }
    let (mut body, mut stream) = (Vec::new(), response.bytes_stream());
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| BackupTransportError::Transport)?;
        let next = body
            .len()
            .checked_add(chunk.len())
            .filter(|b| *b as u64 <= maximum)
            .ok_or(BackupTransportError::InvalidResponse("response bytes"))?;
        body.try_reserve_exact(next - body.len())
            .map_err(|_| BackupTransportError::InvalidResponse("response allocation"))?;
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

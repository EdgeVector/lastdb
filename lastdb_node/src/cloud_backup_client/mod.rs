//! HTTPS client, shared runtime connection pooling, and bounded storage transport.
use cloud_backup_protocol::{ErrorCode, ProtocolError};
use futures::StreamExt;
use reqwest::{header::HeaderMap, Client, Url};
use serde::de::DeserializeOwned;
use std::{
    fmt,
    sync::{Arc, Mutex},
    time::Duration,
};

mod guest_permission;
mod signed_blob;

pub(crate) use guest_permission::{authorization_post, PathPostTarget, ShareEnvironment};
pub use signed_blob::{canonical_backup_endpoint, SignedBlobClient};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
type TransportResult<T> = Result<T, BackupTransportError>;

static SHARED_HTTP_CLIENT: Mutex<Option<(tokio::runtime::Id, Client)>> = Mutex::new(None);
static STORAGE_HTTP_CLIENT: Mutex<Option<(tokio::runtime::Id, Client)>> = Mutex::new(None);

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

fn cached_runtime_client(
    slot: &Mutex<Option<(tokio::runtime::Id, Client)>>,
    build: impl FnOnce() -> Result<Client, reqwest::Error>,
) -> Result<Client, reqwest::Error> {
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        return build();
    };
    let runtime_id = handle.id();
    if let Ok(guard) = slot.lock() {
        if let Some((cached_id, client)) = guard.as_ref() {
            if *cached_id == runtime_id {
                return Ok(client.clone());
            }
        }
    }
    let client = build()?;
    if let Ok(mut guard) = slot.lock() {
        *guard = Some((runtime_id, client.clone()));
    }
    Ok(client)
}

pub fn build_shared_http_client() -> Result<Client, reqwest::Error> {
    cached_runtime_client(&SHARED_HTTP_CLIENT, || {
        // trace-egress: propagate — AuthClient and registration set headers on service requests.
        // trace-egress: skip-s3 — storage transfer preserves signed provider request headers.
        reqwest::Client::builder()
            .https_only(true)
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(15))
            .build()
    })
}

fn build_storage_http_client() -> TransportResult<Client> {
    cached_runtime_client(&STORAGE_HTTP_CLIENT, || {
        // trace-egress: propagate (control); skip-s3 (presigned object methods).
        reqwest::Client::builder()
            .https_only(true)
            .redirect(reqwest::redirect::Policy::none())
            .no_gzip()
            .no_brotli()
            .no_deflate()
            .no_zstd()
            .connect_timeout(REQUEST_TIMEOUT)
            .build()
    })
    .map_err(|_| BackupTransportError::Configuration("HTTP client"))
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

pub async fn read_bounded_bytes(
    response: reqwest::Response,
    max_bytes: u64,
) -> TransportResult<Vec<u8>> {
    if response.content_length().is_some_and(|b| b > max_bytes) {
        return Err(BackupTransportError::InvalidResponse("response bytes"));
    }
    let (mut body, mut stream) = (Vec::new(), response.bytes_stream());
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| BackupTransportError::Transport)?;
        let next = body
            .len()
            .checked_add(chunk.len())
            .filter(|b| *b as u64 <= max_bytes)
            .ok_or(BackupTransportError::InvalidResponse("response bytes"))?;
        body.try_reserve_exact(next - body.len())
            .map_err(|_| BackupTransportError::InvalidResponse("response allocation"))?;
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

#[derive(Clone)]
struct BackupTransport {
    auth: AuthClient,
    http: Client,
    base: Url,
}

impl BackupTransport {
    fn from_auth(auth: AuthClient) -> TransportResult<Self> {
        let base = https(&auth.base_url)?;
        if base.query().is_some() {
            return Err(BackupTransportError::Configuration("endpoint"));
        }
        let http = build_storage_http_client()?;
        Ok(Self { auth, http, base })
    }

    fn api_endpoint(&self, operation: &str) -> Url {
        let mut url = self.base.clone();
        url.set_path(&format!(
            "{}/api/{operation}",
            self.base.path().trim_end_matches('/')
        ));
        url
    }

    async fn post_bounded_with_headers<R: DeserializeOwned>(
        &self,
        url: Url,
        bytes: Vec<u8>,
        maximum: u64,
        headers: HeaderMap,
    ) -> TransportResult<R> {
        if bytes.len() as u64 > maximum {
            return Err(BackupTransportError::Configuration("request bytes"));
        }
        tokio::time::timeout(REQUEST_TIMEOUT, async {
            let resp = self
                .http
                .post(url)
                .headers(headers)
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .header("X-API-Key", self.auth.api_key.as_ref())
                .body(bytes)
                .send()
                .await
                .map_err(|_| BackupTransportError::Transport)?;
            let status = resp.status();
            let body = read_bounded_bytes(resp, maximum).await?;
            if status == reqwest::StatusCode::UNAUTHORIZED {
                return Err(BackupTransportError::Authentication);
            }
            if let Ok(error) = serde_json::from_slice::<ProtocolError>(&body) {
                return Err(BackupTransportError::Protocol(error));
            }
            if !status.is_success() {
                return Err(BackupTransportError::Http(status.as_u16()));
            }
            serde_json::from_slice(&body)
                .map_err(|_| BackupTransportError::InvalidResponse("JSON body"))
        })
        .await
        .map_err(|_| BackupTransportError::Timeout)?
    }
}

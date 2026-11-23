//! HTTPS client, shared runtime connection pooling, and bounded response reads.
use futures::StreamExt;
use reqwest::{Client, Url};
use std::{fmt, sync::Mutex, time::Duration};

type TransportResult<T> = Result<T, BackupTransportError>;

static SHARED_HTTP_CLIENT: Mutex<Option<(tokio::runtime::Id, Client)>> = Mutex::new(None);

/// The `/api/cloud/backups/v2/` endpoint of an Exemem API origin. A saved cloud connection
/// records it and compares it; no request goes there.
pub fn canonical_backup_endpoint(api_url: &str) -> TransportResult<String> {
    let mut url = https(api_url)?;
    if url.query().is_some() {
        return Err(BackupTransportError::Configuration("endpoint"));
    }
    url.set_path(&format!(
        "{}/api/cloud/backups/v2/",
        url.path().trim_end_matches('/')
    ));
    Ok(url.to_string())
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
        // trace-egress: propagate — registration sets headers on service requests.
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
    Transport,
    InvalidResponse(&'static str),
}

impl fmt::Display for BackupTransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Configuration(s) => write!(f, "native backup configuration: {s}"),
            Self::Transport => f.write_str("native backup transport failed"),
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

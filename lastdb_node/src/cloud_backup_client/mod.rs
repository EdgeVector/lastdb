//! HTTPS client and format-neutral bounded byte transport for LastDB cloud backup.
use cloud_backup_protocol::{bytes_sha256, Digest, ProtocolError};
use futures::StreamExt;
use reqwest::{header::HeaderMap, Client, Url};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::{
    fmt,
    io::{self, Read},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{io::AsyncReadExt, sync::OwnedSemaphorePermit};

pub mod byte_backup;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
type TransportResult<T> = Result<T, BackupTransportError>;
use BackupTransportError as E;

const _: () = {
    use cloud_backup_protocol::limits as p;
    use fold_db::storage_policy::{BACKUP_FILES_MAX, BACKUP_RESOURCES as r};
    assert!(
        p::NATIVE_FILES_MAX == BACKUP_FILES_MAX
            && p::DECODED_VALUES_MAX_BYTES == r.decoded_values_bytes
            && p::STREAM_BUFFER_MAX_BYTES == r.stream_buffer_bytes
            && p::DURABLE_LOCAL_SPOOL_MAX_BYTES == r.durable_spool_bytes
    );
};

#[derive(Clone)]
pub struct AuthClient {
    base_url: String,
    api_key: Arc<str>,
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
    // trace-egress: propagate — AuthClient injects W3C on its service requests.
    // trace-egress: skip-s3 — byte transport preserves signed provider request headers.
    Client::builder()
        .https_only(true)
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(15))
        .build()
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
    Source,
}

impl From<ProtocolError> for BackupTransportError {
    fn from(v: ProtocolError) -> Self {
        Self::Protocol(v)
    }
}

impl fmt::Display for BackupTransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Configuration(s) => write!(f, "native backup configuration: {s}"),
            Self::Protocol(e) => write!(f, "native backup service error: {:?}", e.code),
            Self::Authentication => f.write_str("native backup authentication refused"),
            Self::Timeout => f.write_str("native backup request deadline expired"),
            Self::Transport => f.write_str("native backup transport failed"),
            Self::Http(s) => write!(f, "native backup HTTP status {s}"),
            Self::InvalidResponse(s) => write!(f, "native backup invalid response: {s}"),
            Self::Integrity(s) => write!(f, "native backup byte integrity: {s}"),
            Self::Source => f.write_str("native backup source read failed"),
        }
    }
}

impl fmt::Debug for BackupTransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
impl std::error::Error for BackupTransportError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UploadObservation {
    HttpSuccess,
    AlreadyExists,
}

#[derive(Deserialize)]
struct ErrorTag<'a> {
    result: Option<&'a str>,
}

#[derive(Clone)]
pub(crate) struct BackupTransport {
    auth: Option<AuthClient>,
    http: Arc<Client>,
    base: Url,
    uploads: Arc<tokio::sync::Semaphore>,
}

impl BackupTransport {
    pub(crate) fn from_auth(auth: AuthClient) -> TransportResult<Self> {
        Self::new(https(&auth.base_url)?, Some(auth))
    }

    pub(crate) fn for_guest(base_url: &str) -> TransportResult<Self> {
        Self::new(https(base_url)?, None)
    }

    fn new(base: Url, auth: Option<AuthClient>) -> TransportResult<Self> {
        if base.query().is_some() {
            return Err(E::Configuration("endpoint"));
        }
        // trace-egress: propagate (control); skip-s3 (presigned object methods).
        let http = Client::builder()
            .https_only(true)
            .redirect(reqwest::redirect::Policy::none())
            .no_gzip()
            .no_brotli()
            .no_deflate()
            .no_zstd()
            .connect_timeout(REQUEST_TIMEOUT)
            .build()
            .map_err(|_| E::Configuration("HTTP client"))?;
        let streams = fold_db::storage_policy::BACKUP_RESOURCES.concurrent_streams;
        Ok(Self {
            auth,
            http: Arc::new(http),
            base,
            uploads: Arc::new(tokio::sync::Semaphore::new(streams)),
        })
    }

    pub(crate) fn api_endpoint(&self, operation: &str) -> Url {
        let mut url = self.base.clone();
        url.set_path(&format!(
            "{}/api/{operation}",
            self.base.path().trim_end_matches('/')
        ));
        url
    }

    pub(crate) async fn post_bounded_with_headers<Q: Serialize, R: DeserializeOwned>(
        &self,
        url: Url,
        request: &Q,
        maximum: u64,
        headers: HeaderMap,
    ) -> TransportResult<R> {
        let bytes = cloud_backup_protocol::canonical_bytes(request)?;
        if bytes.len() as u64 > maximum {
            return Err(E::Configuration("request bytes"));
        }
        tokio::time::timeout(REQUEST_TIMEOUT, async {
            let mut req = self
                .http
                .post(url)
                .headers(headers)
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(bytes);
            if let Some(auth) = &self.auth {
                req = req.header("X-API-Key", auth.api_key.as_ref());
            }
            let resp = observability::propagation::inject_w3c(req)
                .send()
                .await
                .map_err(|_| E::Transport)?;
            let status = resp.status();
            let body = bounded(resp, maximum).await?;
            if status == reqwest::StatusCode::UNAUTHORIZED {
                return Err(E::Authentication);
            }
            if let Ok(error) = serde_json::from_slice::<ProtocolError>(&body) {
                return Err(E::Protocol(error));
            }
            let tagged = serde_json::from_slice::<ErrorTag<'_>>(&body)
                .is_ok_and(|t| t.result == Some("error"));
            if !status.is_success() && !tagged {
                return Err(E::Http(status.as_u16()));
            }
            serde_json::from_slice(&body).map_err(|_| E::InvalidResponse("JSON body"))
        })
        .await
        .map_err(|_| E::Timeout)?
    }

    pub(crate) async fn upload(
        &self,
        sha256: &Digest,
        bytes: u64,
        grant_url: &str,
        expires_ms: u64,
        source: Box<dyn Read + Send>,
        multipart_part: bool,
    ) -> TransportResult<UploadObservation> {
        let (url, if_none_match) = upload_url(grant_url, multipart_part)?;
        let mut data = Vec::new();
        source
            .take(bytes.saturating_add(1))
            .read_to_end(&mut data)
            .map_err(|_| E::Source)?;
        if data.len() as u64 != bytes || bytes_sha256(&data) != *sha256 {
            return Err(E::Integrity("upload source"));
        }
        let (deadline, _permit) = self.acquire(bytes, expires_ms).await?;
        let mut req = self
            .http
            .put(url)
            .header(reqwest::header::CONTENT_LENGTH, bytes);
        if if_none_match {
            req = req.header(reqwest::header::IF_NONE_MATCH, "*");
        }
        let resp = tokio::time::timeout_at(deadline, req.body(data).send())
            .await
            .map_err(|_| E::Timeout)?
            .map_err(|_| E::Transport)?;
        match resp.status() {
            s if s.is_success() => Ok(UploadObservation::HttpSuccess),
            reqwest::StatusCode::PRECONDITION_FAILED => Ok(UploadObservation::AlreadyExists),
            s => Err(E::Http(s.as_u16())),
        }
    }

    async fn acquire(
        &self,
        bytes: u64,
        expires_ms: u64,
    ) -> TransportResult<(tokio::time::Instant, OwnedSemaphorePermit)> {
        let deadline = tokio::time::Instant::now() + transfer_timeout(bytes, expires_ms)?;
        let permit = tokio::time::timeout_at(deadline, Arc::clone(&self.uploads).acquire_owned())
            .await
            .map_err(|_| E::Timeout)?
            .map_err(|_| E::Transport)?;
        Ok((deadline, permit))
    }

    pub(crate) async fn download_to(
        &self,
        bytes: u64,
        expected: &Digest,
        grant_url: &str,
        expires_ms: u64,
        out: &mut impl io::Write,
    ) -> TransportResult<()> {
        let url = https(grant_url)?;
        if signed_headers(&url)? != "host" {
            return Err(E::InvalidResponse("signed headers"));
        }
        let (deadline, _permit) = self.acquire(bytes, expires_ms).await?;
        let resp = tokio::time::timeout_at(deadline, self.http.get(url).send())
            .await
            .map_err(|_| E::Timeout)?
            .map_err(|_| E::Transport)?;
        if resp.status() != reqwest::StatusCode::OK {
            return Err(E::Http(resp.status().as_u16()));
        }
        if resp.content_length().is_some_and(|len| len != bytes) {
            return Err(E::Integrity("download length"));
        }
        let cap = (bytes.min(fold_db::storage_policy::BACKUP_RESOURCES.stream_buffer_bytes)
            as usize)
            .max(1);
        let mut reader = tokio_util::io::StreamReader::new(
            resp.bytes_stream()
                .map(|i| i.map_err(|_| io::Error::other("native object transport failed"))),
        );
        let (mut buf, mut done, mut hash) = (vec![0u8; cap], 0u64, Sha256::new());
        loop {
            let n = tokio::time::timeout_at(deadline, reader.read(&mut buf))
                .await
                .map_err(|_| E::Timeout)?
                .map_err(|_| E::Transport)?;
            if n == 0 {
                break;
            }
            done = done
                .checked_add(n as u64)
                .filter(|&d| d <= bytes)
                .ok_or(E::Integrity("download length"))?;
            hash.update(&buf[..n]);
            out.write_all(&buf[..n]).map_err(|_| E::Source)?;
        }
        (done == bytes && fold_db::hex::hex_lower(hash.finalize()) == expected.as_str())
            .then_some(())
            .ok_or(E::Integrity("download digest"))
    }
}

pub(crate) fn https(value: &str) -> TransportResult<Url> {
    let u = Url::parse(value).map_err(|_| E::Configuration("HTTPS URL"))?;
    (u.scheme() == "https"
        && u.host_str().is_some()
        && u.username().is_empty()
        && u.password().is_none()
        && u.fragment().is_none())
    .then_some(u)
    .ok_or(E::Configuration("HTTPS URL"))
}

fn signed_headers(url: &Url) -> TransportResult<String> {
    let mut it = url
        .query_pairs()
        .filter(|(k, _)| k.eq_ignore_ascii_case("x-amz-signedheaders"));
    if let (Some((_, h)), None) = (it.next(), it.next()) {
        Ok(h.into_owned())
    } else {
        Err(E::InvalidResponse("signed headers"))
    }
}

fn upload_url(value: &str, multipart: bool) -> TransportResult<(Url, bool)> {
    let url = https(value)?;
    let if_none_match = match (multipart, signed_headers(&url)?.as_str()) {
        (false, "content-length;host;if-none-match") => true,
        (_, "content-length;host") => false,
        _ => return Err(E::InvalidResponse("signed headers")),
    };
    let (mut up, mut pt) = (0usize, 0usize);
    for (k, v) in url.query_pairs() {
        up += usize::from(k == "uploadId") * if v.is_empty() { 2 } else { 1 };
        pt += usize::from(k == "partNumber") * if v == "1" { 1 } else { 2 };
    }
    let expected = usize::from(multipart);
    ((up, pt) == (expected, expected))
        .then_some((url, if_none_match))
        .ok_or(E::InvalidResponse("upload operation"))
}

pub(crate) async fn bounded(response: reqwest::Response, maximum: u64) -> TransportResult<Vec<u8>> {
    if response.content_length().is_some_and(|b| b > maximum) {
        return Err(E::InvalidResponse("response bytes"));
    }
    let (mut body, mut stream) = (Vec::new(), response.bytes_stream());
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| E::Transport)?;
        let next = body
            .len()
            .checked_add(chunk.len())
            .filter(|b| *b as u64 <= maximum)
            .ok_or(E::InvalidResponse("response bytes"))?;
        body.try_reserve_exact(next - body.len())
            .map_err(|_| E::InvalidResponse("response allocation"))?;
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn transfer_timeout(bytes: u64, expires_ms: u64) -> TransportResult<Duration> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| E::Configuration("clock"))?
        .as_millis();
    let rem = u128::from(expires_ms)
        .checked_sub(now)
        .and_then(|ms| u64::try_from(ms / 1000).ok())
        .filter(|&s| s > 0)
        .ok_or(E::Timeout)?;
    usize::try_from(bytes).map_err(|_| E::Configuration("platform size"))?;
    let timeout = Duration::from_secs(300.max(30u64.saturating_add(bytes.div_ceil(256 * 1024))));
    let (exp, max) = (
        Duration::from_secs(rem),
        Duration::from_secs(rem.saturating_sub(30)),
    );
    Ok(timeout.min(if max.is_zero() { exp } else { max }))
}

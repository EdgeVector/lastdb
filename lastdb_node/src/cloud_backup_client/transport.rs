//! Authenticated ExeMem control-plane HTTP calls and presigned S3 object PUTs.
use super::{
    bounded, https, AuthClient, BackupTransportError as E, TransportResult, UploadObservation,
    REQUEST_TIMEOUT,
};
use cloud_backup_protocol::{bytes_sha256, Digest, ProtocolError};
use reqwest::{header::HeaderMap, Client, Url};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    io::Read,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::OwnedSemaphorePermit;

static STORAGE_HTTP_CLIENT: Mutex<Option<(tokio::runtime::Id, Client)>> = Mutex::new(None);

pub(crate) fn cached_runtime_client(
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
    .map_err(|_| E::Configuration("HTTP client"))
}

#[derive(Deserialize)]
struct ErrorTag<'a> {
    result: Option<&'a str>,
}

pub(crate) struct PresignedPut<'a> {
    pub sha256: &'a Digest,
    pub bytes: u64,
    pub grant_url: &'a str,
    pub expires_ms: u64,
    pub multipart_part: bool,
}

#[derive(Clone)]
pub(crate) struct BackupTransport {
    auth: AuthClient,
    http: Client,
    base: Url,
    uploads: Arc<tokio::sync::Semaphore>,
}

impl BackupTransport {
    pub(crate) fn from_auth(auth: AuthClient) -> TransportResult<Self> {
        let base = https(&auth.base_url)?;
        if base.query().is_some() {
            return Err(E::Configuration("endpoint"));
        }
        let http = build_storage_http_client()?;
        let streams = fold_db::storage_policy::BACKUP_RESOURCES.concurrent_streams;
        Ok(Self {
            auth,
            http,
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
            let req = self
                .http
                .post(url)
                .headers(headers)
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .header("X-API-Key", self.auth.api_key.as_ref())
                .body(bytes);
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
        put: &PresignedPut<'_>,
        source: Box<dyn Read + Send>,
    ) -> TransportResult<UploadObservation> {
        let (url, if_none_match) = upload_url(put.grant_url, put.multipart_part)?;
        let mut data = Vec::new();
        source
            .take(put.bytes.saturating_add(1))
            .read_to_end(&mut data)
            .map_err(|_| E::Source)?;
        if data.len() as u64 != put.bytes || bytes_sha256(&data) != *put.sha256 {
            return Err(E::Integrity("upload source"));
        }
        let (deadline, _permit) = self.acquire(put.bytes, put.expires_ms).await?;
        let mut req = self
            .http
            .put(url)
            .header(reqwest::header::CONTENT_LENGTH, put.bytes);
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

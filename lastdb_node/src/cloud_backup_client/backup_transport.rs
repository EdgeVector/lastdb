//! Format-neutral bounded byte transport.
use super::{
    abort, bounded, https, signed_headers, start_feeder, upload_url, AuthClient, TransportResult,
    DEFAULT_AUTH_REQUEST_TIMEOUT,
};
pub use super::{BackupTransportError, UploadObservation};
use cloud_backup_protocol::v2::limits::CONTROL_JSON_MAX_BYTES;
use cloud_backup_protocol::{Digest, ProtocolError};
use futures::StreamExt;
use reqwest::{header::HeaderMap, Client, Url};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::future::Future;
use std::io::{self, Read};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncRead, ReadBuf};
use tokio::sync::OwnedSemaphorePermit;

pub type ObjectStream = Pin<Box<dyn AsyncRead + Send>>;

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
    pub fn from_auth(auth: AuthClient) -> TransportResult<Self> {
        Self::new(https(&auth.base_url)?, Some(auth))
    }

    pub(crate) fn for_guest(base_url: &str) -> TransportResult<Self> {
        Self::new(https(base_url)?, None)
    }

    fn new(base: Url, auth: Option<AuthClient>) -> TransportResult<Self> {
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
        let streams = fold_db::storage_policy::BACKUP_RESOURCES.concurrent_streams;
        Ok(Self {
            timeout: DEFAULT_AUTH_REQUEST_TIMEOUT,
            auth,
            http: Arc::new(http),
            base,
            uploads: Arc::new(tokio::sync::Semaphore::new(streams)),
        })
    }

    pub fn endpoint(&self, operation: &str) -> String {
        self.api_endpoint(operation).to_string()
    }

    pub(crate) fn api_endpoint(&self, operation: &str) -> Url {
        let mut url = self.base.clone();
        url.set_path(&format!(
            "{}/api/{operation}",
            self.base.path().trim_end_matches('/')
        ));
        url
    }

    pub(super) async fn post_v2<Q: Serialize, R: DeserializeOwned>(
        &self,
        op: &str,
        request: &Q,
        headers: HeaderMap,
    ) -> TransportResult<R> {
        let url = self.api_endpoint(&format!("cloud/backups/v2/{op}"));
        self.post_bounded_with_headers(url, request, CONTROL_JSON_MAX_BYTES, headers)
            .await
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
    ) -> TransportResult<R> {
        let mut req = self
            .http
            .post(url.clone())
            .headers(headers)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(bytes);
        if let Some(auth) = &self.auth {
            req = auth.apply_auth(req);
        }
        let resp = observability::propagation::inject_w3c(req)
            .send()
            .await
            .map_err(|_| BackupTransportError::Transport)?;
        let status = resp.status();
        let body = bounded(resp, maximum).await?;
        if status == reqwest::StatusCode::UNAUTHORIZED {
            return Err(BackupTransportError::Authentication);
        }
        if let Ok(error) = serde_json::from_slice::<ProtocolError>(&body) {
            return Err(BackupTransportError::Protocol(error));
        }
        let tagged =
            serde_json::from_slice::<ErrorTag<'_>>(&body).is_ok_and(|t| t.result == Some("error"));
        if !status.is_success() && !tagged {
            return Err(BackupTransportError::Http(status.as_u16()));
        }
        serde_json::from_slice(&body)
            .map_err(|_| BackupTransportError::InvalidResponse("JSON body"))
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
        let (deadline, permit) = self.acquire(bytes, expires_ms).await?;
        let (body, mut feeder, cancel) = start_feeder(source, bytes, sha256.clone(), permit);
        let mut req = self
            .http
            .put(url)
            .header(reqwest::header::CONTENT_LENGTH, bytes);
        if if_none_match {
            req = req.header(reqwest::header::IF_NONE_MATCH, "*");
        }
        let resp = match tokio::time::timeout_at(deadline, req.body(body).send()).await {
            Ok(Ok(r)) => r,
            Err(_) => return abort(cancel, feeder, Err(BackupTransportError::Timeout)).await,
            Ok(Err(_)) => return abort(cancel, feeder, Err(BackupTransportError::Transport)).await,
        };
        let status = resp.status();
        drop(resp);
        if !status.is_success() {
            let res = (status == reqwest::StatusCode::PRECONDITION_FAILED)
                .then_some(UploadObservation::AlreadyExists)
                .ok_or_else(|| BackupTransportError::Http(status.as_u16()));
            return abort(cancel, feeder, res).await;
        }
        let Ok(res) = tokio::time::timeout_at(deadline, &mut feeder).await else {
            return abort(cancel, feeder, Err(BackupTransportError::Timeout)).await;
        };
        res.map_err(|_| BackupTransportError::Task)??;
        Ok(UploadObservation::HttpSuccess)
    }

    async fn acquire(
        &self,
        bytes: u64,
        expires_ms: u64,
    ) -> TransportResult<(tokio::time::Instant, OwnedSemaphorePermit)> {
        let deadline = tokio::time::Instant::now() + self.transfer_timeout(bytes, expires_ms)?;
        let permit = tokio::time::timeout_at(deadline, Arc::clone(&self.uploads).acquire_owned())
            .await
            .map_err(|_| BackupTransportError::Timeout)?
            .map_err(|_| BackupTransportError::Task)?;
        Ok((deadline, permit))
    }

    fn transfer_timeout(&self, bytes: u64, expires_ms: u64) -> TransportResult<Duration> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| BackupTransportError::Configuration("clock"))?
            .as_millis();
        let rem = u128::from(expires_ms)
            .checked_sub(now)
            .and_then(|ms| u64::try_from(ms / 1000).ok())
            .filter(|&s| s > 0)
            .ok_or(BackupTransportError::Timeout)?;
        let size = usize::try_from(bytes)
            .map_err(|_| BackupTransportError::Configuration("platform size"))?;
        let extra = Duration::from_secs((size as u64).div_ceil(256 * 1024));
        let timeout = self
            .timeout
            .saturating_mul(10)
            .max(self.timeout.saturating_add(extra));
        let exp = Duration::from_secs(rem);
        let max = exp.saturating_sub(Duration::from_secs(30));
        Ok(timeout.min(if max.is_zero() {
            exp.max(Duration::from_secs(1))
        } else {
            max
        }))
    }

    pub(crate) async fn open_bounded(
        &self,
        bytes: u64,
        expected: Option<Digest>,
        grant_url: &str,
        expires_ms: u64,
    ) -> TransportResult<ObjectStream> {
        let url = https(grant_url)?;
        if signed_headers(&url)? != "host" {
            return Err(BackupTransportError::InvalidResponse("signed headers"));
        }
        let (deadline, permit) = self.acquire(bytes, expires_ms).await?;
        let resp = tokio::time::timeout_at(deadline, self.http.get(url).send())
            .await
            .map_err(|_| BackupTransportError::Timeout)?
            .map_err(|_| BackupTransportError::Transport)?;
        if resp.status() != reqwest::StatusCode::OK {
            return Err(BackupTransportError::Http(resp.status().as_u16()));
        }
        if resp.content_length().is_some_and(|len| len != bytes) {
            return Err(BackupTransportError::Integrity("download length"));
        }
        let stream = resp
            .bytes_stream()
            .map(|item| item.map_err(|_| io::Error::other("native object transport failed")));
        Ok(Box::pin(Bounded {
            input: Box::pin(tokio_util::io::StreamReader::new(stream)),
            deadline: Box::pin(tokio::time::sleep_until(deadline)),
            left: bytes,
            hash: Sha256::new(),
            expected,
            finished: false,
            _permit: permit,
        }))
    }
}

struct Bounded {
    input: ObjectStream,
    deadline: Pin<Box<tokio::time::Sleep>>,
    left: u64,
    hash: Sha256,
    expected: Option<Digest>,
    finished: bool,
    _permit: OwnedSemaphorePermit,
}

impl AsyncRead for Bounded {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if output.remaining() == 0 || this.finished {
            return Poll::Ready(Ok(()));
        }
        if this.deadline.as_mut().poll(cx).is_ready() {
            this.finished = true;
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "object deadline",
            )));
        }
        if this.left == 0 {
            let mut extra = [0u8; 1];
            let mut buf = ReadBuf::new(&mut extra);
            return match this.input.as_mut().poll_read(cx, &mut buf) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(res) => {
                    this.finished = true;
                    let bad = this.expected.as_ref().is_some_and(|exp| {
                        fold_db::hex::hex_lower(this.hash.clone().finalize()) != exp.as_str()
                    });
                    if res.is_err() || !buf.filled().is_empty() || bad {
                        Poll::Ready(Err(io::Error::other("native object bytes differ")))
                    } else {
                        Poll::Ready(Ok(()))
                    }
                }
            };
        }
        let sbuf = fold_db::storage_policy::BACKUP_RESOURCES.stream_buffer_bytes;
        let max = this.left.min(output.remaining() as u64).min(sbuf) as usize;
        let mut buf = ReadBuf::new(output.initialize_unfilled_to(max));
        match this.input.as_mut().poll_read(cx, &mut buf) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(_)) => {
                this.finished = true;
                return Poll::Ready(Err(io::Error::other("native object read failed")));
            }
            Poll::Ready(Ok(())) => {}
        }
        let count = buf.filled().len();
        this.hash.update(buf.filled());
        if count == 0 {
            this.finished = true;
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "truncated",
            )));
        }
        this.left -= count as u64;
        output.advance(count);
        Poll::Ready(Ok(()))
    }
}

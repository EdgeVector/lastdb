use super::*;
use futures::StreamExt;
use sha2::{Digest as _, Sha256};
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncRead, ReadBuf};

pub type ObjectStream = Pin<Box<dyn AsyncRead + Send>>;

pub(super) fn is_multipart(value: &str) -> Result<bool> {
    let url = https(value)?;
    let parts: Vec<_> = url.query_pairs().collect();
    let uploads = parts.iter().filter(|(key, _)| key == "uploadId").count();
    let numbers = parts.iter().filter(|(key, _)| key == "partNumber").count();
    match (uploads, numbers) {
        (0, 0) => Ok(false),
        (1, 1) => Ok(true),
        _ => Err(BackupTransportError::InvalidResponse("upload operation")),
    }
}

pub(super) fn https(value: &str) -> Result<Url> {
    let url = Url::parse(value).map_err(|_| BackupTransportError::Configuration("HTTPS URL"))?;
    if url.scheme() != "https"
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(BackupTransportError::Configuration("HTTPS URL"));
    }
    Ok(url)
}

pub(super) fn upload_url(value: &str, multipart_part: bool) -> Result<(Url, bool)> {
    let url = https(value)?;
    let names: Vec<_> = url
        .query_pairs()
        .filter(|(name, _)| name.eq_ignore_ascii_case("x-amz-signedheaders"))
        .map(|(_, value)| value.into_owned())
        .collect();
    if names.len() != 1 {
        return Err(BackupTransportError::InvalidResponse("signed headers"));
    }
    let if_none_match = match (multipart_part, names[0].as_str()) {
        (false, "content-length;host;if-none-match") => true,
        (true | false, "content-length;host") => false,
        _ => return Err(BackupTransportError::InvalidResponse("signed headers")),
    };
    let parts: Vec<_> = url.query_pairs().collect();
    let uploads: Vec<_> = parts.iter().filter(|(key, _)| key == "uploadId").collect();
    let numbers: Vec<_> = parts
        .iter()
        .filter(|(key, _)| key == "partNumber")
        .collect();
    let valid = if multipart_part {
        uploads.len() == 1 && !uploads[0].1.is_empty() && numbers.len() == 1 && numbers[0].1 == "1"
    } else {
        uploads.is_empty() && numbers.is_empty()
    };
    if !valid {
        return Err(BackupTransportError::InvalidResponse("upload operation"));
    }
    drop(parts);
    Ok((url, if_none_match))
}

fn signed_download(value: &str) -> Result<Url> {
    let url = https(value)?;
    let names: Vec<_> = url
        .query_pairs()
        .filter(|(name, _)| name.eq_ignore_ascii_case("x-amz-signedheaders"))
        .map(|(_, value)| value.into_owned())
        .collect();
    if names.len() != 1 || names[0] != "host" {
        return Err(BackupTransportError::InvalidResponse("signed headers"));
    }
    Ok(url)
}

impl BackupTransport {
    pub(crate) fn api_endpoint(&self, operation: &str) -> Url {
        let mut url = self.base.clone();
        let base = self.base.path().trim_end_matches('/');
        url.set_path(&format!("{base}/api/{operation}"));
        url
    }

    pub(super) fn transfer_timeout(&self, bytes: u64, expires_ms: u64) -> Result<Duration> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| BackupTransportError::Configuration("clock"))?
            .as_millis();
        let remaining = u128::from(expires_ms)
            .checked_sub(now)
            .filter(|value| *value > 0)
            .ok_or(BackupTransportError::Timeout)?;
        let seconds = u64::try_from(remaining / 1000).map_err(|_| BackupTransportError::Timeout)?;
        if seconds == 0 {
            return Err(BackupTransportError::Timeout);
        }
        let size = usize::try_from(bytes)
            .map_err(|_| BackupTransportError::Configuration("platform size"))?;
        Ok(transfer_budget(self.timeout, size, seconds))
    }

    pub(crate) async fn open(
        &self,
        object: StoredObject<'_>,
        grant: TemporaryGrant<'_>,
    ) -> Result<ObjectStream> {
        self.open_bounded(object.bytes, Some(object.sha256.clone()), grant)
            .await
    }

    /// Read exactly `bytes` bytes with no stored SHA-256 (record blob
    /// objects). The caller authenticates the content itself.
    pub(crate) async fn open_exact_length(
        &self,
        bytes: u64,
        grant: TemporaryGrant<'_>,
    ) -> Result<ObjectStream> {
        self.open_bounded(bytes, None, grant).await
    }

    async fn open_bounded(
        &self,
        bytes: u64,
        expected: Option<cloud_backup_protocol::Digest>,
        grant: TemporaryGrant<'_>,
    ) -> Result<ObjectStream> {
        let url = signed_download(grant.url)?;
        let budget = self.transfer_timeout(bytes, grant.expires_ms)?;
        let deadline = tokio::time::Instant::now() + budget;
        let permit = tokio::time::timeout_at(deadline, Arc::clone(&self.uploads).acquire_owned())
            .await
            .map_err(|_| BackupTransportError::Timeout)?
            .map_err(|_| BackupTransportError::Task)?;
        let response = tokio::time::timeout_at(deadline, self.http.get(url).send())
            .await
            .map_err(|_| BackupTransportError::Timeout)?
            .map_err(|_| BackupTransportError::Transport)?;
        if response.status() != reqwest::StatusCode::OK {
            return Err(BackupTransportError::Http(response.status().as_u16()));
        }
        if response
            .content_length()
            .is_some_and(|length| length != bytes)
        {
            return Err(BackupTransportError::Integrity("download length"));
        }
        let stream = response
            .bytes_stream()
            .map(|item| item.map_err(|_| io::Error::other("native object transport failed")));
        let input = tokio_util::io::StreamReader::new(stream);
        Ok(Box::pin(Bounded {
            input: Box::pin(input),
            deadline: Box::pin(tokio::time::sleep_until(deadline)),
            left: bytes,
            hash: Sha256::new(),
            expected,
            finished: false,
            _permit: permit,
        }))
    }
}

// Preserve the established byte-transfer deadline without the legacy S3 client.
fn transfer_budget(request_timeout: Duration, bytes: usize, expires_in_secs: u64) -> Duration {
    let transfer_secs = (bytes as u64).saturating_add(256 * 1024 - 1) / (256 * 1024);
    let slow_uplink_floor = request_timeout.saturating_mul(10);
    let size_budget = request_timeout.saturating_add(Duration::from_secs(transfer_secs));
    let timeout = slow_uplink_floor.max(size_budget);
    let expires_in = Duration::from_secs(expires_in_secs);
    let maximum = expires_in.saturating_sub(Duration::from_secs(30));
    if maximum.is_zero() {
        let remaining = if expires_in.is_zero() {
            Duration::from_secs(1)
        } else {
            expires_in
        };
        timeout.min(remaining)
    } else {
        timeout.min(maximum)
    }
}

struct Bounded {
    input: ObjectStream,
    deadline: Pin<Box<tokio::time::Sleep>>,
    left: u64,
    hash: Sha256,
    /// `None` checks the exact length only.
    expected: Option<cloud_backup_protocol::Digest>,
    finished: bool,
    _permit: tokio::sync::OwnedSemaphorePermit,
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
                "native object deadline",
            )));
        }
        if this.left == 0 {
            return this.eof(cx);
        }
        let maximum = this
            .left
            .min(output.remaining() as u64)
            .min(fold_db::storage_policy::BACKUP_RESOURCES.stream_buffer_bytes)
            as usize;
        let count = {
            let mut buffer = ReadBuf::new(output.initialize_unfilled_to(maximum));
            match this.input.as_mut().poll_read(cx, &mut buffer) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(_)) => {
                    this.finished = true;
                    return Poll::Ready(Err(io::Error::other("native object read failed")));
                }
                Poll::Ready(Ok(())) => {}
            }
            this.hash.update(buffer.filled());
            buffer.filled().len()
        };
        if count == 0 {
            this.finished = true;
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "native object truncated",
            )));
        }
        this.left -= count as u64;
        output.advance(count);
        Poll::Ready(Ok(()))
    }
}
impl Bounded {
    fn eof(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut extra = [0u8; 1];
        let mut buffer = ReadBuf::new(&mut extra);
        match self.input.as_mut().poll_read(cx, &mut buffer) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                self.finished = true;
                if result.is_err()
                    || !buffer.filled().is_empty()
                    || self.expected.as_ref().is_some_and(|expected| {
                        fold_db::hex::hex_lower(self.hash.clone().finalize()) != expected.as_str()
                    })
                {
                    Poll::Ready(Err(io::Error::other("native object bytes differ")))
                } else {
                    Poll::Ready(Ok(()))
                }
            }
        }
    }
}

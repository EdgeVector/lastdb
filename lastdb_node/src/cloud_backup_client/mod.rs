//! HTTPS client for the native LastDB cloud backup service.
use std::sync::Arc;
use std::time::Duration;

mod backup_transport;
pub mod byte_backup;

pub(crate) const DEFAULT_AUTH_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const SHARED_HTTP_CONNECT_TIMEOUT: Duration = Duration::from_secs(15);

const _: () = assert!(
    cloud_backup_protocol::limits::NATIVE_FILES_MAX == fold_db::storage_policy::BACKUP_FILES_MAX
        && cloud_backup_protocol::limits::DECODED_VALUES_MAX_BYTES
            == fold_db::storage_policy::BACKUP_RESOURCES.decoded_values_bytes
        && cloud_backup_protocol::limits::STREAM_BUFFER_MAX_BYTES
            == fold_db::storage_policy::BACKUP_RESOURCES.stream_buffer_bytes
        && cloud_backup_protocol::limits::DURABLE_LOCAL_SPOOL_MAX_BYTES
            == fold_db::storage_policy::BACKUP_RESOURCES.durable_spool_bytes
);

/// Explicit account credentials only. Construction starts no request or claim.
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

    pub(crate) fn apply_auth(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        request.header("X-API-Key", self.api_key.as_ref())
    }
}

/// Protected HTTP construction for explicit account and V2 byte operations.
pub fn build_shared_http_client() -> Result<reqwest::Client, reqwest::Error> {
    // trace-egress: propagate — AuthClient injects W3C on its service requests.
    // trace-egress: skip-s3 — byte transport preserves signed provider request headers.
    reqwest::Client::builder()
        .https_only(true)
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(SHARED_HTTP_CONNECT_TIMEOUT)
        .build()
}

mod wire {
    use cloud_backup_protocol::{Digest, ProtocolError};
    use futures::StreamExt;
    use reqwest::Url;
    use sha2::{Digest as _, Sha256};
    use std::io::{self, Read};
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };
    use tokio::sync::{mpsc, Notify, OwnedSemaphorePermit};

    pub(super) type Result<T> = std::result::Result<T, BackupTransportError>;

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
        Source,
        Task,
    }
    impl From<ProtocolError> for BackupTransportError {
        fn from(v: ProtocolError) -> Self {
            Self::Protocol(v)
        }
    }
    impl std::fmt::Display for BackupTransportError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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

    pub(super) fn https(value: &str) -> Result<Url> {
        let url =
            Url::parse(value).map_err(|_| BackupTransportError::Configuration("HTTPS URL"))?;
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

    pub(super) fn signed_headers(url: &Url) -> Result<String> {
        let mut it = url
            .query_pairs()
            .filter(|(k, _)| k.eq_ignore_ascii_case("x-amz-signedheaders"))
            .map(|(_, v)| v.into_owned());
        match (it.next(), it.next()) {
            (Some(h), None) => Ok(h),
            _ => Err(BackupTransportError::InvalidResponse("signed headers")),
        }
    }

    pub(super) fn upload_url(value: &str, multipart_part: bool) -> Result<(Url, bool)> {
        let url = https(value)?;
        let if_none_match = match (multipart_part, signed_headers(&url)?.as_str()) {
            (false, "content-length;host;if-none-match") => true,
            (true | false, "content-length;host") => false,
            _ => return Err(BackupTransportError::InvalidResponse("signed headers")),
        };
        let (mut uploads, mut parts, mut non_empty, mut part_one) = (0usize, 0usize, false, false);
        for (k, v) in url.query_pairs() {
            match k.as_ref() {
                "uploadId" => (uploads, non_empty) = (uploads + 1, !v.is_empty()),
                "partNumber" => (parts, part_one) = (parts + 1, v == "1"),
                _ => {}
            }
        }
        let valid = if multipart_part {
            uploads == 1 && non_empty && parts == 1 && part_one
        } else {
            uploads == 0 && parts == 0
        };
        valid
            .then_some((url, if_none_match))
            .ok_or(BackupTransportError::InvalidResponse("upload operation"))
    }

    pub(super) struct Stop {
        stopped: AtomicBool,
        changed: Notify,
    }
    pub(super) struct Cancel(Arc<Stop>);
    impl Drop for Cancel {
        fn drop(&mut self) {
            self.0.stopped.store(true, Ordering::Release);
            self.0.changed.notify_one();
        }
    }

    pub(super) async fn abort<T>(
        cancel: Cancel,
        feeder: tokio::task::JoinHandle<Result<()>>,
        outcome: Result<T>,
    ) -> Result<T> {
        drop(cancel);
        let _ = feeder.await;
        outcome
    }

    pub(super) fn start_feeder(
        source: Box<dyn Read + Send>,
        bytes: u64,
        expected: Digest,
        permit: OwnedSemaphorePermit,
    ) -> (reqwest::Body, tokio::task::JoinHandle<Result<()>>, Cancel) {
        let (sender, receiver) = mpsc::channel::<io::Result<Vec<u8>>>(1);
        let cancel = Arc::new(Stop {
            stopped: AtomicBool::new(false),
            changed: Notify::new(),
        });
        let (stopped, span) = (Arc::clone(&cancel), tracing::Span::current());
        let task = tokio::task::spawn_blocking(move || {
            span.in_scope(|| {
                let _permit = permit;
                let res = produce(source, bytes, &expected, &sender, &stopped);
                if res.is_err() && !stopped.stopped.load(Ordering::Acquire) {
                    let _ = send(
                        &sender,
                        Err(io::Error::other("native backup source failed")),
                        &stopped,
                    );
                }
                res
            })
        });
        let stream = futures::stream::unfold(receiver, |mut rx| async move {
            rx.recv().await.map(|v| (v, rx))
        });
        (reqwest::Body::wrap_stream(stream), task, Cancel(cancel))
    }

    fn produce(
        mut source: Box<dyn Read + Send>,
        mut left: u64,
        expected: &Digest,
        sender: &mpsc::Sender<io::Result<Vec<u8>>>,
        cancel: &Stop,
    ) -> Result<()> {
        let buf_max = fold_db::storage_policy::BACKUP_RESOURCES.stream_buffer_bytes;
        let mut hash = Sha256::new();
        while left > 0 {
            if cancel.stopped.load(Ordering::Acquire) {
                return Err(BackupTransportError::Transport);
            }
            let mut chunk = vec![0u8; left.min(buf_max) as usize];
            source
                .read_exact(&mut chunk)
                .map_err(|_| BackupTransportError::Source)?;
            hash.update(&chunk);
            left -= chunk.len() as u64;
            send(sender, Ok(chunk), cancel)?;
        }
        let mut extra = [0u8; 1];
        let eof = source
            .read(&mut extra)
            .map_err(|_| BackupTransportError::Source)?
            == 0;
        (eof && fold_db::hex::hex_lower(hash.finalize()) == expected.as_str())
            .then_some(())
            .ok_or(BackupTransportError::Integrity("upload source"))
    }

    fn send(
        sender: &mpsc::Sender<io::Result<Vec<u8>>>,
        item: io::Result<Vec<u8>>,
        stop: &Stop,
    ) -> Result<()> {
        futures::executor::block_on(async {
            if stop.stopped.load(Ordering::Acquire) {
                return Err(BackupTransportError::Transport);
            }
            tokio::select! {
                _ = stop.changed.notified() => Err(BackupTransportError::Transport),
                res = sender.send(item) => res.map_err(|_| BackupTransportError::Transport),
            }
        })
    }

    pub(super) async fn bounded(response: reqwest::Response, maximum: u64) -> Result<Vec<u8>> {
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
}

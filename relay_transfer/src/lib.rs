//! Presigned blob transfers between a client and the relay's object store (Backblaze B2 on
//! Exemem). The node's log sync and the relay e2e proof both use this one definition, so the
//! proof measures the uploader that users run.
//!
//! A transfer fails when the connection stalls, not when it is slow: an upload when the
//! transport takes no byte of the body (or sends no answer after the last one) for
//! [`STALL_TIMEOUT`], a download when no byte arrives for that long. No minimum rate is
//! assumed. Each upload uses a fresh connection: a body cannot be sent again on a pooled
//! connection that the store closed while it was idle ("connection closed before message
//! completed").
//!
//! [`put_presigned`] returns every attempt with its duration, the bytes sent and the outcome,
//! so a caller can report why an upload was slow or what it retried.

mod body;

pub use body::UploadSource;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use reqwest::header::{HeaderName, HeaderValue};

/// The time to open a connection to the store.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// A transfer fails when it makes no progress for this long, however slow the link.
pub const STALL_TIMEOUT: Duration = Duration::from_secs(30);

/// An upload or download is tried at most this many times in all, after waits of 0.5 s and 1 s,
/// each cut to a random value between half and all of it.
pub const UPLOAD_TRIES: u32 = 3;

/// What the kernel may still hold unsent when the transport has taken the last byte of a body, and
/// the rate at which that tail is assumed to drain.
const TAIL_BYTES: u64 = 4 << 20;
const TAIL_FLOOR_BYTES_PER_SECOND: u64 = 64 << 10;

/// The wait before try `tried + 1`.
pub fn upload_backoff(tried: u32) -> Duration {
    use rand::Rng;
    let full = Duration::from_millis(500) * tried;
    rand::thread_rng().gen_range(full / 2..=full)
}

/// A store answer that a new try may change.
pub fn is_retryable_store_status(status: reqwest::StatusCode) -> bool {
    matches!(status.as_u16(), 408 | 429 | 500 | 502 | 503 | 504)
}

/// The client for presigned uploads: a fresh connection each time and no redirects.
pub fn upload_client() -> Result<reqwest::Client, String> {
    // trace-egress: public — presigned blob URLs; the signed headers go only to the relay.
    reqwest::Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .pool_max_idle_per_host(0)
        .build()
        .map_err(|e| format!("blob upload client: {e}"))
}

/// The client for presigned downloads: [`STALL_TIMEOUT`] per read, no idle pool, no redirects.
pub fn download_client() -> Result<reqwest::Client, String> {
    // trace-egress: public — presigned blob URLs; the signed headers go only to the relay.
    reqwest::Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .read_timeout(STALL_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .pool_max_idle_per_host(0)
        .build()
        .map_err(|e| format!("blob download client: {e}"))
}

/// The error with its causes (`source` chain), since reqwest alone says "error sending request",
/// and the place: scheme, host and path of the URL. A presigned URL carries its signature and
/// credential in the query, so the query is never printed.
pub fn describe(error: reqwest::Error, url: &str) -> String {
    use std::error::Error as _;
    let error = error.without_url();
    let mut text = error.to_string();
    let mut cause = error.source();
    while let Some(next) = cause {
        text.push_str(": ");
        text.push_str(&next.to_string());
        cause = next.source();
    }
    format!("{text} (for {})", place(url))
}

/// `scheme://host[:port]/path` of a URL, without the query or the fragment.
pub fn place(url: &str) -> String {
    match reqwest::Url::parse(url) {
        Ok(url) => format!(
            "{}://{}{}{}",
            url.scheme(),
            url.host_str().unwrap_or("?"),
            url.port().map_or(String::new(), |port| format!(":{port}")),
            url.path()
        ),
        Err(_) => "an unreadable URL".to_owned(),
    }
}

/// One try of an upload.
#[derive(Clone, Debug)]
pub struct Attempt {
    /// From the start of the request to its answer or its failure.
    pub seconds: f64,
    /// The bytes of the body the transport had taken.
    pub sent_bytes: u64,
    /// `HTTP <status>`, or why no answer came (a stall, a transport error).
    pub outcome: String,
}

/// The last answer of the store to an upload, and every try that led to it.
#[derive(Debug)]
pub struct Uploaded {
    pub status: reqwest::StatusCode,
    /// The URL of the answer: a caller checks that no redirect moved it.
    pub final_url: String,
    pub attempts: Vec<Attempt>,
}

/// An upload that got no answer in [`UPLOAD_TRIES`] tries.
#[derive(Debug)]
pub struct UploadFailed {
    pub message: String,
    pub attempts: Vec<Attempt>,
}

/// Put `source` (`size` bytes) to the presigned `url` with `headers`, through `client` (see
/// [`upload_client`]). A transient store status or a failure before an answer is tried again,
/// [`UPLOAD_TRIES`] in all; `on_retry` hears of each try that will be followed by another.
/// Any other answer ends it, a success or not: the caller judges the status and the final URL.
pub async fn put_presigned(
    client: &reqwest::Client,
    url: &str,
    headers: &[(HeaderName, HeaderValue)],
    source: &UploadSource,
    size: u64,
    on_retry: &(dyn Fn(u32, &Attempt) + Sync),
) -> Result<Uploaded, UploadFailed> {
    let mut attempts = Vec::new();
    loop {
        let tried = u32::try_from(attempts.len()).unwrap_or(u32::MAX) + 1;
        let (result, attempt) = put_once(client, url, headers, source, size).await;
        attempts.push(attempt.clone());
        let again = tried < UPLOAD_TRIES
            && match &result {
                Ok((status, _)) => is_retryable_store_status(*status),
                Err(_) => true,
            };
        if again {
            on_retry(tried, &attempt);
            tokio::time::sleep(upload_backoff(tried)).await;
            continue;
        }
        return match result {
            Ok((status, final_url)) => Ok(Uploaded {
                status,
                final_url,
                attempts,
            }),
            Err(message) => Err(UploadFailed { message, attempts }),
        };
    }
}

type Answer = Result<(reqwest::StatusCode, String), String>;

async fn put_once(
    client: &reqwest::Client,
    url: &str,
    headers: &[(HeaderName, HeaderValue)],
    source: &UploadSource,
    size: u64,
) -> (Answer, Attempt) {
    let taken = Arc::new(AtomicU64::new(0));
    let body = source.body(Arc::clone(&taken));
    let mut upload = client.put(url).header("content-length", size).body(body);
    for (name, value) in headers {
        upload = upload.header(name, value);
    }
    let started = Instant::now();
    let result = tokio::select! {
        sent = upload.send() => sent
            .map(|response| (response.status(), response.url().as_str().to_owned()))
            .map_err(|e| {
                format!(
                    "blob upload: {} after {} s with {} of {size} bytes sent",
                    describe(e, url),
                    started.elapsed().as_secs(),
                    taken.load(Ordering::Relaxed)
                )
            }),
        message = stalled(&taken, size, url) => Err(message),
    };
    let attempt = Attempt {
        seconds: started.elapsed().as_secs_f64(),
        sent_bytes: taken.load(Ordering::Relaxed),
        outcome: match &result {
            Ok((status, _)) => format!("HTTP {}", status.as_u16()),
            Err(message) => message.clone(),
        },
    };
    (result, attempt)
}

/// Fails when `taken`, the bytes the transport has taken from an upload body, does not move for
/// [`STALL_TIMEOUT`]. Once the whole body is taken it waits for the answer that long, plus the
/// time the tail in the send buffer needs at the floor rate.
async fn stalled(taken: &AtomicU64, size: u64, url: &str) -> String {
    let tail = Duration::from_secs(size.min(TAIL_BYTES) / TAIL_FLOOR_BYTES_PER_SECOND);
    let (mut last, mut moved) = (0, Instant::now());
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let now = taken.load(Ordering::Relaxed);
        let allowed = if now >= size {
            STALL_TIMEOUT + tail
        } else {
            STALL_TIMEOUT
        };
        if now != last {
            (last, moved) = (now, Instant::now());
        } else if moved.elapsed() >= allowed {
            return format!(
                "blob upload stalled for {} s with {last} of {size} bytes sent (for {})",
                moved.elapsed().as_secs(),
                place(url)
            );
        }
    }
}

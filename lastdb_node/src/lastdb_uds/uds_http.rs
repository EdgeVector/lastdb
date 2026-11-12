//! Minimal HTTP/1.1 framing and connection deadlines for the Unix-domain socket.

use std::io::{self, BufRead, Read, Write};
use std::os::unix::net::UnixStream;
use std::time::{Duration, Instant};

use fold_db::access::AccessContext;

/// Largest single request/header line accepted, in bytes (excluding CRLF).
pub const MAX_LINE_LEN: usize = 8 * 1024;
/// Largest total header-section size accepted, in bytes.
pub const MAX_HEADERS_TOTAL: usize = 64 * 1024;
/// Largest general request body accepted, in bytes.
pub const MAX_BODY_LEN: usize = 128 * 1024 * 1024;

const MAX_HEADER_COUNT: usize = 256;
const UDS_CONN_BUDGET: Duration = Duration::from_secs(10);

/// A request read off the control socket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UdsRequest {
    /// Request method token (e.g. `GET`, `POST`), verbatim from the wire.
    pub method: String,
    /// Request target (path and optional query), verbatim from the wire.
    pub target: String,
    /// Header lines in wire order, each `(name, value)` trimmed.
    pub headers: Vec<(String, String)>,
    /// Request body — `Content-Length` bytes, or empty when absent.
    pub body: Vec<u8>,
}

impl UdsRequest {
    /// Look up a header by name, case-insensitively, returning the first match.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

/// An HTTP/1.1 response to write back over the control socket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UdsResponse {
    /// Numeric HTTP status code.
    pub status: u16,
    /// Reason phrase for the status line.
    pub reason: &'static str,
    /// Extra response headers in wire order.
    pub headers: Vec<(String, String)>,
    /// Response body bytes.
    pub body: Vec<u8>,
}

impl UdsResponse {
    /// Build a response with status, reason, and body and no extra headers.
    pub fn new(status: u16, reason: &'static str, body: Vec<u8>) -> Self {
        Self {
            status,
            reason,
            headers: Vec::new(),
            body,
        }
    }

    /// Append a header to the response, returning `self` for chaining.
    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }
}

enum UdsHttpError {
    Silent,
    BadRequest,
    UnsupportedHttpVersion,
    HeadersTooLarge,
    BodyTooLarge,
}

impl From<io::Error> for UdsHttpError {
    fn from(_: io::Error) -> Self {
        Self::Silent
    }
}

fn read_line<R: BufRead>(reader: &mut R, max: usize) -> Result<Option<Vec<u8>>, UdsHttpError> {
    let mut buf = Vec::new();
    if reader.take((max as u64) + 2).read_until(b'\n', &mut buf)? == 0 {
        return Ok(None);
    }
    if buf.pop() != Some(b'\n') {
        return Err(if buf.len() > max {
            UdsHttpError::HeadersTooLarge
        } else {
            UdsHttpError::BadRequest
        });
    }
    if buf.last() == Some(&b'\r') {
        buf.pop();
    }
    (buf.len() <= max)
        .then_some(Some(buf))
        .ok_or(UdsHttpError::HeadersTooLarge)
}

fn read_request<R: BufRead>(
    mut reader: R,
    allow_owner_blob: bool,
) -> Result<UdsRequest, UdsHttpError> {
    let line = read_line(&mut reader, MAX_LINE_LEN)?.ok_or(UdsHttpError::Silent)?;
    let [method, target, version] = std::str::from_utf8(&line)
        .map_err(|_| UdsHttpError::BadRequest)?
        .split(' ')
        .collect::<Vec<_>>()[..]
    else {
        return Err(UdsHttpError::BadRequest);
    };
    if method.is_empty() || !method.bytes().all(|b| b.is_ascii_graphic()) || target.is_empty() {
        return Err(UdsHttpError::BadRequest);
    }
    if version != "HTTP/1.0" && version != "HTTP/1.1" {
        return Err(UdsHttpError::UnsupportedHttpVersion);
    }
    let (method, target) = (method.to_string(), target.to_string());
    let (mut headers, mut header_bytes) = (Vec::new(), 0usize);
    loop {
        let line = read_line(&mut reader, MAX_LINE_LEN)?.ok_or(UdsHttpError::BadRequest)?;
        if line.is_empty() {
            break;
        }
        header_bytes = header_bytes.saturating_add(line.len());
        if header_bytes > MAX_HEADERS_TOTAL || headers.len() >= MAX_HEADER_COUNT {
            return Err(UdsHttpError::HeadersTooLarge);
        }
        let (raw_name, value) = std::str::from_utf8(&line)
            .ok()
            .and_then(|s| s.split_once(':'))
            .ok_or(UdsHttpError::BadRequest)?;
        let name = raw_name.trim();
        if name.is_empty() || name.bytes().any(|b| b == b' ' || b == b'\t') {
            return Err(UdsHttpError::BadRequest);
        }
        headers.push((name.to_string(), value.trim().to_string()));
    }
    if headers
        .iter()
        .any(|(n, _)| n.eq_ignore_ascii_case("transfer-encoding"))
    {
        return Err(UdsHttpError::BadRequest);
    }
    let body = read_body(&mut reader, &headers, &method, &target, allow_owner_blob)?;
    Ok(UdsRequest {
        method,
        target,
        headers,
        body,
    })
}

fn read_body(
    reader: &mut impl Read,
    headers: &[(String, String)],
    method: &str,
    target: &str,
    allow_owner_blob: bool,
) -> Result<Vec<u8>, UdsHttpError> {
    let raw_blob = allow_owner_blob
        && method == "POST"
        && (target == "/api/db/put-blob"
            || crate::lastdb_uds::database_path::split(target)
                .is_some_and(|(_, p)| p == "db/put-blob"))
        && headers
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case("content-type"))
            .and_then(|(_, v)| v.split(';').next())
            .is_some_and(|v| v.trim().eq_ignore_ascii_case("application/octet-stream"));
    let maximum = if raw_blob {
        fold_db::storage_policy::native_write::LOCAL_FILE_INPUT_MEMORY_BYTES
    } else {
        MAX_BODY_LEN as u64
    };
    let (mut length, mut seen) = (0usize, false);
    for (name, value) in headers {
        if name.eq_ignore_ascii_case("content-length") {
            let parsed = value
                .trim()
                .parse::<usize>()
                .map_err(|_| UdsHttpError::BadRequest)?;
            if seen && parsed != length {
                return Err(UdsHttpError::BadRequest);
            }
            (length, seen) = (parsed, true);
        }
    }
    if length as u64 > maximum {
        return Err(UdsHttpError::BodyTooLarge);
    }
    let mut body = Vec::new();
    body.try_reserve_exact(length)
        .map_err(|_| UdsHttpError::Silent)?;
    body.resize(length, 0);
    reader.read_exact(&mut body).map_err(|e| {
        if e.kind() == io::ErrorKind::UnexpectedEof {
            UdsHttpError::BadRequest
        } else {
            UdsHttpError::Silent
        }
    })?;
    Ok(body)
}

pub(super) fn write_busy_and_close(stream: &mut UnixStream) -> io::Result<()> {
    let resp = UdsResponse::new(
        503,
        "Service Unavailable",
        br#"{"status":"busy","error":"uds_worker_queue_full"}"#.to_vec(),
    )
    .with_header("Content-Type", "application/json")
    .with_header("Retry-After", "1");
    write_with_budget(stream, &resp, Duration::from_secs(2))
}

struct DeadlineStream<'a> {
    stream: &'a UnixStream,
    deadline: Instant,
}

impl DeadlineStream<'_> {
    fn arm(&self, set: impl Fn(Option<Duration>) -> io::Result<()>) -> io::Result<()> {
        let rem = self.deadline.saturating_duration_since(Instant::now());
        if rem.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "uds: aggregate per-connection deadline exhausted",
            ));
        }
        set(Some(rem))
    }
}

impl Read for DeadlineStream<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.arm(|d| self.stream.set_read_timeout(d))?;
        (&*self.stream).read(buf)
    }
}

impl Write for DeadlineStream<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.arm(|d| self.stream.set_write_timeout(d))?;
        (&*self.stream).write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        (&*self.stream).flush()
    }
}

fn write_with_budget(
    stream: &mut UnixStream,
    resp: &UdsResponse,
    budget: Duration,
) -> io::Result<()> {
    let mut w = DeadlineStream {
        stream,
        deadline: Instant::now() + budget,
    };
    write!(
        w,
        "HTTP/1.1 {} {}\r\nContent-Length: {}\r\nConnection: close\r\n",
        resp.status,
        resp.reason,
        resp.body.len()
    )?;
    for (name, value) in &resp.headers {
        if !name.eq_ignore_ascii_case("content-length") && !name.eq_ignore_ascii_case("connection")
        {
            write!(w, "{name}: {value}\r\n")?;
        }
    }
    write!(w, "\r\n")?;
    w.write_all(&resp.body)?;
    w.flush()?;
    let _ = stream.shutdown(std::net::Shutdown::Write);
    Ok(())
}

/// Parse one request with the connection deadline and owner access context.
pub fn receive_connection(
    stream: &mut UnixStream,
    owner_user_id: &str,
) -> io::Result<Option<(UdsRequest, AccessContext)>> {
    let read_half = stream.try_clone()?;
    let base = AccessContext::owner(owner_user_id);
    let reader = io::BufReader::new(DeadlineStream {
        stream: &read_half,
        deadline: Instant::now() + UDS_CONN_BUDGET,
    });
    match read_request(reader, base.is_owner) {
        Ok(request) => Ok(Some((request, base))),
        Err(UdsHttpError::Silent) => Ok(None),
        Err(error) => {
            let (status, reason) = match error {
                UdsHttpError::UnsupportedHttpVersion => (505, "HTTP Version Not Supported"),
                UdsHttpError::HeadersTooLarge => (431, "Request Header Fields Too Large"),
                UdsHttpError::BodyTooLarge => (413, "Payload Too Large"),
                UdsHttpError::BadRequest | UdsHttpError::Silent => (400, "Bad Request"),
            };
            finish_connection(stream, &UdsResponse::new(status, reason, reason.into()))?;
            Ok(None)
        }
    }
}

/// Write one final response with a fresh deadline and explicit EOF.
pub fn finish_connection(stream: &mut UnixStream, response: &UdsResponse) -> io::Result<()> {
    write_with_budget(stream, response, UDS_CONN_BUDGET)
}

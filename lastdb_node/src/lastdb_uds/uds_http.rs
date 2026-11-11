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
const BUSY_ERROR_QUEUE_FULL: &str = "uds_worker_queue_full";
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
    Io,
    Empty,
    LineTooLong,
    UnexpectedEof,
    MalformedRequestLine,
    UnsupportedHttpVersion,
    MalformedHeader,
    HeadersTooLarge,
    InvalidContentLength,
    BodyTooLarge,
    UnsupportedTransferEncoding,
}

impl From<io::Error> for UdsHttpError {
    fn from(_: io::Error) -> Self {
        Self::Io
    }
}

fn read_line<R: BufRead>(reader: &mut R, max: usize) -> Result<Option<Vec<u8>>, UdsHttpError> {
    let mut buf = Vec::new();
    let n = reader.take((max as u64) + 2).read_until(b'\n', &mut buf)?;
    if n == 0 {
        return Ok(None);
    }
    if buf.last() != Some(&b'\n') {
        return Err(if buf.len() >= max + 2 {
            UdsHttpError::LineTooLong
        } else {
            UdsHttpError::UnexpectedEof
        });
    }
    buf.pop();
    if buf.last() == Some(&b'\r') {
        buf.pop();
    }
    if buf.len() > max {
        return Err(UdsHttpError::LineTooLong);
    }
    Ok(Some(buf))
}

fn parse_request_line(line: &[u8]) -> Result<(String, String), UdsHttpError> {
    let s = std::str::from_utf8(line).map_err(|_| UdsHttpError::MalformedRequestLine)?;
    let mut parts = s.split(' ');
    let method = parts
        .next()
        .filter(|m| !m.is_empty() && m.bytes().all(|b| b.is_ascii_graphic()))
        .ok_or(UdsHttpError::MalformedRequestLine)?;
    let target = parts
        .next()
        .filter(|t| !t.is_empty())
        .ok_or(UdsHttpError::MalformedRequestLine)?;
    let version = parts.next().ok_or(UdsHttpError::MalformedRequestLine)?;
    if parts.next().is_some() {
        return Err(UdsHttpError::MalformedRequestLine);
    }
    if version != "HTTP/1.0" && version != "HTTP/1.1" {
        return Err(UdsHttpError::UnsupportedHttpVersion);
    }
    Ok((method.to_string(), target.to_string()))
}

fn parse_header(line: &[u8]) -> Result<(String, String), UdsHttpError> {
    let s = std::str::from_utf8(line).map_err(|_| UdsHttpError::MalformedHeader)?;
    let idx = s.find(':').ok_or(UdsHttpError::MalformedHeader)?;
    let name = s[..idx].trim();
    if name.is_empty() || name.bytes().any(|b| b == b' ' || b == b'\t') {
        return Err(UdsHttpError::MalformedHeader);
    }
    Ok((name.to_string(), s[idx + 1..].trim().to_string()))
}

fn read_request<R: BufRead>(
    mut reader: R,
    allow_owner_blob: bool,
) -> Result<UdsRequest, UdsHttpError> {
    let Some(request_line) = read_line(&mut reader, MAX_LINE_LEN)? else {
        return Err(UdsHttpError::Empty);
    };
    let (method, target) = parse_request_line(&request_line)?;
    let mut headers = Vec::new();
    let mut header_bytes = 0usize;
    loop {
        let line = read_line(&mut reader, MAX_LINE_LEN)?.ok_or(UdsHttpError::UnexpectedEof)?;
        if line.is_empty() {
            break;
        }
        header_bytes = header_bytes.saturating_add(line.len());
        if header_bytes > MAX_HEADERS_TOTAL || headers.len() >= MAX_HEADER_COUNT {
            return Err(UdsHttpError::HeadersTooLarge);
        }
        headers.push(parse_header(&line)?);
    }
    if headers
        .iter()
        .any(|(name, _)| name.eq_ignore_ascii_case("transfer-encoding"))
    {
        return Err(UdsHttpError::UnsupportedTransferEncoding);
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
                .is_some_and(|(_, path)| path == "db/put-blob"))
        && headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("content-type"))
            .and_then(|(_, value)| value.split(';').next())
            .is_some_and(|v| v.trim().eq_ignore_ascii_case("application/octet-stream"));
    let maximum = if raw_blob {
        fold_db::storage_policy::native_write::LOCAL_FILE_INPUT_MEMORY_BYTES
    } else {
        MAX_BODY_LEN as u64
    };
    let mut length = 0usize;
    let mut seen = false;
    for (name, value) in headers {
        if name.eq_ignore_ascii_case("content-length") {
            let parsed = value
                .trim()
                .parse::<usize>()
                .map_err(|_| UdsHttpError::InvalidContentLength)?;
            if seen && parsed != length {
                return Err(UdsHttpError::InvalidContentLength);
            }
            length = parsed;
            seen = true;
        }
    }
    if length as u64 > maximum {
        return Err(UdsHttpError::BodyTooLarge);
    }
    let mut body = Vec::new();
    body.try_reserve_exact(length)
        .map_err(|_| UdsHttpError::Io)?;
    body.resize(length, 0);
    reader.read_exact(&mut body).map_err(|e| {
        if e.kind() == io::ErrorKind::UnexpectedEof {
            UdsHttpError::UnexpectedEof
        } else {
            UdsHttpError::Io
        }
    })?;
    Ok(body)
}

fn write_response<W: Write>(w: &mut W, resp: &UdsResponse) -> io::Result<()> {
    write!(w, "HTTP/1.1 {} {}\r\n", resp.status, resp.reason)?;
    write!(
        w,
        "Content-Length: {}\r\nConnection: close\r\n",
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
    w.flush()
}

pub(super) fn write_busy_and_close(stream: &mut UnixStream) -> io::Result<()> {
    let body = format!(r#"{{"status":"busy","error":"{BUSY_ERROR_QUEUE_FULL}"}}"#);
    let resp = UdsResponse::new(503, "Service Unavailable", body.into_bytes())
        .with_header("Content-Type", "application/json")
        .with_header("Retry-After", "1");
    let mut w = DeadlineStream {
        stream,
        deadline: Instant::now() + Duration::from_secs(2),
    };
    write_response(&mut w, &resp)?;
    let _ = stream.shutdown(std::net::Shutdown::Write);
    Ok(())
}

struct DeadlineStream<'a> {
    stream: &'a UnixStream,
    deadline: Instant,
}

impl DeadlineStream<'_> {
    fn arm(&self, set: impl Fn(Option<Duration>) -> io::Result<()>) -> io::Result<()> {
        let remaining = self.deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "uds: aggregate per-connection deadline exhausted",
            ));
        }
        set(Some(remaining))
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

/// Parse one request with the connection deadline and owner access context.
pub fn receive_connection(
    stream: &mut UnixStream,
    owner_user_id: &str,
) -> io::Result<Option<(UdsRequest, AccessContext)>> {
    let read_half = stream.try_clone()?;
    let deadline = Instant::now() + UDS_CONN_BUDGET;
    let base = AccessContext::owner(owner_user_id);
    let reader = io::BufReader::new(DeadlineStream {
        stream: &read_half,
        deadline,
    });
    match read_request(reader, base.is_owner) {
        Ok(request) => Ok(Some((request, base))),
        Err(UdsHttpError::Empty | UdsHttpError::Io) => Ok(None),
        Err(error) => {
            let (status, reason) = match error {
                UdsHttpError::UnsupportedHttpVersion => (505, "HTTP Version Not Supported"),
                UdsHttpError::LineTooLong | UdsHttpError::HeadersTooLarge => {
                    (431, "Request Header Fields Too Large")
                }
                UdsHttpError::BodyTooLarge => (413, "Payload Too Large"),
                _ => (400, "Bad Request"),
            };
            finish_connection(stream, &UdsResponse::new(status, reason, reason.into()))?;
            Ok(None)
        }
    }
}

/// Write one final response with a fresh deadline and explicit EOF.
pub fn finish_connection(stream: &mut UnixStream, response: &UdsResponse) -> io::Result<()> {
    let mut w = DeadlineStream {
        stream,
        deadline: Instant::now() + UDS_CONN_BUDGET,
    };
    write_response(&mut w, response)?;
    let _ = stream.shutdown(std::net::Shutdown::Write);
    Ok(())
}

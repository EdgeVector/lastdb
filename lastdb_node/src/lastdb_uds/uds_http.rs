//! HTTP/1.1 request framing for the Unix-domain socket.
//!
//! **I4 (side-channel closure).** This module does not log a request line,
//! header value, or body byte. A parse failure uses a content-free
//! [`UdsHttpError`] variant, so the error cannot disclose request content.

use std::io::{self, BufRead, Read, Write};

use fold_db::access::AccessContext;

/// Largest single request/header line accepted, in bytes (excluding CRLF).
pub const MAX_LINE_LEN: usize = 8 * 1024;

/// Largest total header-section size accepted, in bytes.
pub const MAX_HEADERS_TOTAL: usize = 64 * 1024;

/// Largest number of header lines accepted.
pub const MAX_HEADER_COUNT: usize = 256;

/// Largest general request body accepted, in bytes.
pub const MAX_BODY_LEN: usize = 128 * 1024 * 1024;

/// A request read off the control socket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UdsRequest {
    /// Request method token (e.g. `GET`, `POST`), verbatim from the wire.
    pub method: String,
    /// Request target (path and optional query), verbatim from the wire.
    pub target: String,
    /// Header lines in wire order, each `(name, value)` trimmed.
    pub headers: Vec<(String, String)>,
    /// Request body — exactly `Content-Length` bytes, or empty when absent.
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

/// Why reading a control-socket request failed (content-free for **I4**).
#[derive(Debug)]
pub enum UdsHttpError {
    /// Underlying transport error reading the stream.
    Io(io::Error),
    /// The peer closed the connection before sending any request line.
    Empty,
    /// A request or header line exceeded the configured length cap.
    LineTooLong,
    /// The stream ended in the middle of the request line, headers, or body.
    UnexpectedEof,
    /// The request line was not `METHOD SP target SP HTTP-version`.
    MalformedRequestLine,
    /// The HTTP version was neither `HTTP/1.0` nor `HTTP/1.1`.
    UnsupportedHttpVersion,
    /// A header line had no `:` separator or an empty/invalid field name.
    MalformedHeader,
    /// The header section exceeded the total-size or count cap.
    HeadersTooLarge,
    /// `Content-Length` was absent-but-required, unparseable, or contradictory.
    InvalidContentLength,
    /// The declared body length exceeded the request's body allowance.
    BodyTooLarge,
    /// The request carried a `Transfer-Encoding` header.
    UnsupportedTransferEncoding,
}

impl std::fmt::Display for UdsHttpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "control-socket read error: {e}"),
            Self::Empty => write!(f, "connection closed before a request was sent"),
            Self::LineTooLong => write!(f, "request or header line exceeds the length cap"),
            Self::UnexpectedEof => write!(f, "connection ended mid-request"),
            Self::MalformedRequestLine => write!(f, "malformed HTTP request line"),
            Self::UnsupportedHttpVersion => write!(f, "unsupported HTTP version"),
            Self::MalformedHeader => write!(f, "malformed HTTP header line"),
            Self::HeadersTooLarge => write!(f, "header section exceeds the size or count cap"),
            Self::InvalidContentLength => {
                write!(f, "missing, invalid, or contradictory Content-Length")
            }
            Self::BodyTooLarge => write!(f, "request body exceeds the size cap"),
            Self::UnsupportedTransferEncoding => {
                write!(
                    f,
                    "Transfer-Encoding is not supported on the control socket"
                )
            }
        }
    }
}

impl std::error::Error for UdsHttpError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for UdsHttpError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

fn read_line<R: BufRead>(reader: &mut R, max: usize) -> Result<Option<Vec<u8>>, UdsHttpError> {
    let mut buf = Vec::new();
    let mut limited = reader.take((max as u64) + 2);
    let n = limited.read_until(b'\n', &mut buf)?;
    if n == 0 {
        return Ok(None);
    }
    if buf.last() != Some(&b'\n') {
        return if buf.len() >= max + 2 {
            Err(UdsHttpError::LineTooLong)
        } else {
            Err(UdsHttpError::UnexpectedEof)
        };
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
        .filter(|m| !m.is_empty())
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
    if !method.bytes().all(|b| b.is_ascii_graphic()) {
        return Err(UdsHttpError::MalformedRequestLine);
    }
    Ok((method.to_string(), target.to_string()))
}

fn parse_header(line: &[u8]) -> Result<(String, String), UdsHttpError> {
    let s = std::str::from_utf8(line).map_err(|_| UdsHttpError::MalformedHeader)?;
    let idx = s.find(':').ok_or(UdsHttpError::MalformedHeader)?;
    let name = s[..idx].trim();
    let value = s[idx + 1..].trim();
    if name.is_empty() || name.bytes().any(|b| b == b' ' || b == b'\t') {
        return Err(UdsHttpError::MalformedHeader);
    }
    Ok((name.to_string(), value.to_string()))
}

fn read_request_with_owner_blob<R: BufRead>(
    mut reader: R,
    allow_owner_blob: bool,
) -> Result<UdsRequest, UdsHttpError> {
    let Some(request_line) = read_line(&mut reader, MAX_LINE_LEN)? else {
        return Err(UdsHttpError::Empty);
    };
    let (method, target) = parse_request_line(&request_line)?;

    let mut headers: Vec<(String, String)> = Vec::new();
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
            .is_some_and(|value| {
                value
                    .trim()
                    .eq_ignore_ascii_case("application/octet-stream")
            });
    let maximum = if raw_blob {
        fold_db::storage_policy::native_write::LOCAL_FILE_INPUT_MEMORY_BYTES
    } else {
        MAX_BODY_LEN as u64
    };
    let content_length = resolve_content_length(headers, maximum)?;
    let mut body = Vec::new();
    body.try_reserve_exact(content_length)
        .map_err(|_| UdsHttpError::Io(io::Error::other("request body allocation refused")))?;
    body.resize(content_length, 0);
    reader
        .read_exact(&mut body)
        .map_err(|error| match error.kind() {
            io::ErrorKind::UnexpectedEof => UdsHttpError::UnexpectedEof,
            _ => UdsHttpError::Io(error),
        })?;
    Ok(body)
}

fn resolve_content_length(
    headers: &[(String, String)],
    maximum: u64,
) -> Result<usize, UdsHttpError> {
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
    Ok(length)
}

/// Build the owner [`AccessContext`] for a request accepted on the control socket.
pub fn uds_access_context(owner_user_id: &str) -> AccessContext {
    AccessContext::owner(owner_user_id)
}

mod serve;

pub use serve::{
    error_response, finish_connection, receive_connection, write_busy_and_close, write_response,
    UdsResponse, BUSY_ERROR_QUEUE_FULL,
};

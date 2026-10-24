//! HTTP/1.1 request framing for the Unix-domain socket (invariant **I3** in
//! `exemem-workspace/docs/designs/app_security_model.md`).
//!
//! [`super::uds::UdsSocket::serve`] accepts a same-user stream and supplies
//! its [`CallerTransport`] and [`CallerVerification`] posture. This module
//! parses the [`UdsRequest`], builds its [`AccessContext`], and calls the
//! daemon's route handler. The accept path reads kernel peer credentials before
//! this parser sees request bytes.
//!
//! The default owner socket serves data routes under the device-trust posture.
//! The optional `app-isolation` feature can upgrade verification through a
//! macOS code-signature check in the accept path.
//!
//! **I4 (side-channel closure).** This module does not log a request line,
//! header value, or body byte. A parse failure uses a content-free
//! [`UdsHttpError`] variant, so the error cannot disclose request content.

use std::io::{self, BufRead, Read, Write};

use fold_db::access::{AccessContext, CallerTransport, CallerVerification};

use super::uds_router::SocketKind;

/// Largest single request/header line accepted, in bytes (excluding CRLF).
///
/// Bounds the per-line allocation so a same-user process that never sends a line
/// terminator cannot drive unbounded memory growth. 8 KiB comfortably fits a
/// request line and any individual header the node emits.
pub const MAX_LINE_LEN: usize = 8 * 1024;

/// Largest total header-section size accepted, in bytes.
///
/// Caps the sum of all header lines so a flood of small headers is bounded even
/// though each line is individually under [`MAX_LINE_LEN`].
pub const MAX_HEADERS_TOTAL: usize = 64 * 1024;

/// Largest number of header lines accepted.
pub const MAX_HEADER_COUNT: usize = 256;

/// Largest general request body accepted, in bytes.
/// Owner raw local-file PUTs use native input memory admission instead.
pub const MAX_BODY_LEN: usize = 128 * 1024 * 1024;

/// A request read off the control socket.
///
/// Headers are kept as an ordered list of `(name, value)` pairs preserving wire
/// order; use [`UdsRequest::header`] for a case-insensitive lookup. The body is
/// the exact `Content-Length` bytes (empty when the header is absent).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UdsRequest {
    /// Request method token (e.g. `GET`, `POST`), verbatim from the wire.
    pub method: String,
    /// Request target (path and optional query), verbatim from the wire.
    pub target: String,
    /// Header lines in wire order, each `(name, value)` with surrounding
    /// whitespace trimmed.
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

/// Why reading a control-socket request failed.
///
/// Every variant is content-free — none carries a caller-supplied byte — so a
/// failed read can be logged without leaking request content (the I4 concern).
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
    /// `Content-Length` was absent-but-required, unparseable, or contradictory
    /// across duplicate headers.
    InvalidContentLength,
    /// The declared body length exceeded the request's body allowance.
    BodyTooLarge,
    /// The request carried a `Transfer-Encoding` header. The control socket
    /// frames bodies by `Content-Length` only; a `Transfer-Encoding` (e.g.
    /// `chunked`) request would otherwise be silently mis-framed, the classic
    /// CL/TE desync shape. We refuse it explicitly. (sec review 2026-06-15.)
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

/// Read one CRLF-terminated line, returning its content without the terminator.
///
/// `Ok(None)` signals a clean EOF before any byte (the peer hung up); `Ok(Some)`
/// a complete line. A line longer than `max` (excluding CRLF) is
/// [`UdsHttpError::LineTooLong`]; a stream that ends mid-line is
/// [`UdsHttpError::UnexpectedEof`]. Both `\r\n` and a bare `\n` terminate a line
/// (lenient on the CR, as servers commonly are).
fn read_line<R: BufRead>(reader: &mut R, max: usize) -> Result<Option<Vec<u8>>, UdsHttpError> {
    let mut buf = Vec::new();
    // Cap the read at max + 2 (line + CRLF) so a terminator-free line cannot
    // grow the buffer without bound.
    let mut limited = reader.take((max as u64) + 2);
    let n = limited.read_until(b'\n', &mut buf)?;
    if n == 0 {
        return Ok(None);
    }
    if buf.last() != Some(&b'\n') {
        // No terminator: either the cap was hit (too long) or the stream ended.
        return if buf.len() >= max + 2 {
            Err(UdsHttpError::LineTooLong)
        } else {
            Err(UdsHttpError::UnexpectedEof)
        };
    }
    buf.pop(); // drop '\n'
    if buf.last() == Some(&b'\r') {
        buf.pop(); // drop '\r'
    }
    if buf.len() > max {
        return Err(UdsHttpError::LineTooLong);
    }
    Ok(Some(buf))
}

/// Parse the request line into `(method, target)`, validating the HTTP version.
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
        // A target containing a raw space would split into a 4th token; reject.
        return Err(UdsHttpError::MalformedRequestLine);
    }
    if version != "HTTP/1.0" && version != "HTTP/1.1" {
        return Err(UdsHttpError::UnsupportedHttpVersion);
    }
    // A method token is visible ASCII with no spaces (split already removed
    // spaces, so this rejects control bytes such as an embedded CR).
    if !method.bytes().all(|b| b.is_ascii_graphic()) {
        return Err(UdsHttpError::MalformedRequestLine);
    }
    Ok((method.to_string(), target.to_string()))
}

/// Parse one header line into a trimmed `(name, value)` pair.
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

/// Read and parse one HTTP/1.1 request from a control-socket stream.
///
/// The reader is any [`BufRead`] — in production a `BufReader<UnixStream>`; in
/// tests an in-memory byte slice. Reads the request line, the header section up
/// to the blank line, and exactly `Content-Length` body bytes. All size caps
/// ([`MAX_LINE_LEN`], [`MAX_HEADERS_TOTAL`], [`MAX_HEADER_COUNT`],
/// [`MAX_BODY_LEN`]) are enforced; every failure is a content-free
/// [`UdsHttpError`].
pub fn read_request<R: BufRead>(mut reader: R) -> Result<UdsRequest, UdsHttpError> {
    read_request_with_owner_blob(&mut reader, false)
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
            break; // blank line terminates the header section
        }
        header_bytes = header_bytes.saturating_add(line.len());
        if header_bytes > MAX_HEADERS_TOTAL || headers.len() >= MAX_HEADER_COUNT {
            return Err(UdsHttpError::HeadersTooLarge);
        }
        headers.push(parse_header(&line)?);
    }

    // Reject Transfer-Encoding: we frame the body by Content-Length only, so a
    // chunked request would be silently mis-framed (CL/TE desync). The control
    // socket's in-house clients always send Content-Length; refuse TE outright
    // rather than rely on the one-request-per-connection model to mask it.
    if headers
        .iter()
        .any(|(name, _)| name.eq_ignore_ascii_case("transfer-encoding"))
    {
        return Err(UdsHttpError::UnsupportedTransferEncoding);
    }

    let body = body::read(&mut reader, &headers, &method, &target, allow_owner_blob)?;

    Ok(UdsRequest {
        method,
        target,
        headers,
        body,
    })
}

/// Build the [`AccessContext`] for a request that arrived over the control
/// socket, from the accept loop's transport + verification posture **and the
/// [`SocketKind`] it arrived on**.
///
/// A connection accepted on the control socket has passed the I3a same-user
/// peer-credential gate ([`super::uds::UdsConnVerdict::Accepted`]), so the caller
/// runs as the node owner's OS user. Which principal it becomes depends on the
/// socket and on whether the OS code-signature check (I3b) verified an *app*:
///
/// * [`SocketKind::Owner`] + [`CallerVerification::Unverified`] — a same-user
///   local caller on the owner control socket with no verified app identity.
///   Under device-trust such a caller *is* the data owner (the loopback posture
///   today), so the context is [`AccessContext::owner`] and the I3c read gate
///   short-circuits on an attesting transport.
/// * [`SocketKind::App`] + [`CallerVerification::Unverified`] — a **jailed app**
///   reaching the node over its per-app socket (fold#826). It is a scoped,
///   **non-owner** principal and MUST NOT inherit the owner short-circuit: a
///   read/write it drives must run under the scoped (unverified) identity so the
///   I3c read gate / I2 write guard engage and fail closed for a governed /
///   cross-app namespace, instead of being owner-bypassed. So the context is
///   [`AccessContext::remote`] (`is_owner = false`) even though no app identity
///   was code-signature verified — fail-closed pin (security-critical). Its
///   `user_id` stays the owner's hash (apps act on the owner's data), so data
///   attribution is unchanged; with no verified `app_id` the ACL denies it any
///   governed namespace while leaving un-governed data on the device-trust path.
/// * [`CallerVerification::CodeSignatureVerified`] (either socket) — a verified
///   app acting on the owner's node. It is a **non-owner** principal
///   ([`AccessContext::remote`], `is_owner = false`) so the I3c read gate
///   consults the namespace ACL against its verified `app_id` instead of
///   short-circuiting. Its `user_id` stays the node owner's `user_hash` — apps
///   act on the owner's data, not their own — so data attribution is unchanged;
///   the verified app identity rides on `verification`, which is what the ACL
///   matches.
pub fn uds_access_context(
    owner_user_id: &str,
    socket_kind: SocketKind,
    transport: CallerTransport,
    verification: CallerVerification,
) -> AccessContext {
    // The caller is the data owner ONLY when an unverified same-user caller
    // arrives on the OWNER control socket (device-trust loopback posture). Every
    // other case is a non-owner principal:
    //   * a code-signature-verified app (either socket) — gated by its app_id;
    //   * an UNVERIFIED caller on the per-app socket — a jailed app (fold#826).
    //     The owner short-circuit MUST NOT apply to it, so the namespace ACL /
    //     write guard engage and fail closed for governed data instead of the
    //     data plane running as the owner. This is the fail-closed pin.
    let is_owner_principal =
        matches!(verification, CallerVerification::Unverified) && socket_kind == SocketKind::Owner;
    let base = if is_owner_principal {
        AccessContext::owner(owner_user_id)
    } else {
        AccessContext::remote(owner_user_id)
    };
    base.with_transport(transport)
        .with_verification(verification)
}

/// Best-effort peer pid from a connected UDS stream (ops attribution).
///
/// Re-reads kernel credentials on the live fd. Returns `None` when the
/// platform has no peer-cred FFI, the read fails, or the kernel did not
/// report a pid. Never fails the request.
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn peer_pid_from_stream(stream: &std::os::unix::net::UnixStream) -> Option<i32> {
    use std::os::unix::io::AsRawFd;
    fold_db::access::peer_cred::read_peer_credential(stream.as_raw_fd())
        .ok()
        .and_then(|cred| cred.pid())
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn peer_pid_from_stream(_stream: &std::os::unix::net::UnixStream) -> Option<i32> {
    None
}

mod body;
mod serve;

pub use serve::{
    error_response, finish_connection, receive_connection, serve_connection, write_busy_and_close,
    write_response, UdsResponse, BUSY_ERROR_QUEUE_FULL,
};

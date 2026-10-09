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

/// Largest request body accepted, in bytes.
///
/// The app-socket raw blob CAS route carries pack blobs whose base64 JSON
/// envelope can exceed 100 MiB, so the socket-level cap permits those bodies.
/// Individual JSON routes reject malformed or non-JSON payloads.
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
    /// The declared body length exceeded [`MAX_BODY_LEN`].
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

/// Resolve the body length from parsed headers, rejecting contradictory
/// duplicate `Content-Length` values and an over-cap length.
fn resolve_content_length(headers: &[(String, String)]) -> Result<usize, UdsHttpError> {
    let mut length = 0usize;
    let mut seen = false;
    for (name, value) in headers {
        if name.eq_ignore_ascii_case("content-length") {
            let parsed = value
                .trim()
                .parse::<usize>()
                .map_err(|_| UdsHttpError::InvalidContentLength)?;
            if seen && parsed != length {
                // Duplicate, disagreeing Content-Length is a request-smuggling
                // shape; refuse rather than guess.
                return Err(UdsHttpError::InvalidContentLength);
            }
            length = parsed;
            seen = true;
        }
    }
    if length > MAX_BODY_LEN {
        return Err(UdsHttpError::BodyTooLarge);
    }
    Ok(length)
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

    let content_length = resolve_content_length(&headers)?;
    let mut body = vec![0u8; content_length];
    reader.read_exact(&mut body).map_err(|e| match e.kind() {
        io::ErrorKind::UnexpectedEof => UdsHttpError::UnexpectedEof,
        _ => UdsHttpError::Io(e),
    })?;

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

/// An HTTP/1.1 response to write back over the control socket.
///
/// The `status`/`reason` form the status line; `headers` are written verbatim in
/// order *except* `Content-Length` and `Connection`, which [`write_response`]
/// always sets itself from the body and the one-shot connection model (a
/// handler-supplied value for either is dropped to avoid a duplicate or a
/// contradictory framing header). The body is written as-is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UdsResponse {
    /// Numeric HTTP status code (e.g. `200`, `400`).
    pub status: u16,
    /// Reason phrase for the status line (e.g. `OK`, `Bad Request`).
    pub reason: &'static str,
    /// Response headers in wire order. `Content-Length` and `Connection` are
    /// ignored here — [`write_response`] owns those.
    pub headers: Vec<(String, String)>,
    /// Response body bytes, written verbatim.
    pub body: Vec<u8>,
}

impl UdsResponse {
    /// Build a response with the given status, reason, and body and no extra
    /// headers. The handler adds any it needs via [`UdsResponse::with_header`].
    pub fn new(status: u16, reason: &'static str, body: Vec<u8>) -> Self {
        Self {
            status,
            reason,
            headers: Vec::new(),
            body,
        }
    }

    /// Append a header to the response, returning `self` for chaining.
    ///
    /// `Content-Length` and `Connection` added this way are ignored by
    /// [`write_response`], which owns the framing headers.
    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }
}

/// Header names [`write_response`] owns and refuses to take from a handler.
///
/// Both control message framing: `Content-Length` is derived from the body and
/// `Connection: close` reflects the one-request-per-connection control-socket
/// model. Letting a handler set either risks a duplicate or contradictory
/// framing header (a response-splitting / smuggling shape).
fn is_reserved_response_header(name: &str) -> bool {
    name.eq_ignore_ascii_case("content-length") || name.eq_ignore_ascii_case("connection")
}

/// Write an HTTP/1.1 response to `w`.
///
/// Emits the status line, the framing headers this layer owns (`Content-Length`
/// from the body, `Connection: close`), then the handler's remaining headers in
/// order (skipping any reserved framing header), the blank line, and the body.
/// The control socket is one request per connection, so `Connection: close` is
/// always sent. Returns the underlying write error if the stream breaks.
/// JSON `error` field when the **worker queue** is full (primary backpressure).
pub const BUSY_ERROR_QUEUE_FULL: &str = "uds_worker_queue_full";

/// Immediate busy response when the UDS worker queue is full.
///
/// Writes a short JSON 503 and shuts down the write half so the peer sees EOF.
/// Call this on the accept path **without** enqueuing a long-lived handler —
/// the stream is dropped by the caller after this returns.
pub fn write_busy_and_close(stream: &mut std::os::unix::net::UnixStream) -> io::Result<()> {
    let body = format!(r#"{{"status":"busy","error":"{BUSY_ERROR_QUEUE_FULL}"}}"#);
    let resp = UdsResponse::new(503, "Service Unavailable", body.into_bytes())
        .with_header("Content-Type", "application/json")
        .with_header("Connection", "close")
        .with_header("Retry-After", "1");
    // Short write budget: we only need to push a tiny fixed response.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    let mut w = DeadlineStream::new(stream, deadline);
    write_response(&mut w, &resp)?;
    let _ = stream.shutdown(std::net::Shutdown::Write);
    Ok(())
}

pub fn write_response<W: Write>(w: &mut W, resp: &UdsResponse) -> io::Result<()> {
    write!(w, "HTTP/1.1 {} {}\r\n", resp.status, resp.reason)?;
    write!(w, "Content-Length: {}\r\n", resp.body.len())?;
    write!(w, "Connection: close\r\n")?;
    for (name, value) in &resp.headers {
        if is_reserved_response_header(name) {
            continue;
        }
        write!(w, "{name}: {value}\r\n")?;
    }
    write!(w, "\r\n")?;
    w.write_all(&resp.body)?;
    w.flush()
}

/// Map a content-free [`UdsHttpError`] to the HTTP response sent back when a
/// request cannot be parsed.
///
/// **I4.** The response body is a fixed status phrase — never a caller-supplied
/// byte — so answering a malformed request cannot echo its content back into a
/// log or to another local listener. Each variant maps to the status that best
/// describes the framing failure; the broken-transport variants (`Empty`, `Io`,
/// `UnexpectedEof`) still map to a `400` for completeness, though
/// [`serve_connection`] does not bother writing a response when the peer has
/// already hung up.
pub fn error_response(err: &UdsHttpError) -> UdsResponse {
    let (status, reason): (u16, &'static str) = match err {
        UdsHttpError::UnsupportedHttpVersion => (505, "HTTP Version Not Supported"),
        UdsHttpError::LineTooLong | UdsHttpError::HeadersTooLarge => {
            (431, "Request Header Fields Too Large")
        }
        UdsHttpError::BodyTooLarge => (413, "Payload Too Large"),
        UdsHttpError::MalformedRequestLine
        | UdsHttpError::MalformedHeader
        | UdsHttpError::InvalidContentLength
        | UdsHttpError::UnsupportedTransferEncoding
        | UdsHttpError::UnexpectedEof
        | UdsHttpError::Empty
        | UdsHttpError::Io(_) => (400, "Bad Request"),
    };
    // The body is the reason phrase only — fixed text, no caller bytes (I4).
    UdsResponse::new(status, reason, reason.as_bytes().to_vec())
}

/// Read one request off an accepted control-socket stream, run it through
/// `handler` under the connection's [`AccessContext`], and write the response
/// back — the connective glue between the parser ([`read_request`]), the posture
/// mapping ([`uds_access_context`]), and a request handler.
///
/// `owner_user_id` is the node owner the same-user peer runs as; `socket_kind`
/// is the socket the connection was accepted on (owner vs per-app); `transport` /
/// `verification` are the posture the accept loop
/// ([`super::uds::UdsConnVerdict::access_posture`]) attached to this connection.
/// On a successful parse, `handler` is called with the parsed request and the
/// stamped context and its [`UdsResponse`] is written back. On a parse failure
/// the content-free [`error_response`] is written instead — except when the peer
/// hung up before sending anything ([`UdsHttpError::Empty`]) or the transport
/// itself failed ([`UdsHttpError::Io`]), where there is nothing to answer and
/// the connection is simply closed.
///
/// One request per connection: the stream is read once, answered once
/// (`Connection: close`), and dropped by the caller. `handler` maps
/// `(&UdsRequest, &AccessContext)` to a response from the daemon.
pub fn serve_connection<H>(
    stream: &mut std::os::unix::net::UnixStream,
    owner_user_id: &str,
    socket_kind: SocketKind,
    transport: CallerTransport,
    verification: CallerVerification,
    handler: H,
) -> io::Result<()>
where
    H: FnOnce(&UdsRequest, &AccessContext) -> UdsResponse,
{
    serve_connection_with_timeout(
        stream,
        owner_user_id,
        socket_kind,
        transport,
        verification,
        UDS_CONN_BUDGET,
        handler,
    )
}

/// Aggregate per-connection wall-clock budget on the control socket (blocker
/// **B4**).
///
/// The accept loop is single-threaded and serves one connection at a time, so
/// without a bound one same-user peer can wedge the whole control socket for
/// every other app — a local slowloris. The earlier per-stage
/// `set_read_timeout` (MED-3) bounds a peer that stalls *completely*, but a
/// **slow-drip** peer that delivers just enough bytes to make forward progress
/// within each window resets that window indefinitely and never trips it.
///
/// The fix is an aggregate `Instant` deadline taken at accept: the remaining
/// budget is recomputed and re-armed before *every* recv/send syscall (see
/// [`DeadlineStream`]), so it shrinks **monotonically** across the whole
/// connection regardless of how the peer paces its bytes. 10s is generous for a
/// loopback-local peer (requests are small, and the handler's own work happens
/// after the read completes) while bounding the total wedge a single peer can
/// impose. (A bounded worker pool to move serving off the accept thread is a
/// possible further hardening — noted as a follow-up; the aggregate deadline
/// alone closes B4.)
const UDS_CONN_BUDGET: std::time::Duration = std::time::Duration::from_secs(10);

/// A `Read`/`Write` wrapper over a `UnixStream` that enforces an **aggregate**
/// wall-clock deadline (blocker **B4**) by re-arming the socket's
/// read/write timeout to the *remaining* budget before each syscall.
///
/// Because the remaining budget (`deadline - now`) only ever shrinks, a peer
/// cannot extend the connection's total lifetime by dribbling bytes: each recv
/// or send is bounded by what's left, and once the budget is exhausted every
/// further syscall fails immediately with [`io::ErrorKind::TimedOut`]. The
/// parser maps that to [`UdsHttpError::Io`] and the connection is dropped
/// content-free, exactly like a peer that hung up.
struct DeadlineStream<'a> {
    stream: &'a std::os::unix::net::UnixStream,
    deadline: std::time::Instant,
}

impl<'a> DeadlineStream<'a> {
    fn new(stream: &'a std::os::unix::net::UnixStream, deadline: std::time::Instant) -> Self {
        Self { stream, deadline }
    }

    /// Re-arm the relevant socket timeout to the remaining budget, or fail with
    /// `TimedOut` if the budget is already spent. `set` is the per-direction
    /// arming call (`set_read_timeout` / `set_write_timeout`); a same-user peer
    /// that already disconnected can make the arming call fail (`EINVAL`), which
    /// surfaces as a normal `io::Error` and drops the connection — never served
    /// budget-less.
    fn arm(&self, set: impl Fn(Option<std::time::Duration>) -> io::Result<()>) -> io::Result<()> {
        let remaining = self
            .deadline
            .saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "uds: aggregate per-connection deadline exhausted",
            ));
        }
        set(Some(remaining))
    }
}

impl io::Read for DeadlineStream<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.arm(|d| self.stream.set_read_timeout(d))?;
        (&*self.stream).read(buf)
    }
}

impl io::Write for DeadlineStream<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.arm(|d| self.stream.set_write_timeout(d))?;
        (&*self.stream).write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        (&*self.stream).flush()
    }
}

/// [`serve_connection`] with an explicit aggregate budget — split out so tests
/// can drive the deadline path in milliseconds instead of waiting out the
/// production constant.
///
/// The read phase has one aggregate deadline. [`DeadlineStream`] re-arms
/// the remaining budget before every receive, so a peer cannot extend that
/// phase by sending bytes slowly (blocker **B4**). The handler runs outside
/// this budget. The response write gets a fresh budget after the handler.
/// A read timeout maps to [`UdsHttpError::Io`] and closes the connection
/// without a response.
fn serve_connection_with_timeout<H>(
    stream: &mut std::os::unix::net::UnixStream,
    owner_user_id: &str,
    socket_kind: SocketKind,
    transport: CallerTransport,
    verification: CallerVerification,
    budget: std::time::Duration,
    handler: H,
) -> io::Result<()>
where
    H: FnOnce(&UdsRequest, &AccessContext) -> UdsResponse,
{
    // Read-phase budget (B4): bounds a slowloris peer during request parse.
    // Handler work is deliberately NOT under this budget — setup verbs such as
    // `POST /api/schemas/load` fetch the published catalog over the network and
    // routinely take longer than `UDS_CONN_BUDGET` (observed ~20s+). Counting
    // that work against the same deadline made the subsequent response write
    // fail with TimedOut → empty reply to the client even after a successful
    // load (first-run `brain init` / `kanban init` against Mini).
    let read_deadline = std::time::Instant::now() + budget;
    // Read from a buffered clone so the parser can own its reader while the
    // original handle stays free for the response write. The read half is
    // wrapped in a DeadlineStream so every recv re-arms the *remaining*
    // aggregate budget — a slow-drip peer can't reset the window per byte (B4).
    let read_half = stream.try_clone()?;
    let response = match read_request(io::BufReader::new(DeadlineStream::new(
        &read_half,
        read_deadline,
    ))) {
        Ok(request) => {
            // Re-read peer pid on the live stream for ops attribution. Accept
            // already gated same-user; this is best-effort and never blocks
            // the request if the credential read fails.
            let peer_pid = peer_pid_from_stream(stream);
            let base = uds_access_context(owner_user_id, socket_kind, transport, verification)
                .with_peer_pid(peer_pid);
            // Multi-DB handle (design-org-db-handle-platform-gap): SDK sends
            // `X-LastDB-Db`; Mini scopes mutate/query via storage_prefix.
            // Invalid locators fail closed with 400 so a typo cannot silently
            // land in personal home.
            let ctx = match base.with_db_handle(request.header(fold_db::access::LASTDB_DB_HEADER)) {
                Ok(ctx) => ctx,
                Err(msg) => {
                    return write_response(
                        &mut DeadlineStream::new(stream, std::time::Instant::now() + budget),
                        &UdsResponse::new(400, "Bad Request", msg.into_bytes())
                            .with_header("Content-Type", "text/plain; charset=utf-8"),
                    );
                }
            };
            handler(&request, &ctx)
        }
        // Peer hung up before a request, the transport failed, or the aggregate
        // deadline was exhausted mid-read (TimedOut → Io): nothing to answer, and
        // the write would just fail too. Close the connection content-free.
        Err(UdsHttpError::Empty | UdsHttpError::Io(_)) => return Ok(()),
        Err(e) => error_response(&e),
    };
    // Write-phase budget: a *fresh* window starting after the handler returns.
    // A peer that stops reading the response still can't wedge the accept loop
    // (same B4 write-side bound as before), but long-running local handler work
    // no longer starves the response write.
    let write_deadline = std::time::Instant::now() + budget;
    let mut write_half = DeadlineStream::new(stream, write_deadline);
    write_response(&mut write_half, &response)?;
    // `Connection: close`, one request per connection: shut down the write half
    // so the peer's read sees a clean EOF and its `read_to_end` returns instead
    // of blocking on a half-open socket. A failure here means the peer already
    // went away after we flushed the response — there is nothing left to do, so
    // it does not override the successful write.
    let _ = stream.shutdown(std::net::Shutdown::Write);
    Ok(())
}

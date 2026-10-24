//! Response framing and the per-connection serve loop (with its deadline
//! wrapper) for the control-socket HTTP/1.1 subset.

use super::*;
mod connection;
pub use connection::{finish_connection, receive_connection};

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
    let Some((request, context)) = connection::receive_with_budget(
        stream,
        owner_user_id,
        socket_kind,
        transport,
        verification,
        budget,
    )?
    else {
        return Ok(());
    };
    let response = handler(&request, &context);
    connection::finish_with_budget(stream, &response, budget)
}

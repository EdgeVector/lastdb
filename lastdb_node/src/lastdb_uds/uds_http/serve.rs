//! Response framing and per-connection deadline handling for the control socket.

use super::*;

/// An HTTP/1.1 response to write back over the control socket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UdsResponse {
    /// Numeric HTTP status code (e.g. `200`, `400`).
    pub status: u16,
    /// Reason phrase for the status line (e.g. `OK`, `Bad Request`).
    pub reason: &'static str,
    /// Response headers in wire order (`Content-Length` and `Connection` are owned by [`write_response`]).
    pub headers: Vec<(String, String)>,
    /// Response body bytes, written verbatim.
    pub body: Vec<u8>,
}

impl UdsResponse {
    /// Build a response with the given status, reason, and body and no extra headers.
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

fn is_reserved_response_header(name: &str) -> bool {
    name.eq_ignore_ascii_case("content-length") || name.eq_ignore_ascii_case("connection")
}

/// JSON `error` field when the worker queue is full (primary backpressure).
pub const BUSY_ERROR_QUEUE_FULL: &str = "uds_worker_queue_full";

/// Immediate busy response when the UDS worker queue is full.
pub fn write_busy_and_close(stream: &mut std::os::unix::net::UnixStream) -> io::Result<()> {
    let body = format!(r#"{{"status":"busy","error":"{BUSY_ERROR_QUEUE_FULL}"}}"#);
    let resp = UdsResponse::new(503, "Service Unavailable", body.into_bytes())
        .with_header("Content-Type", "application/json")
        .with_header("Connection", "close")
        .with_header("Retry-After", "1");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    let mut w = DeadlineStream::new(stream, deadline);
    write_response(&mut w, &resp)?;
    let _ = stream.shutdown(std::net::Shutdown::Write);
    Ok(())
}

/// Write an HTTP/1.1 response: status line, content length, `Connection: close`, headers, and body.
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

/// Map a content-free [`UdsHttpError`] to the HTTP response sent back when a request cannot be parsed (**I4**).
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
    UdsResponse::new(status, reason, reason.as_bytes().to_vec())
}

const UDS_CONN_BUDGET: std::time::Duration = std::time::Duration::from_secs(10);

struct DeadlineStream<'a> {
    stream: &'a std::os::unix::net::UnixStream,
    deadline: std::time::Instant,
}

impl<'a> DeadlineStream<'a> {
    fn new(stream: &'a std::os::unix::net::UnixStream, deadline: std::time::Instant) -> Self {
        Self { stream, deadline }
    }

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

/// Parse one request with the connection deadline and owner access context.
pub fn receive_connection(
    stream: &mut std::os::unix::net::UnixStream,
    owner_user_id: &str,
) -> io::Result<Option<(UdsRequest, AccessContext)>> {
    let read_half = stream.try_clone()?;
    let deadline = std::time::Instant::now() + UDS_CONN_BUDGET;
    let base = uds_access_context(owner_user_id);
    let request = match read_request_with_owner_blob(
        io::BufReader::new(DeadlineStream::new(&read_half, deadline)),
        base.is_owner,
    ) {
        Ok(request) => request,
        Err(UdsHttpError::Empty | UdsHttpError::Io(_)) => return Ok(None),
        Err(error) => {
            finish_connection(stream, &error_response(&error))?;
            return Ok(None);
        }
    };
    Ok(Some((request, base)))
}

/// Write one final response with a fresh deadline and an explicit EOF.
pub fn finish_connection(
    stream: &mut std::os::unix::net::UnixStream,
    response: &UdsResponse,
) -> io::Result<()> {
    let deadline = std::time::Instant::now() + UDS_CONN_BUDGET;
    write_response(&mut DeadlineStream::new(stream, deadline), response)?;
    let _ = stream.shutdown(std::net::Shutdown::Write);
    Ok(())
}

//! Minimal HTTP/1.1 framing and connection deadlines for the Unix-domain socket.

use std::io::{self, BufRead, Read, Write};
use std::os::unix::net::UnixStream;
use std::time::{Duration, Instant};

use fold_db::access::AccessContext;

pub const MAX_LINE_LEN: usize = 8 * 1024;
pub const MAX_HEADERS_TOTAL: usize = 64 * 1024;
pub const MAX_BODY_LEN: usize = 128 * 1024 * 1024;

const MAX_HEADER_COUNT: usize = 256;
const UDS_CONN_BUDGET: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UdsRequest {
    pub method: String,
    pub target: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl UdsRequest {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find_map(|(n, v)| n.eq_ignore_ascii_case(name).then_some(v.as_str()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UdsResponse {
    pub status: u16,
    pub reason: &'static str,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl UdsResponse {
    pub fn new(status: u16, reason: &'static str, body: Vec<u8>) -> Self {
        Self {
            status,
            reason,
            headers: Vec::new(),
            body,
        }
    }

    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }
}

enum HttpErr {
    Silent,
    Bad,
    Version,
    HeadersLarge,
    BodyLarge,
}

impl From<io::Error> for HttpErr {
    fn from(_: io::Error) -> Self {
        Self::Silent
    }
}

fn read_line<R: BufRead>(r: &mut R) -> Result<Option<Vec<u8>>, HttpErr> {
    let mut buf = Vec::new();
    if r.take((MAX_LINE_LEN as u64) + 2)
        .read_until(b'\n', &mut buf)?
        == 0
    {
        return Ok(None);
    }
    if buf.pop() != Some(b'\n') {
        return Err(if buf.len() > MAX_LINE_LEN {
            HttpErr::HeadersLarge
        } else {
            HttpErr::Bad
        });
    }
    if buf.last() == Some(&b'\r') {
        buf.pop();
    }
    if buf.len() > MAX_LINE_LEN {
        Err(HttpErr::HeadersLarge)
    } else {
        Ok(Some(buf))
    }
}

fn read_request<R: BufRead>(mut reader: R) -> Result<UdsRequest, HttpErr> {
    let line = read_line(&mut reader)?.ok_or(HttpErr::Silent)?;
    let s = std::str::from_utf8(&line).map_err(|_| HttpErr::Bad)?;
    let [method, target, version] = s.split(' ').collect::<Vec<_>>()[..] else {
        return Err(HttpErr::Bad);
    };
    if method.is_empty() || !method.bytes().all(|b| b.is_ascii_graphic()) || target.is_empty() {
        return Err(HttpErr::Bad);
    }
    if version != "HTTP/1.0" && version != "HTTP/1.1" {
        return Err(HttpErr::Version);
    }
    let (method, target) = (method.to_string(), target.to_string());
    let (mut headers, mut total) = (Vec::new(), 0usize);
    loop {
        let l = read_line(&mut reader)?.ok_or(HttpErr::Bad)?;
        if l.is_empty() {
            break;
        }
        total = total.saturating_add(l.len());
        if total > MAX_HEADERS_TOTAL || headers.len() >= MAX_HEADER_COUNT {
            return Err(HttpErr::HeadersLarge);
        }
        let (raw, val) = std::str::from_utf8(&l)
            .ok()
            .and_then(|s| s.split_once(':'))
            .ok_or(HttpErr::Bad)?;
        let n = raw.trim();
        if n.is_empty() || n.bytes().any(|b| b == b' ' || b == b'\t') {
            return Err(HttpErr::Bad);
        }
        headers.push((n.to_string(), val.trim().to_string()));
    }
    if headers
        .iter()
        .any(|(n, _)| n.eq_ignore_ascii_case("transfer-encoding"))
    {
        return Err(HttpErr::Bad);
    }
    let body = read_body(&mut reader, &headers, &method, &target)?;
    Ok(UdsRequest {
        method,
        target,
        headers,
        body,
    })
}

fn read_body(
    r: &mut impl Read,
    headers: &[(String, String)],
    method: &str,
    target: &str,
) -> Result<Vec<u8>, HttpErr> {
    let put_path = target == "/api/db/put-blob"
        || super::database_path::split(target).is_some_and(|(_, p)| p == "db/put-blob");
    let octet = headers
        .iter()
        .find(|(n, _)| n.eq_ignore_ascii_case("content-type"))
        .and_then(|(_, v)| v.split(';').next());
    let raw_blob = method == "POST"
        && put_path
        && octet.is_some_and(|v| v.trim().eq_ignore_ascii_case("application/octet-stream"));
    let max = if raw_blob {
        fold_db::storage_policy::native_write::LOCAL_FILE_INPUT_MEMORY_BYTES
    } else {
        MAX_BODY_LEN as u64
    };
    let (mut len, mut seen) = (0usize, false);
    for (n, v) in headers {
        if n.eq_ignore_ascii_case("content-length") {
            let p: usize = v.trim().parse().map_err(|_| HttpErr::Bad)?;
            if seen && p != len {
                return Err(HttpErr::Bad);
            }
            (len, seen) = (p, true);
        }
    }
    if len as u64 > max {
        return Err(HttpErr::BodyLarge);
    }
    let mut body = Vec::new();
    body.try_reserve_exact(len).map_err(|_| HttpErr::Silent)?;
    body.resize(len, 0);
    r.read_exact(&mut body).map_err(|e| {
        if e.kind() == io::ErrorKind::UnexpectedEof {
            HttpErr::Bad
        } else {
            HttpErr::Silent
        }
    })?;
    Ok(body)
}

pub(super) fn write_busy_and_close(stream: &mut UnixStream) -> io::Result<()> {
    let body = br#"{"status":"busy","error":"uds_worker_queue_full"}"#.to_vec();
    let resp = UdsResponse::new(503, "Service Unavailable", body)
        .with_header("Content-Type", "application/json")
        .with_header("Retry-After", "1");
    write_with_budget(stream, &resp, Duration::from_secs(2))
}

struct DeadlineStream<'a>(&'a UnixStream, Instant);

impl DeadlineStream<'_> {
    fn arm(&self, set: impl Fn(Option<Duration>) -> io::Result<()>) -> io::Result<()> {
        let rem = self.1.saturating_duration_since(Instant::now());
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
        self.arm(|d| self.0.set_read_timeout(d))?;
        self.0.read(buf)
    }
}

impl Write for DeadlineStream<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.arm(|d| self.0.set_write_timeout(d))?;
        self.0.write(buf)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

fn write_with_budget(
    stream: &mut UnixStream,
    resp: &UdsResponse,
    budget: Duration,
) -> io::Result<()> {
    let mut w = DeadlineStream(stream, Instant::now() + budget);
    write!(
        w,
        "HTTP/1.1 {} {}\r\nContent-Length: {}\r\nConnection: close\r\n",
        resp.status,
        resp.reason,
        resp.body.len()
    )?;
    for (n, v) in &resp.headers {
        if !n.eq_ignore_ascii_case("content-length") && !n.eq_ignore_ascii_case("connection") {
            write!(w, "{n}: {v}\r\n")?;
        }
    }
    write!(w, "\r\n")?;
    w.write_all(&resp.body)?;
    w.flush()?;
    let _ = stream.shutdown(std::net::Shutdown::Write);
    Ok(())
}

pub fn receive_connection(
    stream: &mut UnixStream,
    owner: &str,
) -> io::Result<Option<(UdsRequest, AccessContext)>> {
    let r = stream.try_clone()?;
    match read_request(io::BufReader::new(DeadlineStream(
        &r,
        Instant::now() + UDS_CONN_BUDGET,
    ))) {
        Ok(req) => Ok(Some((req, AccessContext::owner(owner)))),
        Err(HttpErr::Silent) => Ok(None),
        Err(err) => {
            let (st, reason) = match err {
                HttpErr::Version => (505, "HTTP Version Not Supported"),
                HttpErr::HeadersLarge => (431, "Request Header Fields Too Large"),
                HttpErr::BodyLarge => (413, "Payload Too Large"),
                HttpErr::Bad | HttpErr::Silent => (400, "Bad Request"),
            };
            finish_connection(stream, &UdsResponse::new(st, reason, reason.into()))?;
            Ok(None)
        }
    }
}

pub fn finish_connection(stream: &mut UnixStream, response: &UdsResponse) -> io::Result<()> {
    write_with_budget(stream, response, UDS_CONN_BUDGET)
}

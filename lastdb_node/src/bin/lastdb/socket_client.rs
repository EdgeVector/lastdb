//! Blocking unix-socket HTTP client and response parsers for the `lastdb` CLI.

use super::*;

pub(super) fn client_headers() -> String {
    format!(
        "X-LastDB-Client: lastdb\r\nX-LastDB-Request-Id: {}\r\n",
        uuid::Uuid::new_v4()
    )
}

/// POST JSON over the owner socket with the **admin** UDS deadline.
pub(super) fn post_json_admin(
    socket: &Path,
    path: &str,
    body: &serde_json::Value,
) -> Result<String, String> {
    post_json_with_timeout(socket, path, body, admin_scan_client_timeout())
}

pub(super) fn post_json_with_timeout(
    socket: &Path,
    path: &str,
    body: &serde_json::Value,
    timeout: Duration,
) -> Result<String, String> {
    post_json_with_id(socket, path, body, timeout, None)
}

pub(super) fn post_json_with_id(
    socket: &Path,
    path: &str,
    body: &serde_json::Value,
    timeout: Duration,
    id: Option<uuid::Uuid>,
) -> Result<String, String> {
    let body = serde_json::to_vec(body).map_err(|e| format!("failed to encode JSON: {e}"))?;
    let id_header = id.map_or_else(String::new, |id| format!("Idempotency-Key: {id}\r\n"));
    let mut request = format!(
        "POST {path} HTTP/1.1\r\nHost: localhost\r\n{}{id_header}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        client_headers(),
        body.len()
    )
    .into_bytes();
    request.extend_from_slice(&body);
    request_with_timeout(socket, &request, timeout)
}

pub(super) fn request_with_timeout(
    socket: &Path,
    bytes: &[u8],
    timeout: Duration,
) -> Result<String, String> {
    request_with_timeout_raw(socket, bytes, timeout)
        .map_err(|(op, err, _)| format_socket_io_error(op, &err, timeout))
}

pub(super) type SocketRequestResult = Result<String, (&'static str, std::io::Error, Duration)>;

pub(super) fn request_with_timeout_raw(
    socket: &Path,
    bytes: &[u8],
    timeout: Duration,
) -> SocketRequestResult {
    let response = request_with_timeout_raw_bytes(socket, bytes, timeout)?;
    let read_started = Instant::now();
    String::from_utf8(response).map_err(|error| {
        (
            "decode response",
            std::io::Error::new(ErrorKind::InvalidData, error),
            read_started.elapsed(),
        )
    })
}

pub(super) fn request_with_timeout_raw_bytes(
    socket: &Path,
    bytes: &[u8],
    timeout: Duration,
) -> Result<Vec<u8>, (&'static str, std::io::Error, Duration)> {
    let mut stream =
        UnixStream::connect(socket).map_err(|e| ("connect to daemon socket", e, Duration::ZERO))?;
    stream
        .set_read_timeout(Some(timeout))
        .map_err(|e| ("set socket read timeout", e, Duration::ZERO))?;
    stream
        .set_write_timeout(Some(timeout))
        .map_err(|e| ("set socket write timeout", e, Duration::ZERO))?;
    stream
        .write_all(bytes)
        .map_err(|e| ("write request", e, Duration::ZERO))?;

    let mut response = Vec::new();
    let read_started = Instant::now();
    stream
        .read_to_end(&mut response)
        .map_err(|e| ("read response", e, read_started.elapsed()))?;
    Ok(response)
}

pub(super) const READ_ONLY_TRANSIENT_ATTEMPTS: usize = 3;
pub(super) const READ_ONLY_RETRY_BACKOFF_MS: u64 = 25;
pub(super) const READ_ONLY_TRANSIENT_MAX_ELAPSED: Duration = Duration::from_secs(1);

/// Retry transient read-side EAGAIN/EWOULDBLOCK for idempotent status probes.
pub(super) fn request_readonly_with_retry(
    socket: &Path,
    bytes: &[u8],
    timeout: Duration,
) -> Result<String, String> {
    request_readonly_with_retry_using(
        timeout,
        |attempt_timeout| request_with_timeout_raw(socket, bytes, attempt_timeout),
        std::thread::sleep,
    )
}

pub(super) fn request_readonly_with_retry_using<Request, Backoff>(
    timeout: Duration,
    mut request: Request,
    mut backoff: Backoff,
) -> Result<String, String>
where
    Request: FnMut(Duration) -> SocketRequestResult,
    Backoff: FnMut(Duration),
{
    let started = Instant::now();

    for attempt in 1..=READ_ONLY_TRANSIENT_ATTEMPTS {
        let attempt_timeout = timeout
            .saturating_sub(started.elapsed())
            .max(Duration::from_millis(1));
        match request(attempt_timeout) {
            Ok(response) => return Ok(response),
            Err(("read response", err, elapsed))
                if err.kind() == ErrorKind::WouldBlock
                    && elapsed < attempt_timeout
                    && elapsed < READ_ONLY_TRANSIENT_MAX_ELAPSED =>
            {
                if attempt == READ_ONLY_TRANSIENT_ATTEMPTS {
                    return Err(format!(
                        "transient socket read remained unavailable after \
                         {READ_ONLY_TRANSIENT_ATTEMPTS} attempts; retry this command. \
                         The node may be healthy but busy; this is not an outage."
                    ));
                }
                backoff(Duration::from_millis(
                    READ_ONLY_RETRY_BACKOFF_MS * attempt as u64,
                ));
            }
            Err((op, err, _)) => return Err(format_socket_io_error(op, &err, timeout)),
        }
    }

    unreachable!("bounded retry loop always returns")
}

/// Map socket I/O failures to operator-honest language.
pub(super) fn format_socket_io_error(op: &str, err: &std::io::Error, timeout: Duration) -> String {
    if is_client_deadline_error(err) {
        let secs = timeout.as_secs().max(1);
        format!(
            "client timeout after {secs}s while waiting to {op} \
             (daemon may still be working). This is the CLI socket deadline, \
             not node backpressure. Raise it with \
             LASTDB_UDS_ADMIN_TIMEOUT_SECS=<secs> in the environment — the CLI \
             and the daemon both read it, so server and client stay matched. \
             Some subcommands also accept --timeout <secs>; not all define it."
        )
    } else {
        format!("failed to {op}: {err}")
    }
}

/// True when `err` is the OS surface of a socket read/write timeout.
pub(super) fn is_client_deadline_error(err: &std::io::Error) -> bool {
    matches!(
        err.kind(),
        ErrorKind::WouldBlock | ErrorKind::TimedOut | ErrorKind::Interrupted
    )
}

/// Probe `GET /health` over the owner socket with transient read retry.
pub(crate) fn probe_health(socket: &Path) -> Result<(), String> {
    let raw = request_readonly_with_retry(
        socket,
        b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        Duration::from_secs(2),
    )?;
    let first_line = raw.lines().next().unwrap_or("");
    if first_line.starts_with("HTTP/1.1 200") || first_line.starts_with("HTTP/1.0 200") {
        Ok(())
    } else {
        Err(format!("unexpected /health status: {first_line}"))
    }
}

pub(crate) fn format_bytes(n: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = 1024.0 * 1024.0;
    const GIB: f64 = 1024.0 * 1024.0 * 1024.0;
    let f = n as f64;
    if f >= GIB {
        format!("{:.2} GiB", f / GIB)
    } else if f >= MIB {
        format!("{:.1} MiB", f / MIB)
    } else if f >= KIB {
        format!("{:.1} KiB", f / KIB)
    } else {
        format!("{n} B")
    }
}

pub(crate) fn parse_json_response(
    response: &str,
    label: &str,
) -> Result<serde_json::Value, String> {
    let status_line = response.lines().next().unwrap_or("<empty response>");
    if !response.starts_with("HTTP/1.1 200 ") {
        let body = response
            .split_once("\r\n\r\n")
            .map_or("", |(_, b)| b.trim());
        return Err(format!("{label} returned {status_line}: {body}"));
    }
    let body = response
        .split_once("\r\n\r\n")
        .map(|(_, body)| body)
        .ok_or_else(|| format!("{label} response had no body"))?;
    serde_json::from_str(body).map_err(|e| format!("invalid {label} JSON: {e}"))
}

pub(crate) fn parse_binary_response(response: &[u8], label: &str) -> Result<Vec<u8>, String> {
    let header_end = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or_else(|| format!("{label} response had no headers"))?;
    let header = std::str::from_utf8(&response[..header_end])
        .map_err(|e| format!("invalid {label} response headers: {e}"))?;
    let status_line = header.lines().next().unwrap_or("<empty response>");
    if !status_line.starts_with("HTTP/1.1 200 ") {
        let body = String::from_utf8_lossy(&response[header_end + 4..]);
        return Err(format!("{label} returned {status_line}: {}", body.trim()));
    }
    Ok(response[header_end + 4..].to_vec())
}

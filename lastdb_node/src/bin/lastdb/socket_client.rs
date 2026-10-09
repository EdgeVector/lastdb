//! Blocking unix-socket HTTP client for the `lastdb` CLI: request headers,
//! `post_json*`, timeouts, and the read-only retry loop.

use super::*;

/// Headers every hand-built `lastdb` CLI request sends: the client self-ID
/// for Mini request-ops telemetry plus a fresh per-request correlation ID
/// (`x-lastdb-request-id`, rendered as `req=<id>` in `lastdb ops` Slowest
/// recent). Minted per call so two CLI requests never share an ID.
pub(super) fn client_headers() -> String {
    format!(
        "X-LastDB-Client: lastdb\r\nX-LastDB-Request-Id: {}\r\n",
        uuid::Uuid::new_v4()
    )
}

pub(super) fn post_json(
    socket: &Path,
    path: &str,
    body: &serde_json::Value,
) -> Result<String, String> {
    post_json_with_timeout(socket, path, body, Duration::from_secs(10))
}

/// POST JSON over the owner socket with the **admin** UDS deadline.
///
/// Use for long owner routes that the server already dispatches via
/// [`lastdb_node::exec::block_on_admin_route`] (inventory-class walks,
/// laststore snapshot, heal-staging, …). Generic [`post_json`] stays at
/// 10s for status calls. Durable GC admission and attachment use that short
/// deadline; neither request waits for the cloud executor.
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
    let body = serde_json::to_vec(body).map_err(|e| format!("failed to encode JSON: {e}"))?;
    let mut request = format!(
        "POST {path} HTTP/1.1\r\nHost: localhost\r\n{}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
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
///
/// The caller's timeout remains an overall budget: retries receive only the
/// time left after earlier attempts and backoff. A `WouldBlock` returned at the
/// configured deadline is still reported as a client timeout; only an early
/// read-side EAGAIN is retried. Mutating requests continue to use
/// [`request_with_timeout`] and are never replayed here.
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
///
/// On macOS/Unix a `set_read_timeout` deadline often surfaces as
/// `WouldBlock` / "Resource temporarily unavailable (os error 35)" — that is
/// **not** node backpressure (EAGAIN from a full accept queue); it is the CLI
/// giving up on its own deadline while the daemon may still be walking. Name
/// the duration and how to raise it.
///
/// Every socket call site shares this helper, so it cannot know which flags
/// its caller defines and must not prescribe one. `LASTDB_UDS_ADMIN_TIMEOUT_SECS`
/// is the remediation that always applies: [`admin_scan_client_timeout`] reads
/// it for the CLI's own deadline, and the daemon reads it for the server side,
/// so setting it in the environment raises both. `--timeout` exists only on
/// `db inventory`, `db schemas` and `ops`; naming it unconditionally sent
/// operators of `db gc-atoms` and `db compact-order-log` — the two verbs that
/// most often hit this deadline — to `unexpected argument '--timeout' found`.
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

pub(super) fn resolve_exemem_url(
    api_url: Option<String>,
    env: Option<&str>,
) -> Result<String, String> {
    match (api_url, env) {
        (Some(url), _) => Ok(url),
        (None, Some("dev")) => Ok(folddb_profile::endpoints::exemem_api_url_for(
            folddb_profile::endpoints::Environment::Dev,
        )
        .to_string()),
        (None, Some("prod")) => Ok(folddb_profile::endpoints::exemem_api_url_for(
            folddb_profile::endpoints::Environment::Prod,
        )
        .to_string()),
        (None, Some(other)) => Err(format!("unknown --env '{other}' (expected dev or prod)")),
        (None, None) => Ok(folddb_profile::endpoints::exemem_api_url()),
    }
}

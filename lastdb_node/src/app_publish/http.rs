//! Socket and HTTP helpers.

use super::*;

/// Headers every hand-built publish-path request sends: the client self-ID
/// for Mini request-ops telemetry plus a fresh per-request correlation ID
/// (`x-lastdb-request-id`, rendered as `req=<id>` in `lastdb ops` Slowest
/// recent). Minted per call so two requests never share an ID.
pub(super) fn client_headers() -> String {
    format!(
        "X-LastDB-Client: lastdb\r\nX-LastDB-Request-Id: {}\r\n",
        uuid::Uuid::new_v4()
    )
}

pub(super) fn socket_auto_identity(socket: &Path) -> Result<String, String> {
    let request = format!(
        "GET /api/system/auto-identity HTTP/1.1\r\nHost: localhost\r\n{}Connection: close\r\n\r\n",
        client_headers()
    );
    let (status, payload) = socket_request(socket, request.as_bytes())?;
    if status != 200 {
        return Err(format!("auto-identity returned {status}"));
    }
    payload
        .get("user_hash")
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| "auto-identity response missing user_hash".to_string())
}

pub(super) fn socket_post_json(
    socket: &Path,
    path: &str,
    user_hash: &str,
    body: &Value,
) -> Result<(u16, Value), String> {
    let body = serde_json::to_vec(body).map_err(|e| format!("failed to encode JSON: {e}"))?;
    let mut request = format!(
        "POST {path} HTTP/1.1\r\nHost: localhost\r\n{}X-User-Hash: {user_hash}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        client_headers(),
        body.len()
    )
    .into_bytes();
    request.extend_from_slice(&body);
    let timeout = if path == "/api/apps/declare-schema" {
        env_flag::var_parsed::<u64>("LASTDB_UDS_ADMIN_TIMEOUT_SECS")
            .filter(|value| *value > 0)
            .map_or(Duration::from_secs(600), Duration::from_secs)
    } else {
        Duration::from_secs(crate::exec::DEFAULT_CLI_UDS_TIMEOUT_SECS)
    };
    socket_request_with_timeout(socket, &request, timeout)
}

pub(super) fn socket_request(socket: &Path, bytes: &[u8]) -> Result<(u16, Value), String> {
    socket_request_with_timeout(
        socket,
        bytes,
        Duration::from_secs(crate::exec::DEFAULT_CLI_UDS_TIMEOUT_SECS),
    )
}

pub(super) fn socket_request_with_timeout(
    socket: &Path,
    bytes: &[u8],
    timeout: Duration,
) -> Result<(u16, Value), String> {
    let mut stream = UnixStream::connect(socket)
        .map_err(|e| format!("failed to connect to daemon socket: {e}"))?;
    stream
        .set_read_timeout(Some(timeout))
        .map_err(|e| format!("failed to set socket read timeout: {e}"))?;
    stream
        .set_write_timeout(Some(timeout))
        .map_err(|e| format!("failed to set socket write timeout: {e}"))?;
    stream
        .write_all(bytes)
        .map_err(|e| format!("failed to write request: {e}"))?;
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .map_err(|e| format!("failed to read response: {e}"))?;
    parse_http_response(&response)
}

/// Minimal HTTP/1.1 response parse: status code + JSON body (after the
/// header/body separator, tolerating chunked-free `Connection: close` bodies).
pub(super) fn parse_http_response(raw: &str) -> Result<(u16, Value), String> {
    let status = raw
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .ok_or_else(|| format!("unparseable HTTP response: {}", head(raw)))?;
    let body = raw.split_once("\r\n\r\n").map_or("", |(_, b)| b.trim());
    if body.is_empty() {
        return Ok((status, Value::Null));
    }
    // Tolerate chunked transfer encoding by extracting the outermost JSON
    // object if a plain parse fails.
    let payload = serde_json::from_str(body).or_else(|_| {
        let start = body.find('{');
        let end = body.rfind('}');
        match (start, end) {
            (Some(s), Some(e)) if e > s => serde_json::from_str(&body[s..=e]),
            _ => serde_json::from_str(body),
        }
    });
    match payload {
        Ok(v) => Ok((status, v)),
        Err(_) if status != 200 => Ok((status, json!({ "error": body }))),
        Err(e) => Err(format!("invalid JSON response body: {e}")),
    }
}

pub(super) fn head(raw: &str) -> &str {
    &raw[..raw.len().min(80)]
}

pub(super) fn http_client() -> Result<reqwest::Client, String> {
    // trace-egress: propagate (dev publish path; short-lived CLI client)
    reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .connect_timeout(SCHEMA_SERVICE_HTTP_CONNECT_TIMEOUT)
        .no_proxy()
        .build()
        .map_err(|e| format!("failed to build HTTP client: {e}"))
}

//! Unix domain socket HTTP/JSON client helpers for the `lastdb` CLI.

use serde_json::Value;
use std::io::{ErrorKind, Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;
use std::time::{Duration, Instant};
use uuid::Uuid;

type RawErr = (&'static str, std::io::Error, Duration);

pub(crate) fn print_pretty_json<T: serde::Serialize + ?Sized>(value: &T) {
    println!(
        "{}",
        serde_json::to_string_pretty(value).unwrap_or_default()
    );
}

pub(crate) fn client_headers() -> String {
    format!(
        "X-LastDB-Client: lastdb\r\nX-LastDB-Request-Id: {}\r\n",
        Uuid::new_v4()
    )
}

fn get_req(path: &str) -> String {
    format!(
        "GET {path} HTTP/1.1\r\nHost: localhost\r\n{}Connection: close\r\n\r\n",
        client_headers()
    )
}

pub(crate) fn get_path(socket: &Path, path: &str, timeout: Duration) -> Result<String, String> {
    request_with_timeout(socket, get_req(path).as_bytes(), timeout)
}

pub(crate) fn get_path_retry(
    socket: &Path,
    path: &str,
    timeout: Duration,
) -> Result<String, String> {
    let (req, started) = (get_req(path), Instant::now());
    for attempt in 1..=3usize {
        let budget = timeout
            .saturating_sub(started.elapsed())
            .max(Duration::from_millis(1));
        match raw_io(socket, req.as_bytes(), budget) {
            Ok(resp) => {
                return String::from_utf8(resp)
                    .map_err(|e| format!("failed to decode response: {e}"));
            }
            Err(("read response", err, el))
                if err.kind() == ErrorKind::WouldBlock
                    && el < budget
                    && el < Duration::from_secs(1) =>
            {
                if attempt == 3 {
                    return Err("transient socket read remained unavailable after 3 attempts; retry this command. The node may be healthy but busy; this is not an outage.".into());
                }
                std::thread::sleep(Duration::from_millis(25 * attempt as u64));
            }
            Err((op, err, _)) => return Err(format_socket_io_error(op, &err, timeout)),
        }
    }
    unreachable!()
}

pub(crate) fn build_post_request(path: &str, extra_headers: &str, body: &[u8]) -> Vec<u8> {
    let (hdr, len) = (client_headers(), body.len());
    let mut req = format!(
        "POST {path} HTTP/1.1\r\nHost: localhost\r\n{hdr}{extra_headers}Content-Length: {len}\r\nConnection: close\r\n\r\n"
    )
    .into_bytes();
    req.extend_from_slice(body);
    req
}

pub(crate) fn post_json_admin(socket: &Path, path: &str, body: &Value) -> Result<String, String> {
    post_json_with_id(
        socket,
        path,
        body,
        lastdb_node::lastdb_uds::admin_handler_timeout(),
        None,
    )
}

pub(crate) fn post_json_with_timeout(
    socket: &Path,
    path: &str,
    body: &Value,
    timeout: Duration,
) -> Result<String, String> {
    post_json_with_id(socket, path, body, timeout, None)
}

pub(crate) fn post_json_with_id(
    socket: &Path,
    path: &str,
    body: &Value,
    timeout: Duration,
    id: Option<Uuid>,
) -> Result<String, String> {
    let body = serde_json::to_vec(body).map_err(|e| format!("failed to encode JSON: {e}"))?;
    let extra = if let Some(id) = id {
        format!("Idempotency-Key: {id}\r\nContent-Type: application/json\r\n")
    } else {
        "Content-Type: application/json\r\n".into()
    };
    request_with_timeout(socket, &build_post_request(path, &extra, &body), timeout)
}

pub(crate) fn request_with_timeout_bytes(
    socket: &Path,
    bytes: &[u8],
    timeout: Duration,
) -> Result<Vec<u8>, String> {
    raw_io(socket, bytes, timeout).map_err(|(op, err, _)| format_socket_io_error(op, &err, timeout))
}

pub(crate) fn request_with_timeout(
    socket: &Path,
    bytes: &[u8],
    timeout: Duration,
) -> Result<String, String> {
    String::from_utf8(request_with_timeout_bytes(socket, bytes, timeout)?)
        .map_err(|e| format!("failed to decode response: {e}"))
}

fn raw_io(socket: &Path, bytes: &[u8], timeout: Duration) -> Result<Vec<u8>, RawErr> {
    let zero = |op| move |e| (op, e, Duration::ZERO);
    let mut s = UnixStream::connect(socket).map_err(zero("connect to daemon socket"))?;
    s.set_read_timeout(Some(timeout))
        .map_err(zero("set socket read timeout"))?;
    s.set_write_timeout(Some(timeout))
        .map_err(zero("set socket write timeout"))?;
    s.write_all(bytes).map_err(zero("write request"))?;
    let (mut resp, started) = (Vec::new(), Instant::now());
    s.read_to_end(&mut resp)
        .map_err(|e| ("read response", e, started.elapsed()))?;
    Ok(resp)
}

fn format_socket_io_error(op: &str, err: &std::io::Error, timeout: Duration) -> String {
    if matches!(
        err.kind(),
        ErrorKind::WouldBlock | ErrorKind::TimedOut | ErrorKind::Interrupted
    ) {
        let secs = timeout.as_secs().max(1);
        format!("client timeout after {secs}s while waiting to {op} (daemon may still be working). This is the CLI socket deadline, not node backpressure. Raise it with LASTDB_UDS_ADMIN_TIMEOUT_SECS=<secs> in the environment — the CLI and the daemon both read it, so server and client stay matched. Some subcommands also accept --timeout <secs>; not all define it.")
    } else {
        format!("failed to {op}: {err}")
    }
}

pub(crate) fn probe_health(socket: &Path) -> Result<(), String> {
    let raw = get_path_retry(socket, "/health", Duration::from_secs(2))?;
    let line = raw.lines().next().unwrap_or("");
    (line.starts_with("HTTP/1.1 200") || line.starts_with("HTTP/1.0 200"))
        .then_some(())
        .ok_or_else(|| format!("unexpected /health status: {line}"))
}

pub(crate) fn require_reachable(socket: &Path) -> Result<(), String> {
    probe_health(socket).map_err(|_| {
        format!(
            "lastdbd not reachable at {} — start the daemon first",
            socket.display()
        )
    })
}

pub(crate) fn split_http_200<'a>(resp: &'a [u8], label: &str) -> Result<&'a [u8], String> {
    let end = resp.windows(4).position(|w| w == b"\r\n\r\n");
    let head = std::str::from_utf8(end.map_or(resp, |i| &resp[..i]))
        .map_err(|e| format!("invalid {label} response headers: {e}"))?;
    let line = head.lines().next().unwrap_or("<empty response>");
    if !line.starts_with("HTTP/1.1 200 ") {
        let body = end.map_or_else(String::new, |i| {
            String::from_utf8_lossy(&resp[i + 4..]).trim().to_owned()
        });
        return Err(format!("{label} returned {line}: {body}"));
    }
    Ok(&resp[end.ok_or_else(|| format!("{label} response had no body"))? + 4..])
}

pub(crate) fn parse_json_response(response: &str, label: &str) -> Result<Value, String> {
    serde_json::from_slice(split_http_200(response.as_bytes(), label)?)
        .map_err(|e| format!("invalid {label} JSON: {e}"))
}

pub(crate) fn resolve_client_home_and_socket(
    data_dir: Option<PathBuf>,
) -> Result<(PathBuf, PathBuf), String> {
    let env_sock = data_dir
        .is_none()
        .then(|| std::env::var_os("LASTDB_SOCKET_PATH"))
        .flatten()
        .filter(|v| !v.is_empty());
    let home = lastdb_node::host::resolve_home(data_dir)?;
    let sock = if let Some(p) = env_sock {
        lastdb_node::profile::paths::expand_tilde_path(p)?
    } else {
        lastdb_node::lastdb_uds::socket_path(&home.join("data"))
    };
    Ok((home, sock))
}

/// Build, where it was read from, app API version, outbox format version, and
/// the minimum app API version the node still reads registry lists for.
///
/// A running node whose `/api/version` reply has no `app_api_version` is older
/// than this CLI. The error names that build and says to upgrade the node.
/// It does not substitute the socket version. When no node answers, the app
/// API versions are the constants compiled into this build.
pub(crate) fn running_node(home: &Path) -> Result<(String, &'static str, u32, u32, u32), String> {
    let sock = lastdb_node::lastdb_uds::socket_path(&home.join("data"));
    if let Ok(v) = get_path(&sock, "/api/version", Duration::from_secs(2))
        .and_then(|raw| parse_json_response(&raw, "version"))
    {
        let u32_f = |k: &str| v.get(k)?.as_u64().and_then(|n| u32::try_from(n).ok());
        if let (Some(b), Some(_socket_api), Some(o)) = (
            v["build"].as_str(),
            u32_f("api_version"),
            u32_f("current_outbox_format_version"),
        ) {
            let Some(app_api) = u32_f("app_api_version") else {
                return Err(format!(
                    "the running node build {b} does not report app_api_version; upgrade the node before registry commands"
                ));
            };
            // A node that does not report a minimum supports only its own version.
            let min_app = u32_f("min_supported_app_api_version").unwrap_or(app_api);
            return Ok((b.to_string(), "socket", app_api, o, min_app));
        }
    }
    let (app_api, outbox, min_app) = (
        lastdb_node::version::APP_API_VERSION,
        lastdb_node::version::CURRENT_OUTBOX_FORMAT_VERSION,
        lastdb_node::version::MIN_SUPPORTED_APP_API_VERSION,
    );
    let sibling = std::env::current_exe()
        .ok()
        .and_then(|e| e.parent().map(|p| p.join("lastdbd")));
    for c in sibling.into_iter().chain([PathBuf::from("lastdbd")]) {
        let Ok(out) = ProcessCommand::new(c)
            .arg("--version")
            .env("RUST_LOG", "off")
            .output()
        else {
            continue;
        };
        if !out.status.success() {
            continue;
        }
        for l in String::from_utf8_lossy(&out.stdout).lines() {
            if let Some(b) = l
                .trim()
                .strip_prefix("lastdbd ")
                .map(str::trim)
                .filter(|b| !b.is_empty())
            {
                return Ok((b.to_string(), "lastdbd", app_api, outbox, min_app));
            }
        }
    }
    Ok((
        lastdb_node::version::build_version().to_string(),
        "cli",
        app_api,
        outbox,
        min_app,
    ))
}

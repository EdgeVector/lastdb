//! The build string of the node this machine runs.

use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;
use std::time::Duration;

use serde_json::Value;

// ─── Node version ─────────────────────────────────────────────────────────

/// The build string of the node this machine runs.
///
/// Order: `--lastdb-version` override, then `GET /api/version` on the owner
/// socket (the running daemon is the truth), then `lastdbd --version` from
/// the daemon next to this CLI (a node older than the handshake route), then
/// this binary's own build string (the CLI and the daemon ship in one bottle).
#[must_use]
pub fn running_lastdb_version(
    override_version: Option<&str>,
    socket: &Path,
) -> (String, &'static str) {
    if let Some(v) = override_version.map(str::trim).filter(|v| !v.is_empty()) {
        return (v.to_string(), "flag");
    }
    if let Some(build) = socket_api_version_build(socket) {
        return (build, "socket");
    }
    if let Some(build) = sibling_lastdbd_version() {
        return (build, "lastdbd");
    }
    (
        crate::ops::crash_attribution::build_version().to_string(),
        "cli",
    )
}

/// `lastdbd --version` from the daemon that ships next to this CLI (same
/// bottle), then from `PATH`. A node that predates `/api/version` still has
/// a build string; this is how a pre-handshake node is named.
fn sibling_lastdbd_version() -> Option<String> {
    let mut candidates: Vec<PathBuf> = Vec::new();
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            candidates.push(dir.join("lastdbd"));
        }
    }
    candidates.push(PathBuf::from("lastdbd"));
    for candidate in candidates {
        let output = ProcessCommand::new(&candidate)
            .arg("--version")
            .env("RUST_LOG", "off")
            .output();
        let Ok(output) = output else { continue };
        if !output.status.success() {
            continue;
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        for line in stdout.lines() {
            if let Some(rest) = line.trim().strip_prefix("lastdbd ") {
                let build = rest.trim();
                if !build.is_empty() {
                    return Some(build.to_string());
                }
            }
        }
    }
    None
}

fn socket_api_version_build(socket: &Path) -> Option<String> {
    use std::io::{Read, Write};
    use std::os::unix::net::UnixStream;
    let mut stream = UnixStream::connect(socket).ok()?;
    let timeout = Some(Duration::from_secs(5));
    stream.set_read_timeout(timeout).ok()?;
    stream.set_write_timeout(timeout).ok()?;
    stream
        .write_all(
            b"GET /api/version HTTP/1.1\r\nHost: localhost\r\nX-LastDB-Client: lastdb\r\nConnection: close\r\n\r\n",
        )
        .ok()?;
    let mut raw = String::new();
    stream.read_to_string(&mut raw).ok()?;
    let status: u16 = raw.split_whitespace().nth(1)?.parse().ok()?;
    if status != 200 {
        return None;
    }
    let body = raw.split_once("\r\n\r\n")?.1.trim();
    let start = body.find('{')?;
    let end = body.rfind('}')?;
    let payload: Value = serde_json::from_str(&body[start..=end]).ok()?;
    payload
        .get("build")
        .and_then(Value::as_str)
        .map(str::to_string)
        .filter(|b| !b.is_empty())
}

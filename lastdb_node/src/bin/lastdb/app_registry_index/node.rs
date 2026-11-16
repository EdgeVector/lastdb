//! The running node's build and contract versions, read from its socket or its binary.

use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;
use std::time::Duration;

use serde_json::Value;

/// Build, where it was read from, API version, outbox format version, and the
/// minimum API version the node still supports.
#[must_use]
pub fn running_node(home: &Path) -> (String, &'static str, u32, u32, u32) {
    if let Some(v) = socket_version(&lastdb_node::lastdb_uds::socket_path(&home.join("data"))) {
        let u32_f = |k| v.get(k)?.as_u64().and_then(|n| u32::try_from(n).ok());
        if let (Some(b), Some(a), Some(o)) = (
            v["build"].as_str(),
            u32_f("api_version"),
            u32_f("current_outbox_format_version"),
        ) {
            // A node that does not report a minimum supports only its own version.
            let min = u32_f("min_supported_api_version").unwrap_or(a);
            return (b.to_string(), "socket", a, o, min);
        }
    }
    let (api, outbox, min_api) = (
        lastdb_node::version::CURRENT_API_VERSION,
        lastdb_node::version::CURRENT_OUTBOX_FORMAT_VERSION,
        lastdb_node::version::MIN_SUPPORTED_API_VERSION,
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
                return (b.to_string(), "lastdbd", api, outbox, min_api);
            }
        }
    }
    (
        lastdb_node::version::build_version().to_string(),
        "cli",
        api,
        outbox,
        min_api,
    )
}

fn socket_version(socket: &Path) -> Option<Value> {
    let mut s = UnixStream::connect(socket).ok()?;
    let t = Some(Duration::from_secs(5));
    s.set_read_timeout(t).ok()?;
    s.set_write_timeout(t).ok()?;
    s.write_all(b"GET /api/version HTTP/1.1\r\nHost: localhost\r\nX-LastDB-Client: lastdb\r\nConnection: close\r\n\r\n").ok()?;
    let mut raw = String::new();
    s.read_to_string(&mut raw).ok()?;
    (raw.split_whitespace().nth(1) == Some("200")).then_some(())?;
    let body = raw.split_once("\r\n\r\n")?.1.trim();
    serde_json::from_str(&body[body.find('{')?..=body.rfind('}')?]).ok()
}

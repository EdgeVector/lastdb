//! What `lastdb upgrade` asks of a running daemon: the preflight snapshot, the drain
//! before a stop, and the checks after a start. All calls go over the owner socket.

use std::path::Path;
use std::time::Duration;

use serde_json::{json, Value};
use sha2::{Digest as _, Sha256};

use crate::{get_path, parse_json_response, post_json_with_timeout, probe_health, DatabaseTarget};

const SOCKET_TIMEOUT: Duration = Duration::from_secs(10);

/// One record of the largest schema, read before the stop and again after the start.
#[derive(Debug, Clone)]
pub(super) struct Sample {
    pub(super) schema: String,
    pub(super) hash: String,
    pub(super) range: Option<String>,
    pub(super) record_hash: String,
}

#[derive(Debug, Clone)]
pub(super) struct Snapshot {
    pub(super) pid: u32,
    pub(super) start_ts: u64,
    pub(super) build: String,
    pub(super) head: u64,
    pub(super) acked: u64,
    pub(super) cloud_connected: bool,
    pub(super) user_hash: String,
    pub(super) sample: Option<Sample>,
}

pub(super) fn status_code(raw: &str) -> u16 {
    raw.lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .unwrap_or(0)
}

fn body_of(raw: &str) -> &str {
    raw.split_once("\r\n\r\n")
        .map_or("", |(_, body)| body.trim())
}

fn get_json(socket: &Path, path: &str, label: &str) -> Result<Value, String> {
    let raw = get_path(socket, path, SOCKET_TIMEOUT)?;
    parse_json_response(&raw, label)
}

pub(super) fn health_ok(socket: &Path) -> bool {
    probe_health(socket).is_ok()
}

/// The pid and start time the daemon reports, and its build.
pub(super) fn boot_identity(socket: &Path) -> Result<(u32, u64, String), String> {
    let v = get_json(socket, "/api/system/boot-identity", "boot identity")?;
    let pid = v["pid"]
        .as_u64()
        .and_then(|p| u32::try_from(p).ok())
        .ok_or("boot identity has no pid")?;
    let start = v["process_start_ts"]
        .as_u64()
        .ok_or("boot identity has no start time")?;
    let build = v["build"].as_str().ok_or("boot identity has no build")?;
    Ok((pid, start, build.to_owned()))
}

pub(super) fn writer_failed(socket: &Path) -> Result<bool, String> {
    let v = get_json(socket, "/api/db/native-status", "native status")?;
    v["native_disk"]["writer_failed"]
        .as_bool()
        .ok_or_else(|| "native status has no writer_failed".to_string())
}

fn spool_and_cloud(socket: &Path) -> Result<(u64, u64, bool), String> {
    let v = get_json(socket, "/api/status", "status")?;
    let status = v.get("status").ok_or("status reply has no status")?;
    let spool = &status["spool"];
    let head = spool["head"].as_u64().unwrap_or(0);
    let acked = spool["acked"].as_u64().unwrap_or(0);
    let connected = status["cloud_sync"]["connected"].as_bool().unwrap_or(false);
    Ok((head, acked, connected))
}

fn target(socket: &Path) -> DatabaseTarget {
    DatabaseTarget {
        socket: socket.to_path_buf(),
        selector: None,
    }
}

/// Read one record and hash what the node returns for it.
pub(super) fn record_hash(
    socket: &Path,
    schema: &str,
    hash: &str,
    range: Option<&str>,
) -> Result<String, String> {
    let item = crate::v1_records::record_item(schema, hash, range, None);
    let page = crate::v1_records::read_page(&target(socket), &item, SOCKET_TIMEOUT)?;
    let bytes = serde_json::to_vec(&page["records"]).map_err(|e| e.to_string())?;
    Ok(fold_db::hex::hex_lower(&Sha256::digest(&bytes)))
}

/// The first key of a `keys_only` listing of the schema with the most records, and
/// the hash of that record. `None` when the home holds no record.
fn pick_sample(socket: &Path) -> Option<Sample> {
    let report = get_json(socket, "/api/db/schemas", "schema list").ok()?;
    let schemas = report["schema_storage"]["per_schema"].as_array()?;
    let largest = schemas
        .iter()
        .filter(|s| s["record_count"].as_u64().unwrap_or(0) > 0)
        .max_by(|a, b| {
            let count = |s: &Value| s["record_count"].as_u64().unwrap_or(0);
            count(a)
                .cmp(&count(b))
                .then_with(|| b["schema_name"].as_str().cmp(&a["schema_name"].as_str()))
        })?;
    let schema = largest["schema_name"].as_str()?.to_owned();
    let item = json!({ "schema": schema, "keys_only": true, "limit": 1 });
    let page = crate::v1_records::read_page(&target(socket), &item, SOCKET_TIMEOUT).ok()?;
    let key = page.get("keys")?.as_array()?.first()?.clone();
    let hash = key["hash"].as_str()?.to_owned();
    let range = key["range"]
        .as_str()
        .filter(|r| !r.is_empty())
        .map(str::to_owned);
    let record_hash = record_hash(socket, &schema, &hash, range.as_deref()).ok()?;
    Some(Sample {
        schema,
        hash,
        range,
        record_hash,
    })
}

/// Step 3: the running daemon is healthy, is the service's process, and its position
/// is recorded. Any failure here changes nothing.
pub(super) fn preflight(socket: &Path, service_pid: Option<u32>) -> Result<Snapshot, String> {
    if !health_ok(socket) {
        return Err("the daemon does not answer /health".into());
    }
    let (pid, start_ts, build) = boot_identity(socket)?;
    match service_pid {
        Some(expected) if expected == pid => {}
        Some(expected) => {
            return Err(format!(
                "the daemon on the socket is pid {pid}, but the service runs pid {expected}; another process holds this home"
            ));
        }
        None => return Err("the service has no running process".into()),
    }
    if writer_failed(socket)? {
        return Err("the daemon's writer has failed; it needs a human before any upgrade".into());
    }
    let (head, acked, cloud_connected) = spool_and_cloud(socket)?;
    let id = get_json(socket, "/api/system/auto-identity", "identity")?;
    let user_hash = id["user_hash"]
        .as_str()
        .ok_or("identity has no user hash")?
        .to_owned();
    Ok(Snapshot {
        pid,
        start_ts,
        build,
        head,
        acked,
        cloud_connected,
        user_hash,
        sample: pick_sample(socket),
    })
}

/// What the drain said.
pub(super) struct Drained {
    pub(super) head: u64,
    pub(super) acked: u64,
    /// `current`, `behind` or `off`.
    pub(super) cloud: String,
    pub(super) detail: String,
}

pub(super) enum Drain {
    Done(Drained),
    /// The daemon has no `prepare-stop`. The sampled status stands in for it.
    Unsupported(Drained),
    Failed(String),
}

/// Step 4: `POST /api/system/prepare-stop`. The daemon refuses new writes and sends what
/// the spool holds. A daemon that answers 404 gets the sampled check of the old driver.
pub(super) fn prepare_stop(socket: &Path, snap: &Snapshot) -> Drain {
    let body = json!({ "upload": true, "deadline_ms": 120_000, "hold_ms": 300_000 });
    let raw = match post_json_with_timeout(
        socket,
        "/api/system/prepare-stop",
        &body,
        Duration::from_secs(180),
    ) {
        Ok(raw) => raw,
        Err(e) => return Drain::Failed(format!("prepare-stop failed: {e}")),
    };
    match status_code(&raw) {
        200 => {
            let Ok(v) = serde_json::from_str::<Value>(body_of(&raw)) else {
                return Drain::Failed("prepare-stop answered with invalid JSON".into());
            };
            let cloud = v["cloud"].as_str().unwrap_or("").to_owned();
            if !matches!(cloud.as_str(), "current" | "behind" | "off") {
                return Drain::Failed("prepare-stop answered with an unknown cloud state".into());
            }
            Drain::Done(Drained {
                head: v["head"].as_u64().unwrap_or(0),
                acked: v["acked"].as_u64().unwrap_or(0),
                cloud,
                detail: v["detail"].as_str().unwrap_or("").to_owned(),
            })
        }
        404 => {
            let behind = snap.cloud_connected && snap.acked < snap.head;
            let cloud = match (snap.cloud_connected, behind) {
                (false, _) => "off",
                (true, true) => "behind",
                (true, false) => "current",
            };
            Drain::Unsupported(Drained {
                head: snap.head,
                acked: snap.acked,
                cloud: cloud.to_owned(),
                detail: "this daemon has no prepare-stop; the status was sampled".into(),
            })
        }
        code => Drain::Failed(format!(
            "prepare-stop answered HTTP {code}: {}",
            body_of(&raw)
        )),
    }
}

/// Let the daemon take writes again. Best effort: a daemon with no hold, or one that
/// is already gone, has nothing to cancel.
pub(super) fn cancel_stop(socket: &Path) {
    let _ = post_json_with_timeout(
        socket,
        "/api/system/prepare-stop/cancel",
        &json!({}),
        SOCKET_TIMEOUT,
    );
}

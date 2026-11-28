//! What `lastdb upgrade` asks of a running daemon: the preflight snapshot, the drain
//! before a stop, and the checks after a start. All calls go over the owner socket.

use std::collections::BTreeMap;
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
    /// The node has saved cloud credentials (`cloud_sync.connected`).
    pub(super) cloud_connected: bool,
    /// Cloud sync is set up at all: credentials, an environment, or an error reading them.
    pub(super) cloud_configured: bool,
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

struct Spool {
    head: u64,
    acked: u64,
    connected: bool,
    configured: bool,
}

/// The spool position and the cloud state. A missing field is refused, never read as 0
/// or false: a wrong 0 would make a backlog look sent.
fn spool_and_cloud(socket: &Path) -> Result<Spool, String> {
    let v = get_json(socket, "/api/status", "status")?;
    let status = v.get("status").ok_or("status reply has no status")?;
    let sync = status
        .get("cloud_sync")
        .filter(|s| s.is_object())
        .ok_or("status reply has no cloud_sync")?;
    let connected = sync["connected"]
        .as_bool()
        .ok_or("status reply has no cloud_sync.connected")?;
    let configured = connected
        || sync["environment"].as_str().is_some()
        || sync["last_error"].as_str().is_some();
    let (head, acked) = match status.get("spool").filter(|s| !s.is_null()) {
        Some(spool) => (
            spool["head"]
                .as_u64()
                .ok_or("status reply has no spool.head")?,
            spool["acked"]
                .as_u64()
                .ok_or("status reply has no spool.acked")?,
        ),
        // A home that captures nothing for the cloud has no spool and no backlog.
        None if !configured => (0, 0),
        None => return Err("cloud sync is set up, but the status reply has no spool".into()),
    };
    Ok(Spool {
        head,
        acked,
        connected,
        configured,
    })
}

fn target(socket: &Path) -> DatabaseTarget {
    DatabaseTarget {
        socket: socket.to_path_buf(),
        selector: None,
    }
}

/// Read one record and hash its canonical JSON response.
///
/// Query records use hash maps for fields and metadata. A daemon restart can therefore
/// change their JSON member order without changing the record. Sort every object before
/// hashing so the updater compares the record value rather than that incidental order.
pub(super) fn record_hash(
    socket: &Path,
    schema: &str,
    hash: &str,
    range: Option<&str>,
) -> Result<String, String> {
    let item = crate::v1_records::record_item(schema, hash, range, None);
    let page = crate::v1_records::read_page(&target(socket), &item, SOCKET_TIMEOUT)?;
    let records = canonical_json(&page["records"]);
    let bytes = serde_json::to_vec(&records).map_err(|e| e.to_string())?;
    Ok(fold_db::hex::hex_lower(Sha256::digest(&bytes)))
}

fn canonical_json(value: &Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.iter().map(canonical_json).collect()),
        Value::Object(fields) => {
            let ordered: BTreeMap<_, _> = fields
                .iter()
                .map(|(key, value)| (key.clone(), canonical_json(value)))
                .collect();
            Value::Object(ordered.into_iter().collect())
        }
        _ => value.clone(),
    }
}

/// The first key of a `keys_only` listing of the schema with the most records, and
/// the hash of that record. `None` only when the home holds no record; any read error is
/// an error, so the sample check is never dropped silently.
fn pick_sample(socket: &Path) -> Result<Option<Sample>, String> {
    let report = get_json(socket, "/api/db/schemas", "schema list")?;
    let schemas = report["schema_storage"]["per_schema"]
        .as_array()
        .ok_or("the schema list has no per_schema array")?;
    let mut largest: Option<(u64, &str)> = None;
    for s in schemas {
        let name = s["schema_name"]
            .as_str()
            .ok_or("a schema list entry has no schema_name")?;
        let count = s["record_count"]
            .as_u64()
            .ok_or("a schema list entry has no record_count")?;
        let better = largest.is_none_or(|(c, n)| count > c || (count == c && name < n));
        if count > 0 && better {
            largest = Some((count, name));
        }
    }
    let Some((_, schema)) = largest else {
        return Ok(None);
    };
    let schema = schema.to_owned();
    let item = json!({ "schema": schema, "keys_only": true, "limit": 1 });
    let page = crate::v1_records::read_page(&target(socket), &item, SOCKET_TIMEOUT)?;
    let key = page
        .get("keys")
        .and_then(Value::as_array)
        .ok_or("the key listing of the largest schema has no keys")?
        .first()
        .ok_or(
            "the schema list counts records, but the key listing of the largest schema is empty",
        )?;
    let hash = key["hash"]
        .as_str()
        .ok_or("the first key of the largest schema has no hash")?
        .to_owned();
    let range = key["range"]
        .as_str()
        .filter(|r| !r.is_empty())
        .map(str::to_owned);
    let record_hash = record_hash(socket, &schema, &hash, range.as_deref())?;
    Ok(Some(Sample {
        schema,
        hash,
        range,
        record_hash,
    }))
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
    let spool = spool_and_cloud(socket)?;
    let id = get_json(socket, "/api/system/auto-identity", "identity")?;
    let user_hash = id["user_hash"]
        .as_str()
        .ok_or("identity has no user hash")?
        .to_owned();
    let sample = pick_sample(socket).map_err(|e| format!("cannot read the sample record: {e}"))?;
    Ok(Snapshot {
        pid,
        start_ts,
        build,
        head: spool.head,
        acked: spool.acked,
        cloud_connected: spool.connected,
        cloud_configured: spool.configured,
        user_hash,
        sample,
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
            drained_reply(&v).map_or_else(Drain::Failed, Drain::Done)
        }
        404 => {
            // An old daemon: the sampled status. A backlog counts as behind whenever cloud
            // sync is set up, connected now or not; only a home with no cloud sync is off.
            let cloud = if snap.cloud_configured && snap.acked < snap.head {
                "behind"
            } else if snap.cloud_configured {
                "current"
            } else {
                "off"
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

/// The fields of a prepare-stop answer. A missing field is refused.
fn drained_reply(v: &Value) -> Result<Drained, String> {
    let field = |name: &str| format!("prepare-stop answered without {name}");
    let cloud = v["cloud"].as_str().ok_or_else(|| field("cloud"))?;
    if !matches!(cloud, "current" | "behind" | "off") {
        return Err("prepare-stop answered with an unknown cloud state".into());
    }
    Ok(Drained {
        head: v["head"].as_u64().ok_or_else(|| field("head"))?,
        acked: v["acked"].as_u64().ok_or_else(|| field("acked"))?,
        cloud: cloud.to_owned(),
        detail: v["detail"]
            .as_str()
            .ok_or_else(|| field("detail"))?
            .to_owned(),
    })
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

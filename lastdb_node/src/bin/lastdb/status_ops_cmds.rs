use super::*;
use lastdb_node::lastdb_uds::database_path::{self, DATABASES_PATH, DATABASE_SCOPE_PREFIX};
use std::borrow::Cow;

pub(crate) struct DatabaseTarget {
    pub(crate) socket: PathBuf,
    selector: Option<String>,
}

impl DatabaseTarget {
    pub(crate) fn open(data_dir: Option<PathBuf>, selector: Option<&str>) -> Result<Self, String> {
        let (_, socket) = resolve_client_home_and_socket(data_dir)?;
        let selector = selector
            .map(|value| resolve_database(&socket, value))
            .transpose()?;
        Ok(Self { socket, selector })
    }

    pub(crate) fn path<'p>(&self, path: &'p str) -> Cow<'p, str> {
        if let Some(selector) = self.selector.as_deref() {
            Cow::Owned(format!(
                "{DATABASE_SCOPE_PREFIX}{selector}{}",
                path.strip_prefix("/api")
                    .expect("database path starts with /api")
            ))
        } else {
            Cow::Borrowed(path)
        }
    }

    pub(crate) fn is_explicit(&self) -> bool {
        self.selector.is_some()
    }

    pub(crate) fn selector(&self) -> Option<&str> {
        self.selector.as_deref()
    }
}

fn resolve_database(socket: &Path, selector: &str) -> Result<String, String> {
    if !database_path::valid_selector(selector) {
        return Err("invalid database ID or name".into());
    }
    if selector == "personal" || cloud_backup_protocol::Id::try_from(selector.to_owned()).is_ok() {
        return Ok(selector.to_owned());
    }
    let raw = get_path(socket, DATABASES_PATH, Duration::from_secs(10))?;
    let response = parse_json_response(&raw, "database list")?;
    let list = response["databases"]
        .as_array()
        .ok_or("missing database list")?;
    let id = list
        .iter()
        .find(|db| db["name"].as_str() == Some(selector))
        .and_then(|db| db["id"].as_str())
        .filter(|id| cloud_backup_protocol::Id::try_from((*id).to_owned()).is_ok())
        .ok_or("database is not registered on this node")?;
    Ok(id.to_owned())
}

pub(super) fn list_databases(data_dir: Option<PathBuf>, json: bool) -> Result<(), String> {
    let target = DatabaseTarget::open(data_dir, None)?;
    let raw = get_path(&target.socket, DATABASES_PATH, Duration::from_secs(10))?;
    let response = parse_json_response(&raw, "database list")?;
    if json {
        println!("{response}");
    } else {
        let list = response["databases"]
            .as_array()
            .ok_or("missing database list")?;
        for db in list {
            let id = db["id"].as_str().ok_or("database has no ID")?;
            let name = db["name"].as_str().unwrap_or(id);
            let access = db["access"].as_str().ok_or("database has no access mode")?;
            let state = db["state"].as_str().ok_or("database has no state")?;
            println!("{name}\t{id}\t{access}\t{state}");
        }
    }
    Ok(())
}

pub(crate) fn truncate_label(label: &str, max: usize) -> String {
    if label.chars().count() <= max {
        return label.to_string();
    }
    let mut out: String = label.chars().take(max.saturating_sub(1)).collect();
    out.push('…');
    out
}

pub(crate) fn inventory_client_timeout(timeout_secs: Option<u64>) -> Duration {
    timeout_secs
        .filter(|s| *s > 0)
        .map_or_else(admin_scan_client_timeout, Duration::from_secs)
}

pub(crate) fn write_product_file_atomic(path: &Path, bytes: &[u8]) -> Result<(), String> {
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("create out dir {}: {e}", parent.display()))?;
    }
    let mut name = path.as_os_str().to_owned();
    name.push(format!(".tmp.{}", std::process::id()));
    let tmp = PathBuf::from(name);
    if let Err(e) = std::fs::write(&tmp, bytes) {
        let _ = std::fs::remove_file(&tmp);
        return Err(format!("write inventory temp {}: {e}", tmp.display()));
    }
    if let Err(e) = std::fs::rename(&tmp, path) {
        let _ = std::fs::remove_file(&tmp);
        return Err(format!("rename inventory out to {}: {e}", path.display()));
    }
    Ok(())
}

pub(super) const STATUS_UNREACHABLE_EXIT: i32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StatusRun {
    Serving,
    Unreachable,
}

fn probe_status_json(socket: &Path, timeout: Duration) -> Result<serde_json::Value, String> {
    let req = format!(
        "GET /api/status HTTP/1.1\r\nHost: localhost\r\n{}Connection: close\r\n\r\n",
        client_headers()
    );
    let raw = request_readonly_with_retry(socket, req.as_bytes(), timeout)?;
    parse_json_response(&raw, "status")?
        .get("status")
        .cloned()
        .ok_or_else(|| "status response missing status object".into())
}

pub(super) fn status(
    data_dir: Option<PathBuf>,
    json: bool,
    timeout_secs: Option<u64>,
) -> Result<StatusRun, String> {
    let default_timeout = lastdb_node::exec::admin_handler_timeout_override().unwrap_or(
        Duration::from_secs(lastdb_node::exec::DEFAULT_CLI_UDS_TIMEOUT_SECS),
    );
    let probe_timeout = timeout_secs
        .filter(|s| *s > 0)
        .map_or(default_timeout, Duration::from_secs);
    let (home, socket) = resolve_client_home_and_socket(data_dir)?;
    let health = probe_health(&socket);
    let running = health.is_ok();
    let health_err = health.as_ref().err().map(String::as_str);

    if json {
        return status_json(&home, &socket, running, health_err, probe_timeout);
    }

    let store_root = home.join("data");
    if let Err(e) = &health {
        println!("lastdbd: not reachable — {e}");
        println!("Home: {}", home.display());
        println!("Socket: {}", socket.display());
        if let Some(line) = layout_status_line(&store_root) {
            println!("{line}");
        }
        return Ok(StatusRun::Unreachable);
    }

    println!("lastdbd: serving");
    println!("Home: {}", home.display());
    println!("Socket: {}", socket.display());
    if let Ok(status_val) = probe_status_json(&socket, probe_timeout) {
        if let Some(ver) = status_val
            .pointer("/build/version")
            .and_then(serde_json::Value::as_str)
        {
            println!("Build: {ver}");
        }
        if let Some(up) = status_val
            .get("uptime_secs")
            .and_then(serde_json::Value::as_u64)
        {
            println!("Uptime: {up}s");
        }
    }
    if let Some(line) = layout_status_line(&store_root) {
        println!("{line}");
    }
    Ok(StatusRun::Serving)
}

fn status_json(
    home: &Path,
    socket: &Path,
    running: bool,
    health_err: Option<&str>,
    timeout: Duration,
) -> Result<StatusRun, String> {
    let status_val = running
        .then(|| probe_status_json(socket, timeout).ok())
        .flatten();
    let daemon_build = status_val
        .as_ref()
        .and_then(|s| s.pointer("/build/version"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned);
    let cli_build = lastdb_node::version::build_version();
    let build_agreement =
        daemon_build
            .as_deref()
            .map(|b| if b == cli_build { "match" } else { "mismatch" });
    let daemon_status = if running {
        "serving".to_string()
    } else {
        format!(
            "lastdbd: not reachable — {}",
            health_err.unwrap_or("socket unreachable")
        )
    };
    let payload = serde_json::json!({
        "home": home.display().to_string(),
        "socket": socket.display().to_string(),
        "running": running,
        "daemon_status": daemon_status,
        "daemon_pid_alive": false,
        "health_error": health_err,
        "daemon_build": daemon_build,
        "cli_build": cli_build,
        "build_agreement": build_agreement,
        "notes": [
            "daemon_build is what the PROCESS reports; cli_build is this binary. null daemon_build = no status reply, build unknown — not agreement",
        ],
    });
    println!(
        "{}",
        serde_json::to_string_pretty(&payload)
            .map_err(|e| format!("serialize status json: {e}"))?
    );
    Ok(if running {
        StatusRun::Serving
    } else {
        StatusRun::Unreachable
    })
}

fn layout_status_line(store_root: &Path) -> Option<String> {
    use fold_db::storage::disk_pack::{
        HOME_FORMAT, NODE_MARKER, RECORD_NODE_BYTES, SHARED_NODE_BYTES,
    };
    match std::fs::read(store_root.join(NODE_MARKER)).ok()?.as_slice() {
        SHARED_NODE_BYTES => Some(format!(
            "Layout: format {HOME_FORMAT} shared home (disk-run for shared records, disk-cas for shared file blobs, disk-private for device-only state)"
        )),
        RECORD_NODE_BYTES => Some(format!(
            "Layout: format {HOME_FORMAT} record home (disk-run for records, disk-cas for file blobs, typed range keys)"
        )),
        _ => None,
    }
}

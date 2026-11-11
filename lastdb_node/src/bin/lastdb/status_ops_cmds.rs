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
        match self.selector.as_deref() {
            Some(selector) => Cow::Owned(format!(
                "{DATABASE_SCOPE_PREFIX}{selector}{}",
                path.strip_prefix("/api")
                    .expect("database path starts with /api")
            )),
            None => Cow::Borrowed(path),
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
    let response = read_databases(socket)?;
    let id = databases(&response)?
        .iter()
        .find(|database| database["name"].as_str() == Some(selector))
        .and_then(|database| database["id"].as_str())
        .filter(|id| cloud_backup_protocol::Id::try_from((*id).to_owned()).is_ok())
        .ok_or("database is not registered on this node")?;
    Ok(id.to_owned())
}

fn read_databases(socket: &Path) -> Result<serde_json::Value, String> {
    let raw = get_path(socket, DATABASES_PATH, Duration::from_secs(10))?;
    parse_json_response(&raw, "database list")
}

fn databases(response: &serde_json::Value) -> Result<&[serde_json::Value], String> {
    response["databases"]
        .as_array()
        .map(Vec::as_slice)
        .ok_or("missing database list".into())
}

pub(super) fn list_databases(data_dir: Option<PathBuf>, json: bool) -> Result<(), String> {
    let target = DatabaseTarget::open(data_dir, None)?;
    let response = read_databases(&target.socket)?;
    if json {
        println!("{response}");
    } else {
        for database in databases(&response)? {
            let id = database["id"].as_str().ok_or("database has no ID")?;
            let name = database["name"].as_str().unwrap_or(id);
            let access = database["access"]
                .as_str()
                .ok_or("database has no access mode")?;
            let state = database["state"].as_str().ok_or("database has no state")?;
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
    match timeout_secs {
        Some(secs) if secs > 0 => Duration::from_secs(secs),
        _ => admin_scan_client_timeout(),
    }
}

pub(crate) const DEFAULT_STATUS_CLIENT_TIMEOUT_SECS: u64 =
    lastdb_node::exec::DEFAULT_CLI_UDS_TIMEOUT_SECS;

pub(crate) fn status_default_client_timeout() -> Duration {
    lastdb_node::exec::admin_handler_timeout_override()
        .unwrap_or(Duration::from_secs(DEFAULT_STATUS_CLIENT_TIMEOUT_SECS))
}

pub(crate) fn status_client_timeout(timeout_secs: Option<u64>, default: Duration) -> Duration {
    match timeout_secs {
        Some(secs) if secs > 0 => Duration::from_secs(secs),
        _ => default,
    }
}

pub(crate) fn write_product_file_atomic(path: &Path, bytes: &[u8]) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("create out dir {}: {e}", parent.display()))?;
        }
    }
    let tmp = {
        let mut name = path.as_os_str().to_owned();
        name.push(format!(".tmp.{}", std::process::id()));
        PathBuf::from(name)
    };
    match std::fs::write(&tmp, bytes) {
        Ok(()) => match std::fs::rename(&tmp, path) {
            Ok(()) => Ok(()),
            Err(e) => {
                let _ = std::fs::remove_file(&tmp);
                Err(format!("rename inventory out to {}: {e}", path.display()))
            }
        },
        Err(e) => {
            let _ = std::fs::remove_file(&tmp);
            Err(format!("write inventory temp {}: {e}", tmp.display()))
        }
    }
}

/// `lastdb status` exit code when the owner socket did not answer `/health`.
pub(super) const STATUS_UNREACHABLE_EXIT: i32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StatusRun {
    Serving,
    Unreachable,
}

pub(super) fn status_unreachable_line(reason: &str) -> String {
    format!("lastdbd: not reachable — {reason}")
}

fn probe_status_json(socket: &Path, timeout: Duration) -> Result<serde_json::Value, String> {
    let req = format!(
        "GET /api/status HTTP/1.1\r\nHost: localhost\r\n{}Connection: close\r\n\r\n",
        client_headers()
    );
    let raw = request_readonly_with_retry(socket, req.as_bytes(), timeout)?;
    let envelope = parse_json_response(&raw, "status")?;
    envelope
        .get("status")
        .cloned()
        .ok_or_else(|| "status response missing status object".to_string())
}

pub(super) fn status(
    data_dir: Option<PathBuf>,
    json: bool,
    timeout_secs: Option<u64>,
) -> Result<StatusRun, String> {
    let probe_timeout = status_client_timeout(timeout_secs, status_default_client_timeout());
    let (home, socket) = resolve_client_home_and_socket(data_dir)?;
    let health = probe_health(&socket);
    let running = health.is_ok();
    let health_err = health.as_ref().err().map(String::as_str);

    if json {
        return status_json(&home, &socket, running, health_err, probe_timeout);
    }

    let store_root = home.join("data");
    if let Err(e) = &health {
        println!("{}", status_unreachable_line(e));
        println!("Home: {}", home.display());
        println!("Socket: {}", socket.display());
        for line in layout_status_lines(&store_root) {
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
    for line in layout_status_lines(&store_root) {
        println!("{line}");
    }
    Ok(StatusRun::Serving)
}

pub(super) fn status_json(
    home: &Path,
    socket: &Path,
    running: bool,
    health_err: Option<&str>,
    timeout: Duration,
) -> Result<StatusRun, String> {
    let status_val = if running {
        probe_status_json(socket, timeout).ok()
    } else {
        None
    };
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
        status_unreachable_line(health_err.unwrap_or("socket unreachable"))
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
            "daemon_build is what the PROCESS reports; cli_build is this binary. \
             null daemon_build = no status reply, build unknown — not agreement",
        ],
    });
    println!(
        "{}",
        serde_json::to_string_pretty(&payload)
            .map_err(|e| format!("serialize status json: {e}"))?
    );
    if running {
        Ok(StatusRun::Serving)
    } else {
        Ok(StatusRun::Unreachable)
    }
}

pub(super) fn layout_status_lines(store_root: &Path) -> Vec<String> {
    run_layout_status_line(store_root).into_iter().collect()
}

fn run_layout_status_line(store_root: &Path) -> Option<String> {
    use fold_db::storage::disk_pack::{
        HOME_FORMAT, NODE_MARKER, RECORD_NODE_BYTES, SHARED_NODE_BYTES,
    };
    let marker = std::fs::read(store_root.join(NODE_MARKER)).ok()?;
    match marker.as_slice() {
        SHARED_NODE_BYTES => Some(format!(
            "Layout: format {HOME_FORMAT} shared home (disk-run for shared records, \
             disk-cas for shared file blobs, disk-private for device-only state)"
        )),
        RECORD_NODE_BYTES => Some(format!(
            "Layout: format {HOME_FORMAT} record home (disk-run for records, \
             disk-cas for file blobs, typed range keys)"
        )),
        _ => None,
    }
}

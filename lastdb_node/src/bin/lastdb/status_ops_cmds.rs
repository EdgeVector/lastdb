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
        let selector = selector.map(|v| resolve_database(&socket, v)).transpose()?;
        Ok(Self { socket, selector })
    }

    pub(crate) fn path<'p>(&self, path: &'p str) -> Cow<'p, str> {
        match self.selector.as_deref() {
            Some(sel) => Cow::Owned(format!(
                "{DATABASE_SCOPE_PREFIX}{sel}{}",
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
    let raw = get_path(socket, DATABASES_PATH, Duration::from_secs(10))?;
    parse_json_response(&raw, "database list")?["databases"]
        .as_array()
        .ok_or("missing database list")?
        .iter()
        .find(|db| db["name"].as_str() == Some(selector))
        .and_then(|db| db["id"].as_str())
        .filter(|id| cloud_backup_protocol::Id::try_from((*id).to_owned()).is_ok())
        .map(str::to_owned)
        .ok_or_else(|| "database is not registered on this node".into())
}

pub(super) fn list_databases(data_dir: Option<PathBuf>, json: bool) -> Result<(), String> {
    let target = DatabaseTarget::open(data_dir, None)?;
    let raw = get_path(&target.socket, DATABASES_PATH, Duration::from_secs(10))?;
    let response = parse_json_response(&raw, "database list")?;
    if json {
        println!("{response}");
        return Ok(());
    }
    for db in response["databases"]
        .as_array()
        .ok_or("missing database list")?
    {
        let id = db["id"].as_str().ok_or("database has no ID")?;
        let name = db["name"].as_str().unwrap_or(id);
        let access = db["access"].as_str().ok_or("database has no access mode")?;
        let state = db["state"].as_str().ok_or("database has no state")?;
        println!("{name}\t{id}\t{access}\t{state}");
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
    parse_json_response(
        &request_readonly_with_retry(socket, req.as_bytes(), timeout)?,
        "status",
    )?
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
    let status_val = running
        .then(|| probe_status_json(&socket, probe_timeout).ok())
        .flatten();
    if json {
        let daemon_build = status_val
            .as_ref()
            .and_then(|s| s.pointer("/build/version"))
            .and_then(serde_json::Value::as_str);
        let cli_build = lastdb_node::version::build_version();
        let daemon_status = match &health {
            Ok(()) => "serving".to_string(),
            Err(e) => format!("lastdbd: not reachable — {e}"),
        };
        let payload = serde_json::json!({
            "home": home.display().to_string(),
            "socket": socket.display().to_string(),
            "running": running,
            "daemon_status": daemon_status,
            "daemon_pid_alive": false,
            "health_error": health.as_ref().err(),
            "daemon_build": daemon_build,
            "cli_build": cli_build,
            "build_agreement": daemon_build.map(|b| if b == cli_build { "match" } else { "mismatch" }),
            "notes": [
                "daemon_build is what the PROCESS reports; cli_build is this binary. null daemon_build = no status reply, build unknown — not agreement",
            ],
        });
        println!(
            "{}",
            serde_json::to_string_pretty(&payload)
                .map_err(|e| format!("serialize status json: {e}"))?
        );
    } else {
        match &health {
            Ok(()) => println!("lastdbd: serving"),
            Err(e) => println!("lastdbd: not reachable — {e}"),
        }
        println!("Home: {}", home.display());
        println!("Socket: {}", socket.display());
        if let Some(s) = &status_val {
            if let Some(ver) = s
                .pointer("/build/version")
                .and_then(serde_json::Value::as_str)
            {
                println!("Build: {ver}");
            }
            if let Some(up) = s.get("uptime_secs").and_then(serde_json::Value::as_u64) {
                println!("Uptime: {up}s");
            }
        }
        use fold_db::storage::disk_pack::{HOME_FORMAT, NODE_MARKER, RECORD_NODE_BYTES};
        if std::fs::read(home.join("data").join(NODE_MARKER))
            .ok()
            .as_deref()
            == Some(RECORD_NODE_BYTES)
        {
            println!(
                "Layout: format {HOME_FORMAT} record home (disk-run for records, disk-cas for file blobs, typed range keys)"
            );
        }
    }
    Ok(if running {
        StatusRun::Serving
    } else {
        StatusRun::Unreachable
    })
}

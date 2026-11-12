use super::*;
use fold_db::storage::disk_pack::{
    NativeCleanupPhase, NativeCleanupStatus, HOME_FORMAT, NODE_MARKER, RECORD_NODE_BYTES,
};
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
        if let Some(sel) = self.selector.as_deref() {
            Cow::Owned(format!(
                "{DATABASE_SCOPE_PREFIX}{sel}{}",
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
        let daemon_status = health.as_ref().map_or_else(
            |e| format!("lastdbd: not reachable — {e}"),
            |()| "serving".to_string(),
        );
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
        if let Err(e) = &health {
            println!("lastdbd: not reachable — {e}");
        } else {
            println!("lastdbd: serving");
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

pub(super) fn run_admin(data_dir: Option<PathBuf>, action: AdminCommand) -> Result<(), String> {
    match action {
        AdminCommand::Storage(AdminStorageCommand::Cleanup {
            status,
            wait,
            timeout,
            json,
        }) => {
            let (_, socket) = resolve_client_home_and_socket(data_dir)?;
            if probe_health(&socket).is_err() {
                return Err(format!(
                    "lastdbd not reachable at {} — start the daemon first",
                    socket.display()
                ));
            }
            db_cleanup(&socket, status, wait, timeout, json)
        }
        AdminCommand::Backup(action) => cloud_command(data_dir, backup_action(action)?),
    }
}

fn backup_action(action: AdminBackupCommand) -> Result<CloudCommand, String> {
    use AdminBackupCommand::{
        Close, Init, InspectDependencies, PrepareRecoveryHome, RecoverCredentialsHome, Resume,
    };
    let action = match action {
        Init(a) => ByteBackupCommand::Init(a),
        Resume(a) => ByteBackupCommand::Resume(a),
        Close(a) => ByteBackupCommand::Close(a),
        InspectDependencies(a) => ByteBackupCommand::InspectDependencies(a),
        RecoverCredentialsHome { args, existing_dev } => {
            if !existing_dev {
                return Ok(CloudCommand::RecoverCredentialsHome(args));
            }
            if args.env != "dev" {
                return Err("--existing-dev requires --env dev".into());
            }
            return Ok(CloudCommand::RecoverDevCredentialsHome {
                into: args.into,
                expected_user_hash: args.expected_user_hash,
                identity_seed_stdin: args.identity_seed_stdin,
                json: args.json,
            });
        }
        PrepareRecoveryHome(a) => return Ok(CloudCommand::PrepareRecoveryHome(a)),
    };
    Ok(CloudCommand::BackupV2 { action })
}

fn decode_cleanup(value: &serde_json::Value) -> Result<NativeCleanupStatus, String> {
    let report = value
        .get("cleanup")
        .cloned()
        .ok_or("response missing cleanup")?;
    let status: NativeCleanupStatus =
        serde_json::from_value(report).map_err(|e| format!("invalid cleanup response: {e}"))?;
    (status.complete == (status.phase == NativeCleanupPhase::Complete))
        .then_some(status)
        .ok_or_else(|| "cleanup response has inconsistent completion fields".into())
}

fn read_cleanup_status(socket: &Path, timeout: Duration) -> Result<NativeCleanupStatus, String> {
    decode_cleanup(&parse_json_response(
        &get_path(socket, "/api/db/cleanup", timeout)?,
        "cleanup status",
    )?)
}

fn db_cleanup(
    socket: &Path,
    status_only: bool,
    wait: bool,
    timeout: u64,
    json: bool,
) -> Result<(), String> {
    let mut status = if status_only {
        read_cleanup_status(socket, Duration::from_secs(30))?
    } else {
        let raw = post_json_admin(socket, "/api/db/cleanup", &serde_json::json!({}))?;
        decode_cleanup(&parse_json_response(&raw, "/api/db/cleanup")?)?
    };
    let id = status.request_id.clone();
    let (started, limit) = (Instant::now(), Duration::from_secs(timeout));
    while wait && !status.complete {
        if status.phase == NativeCleanupPhase::RecoveryRequired || status.request_id.is_none() {
            print_cleanup_status(&status, json)?;
            return Err(status
                .last_error
                .unwrap_or_else(|| "no cleanup request is active".into()));
        }
        let Some(left) = limit.checked_sub(started.elapsed()) else {
            print_cleanup_status(&status, json)?;
            return Err(
                "cleanup remains incomplete; its durable request continues after this client timeout"
                    .into(),
            );
        };
        std::thread::sleep(Duration::from_millis(250).min(left));
        if let Some(left) = limit.checked_sub(started.elapsed()) {
            status = read_cleanup_status(socket, Duration::from_secs(30).min(left))?;
            if status.request_id != id {
                return Err("the latest cleanup request changed while this client waited".into());
            }
        }
    }
    print_cleanup_status(&status, json)
}

fn print_cleanup_status(status: &NativeCleanupStatus, json: bool) -> Result<(), String> {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(status).map_err(|e| e.to_string())?
        );
        return Ok(());
    }
    println!("Cleanup: {:?}", status.phase);
    if let Some(id) = &status.request_id {
        println!("  request: {id}");
    }
    for (label, s) in [("main", &status.main), ("file blobs", &status.cas)] {
        println!(
            "  {label}: {} tiered, {} merge inputs, {} held bytes",
            s.tiered_runs, s.merging_runs, s.held_retired_bytes
        );
    }
    if let Some(b) = status.completed_through {
        println!(
            "  completed through: main {}, file blobs {}",
            b.main_frontier, b.cas_frontier
        );
    }
    if let Some(error) = &status.last_error {
        println!("  error: {error}");
    }
    println!(
        "  Scope: data already deleted from the active stores. Backup copies remain separate."
    );
    Ok(())
}

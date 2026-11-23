//! Node status, database catalog, storage cleanup, and admin backup commands.

use super::*;
use fold_db::storage::disk_pack::{
    NativeCleanupPhase, NativeCleanupStatus, HOME_FORMAT, NODE_MARKER, RECORD_NODE_BYTES,
};
use lastdb_node::lastdb_uds::{
    admin_handler_timeout_override, valid_selector, DATABASES_PATH, DATABASE_SCOPE_PREFIX,
    DEFAULT_CLI_UDS_TIMEOUT_SECS,
};
use serde_json::{json, Value};
use std::borrow::Cow;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

pub(crate) struct DatabaseTarget {
    pub(crate) socket: PathBuf,
    pub(crate) selector: Option<String>,
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
        let Some(sel) = self.selector.as_deref() else {
            return Cow::Borrowed(path);
        };
        let suffix = path
            .strip_prefix("/api")
            .expect("database path starts with /api");
        Cow::Owned(format!("{DATABASE_SCOPE_PREFIX}{sel}{suffix}"))
    }
}

fn fetch_databases(socket: &Path) -> Result<Value, String> {
    let raw = get_path(socket, DATABASES_PATH, Duration::from_secs(10))?;
    parse_json_response(&raw, "database list")
}

fn resolve_database(socket: &Path, selector: &str) -> Result<String, String> {
    if !valid_selector(selector) {
        return Err("invalid database ID or name".into());
    }
    if selector == "personal" {
        return Ok(selector.to_owned());
    }
    let rows = fetch_databases(socket)?["databases"]
        .as_array()
        .cloned()
        .ok_or("missing database list")?;
    if let Some(db) = rows
        .iter()
        .find(|db| db["id"].as_str() == Some(selector) || db["name"].as_str() == Some(selector))
    {
        return resolved_row(selector, db);
    }
    if is_database_id(selector) {
        return Ok(selector.to_owned());
    }
    Err("database is not registered on this node".into())
}

fn resolved_row(selector: &str, db: &Value) -> Result<String, String> {
    if is_database_id(selector) {
        return Ok(selector.to_owned());
    }
    let id = db["id"].as_str().ok_or("missing database list")?;
    if !is_database_id(id) {
        return Err("database is not registered on this node".into());
    }
    Ok(id.to_owned())
}

/// Local owned database ids are Hex32 (db verifying key).
fn is_database_id(value: &str) -> bool {
    mutation_log::DatabaseId::from_hex(value).is_ok()
}

pub(super) fn list_databases(data_dir: Option<PathBuf>, json_only: bool) -> Result<(), String> {
    let target = DatabaseTarget::open(data_dir, None)?;
    let resp = fetch_databases(&target.socket)?;
    if json_only {
        println!("{resp}");
        return Ok(());
    }
    for db in resp["databases"]
        .as_array()
        .ok_or("missing database list")?
    {
        print_database_row(db)?;
    }
    Ok(())
}

pub(super) fn create_database(
    data_dir: Option<PathBuf>,
    name: &str,
    private: bool,
    json: bool,
) -> Result<(), String> {
    if !lastdb_node::profile::valid_database_name(name) {
        return Err("invalid database name".into());
    }
    let target = DatabaseTarget::open(data_dir, None)?;
    let mut body = json!({ "name": name });
    if private {
        body["kind"] = Value::String("personal".into());
    }
    let raw = post_json_with_timeout(
        &target.socket,
        DATABASES_PATH,
        &body,
        Duration::from_secs(30),
    )?;
    let response = parse_json_response(&raw, "database create")?;
    let database = response
        .get("database")
        .ok_or("database create returned no database")?;
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(database).map_err(|e| e.to_string())?
        );
        return Ok(());
    }
    print_database_row(database)
}

fn print_database_row(database: &Value) -> Result<(), String> {
    let id = database["id"].as_str().ok_or("database has no ID")?;
    let name = database["name"].as_str().unwrap_or(id);
    let access = database["access"]
        .as_str()
        .ok_or("database has no access mode")?;
    let state = database["state"].as_str().ok_or("database has no state")?;
    println!("{name}\t{id}\t{access}\t{state}");
    Ok(())
}

pub(super) fn status(
    data_dir: Option<PathBuf>,
    json: bool,
    timeout_secs: Option<u64>,
) -> Result<bool, String> {
    let def = admin_handler_timeout_override()
        .unwrap_or(Duration::from_secs(DEFAULT_CLI_UDS_TIMEOUT_SECS));
    let budget = timeout_secs
        .filter(|&s| s > 0)
        .map_or(def, Duration::from_secs);
    let (home, socket) = resolve_client_home_and_socket(data_dir)?;
    let health = probe_health(&socket);
    let running = health.is_ok();
    let status_val = running
        .then(|| get_path_retry(&socket, "/api/status", budget))
        .and_then(|r| parse_json_response(&r.ok()?, "status").ok())
        .and_then(|v| v.get("status").cloned());
    let daemon_build = status_val
        .as_ref()
        .and_then(|s| s.pointer("/build/version")?.as_str());
    let cloud_sync = status_val
        .as_ref()
        .and_then(|s| s.get("cloud_sync"))
        .cloned();
    let daemon_status = health.as_ref().map_or_else(
        |e| format!("lastdbd: not reachable — {e}"),
        |()| String::from("serving"),
    );
    if json {
        let cli_build = lastdb_node::version::build_version();
        let agree = daemon_build.map(|b| if b == cli_build { "match" } else { "mismatch" });
        let note = "daemon_build is what the PROCESS reports; cli_build is this binary. null daemon_build = no status reply, build unknown — not agreement";
        print_pretty_json(&json!({
            "home": home.display().to_string(), "socket": socket.display().to_string(),
            "running": running, "daemon_status": daemon_status, "daemon_pid_alive": false,
            "health_error": health.as_ref().err(), "daemon_build": daemon_build, "cli_build": cli_build,
            "build_agreement": agree, "cloud_sync": cloud_sync, "notes": [note],
        }));
    } else {
        println!(
            "{}\nHome: {}\nSocket: {}",
            if running {
                "lastdbd: serving"
            } else {
                &daemon_status
            },
            home.display(),
            socket.display()
        );
        if let Some(ver) = daemon_build {
            println!("Build: {ver}");
        }
        if let Some(up) = status_val.as_ref().and_then(|s| s["uptime_secs"].as_u64()) {
            println!("Uptime: {up}s");
        }
        if let Some(line) = cloud_sync.as_ref().and_then(format_cloud_sync_line) {
            println!("{line}");
        }
        for line in cloud_sync
            .iter()
            .flat_map(super::snapshot_line::snapshot_lines)
        {
            println!("{line}");
        }
        if std::fs::read(home.join("data").join(NODE_MARKER))
            .ok()
            .as_deref()
            == Some(RECORD_NODE_BYTES)
        {
            println!("Layout: format {HOME_FORMAT} record home (disk-run for records, disk-cas for file blobs, typed range keys)");
        }
    }
    Ok(running)
}

fn format_cloud_sync_line(sync: &Value) -> Option<String> {
    let connected = sync
        .get("connected")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let payment_hold = sync
        .get("payment_hold")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let last_error = sync.get("last_error").and_then(Value::as_str);
    if !connected && !payment_hold && last_error.is_none() {
        return None;
    }
    let env = sync
        .get("environment")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let captures = sync
        .get("captures_accepted")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let base = if connected {
        if captures {
            format!("connected ({env})")
        } else {
            format!("connected ({env}, local capture pending restart)")
        }
    } else {
        String::from("disconnected")
    };
    if payment_hold {
        let detail = last_error.unwrap_or(lastdb_node::cloud::PAYMENT_REQUIRED_MESSAGE);
        Some(format!("Cloud sync: {base} — payment hold: {detail}"))
    } else if let Some(err) = last_error {
        Some(format!("Cloud sync: {base} — error: {err}"))
    } else {
        Some(format!("Cloud sync: {base}"))
    }
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
            require_reachable(&socket)?;
            db_cleanup(&socket, status, wait, timeout, json)
        }
        AdminCommand::Backup(b) => admin_backup_command(data_dir, b),
        AdminCommand::Identity { json } => {
            let home = lastdb_node::host::resolve_home(data_dir)?;
            let (database_id, node_key) = lastdb_node::offline_home::derived_identity(&home)?;
            if json {
                println!(
                    "{}",
                    json!({ "database_id": database_id, "node_key": node_key })
                );
            } else {
                println!("database_id {database_id}\nnode_key {node_key}");
            }
            Ok(())
        }
    }
}

fn decode_cleanup(raw: &str, label: &str) -> Result<NativeCleanupStatus, String> {
    let v = parse_json_response(raw, label)?
        .get("cleanup")
        .cloned()
        .ok_or("response missing cleanup")?;
    let s: NativeCleanupStatus =
        serde_json::from_value(v).map_err(|e| format!("invalid cleanup response: {e}"))?;
    if s.complete != (s.phase == NativeCleanupPhase::Complete) {
        return Err("cleanup response has inconsistent completion fields".into());
    }
    Ok(s)
}

fn db_cleanup(
    socket: &Path,
    status_only: bool,
    wait: bool,
    timeout: u64,
    json: bool,
) -> Result<(), String> {
    let (raw, label) = if status_only {
        (
            get_path(socket, "/api/db/cleanup", Duration::from_secs(30))?,
            "cleanup status",
        )
    } else {
        (
            post_json_admin(socket, "/api/db/cleanup", &json!({}))?,
            "/api/db/cleanup",
        )
    };
    let mut s = decode_cleanup(&raw, label)?;
    let id = s.request_id.clone();
    let (started, limit) = (Instant::now(), Duration::from_secs(timeout));
    while wait && !s.complete {
        if s.phase == NativeCleanupPhase::RecoveryRequired || s.request_id.is_none() {
            print_cleanup_status(&s, json);
            return Err(s
                .last_error
                .unwrap_or_else(|| "no cleanup request is active".into()));
        }
        let Some(left) = limit.checked_sub(started.elapsed()) else {
            print_cleanup_status(&s, json);
            return Err("cleanup remains incomplete; its durable request continues after this client timeout".into());
        };
        std::thread::sleep(Duration::from_millis(250).min(left));
        if let Some(left) = limit.checked_sub(started.elapsed()) {
            s = decode_cleanup(
                &get_path(socket, "/api/db/cleanup", Duration::from_secs(30).min(left))?,
                "cleanup status",
            )?;
            if s.request_id != id {
                return Err("the latest cleanup request changed while this client waited".into());
            }
        }
    }
    print_cleanup_status(&s, json);
    Ok(())
}

fn print_cleanup_status(s: &NativeCleanupStatus, json: bool) {
    if json {
        print_pretty_json(s);
        return;
    }
    println!("Cleanup: {:?}", s.phase);
    if let Some(id) = &s.request_id {
        println!("  request: {id}");
    }
    for (label, m) in [("main", &s.main), ("file blobs", &s.cas)] {
        println!(
            "  {label}: {} tiered, {} merge inputs, {} held bytes",
            m.tiered_runs, m.merging_runs, m.held_retired_bytes
        );
    }
    if let Some(b) = s.completed_through {
        println!(
            "  completed through: main {}, file blobs {}",
            b.main_frontier, b.cas_frontier
        );
    }
    if let Some(err) = &s.last_error {
        println!("  error: {err}");
    }
    println!(
        "  Scope: data already deleted from the active stores. Backup copies remain separate."
    );
}

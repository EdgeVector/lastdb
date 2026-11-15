//! Operator node status command (`lastdb status`).

use super::*;
use fold_db::storage::disk_pack::{HOME_FORMAT, NODE_MARKER, RECORD_NODE_BYTES};
use lastdb_node::lastdb_uds::{admin_handler_timeout_override, DEFAULT_CLI_UDS_TIMEOUT_SECS};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::time::Duration;

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

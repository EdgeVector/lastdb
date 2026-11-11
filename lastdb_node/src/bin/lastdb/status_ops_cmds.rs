use super::*;

/// `lastdb status` exit code when the owner socket did not answer `/health`.
pub(super) const STATUS_UNREACHABLE_EXIT: i32 = 1;

/// Outcome of a rendered `lastdb status` report.
///
/// `Unreachable` means the report already went to stdout; the process must
/// still exit 1 so scripts can gate on the socket, not on a trailing `Reason:`.
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

/// Machine-readable live status.
///
/// This path must stay safe for health and recovery gates: it walks no
/// directory. `lastdb admin storage inventory` reports the disk totals.
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

/// `status` lines describing the home's durable physical placement.
///
/// Silent when the home has no disk format v2 node marker (one never opened):
/// `status` should not invent a layout it cannot see.
pub(super) fn layout_status_lines(store_root: &Path) -> Vec<String> {
    run_layout_status_line(store_root).into_iter().collect()
}

/// The node marker of a home in the current format (the only format this
/// release opens) names its run homes.
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

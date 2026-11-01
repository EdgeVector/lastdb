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

pub(super) fn status(
    data_dir: Option<PathBuf>,
    json: bool,
    contract: bool,
    timeout_secs: Option<u64>,
) -> Result<StatusRun, String> {
    let probe_timeout = status_client_timeout(timeout_secs, status_default_client_timeout());
    let (home, socket) = resolve_client_home_and_socket(data_dir)?;
    let health = lastdb_node::health_alert::probe_health(&socket);
    let running = health.is_ok();
    let health_err = health.as_ref().err().map(String::as_str);
    let daemon_pid_alive = !running
        && health_err.is_some()
        && lastdb_node::crash_attribution::live_daemon_pid_alive(&home);
    let store_root = home.join("data");

    if contract {
        status_contract(&socket, running, json, probe_timeout)?;
        return Ok(StatusRun::Serving);
    }

    if json {
        return status_json(&home, &socket, running, health_err, probe_timeout);
    }

    let view = StatusView {
        home: &home,
        socket: &socket,
        store_root: &store_root,
        session_live: running || daemon_pid_alive,
    };

    if let Err(e) = &health {
        view.print_unreachable(e);
        return Ok(StatusRun::Unreachable);
    }

    view.print_serving(running, health_err, probe_timeout);
    Ok(StatusRun::Serving)
}

pub(super) fn safe_status_plane_fields() -> (serde_json::Value, serde_json::Value) {
    (
        serde_json::Value::Null,
        serde_json::json!({
            "included": false,
            "source": "lastdb db inventory",
            "reason": "omitted from status to avoid a recursive collection-directory walk",
        }),
    )
}

/// Operator / machine view of the additive gauge contract (PR-5).
///
/// Requires a running daemon so unit/window come from live typed gauges.
/// Prefer `GET /api/status` → `status.contract` for harnesses; this flag is
/// the human-readable sibling of that block.
pub(super) fn status_contract(
    socket: &Path,
    running: bool,
    as_json: bool,
    timeout: Duration,
) -> Result<(), String> {
    if !running {
        return Err(
            "lastdb status --contract needs a running daemon (socket health failed)".into(),
        );
    }
    let snapshot = probe_status(socket, timeout)?;
    let contract = lastdb_node::ops::status_gauge_contract::gauge_contract(&snapshot);
    if as_json {
        println!(
            "{}",
            serde_json::to_string_pretty(&contract)
                .map_err(|e| format!("serialize contract json: {e}"))?
        );
    } else {
        for line in lastdb_node::ops::status_gauge_contract::contract_lines(&contract) {
            println!("{line}");
        }
    }
    Ok(())
}

/// Machine-readable live status.
///
/// This path must stay safe for health and recovery gates. Disk plane totals
/// require a recursive collection-directory walk, so they belong to the
/// explicit `lastdb db inventory` command instead of this status probe.
pub(super) fn status_json(
    home: &Path,
    socket: &Path,
    running: bool,
    health_err: Option<&str>,
    timeout: Duration,
) -> Result<StatusRun, String> {
    let snapshot = if running {
        probe_status(socket, timeout).ok()
    } else {
        None
    };
    // The build the *process* reports, kept distinct from this CLI's own. A
    // machine reader — `lastdb-safe-upgrade` computing which hop to probe — must
    // key off the running build, not the installed file, or it validates an
    // upgrade path the live node never takes. `null` means the daemon predates
    // the field and its build is unknowable from here; it does NOT mean agreement.
    let daemon_build = snapshot
        .as_ref()
        .map(|s| s.build.version.clone())
        .filter(|v| !v.is_empty());
    let build_agreement = snapshot.as_ref().map(|s| {
        lastdb_node::self_metrics::BuildAgreement::classify(&s.build)
            .as_str()
            .to_string()
    });
    let at_rest_compression = snapshot.as_ref().and_then(|s| s.at_rest_compression);
    let (planes, plane_inventory) = safe_status_plane_fields();
    let payload = serde_json::json!({
        "home": home.display().to_string(),
        "socket": socket.display().to_string(),
        "running": running,
        "reachable": running,
        "daemon_status": lastdb_node::crash_attribution::daemon_status_line_with_health_error(
            home,
            running,
            health_err,
        ),
        "daemon_pid_alive": !running
            && health_err.is_some()
            && lastdb_node::crash_attribution::live_daemon_pid_alive(home),
        "health_error": health_err,
        "daemon_build": daemon_build,
        "cli_build": lastdb_node::crash_attribution::build_version(),
        "build_agreement": build_agreement,
        "at_rest_compression": at_rest_compression,
        "planes": planes,
        "plane_inventory": plane_inventory,
        "notes": [
            "planes is null by design — use `lastdb db inventory` for disk plane totals",
            "daemon_build is what the PROCESS reports; cli_build is this binary. \
             null daemon_build = daemon predates the field, build unknowable — not agreement",
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

/// The node marker of a format 8 home (the record model, the only format this
/// release opens) names its run homes.
fn run_layout_status_line(store_root: &Path) -> Option<String> {
    let marker = std::fs::read(store_root.join("laststore-pack-node-v1")).ok()?;
    (marker.as_slice() == b"format=8\nhome=disk-run\ncas=disk-cas\n").then(|| {
        "Layout: format 8 record homes (disk-run for records, one record per key; \
         disk-cas for CAS files; typed range keys; one sorted run file per commit, \
         merged in the background)"
            .to_string()
    })
}

#[path = "status_ops_cmds/ops_rollup.rs"]
mod ops_rollup;
#[path = "status_ops_cmds/ops_rollup_merge.rs"]
mod ops_rollup_merge;
pub(crate) use ops_rollup::*;
#[path = "status_ops_cmds/probes.rs"]
mod probes;
#[path = "status_ops_cmds/status_view.rs"]
mod status_view;
pub(crate) use probes::*;
use status_view::StatusView;

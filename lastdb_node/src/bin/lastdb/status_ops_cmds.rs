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
    let breakdown = status_plane_breakdown_with(json, contract, &store_root, |root| {
        fold_db::mini_cutover::plane_breakdown_for_store_root(root)
    });

    if contract {
        status_contract(&socket, running, json, probe_timeout)?;
        return Ok(StatusRun::Serving);
    }

    if json {
        return status_json(&home, &socket, running, health_err, probe_timeout);
    }

    let breakdown = breakdown
        .as_ref()
        .expect("text status always loads the offline plane inventory");

    if let Err(e) = &health {
        println!("{}", status_unreachable_line(e));
        println!("Home:   {}", home.display());
        println!("Socket: {}", socket.display());
        // File-only plane / session report stays after line 1 so operators
        // still see the home without treating the command as a live probe.
        for line in lastdb_node::crash_attribution::status_lines(&home, running || daemon_pid_alive)
        {
            println!("{line}");
        }
        for line in layout_status_lines(&store_root) {
            println!("{line}");
        }
        for line in fold_db::mini_cutover::plane_status_lines(breakdown) {
            println!("{line}");
        }
        for line in plane_sot_spotlight_lines(breakdown) {
            println!("{line}");
        }
        return Ok(StatusRun::Unreachable);
    }

    println!(
        "lastdbd: {}",
        lastdb_node::crash_attribution::daemon_status_line_with_health_error(
            &home, running, health_err
        )
    );
    println!("Home:   {}", home.display());
    println!("Socket: {}", socket.display());
    // Uptime + "why did it last die?" — read straight from the session ledger
    // the daemon maintains under `home`, so one `lastdb status` answers both.
    for line in lastdb_node::crash_attribution::status_lines(&home, running || daemon_pid_alive) {
        println!("{line}");
    }
    // Physical placement, read from the descriptor without opening the home.
    // A home left on full-key placement pays a full-collection sweep on every
    // HashRange partition read, and nothing else in `status` would show it.
    for line in layout_status_lines(&store_root) {
        println!("{line}");
    }
    // Offline plane attribution (SOT / indexes / history / cold / ops / aside).
    // Walks collection dirs under the store root — no node required; does not
    // open LastStore. Aside reclaim is Tom-gated (never auto-delete).
    for line in fold_db::mini_cutover::plane_status_lines(breakdown) {
        println!("{line}");
    }
    // Named SOT homes (tips/proteins/…) so residue cards do not re-du the tree.
    for line in plane_sot_spotlight_lines(breakdown) {
        println!("{line}");
    }
    match probe_status(&socket, probe_timeout) {
        Ok(snapshot) => {
            // Everything from here down is the daemon's own account of
            // itself. Name the binary giving it first: the lines above are
            // this CLI reading the store offline and are attributable to
            // the CLI, but these are not, and the installed file is not
            // evidence of which build produced them.
            for line in lastdb_node::self_metrics::build_identity_lines(&snapshot.build) {
                println!("{line}");
            }
            for line in lastdb_node::self_metrics::status_lines(&snapshot) {
                println!("{line}");
            }
            // Compact offender summary so `lastdb status` alone surfaces
            // the top clients without a second probe.
            for line in lastdb_node::self_metrics::phase_vocabulary_skew_lines(&snapshot.build) {
                println!("{line}");
            }
            for line in lastdb_node::self_metrics::request_ops_lines(&snapshot)
                .into_iter()
                .take(6)
            {
                println!("{line}");
            }
        }
        Err(e) => println!("Sampler: unavailable ({e})"),
    }
    Ok(StatusRun::Serving)
}

pub(super) fn status_plane_breakdown_with<F>(
    json: bool,
    contract: bool,
    store_root: &Path,
    walk: F,
) -> Option<fold_db::mini_cutover::PlaneBreakdown>
where
    F: FnOnce(&Path) -> fold_db::mini_cutover::PlaneBreakdown,
{
    if json || contract {
        None
    } else {
        Some(walk(store_root))
    }
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
    let capture = snapshot
        .as_ref()
        .and_then(|s| s.sync.capture.as_ref())
        .cloned();
    let at_rest_compression = snapshot.as_ref().and_then(|s| s.at_rest_compression);
    let codec_policy = snapshot
        .as_ref()
        .and_then(|s| s.codec_policy.as_ref())
        .cloned();
    let dual_read = snapshot.map(|s| s.dual_read);
    let dual_read_by_plane = dual_read.as_ref().map(|d| &d.legacy_hits_by_plane);
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
        "capture": capture,
        "at_rest_compression": at_rest_compression,
        "codec_policy": codec_policy,
        "planes": planes,
        "plane_inventory": plane_inventory,
        "dual_read": dual_read,
        "dual_read_legacy_hits_by_plane": dual_read_by_plane,
        "notes": [
            "planes is null by design — use `lastdb db inventory` for disk plane totals",
            "dual_read counters are process-lifetime on the running daemon",
            "capture pin_log_disk_bytes and reexport_disk_bytes are collection-directory \
             stat walks, not compact dry-runs or key inventories",
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

/// Human spotlight lines for named SOT + residue + unattributed + history-adjacent
/// mass.
///
/// `residue:` carries only what the plane map CLASSIFIED as residue. Collections
/// the map does not recognize get their own `unattributed:` line, which says so.
/// Folding them into `residue:` — what this printed until 2026-09-06 — reported
/// `atom_ref_edges_v2`, 936.7 MiB of live atom reverse edges, as reclaimable
/// migration leftovers on the primary. `lastdb db inventory` already named them
/// `UNKNOWN active planes`; `status` is the command operators actually run.
pub(super) fn plane_sot_spotlight_lines(
    breakdown: &fold_db::mini_cutover::PlaneBreakdown,
) -> Vec<String> {
    let report = breakdown.to_plane_map_report();
    let mut lines = Vec::new();
    if !report.sot_named.is_empty() {
        let bits: Vec<String> = report
            .sot_named
            .iter()
            .map(|c| format!("{}={}", c.name, format_bytes(c.bytes)))
            .collect();
        lines.push(format!("  SOT named: {}", bits.join(" ")));
    }
    if !report.residue_named.is_empty() {
        let bits: Vec<String> = report
            .residue_named
            .iter()
            .map(|c| format!("{}={}", c.name, format_bytes(c.bytes)))
            .collect();
        lines.push(format!("  residue: {}", bits.join(" ")));
    }
    if !report.unknown_active_collections.is_empty() {
        let bits: Vec<String> = report
            .unknown_active_collections
            .iter()
            .map(|c| format!("{}={}", c.name, format_bytes(c.bytes)))
            .collect();
        lines.push(format!(
            "  unattributed: {} (not in the plane map; liveness unknown, not a reclaim target)",
            bits.join(" ")
        ));
    }
    if !report.history_adjacent_named.is_empty() {
        let bits: Vec<String> = report
            .history_adjacent_named
            .iter()
            .map(|c| format!("{}={}", c.name, format_bytes(c.bytes)))
            .collect();
        lines.push(format!("  history-adjacent: {}", bits.join(" ")));
    }
    // Drain-listed collections, named as one set with a total.
    //
    // `TIP_RESIDUE_LEGACY_COLLECTIONS` is the sunset's own list, and the two
    // members it holds land in DIFFERENT roles above: `field_tip_headers` is
    // `TipResidue` and prints under `residue:`, while `field_tip_versions` is
    // `HistoryAdjacent` — the one role an operator is told never to reclaim —
    // and prints beside `field_update_order_log`, which is real append-only
    // architecture. On the primary that showed 4.9 MiB of a 12.3 MiB dead pair,
    // and a run sizing the remaining drain work from this output dropped
    // `field_tip_versions` from its list one paragraph after naming it residue.
    //
    // The roles are NOT changed to fix this. `HistoryAdjacent` is the
    // never-blind-GC guard, and `field_tip_versions` keeps it. What was missing
    // is that the operator surface never said the drain list exists.
    let drain_listed: Vec<&fold_db::mini_cutover::PlaneMapCollection> = report
        .collections
        .iter()
        .filter(|c| {
            c.bytes > 0
                && fold_db::storage::TIP_RESIDUE_LEGACY_COLLECTIONS.contains(&c.name.as_str())
        })
        .collect();
    if !drain_listed.is_empty() {
        let total: u64 = drain_listed.iter().map(|c| c.bytes).sum();
        let bits: Vec<String> = drain_listed
            .iter()
            .map(|c| format!("{}={}", c.name, format_bytes(c.bytes)))
            .collect();
        lines.push(format!(
            "  drain-listed: {} (total {}; the tip-family sunset owns these — \
             a drain is expected to empty them, whatever role they print under above)",
            bits.join(" "),
            format_bytes(total),
        ));
    }
    lines
}

/// `status` lines describing the home's durable physical placement.
///
/// Silent when there is no descriptor to read (a home that predates them, or
/// one never opened) — `status` should not invent a layout it cannot see.
pub(super) fn layout_status_lines(store_root: &Path) -> Vec<String> {
    let descriptor = match laststore::describe_home(store_root) {
        Ok(Some(descriptor)) => descriptor,
        Ok(None) => return Vec::new(),
        Err(e) => return vec![format!("Layout: unreadable ({e})")],
    };

    let mut lines = vec![format!(
        "Layout: {:?} key={:?} epoch={} groups={} packaging={:?}",
        descriptor.layout_mode,
        descriptor.hash_group_key,
        descriptor.layout_epoch,
        descriptor.group_count(),
        descriptor.packaging,
    )];

    // The operator-facing consequence, not just the setting: under full-key
    // placement `limit` cannot prune, so a partition read returning one row
    // costs the same sweep as an unbounded one.
    if descriptor.layout_mode == laststore::LayoutMode::HashGroup
        && !descriptor.prunes_partition_reads()
    {
        lines.push(format!(
            "  WARNING: full-key placement — every HashRange partition read visits \
             all {} groups, and `limit` does not prune.",
            descriptor.groups_per_partition_read()
        ));
        lines.push(
            "           Relayout (offline copy, source untouched): lastdb migrate-hash-group \
             --from <stopped home> --into <fresh home> --hash-group-key partition-prefix"
                .to_string(),
        );
    }
    lines
}

#[path = "status_ops_cmds/ops_rollup.rs"]
mod ops_rollup;
pub(crate) use ops_rollup::*;
#[path = "status_ops_cmds/probes.rs"]
mod probes;
pub(crate) use probes::*;

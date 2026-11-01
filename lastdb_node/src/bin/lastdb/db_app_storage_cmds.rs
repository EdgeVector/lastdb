use super::*;

/// `lastdb app storage [--json] [--reconcile]` — live bytes grouped by owning app.
///
/// Bounded read: the node answers from the keep-small projection, so this is
/// safe to run on a busy node. It is NOT `db inventory`. `--reconcile` posts
/// one page of declared layouts, then reads the report.
pub(crate) fn app_storage(
    socket: &Path,
    json_only: bool,
    reconcile: bool,
    page_size: Option<u64>,
) -> Result<(), String> {
    if reconcile {
        let body = match page_size {
            Some(n) => serde_json::json!({ "page_size": n }),
            None => serde_json::json!({}),
        };
        let response = post_json_with_timeout(
            socket,
            "/api/storage/app/reconcile",
            &body,
            Duration::from_secs(lastdb_node::exec::DEFAULT_CLI_UDS_TIMEOUT_SECS),
        )?;
        let _ = parse_json_response(&response, "app storage reconcile")?;
    }
    let response = get_path(
        socket,
        "/api/storage/app",
        Duration::from_secs(lastdb_node::exec::DEFAULT_CLI_UDS_TIMEOUT_SECS),
    )?;
    let value = parse_json_response(&response, "app storage")?;
    let report = value
        .get("app_storage")
        .cloned()
        .ok_or_else(|| "response missing app_storage field".to_string())?;
    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|e| format!("serialize report: {e}"))?
        );
        return Ok(());
    }
    print_app_storage_human(&report);
    Ok(())
}

pub(crate) fn print_app_storage_human(report: &serde_json::Value) {
    let u64_at = |key: &str| {
        report
            .get(key)
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
    };
    let live_total = u64_at("live_total_bytes");
    let complete = report
        .get("complete")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let measured_at = report
        .get("measured_at")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown");

    println!("Live storage by owning app  (measured_at: {measured_at})");
    println!("  live total:    {}", format_bytes(live_total));
    println!("  budget:        {}", format_bytes(u64_at("budget_bytes")));
    println!(
        "  attributed:    {}",
        format_bytes(u64_at("attributed_bytes"))
    );
    println!("  system:        {}", format_bytes(u64_at("system_bytes")));
    println!(
        "  unattributed:  {}  (planes {}, unresolved schemas {})",
        format_bytes(u64_at("unattributed_bytes")),
        format_bytes(u64_at("unattributable_plane_bytes")),
        format_bytes(u64_at("unresolved_schema_bytes"))
    );
    if let Some(lag) = report.get("reconciliation_lag") {
        let lag_u64 = |key: &str| {
            lag.get(key)
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0)
        };
        let last = lag
            .get("last_pass_at")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("never");
        let resume = lag
            .get("resume_after")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("-");
        println!(
            "  reconcile lag: {} schemas / {}  last_pass={}  resume_after={}",
            lag_u64("unresolved_schema_count"),
            format_bytes(lag_u64("unresolved_schema_bytes")),
            last,
            resume
        );
    }
    println!();
    print_app_storage_owners(report);
    println!();
    print_app_storage_completeness(report, complete);
}

fn print_app_storage_owners(report: &serde_json::Value) {
    let rows = report
        .get("owners")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    if rows.is_empty() {
        println!("  (no metered schemas yet — a fresh home reports nothing)");
    } else {
        println!("  OWNER                     KIND           BYTES  SCHEMAS");
        for row in &rows {
            let owner = row
                .get("owner")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("?");
            let kind = row
                .get("kind")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("?");
            let bytes = row
                .get("live_bytes")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0);
            let schemas = row
                .get("schema_count")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0);
            println!(
                "  {owner:<24}  {kind:<12}  {:>6}  {schemas:>7}",
                format_bytes(bytes)
            );
        }
    }
}
fn print_app_storage_completeness(report: &serde_json::Value, complete: bool) {
    let meters_origin = report
        .get("meters_origin")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("");
    let not_hydrated = meters_origin == "not_hydrated";
    if meters_origin == "stale_after_unclean_stop" {
        println!(
            "complete: FALSE — the daemon last stopped without its shutdown flush, so the \
             persisted meters predate the home's last writes. The numbers above are the last \
             gauge, not a measurement."
        );
    } else if not_hydrated {
        // Say this before the byte line: with un-hydrated meters every number
        // above is a floor, so an operator must not read the split at all.
        println!(
            "complete: FALSE — the write-path meters were never hydrated on this home, so \
             every number above counts only what this daemon process has written since it \
             started. The pre-existing corpus is missing, and neither organic writes nor \
             `--reconcile` recover it: reconcile resolves schema OWNERS, not byte totals. \
             Use `lastdb db inventory` for a true byte answer on this home."
        );
    } else if complete {
        println!("complete: every live byte is attributed to an app or to `system`.");
    } else {
        println!(
            "complete: FALSE — {} is not attributed to any app. Treat the ranking as a floor, not an exact split.",
            format_bytes(report_u64(report, "unattributed_bytes"))
        );
    }
}

use super::*;

pub(crate) fn db_molecule_keys(
    socket: &Path,
    molecule: &str,
    max_keys: Option<usize>,
    json_only: bool,
) -> Result<(), String> {
    let body = serde_json::json!({
        "molecule": molecule,
        "max_keys": max_keys.unwrap_or(10_000),
    });
    let value = db_post_json(socket, "/api/db/molecule-keys", &body)?;
    let report = value
        .get("molecule_keys")
        .cloned()
        .ok_or_else(|| "response missing molecule_keys".to_string())?;
    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|e| format!("serialize: {e}"))?
        );
        return Ok(());
    }
    let field = |v: &serde_json::Value, key: &str| {
        v.get(key).and_then(serde_json::Value::as_u64).unwrap_or(0)
    };
    println!(
        "Molecule keys for {} — raw={} unique={} collision_rows={} more={}",
        report
            .get("molecule")
            .and_then(serde_json::Value::as_str)
            .unwrap_or(molecule),
        field(&report, "keys"),
        field(&report, "unique_keys"),
        field(&report, "collision_rows"),
        report
            .get("more_remaining")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
    );
    if let Some(rows) = report.get("rows").and_then(serde_json::Value::as_array) {
        for row in rows.iter().take(200) {
            let sk = row
                .get("storage_key")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("?");
            let h = row
                .get("hash")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("<undecodable>");
            let r = row
                .get("range")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("");
            let coll = row
                .get("collision")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false);
            let mark = if coll { " COLLISION" } else { "" };
            println!("  {sk}  hash={h} range={r}{mark}");
        }
        if rows.len() > 200 {
            println!("  … {} more rows (use --json)", rows.len() - 200);
        }
    }
    Ok(())
}

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
            Some(n) => format!(r#"{{"page_size":{n}}}"#),
            None => "{}".to_string(),
        };
        let req = format!(
            "POST /api/storage/app/reconcile HTTP/1.1\r\nHost: localhost\r\n{}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            client_headers(),
            body.len()
        );
        let response = request_with_timeout(
            socket,
            req.as_bytes(),
            Duration::from_secs(lastdb_node::exec::DEFAULT_CLI_UDS_TIMEOUT_SECS),
        )?;
        let _ = parse_json_response(&response, "app storage reconcile")?;
    }
    let req = format!(
        "GET /api/storage/app HTTP/1.1\r\nHost: localhost\r\n{}Connection: close\r\n\r\n",
        client_headers()
    );
    let response = request_with_timeout(
        socket,
        req.as_bytes(),
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

// lint:fn-size-ok moved verbatim from db_cmds.rs; splitting these functions is separate work.
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

    let rows = report
        .get("owners")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    if rows.is_empty() {
        println!("  (no metered schemas yet — a fresh home reports nothing)");
    } else {
        println!("  OWNER                     KIND           BYTES  ATOMS  SCHEMAS");
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
            let atoms = row
                .get("atom_count")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0);
            let schemas = row
                .get("schema_count")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0);
            println!(
                "  {owner:<24}  {kind:<12}  {:>6}  {atoms:>5}  {schemas:>7}",
                format_bytes(bytes)
            );
        }
    }
    println!();
    let meters_origin = report
        .get("meters_origin")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("");
    let not_hydrated = meters_origin == "not_hydrated";
    if meters_origin == "stale_after_unclean_stop" {
        println!(
            "complete: FALSE — the daemon last stopped without its shutdown flush, so the \
             persisted meters predate the home's last writes. The numbers above are the last \
             gauge, not a measurement. Run the liveness bootstrap (POST \
             /api/storage/liveness/bootstrap on an isolated copy) to re-measure."
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
            format_bytes(u64_at("unattributed_bytes"))
        );
    }
}

pub(crate) fn db_purge_schemaidx(socket: &Path, json_only: bool) -> Result<(), String> {
    let req = format!(
        "POST /api/db/purge-schemaidx HTTP/1.1\r\nHost: localhost\r\n{}Content-Length: 0\r\nConnection: close\r\n\r\n",
        client_headers()
    );
    let response = request_with_timeout(socket, req.as_bytes(), admin_scan_client_timeout())?;
    let value = parse_json_response(&response, "purge-schemaidx")?;
    let report = value
        .get("purge_schemaidx")
        .cloned()
        .ok_or_else(|| "response missing purge_schemaidx field".to_string())?;
    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|e| format!("serialize report: {e}"))?
        );
        return Ok(());
    }
    let keys = report
        .get("keys_deleted")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let bytes = report
        .get("bytes_freed_approx")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    println!("Purged retired schemaidx secondary index");
    println!("  keys_deleted:  {keys}");
    println!("  bytes_approx:  {}", format_bytes(bytes));
    println!();
    println!("Canonical atom: rows were not touched. On-disk freelist may need later compact.");
    Ok(())
}

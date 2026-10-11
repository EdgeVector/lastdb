use super::*;

pub(crate) fn db_reap_dropped_schema(
    socket: &Path,
    schema: &str,
    fields: &[String],
    execute: bool,
    max_ops: u64,
    cursor: Option<&str>,
    json_only: bool,
) -> Result<(), String> {
    let cursor = cursor.map(str::to_string);
    let value = db_post_json(
        socket,
        "/api/db/reap-dropped-schema",
        &serde_json::json!({
            "schema": schema,
            "fields": fields,
            "dry_run": !execute,
            "max_ops": max_ops,
            "cursor": cursor,
        }),
    )?;
    let report = value
        .get("reap_dropped_schema")
        .cloned()
        .ok_or_else(|| "response missing reap_dropped_schema".to_string())?;
    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|e| format!("serialize: {e}"))?
        );
        return Ok(());
    }
    print_reap_dropped_schema_human(&report, schema, execute);
    Ok(())
}

/// Counter rows of the reap report: (label, JSON key).
const REAP_COUNTER_ROWS: [(&str, &str); 8] = [
    ("molecules", "molecules"),
    ("receipt_allowed", "molecules_allowed_by_drop_receipt"),
    ("tips", "tips"),
    ("tips_deleted", "tips_deleted"),
    ("index_keys", "index_keys"),
    ("retained_refs", "molecules_retained_active_edges"),
    ("retained_protein", "molecules_retained_protein"),
    ("physical_handles", "physical_handles_visited"),
];

/// Boolean rows of the reap report: (label, JSON key).
const REAP_FLAG_ROWS: [(&str, &str); 3] = [
    ("tip_inspection", "tip_inspection_available"),
    ("index_cleanup", "index_cleanup_available"),
    ("truncated", "truncated"),
];

pub(crate) fn report_u64(report: &serde_json::Value, key: &str) -> u64 {
    report
        .get(key)
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0)
}

pub(crate) fn report_bool(report: &serde_json::Value, key: &str) -> bool {
    report
        .get(key)
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
}

fn print_reap_dropped_schema_human(report: &serde_json::Value, schema: &str, execute: bool) {
    let mode = if execute { "EXECUTED" } else { "DRY RUN" };
    println!("reap-dropped-schema {mode}");
    println!(
        "  {:<21} {}",
        "schema:",
        report
            .get("schema")
            .and_then(serde_json::Value::as_str)
            .unwrap_or(schema)
    );
    println!(
        "  {:<21} {}",
        "refused_scan:",
        report_bool(report, "refused_scan")
    );
    for (label, key) in REAP_COUNTER_ROWS {
        println!("  {:<21} {}", format!("{label}:"), report_u64(report, key));
    }
    // The cold-shard counter sits between the counters and the flags.
    println!(
        "  {:<21} {}",
        "cold_shard_loads:",
        report_u64(report, "cold_shard_loads")
    );
    for (label, key) in REAP_FLAG_ROWS {
        println!("  {:<21} {}", format!("{label}:"), report_bool(report, key));
    }
    print_reap_dropped_schema_next(report, execute);
}

fn print_reap_dropped_schema_next(report: &serde_json::Value, execute: bool) {
    if report_bool(report, "refused_scan") {
        println!(
            "  NEXT: janitor refused an atom: scan; range schemaidx or receipt molecules instead"
        );
    } else if let Some(cursor) = report.get("next_cursor") {
        if let Some(token) = cursor.as_str() {
            println!("  next_cursor:          {token}");
            println!("  NEXT: re-run with --cursor <next_cursor token>");
        }
    } else if !execute {
        println!("  Execute remains closed until the exact-once meter debit journal lands.");
    }
}

pub(crate) fn db_probe_dropped_tip(
    socket: &Path,
    schema: &str,
    molecule: &str,
    key_hash: &str,
    key_range: &str,
    expected_key_fingerprint: Option<&str>,
    json_only: bool,
) -> Result<(), String> {
    let value = db_post_json(
        socket,
        "/api/db/reap-dropped-schema",
        &serde_json::json!({
            "schema": schema,
            "dry_run": true,
            "probe": {
                "molecule_uuid": molecule,
                "key_hash": key_hash,
                "key_range": key_range,
                "expected_key_fingerprint": expected_key_fingerprint,
            },
        }),
    )?;
    let report = value
        .get("probe_dropped_tip")
        .ok_or_else(|| "response missing probe_dropped_tip".to_string())?;
    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(report).map_err(|error| format!("serialize: {error}"))?
        );
    } else {
        println!("probe-dropped-tip");
        for key in [
            "schema",
            "molecule_uuid",
            "key_fingerprint",
            "candidate_fingerprints",
            "tip_present",
            "atom_body_present",
            "atom_source_schema_matches",
            "schema_index_present",
            "protein_bound",
            "ref_edges_present_conservative",
            "molecule_edges_complete",
        ] {
            println!(
                "  {key}: {}",
                report.get(key).unwrap_or(&serde_json::Value::Null)
            );
        }
    }
    Ok(())
}

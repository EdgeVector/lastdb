use super::*;

// lint:fn-size-ok moved verbatim from db_cmds.rs; splitting these functions is separate work.
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
    let mode = if execute { "EXECUTED" } else { "DRY RUN" };
    println!("reap-dropped-schema {mode}");
    println!(
        "  schema:               {}",
        report
            .get("schema")
            .and_then(serde_json::Value::as_str)
            .unwrap_or(schema)
    );
    println!(
        "  refused_scan:         {}",
        report
            .get("refused_scan")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
    );
    println!(
        "  molecules:            {}",
        report
            .get("molecules")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
    );
    println!(
        "  receipt_allowed:      {}",
        report
            .get("molecules_allowed_by_drop_receipt")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
    );
    println!(
        "  tips:                 {}",
        report
            .get("tips")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
    );
    println!(
        "  tips_deleted:         {}",
        report
            .get("tips_deleted")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
    );
    println!(
        "  index_keys:           {}",
        report
            .get("index_keys")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
    );
    println!(
        "  retained_refs:        {}",
        report
            .get("molecules_retained_active_edges")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
    );
    println!(
        "  retained_protein:     {}",
        report
            .get("molecules_retained_protein")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
    );
    println!(
        "  physical_handles:     {}",
        report
            .get("physical_handles_visited")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
    );
    println!(
        "  cold_shard_loads:     {}",
        report
            .get("cold_shard_loads")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
    );
    println!(
        "  tip_inspection:       {}",
        report
            .get("tip_inspection_available")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
    );
    println!(
        "  index_cleanup:        {}",
        report
            .get("index_cleanup_available")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
    );
    println!(
        "  truncated:            {}",
        report
            .get("truncated")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
    );
    if report
        .get("refused_scan")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
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
    Ok(())
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

use super::*;

/// Per-pass counters that `db_compact_order_log` sums across passes.
const COMPACTION_SUMMED_KEYS: [&str; 23] = [
    "molecules_planned",
    "entries_planned",
    "bytes_planned",
    "molecules_compacted",
    "entries_deleted",
    "bytes_deleted",
    "molecules_skipped_live",
    "molecules_filter_planned",
    "entries_retained_planned",
    "entries_stale_planned",
    "molecules_filtered",
    "entries_retained",
    "entries_stale_removed",
    "bytes_filter_deleted",
    "molecules_skipped_concurrent",
    "molecules_expired_planned",
    "entries_expired_planned",
    "bytes_expired_planned",
    "molecules_expired",
    "entries_expired_deleted",
    "bytes_expired_deleted",
    "tips_bytes_planned",
    "tips_bytes_deleted",
];

fn json_u64(value: &serde_json::Value, key: &str) -> u64 {
    value
        .get(key)
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0)
}

fn compaction_totals_seed(execute: bool, retention_seconds: Option<u64>) -> serde_json::Value {
    let mut totals = serde_json::json!({
        "dry_run": !execute,
        "passes": 0u64,
        "keys_scanned": 0u64,
        "fanout": 0u64,
        "skipped_backup_cut": false,
        "retention_seconds": retention_seconds.unwrap_or(2_592_000u64),
    });
    for key in COMPACTION_SUMMED_KEYS {
        totals[key] = serde_json::json!(0u64);
    }
    totals
}

/// Fold one compaction pass response into the running totals.
fn accumulate_compaction_pass(totals: &mut serde_json::Value, pass: &serde_json::Value) {
    totals["passes"] = serde_json::json!(json_u64(totals, "passes").saturating_add(1));
    for key in COMPACTION_SUMMED_KEYS {
        totals[key] = serde_json::json!(json_u64(totals, key).saturating_add(json_u64(pass, key)));
    }
    if let Some(fanout) = pass.get("fanout").and_then(serde_json::Value::as_u64) {
        totals["fanout"] = serde_json::json!(fanout);
    }
    if pass
        .get("skipped_backup_cut")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
        totals["skipped_backup_cut"] = serde_json::json!(true);
    }
    let scanned = pass
        .get("audit")
        .map_or(0, |audit| json_u64(audit, "keys_scanned"));
    totals["keys_scanned"] =
        serde_json::json!(json_u64(totals, "keys_scanned").saturating_add(scanned));
}

fn print_compaction_report(totals: &serde_json::Value, execute: bool) {
    println!(
        "Order-log compaction (delete zero-live, bloated, and clean logs) — {}",
        if execute { "EXECUTED" } else { "DRY RUN" }
    );
    let keys = ["passes", "keys_scanned"]
        .into_iter()
        .chain(COMPACTION_SUMMED_KEYS)
        .chain(["fanout", "retention_seconds", "wall_ms", "deletes_per_sec"]);
    for key in keys {
        println!("  {key}: {}", json_u64(totals, key));
    }
    if execute {
        println!(
            "\nLogical deletes are appends. Return the bytes with:\n  \
             lastdb db compact --collection tips --execute\n  \
             lastdb db compact --collection field_update_order_log --execute\n  \
             lastdb db compact --collection field_update_order_count --execute"
        );
    } else {
        println!("\nNo rows were deleted. Re-run with --execute on a CoW clone first.");
    }
}

pub(crate) fn db_compact_order_log(
    socket: &Path,
    execute: bool,
    max_keys: Option<usize>,
    retention_seconds: Option<u64>,
    json_only: bool,
) -> Result<(), String> {
    let max_keys = max_keys.unwrap_or(ORDER_LOG_AUDIT_KEYS_PER_CALL);
    let started = std::time::Instant::now();
    let mut totals = compaction_totals_seed(execute, retention_seconds);
    let mut after_key: Option<String> = None;
    loop {
        let mut body = serde_json::json!({
            "dry_run": !execute,
            "max_keys": max_keys,
        });
        if let Some(secs) = retention_seconds {
            body["retention_seconds"] = serde_json::json!(secs);
        }
        if let Some(after) = &after_key {
            body["after_key"] = serde_json::Value::String(after.clone());
        }
        let value = db_post_json(socket, "/api/db/compact-order-log", &body)?;
        let pass = value
            .get("order_log_compaction")
            .ok_or_else(|| "response missing order_log_compaction".to_string())?;
        accumulate_compaction_pass(&mut totals, pass);
        let more = pass
            .get("more_remaining")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        after_key = pass
            .get("next_after_key")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string);
        if totals["skipped_backup_cut"].as_bool().unwrap_or(false) || !more || after_key.is_none() {
            break;
        }
        if !json_only {
            println!(
                "{}",
                pass_progress_line(
                    json_u64(&totals, "passes"),
                    json_u64(&totals, "keys_scanned"),
                    &[
                        (
                            "zero-live molecule(s) planned",
                            json_u64(pass, "molecules_planned")
                        ),
                        ("live-filter", json_u64(pass, "molecules_filter_planned")),
                        ("expired", json_u64(pass, "molecules_expired_planned")),
                    ],
                )
            );
        }
    }

    let wall_ms = started.elapsed().as_millis() as u64;
    let deleted = json_u64(&totals, "entries_deleted")
        .saturating_add(json_u64(&totals, "entries_expired_deleted"))
        .saturating_add(json_u64(&totals, "entries_stale_removed"));
    let deletes_per_sec = deleted
        .saturating_mul(1000)
        .checked_div(wall_ms)
        .unwrap_or(0);
    totals["wall_ms"] = serde_json::json!(wall_ms);
    totals["deletes_per_sec"] = serde_json::json!(deletes_per_sec);

    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&totals).map_err(|error| error.to_string())?
        );
        return Ok(());
    }
    if totals["skipped_backup_cut"].as_bool().unwrap_or(false) {
        println!("Order-log compaction skipped: a backup cut is held.");
        return Ok(());
    }
    print_compaction_report(&totals, execute);
    Ok(())
}

/// Fold one shortfall-repair pass response into the running totals.
fn accumulate_shortfall_pass(totals: &mut serde_json::Value, pass: &serde_json::Value) {
    totals["passes"] = serde_json::json!(json_u64(totals, "passes").saturating_add(1));
    for key in [
        "molecules_planned",
        "entries_planned",
        "molecules_repaired",
        "entries_appended",
    ] {
        totals[key] = serde_json::json!(json_u64(totals, key).saturating_add(json_u64(pass, key)));
    }
    let audit = pass.get("audit").cloned().unwrap_or_default();
    for key in [
        "keys_scanned",
        "molecules_short",
        "entries_missing",
        "logical_entries_missing",
    ] {
        totals[key] =
            serde_json::json!(json_u64(totals, key).saturating_add(json_u64(&audit, key)));
    }
}

pub(crate) fn db_repair_order_log_shortfall(
    socket: &Path,
    execute: bool,
    max_keys: Option<usize>,
    json_only: bool,
) -> Result<(), String> {
    let max_keys = max_keys.unwrap_or(ORDER_LOG_AUDIT_KEYS_PER_CALL);
    let mut totals = serde_json::json!({
        "dry_run": !execute,
        "passes": 0u64,
        "keys_scanned": 0u64,
        "molecules_short": 0u64,
        "entries_missing": 0u64,
        "logical_entries_missing": 0u64,
        "molecules_planned": 0u64,
        "entries_planned": 0u64,
        "molecules_repaired": 0u64,
        "entries_appended": 0u64,
    });
    let mut after_key: Option<String> = None;
    loop {
        let mut body = serde_json::json!({
            "dry_run": !execute,
            "max_keys": max_keys,
        });
        if let Some(after) = &after_key {
            body["after_key"] = serde_json::Value::String(after.clone());
        }
        let value = db_post_json(socket, "/api/db/repair-order-log-shortfall", &body)?;
        let pass = value
            .get("order_log_repair")
            .ok_or_else(|| "response missing order_log_repair".to_string())?;
        accumulate_shortfall_pass(&mut totals, pass);
        let more = pass
            .get("more_remaining")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        after_key = pass
            .get("next_after_key")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string);
        if !more || after_key.is_none() {
            break;
        }
        if !json_only {
            println!(
                "{}",
                pass_progress_line(
                    json_u64(&totals, "passes"),
                    json_u64(&totals, "keys_scanned"),
                    &[("entries planned", json_u64(pass, "entries_planned"))],
                )
            );
        }
    }

    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&totals).map_err(|error| error.to_string())?
        );
        return Ok(());
    }
    println!(
        "Order-log shortfall repair — {}",
        if execute { "EXECUTED" } else { "DRY RUN" }
    );
    for key in [
        "passes",
        "keys_scanned",
        "molecules_short",
        "entries_missing",
        "logical_entries_missing",
        "molecules_planned",
        "entries_planned",
        "molecules_repaired",
        "entries_appended",
    ] {
        println!("  {key}: {}", json_u64(&totals, key));
    }
    if !execute {
        println!("\nNo rows were written. Re-run with --execute on a CoW clone first.");
    }
    Ok(())
}

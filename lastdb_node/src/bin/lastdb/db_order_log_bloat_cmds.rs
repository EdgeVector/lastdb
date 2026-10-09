use super::*;

pub(crate) fn order_log_bloat_field(value: &serde_json::Value, key: &str) -> u64 {
    value
        .get(key)
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0)
}

pub(crate) fn merge_order_log_bloat_pass(
    totals: &mut std::collections::BTreeMap<String, u64>,
    bloated: &mut Vec<serde_json::Value>,
    zero_live: &mut Vec<serde_json::Value>,
    per_schema: &mut OrderLogBloatSchemaTotals,
    pass: &serde_json::Value,
) {
    for key in [
        "keys_scanned",
        "molecules_decided",
        "molecules_ok",
        "molecules_without_order_count",
        "molecules_bloated",
        "molecules_zero_live",
        "stale_entries",
        "order_log_bytes",
        "order_count_bytes",
        "zero_live_bytes",
    ] {
        *totals.entry(key.to_string()).or_default() += order_log_bloat_field(pass, key);
    }
    // The daemon re-reads all `moc:` rows on every cursor page; these are
    // store properties, not values to multiply by the number of pages.
    for key in ["order_counts_read", "order_counts_unreadable"] {
        totals.insert(key.to_string(), order_log_bloat_field(pass, key));
    }
    if let Some(rows) = pass
        .get("bloated_molecules")
        .and_then(serde_json::Value::as_array)
    {
        bloated.extend(rows.iter().cloned());
    }
    if let Some(rows) = pass
        .get("zero_live_molecules")
        .and_then(serde_json::Value::as_array)
    {
        zero_live.extend(rows.iter().cloned());
    }
    if let Some(rows) = pass.get("per_schema").and_then(serde_json::Value::as_array) {
        for row in rows {
            let name = row
                .get("schema_name")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("<unattributed>")
                .to_string();
            let entry = per_schema.entry(name.clone()).or_insert_with(|| {
                let mut map = serde_json::Map::new();
                map.insert("schema_name".into(), serde_json::json!(name));
                for key in [
                    "molecules",
                    "zero_live_molecules",
                    "order_log_entries",
                    "order_log_bytes",
                    "live_unique_keys",
                    "stale_entries",
                    "zero_live_bytes",
                ] {
                    map.insert(key.into(), serde_json::json!(0u64));
                }
                map
            });
            for key in [
                "molecules",
                "zero_live_molecules",
                "order_log_entries",
                "order_log_bytes",
                "live_unique_keys",
                "stale_entries",
                "zero_live_bytes",
            ] {
                let current = entry
                    .get(key)
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0);
                entry.insert(
                    key.into(),
                    serde_json::json!(current + order_log_bloat_field(row, key)),
                );
            }
        }
    }
}

/// Default keys one `pin-log-audit` daemon call walks (matches core constant).
pub(crate) const PIN_LOG_AUDIT_KEYS_PER_CALL: usize = 50_000;

pub(crate) fn db_pin_log_audit(
    socket: &Path,
    max_keys: Option<usize>,
    after_key: Option<&str>,
    json_only: bool,
) -> Result<(), String> {
    let max_keys = max_keys.unwrap_or(PIN_LOG_AUDIT_KEYS_PER_CALL);
    let mut body = serde_json::json!({ "max_keys": max_keys });
    if let Some(after) = after_key {
        body["after_key"] = serde_json::Value::String(after.to_string());
    }
    let value = db_post_json(socket, "/api/db/pin-log-audit", &body)?;
    let report = value
        .get("pin_log_audit")
        .cloned()
        .ok_or_else(|| "response missing pin_log_audit".to_string())?;

    println!("{}", pin_log_audit_output(&report, json_only)?);
    Ok(())
}

pub(crate) fn pin_log_audit_output(
    report: &serde_json::Value,
    json_only: bool,
) -> Result<String, String> {
    use std::fmt::Write as _;

    if json_only {
        return serde_json::to_string_pretty(report).map_err(|e| format!("serialize: {e}"));
    }

    let field = |v: &serde_json::Value, key: &str| {
        v.get(key).and_then(serde_json::Value::as_u64).unwrap_or(0)
    };
    let mut out = String::new();
    writeln!(
        out,
        "Pin-log audit — confirmed = frontier ≤ durable published-F"
    )
    .map_err(|e| format!("format: {e}"))?;
    for key in [
        "keys_scanned",
        "entry_rows",
        "entry_bytes",
        "confirmed_rows",
        "confirmed_bytes",
        "pending_rows",
        "pending_bytes",
        "unreadable_rows",
    ] {
        writeln!(out, "  {key}: {}", field(report, key)).map_err(|e| format!("format: {e}"))?;
    }
    writeln!(out, "  targets_seen: {}", field(report, "targets_seen"))
        .map_err(|e| format!("format: {e}"))?;
    let writers: &[serde_json::Value] = report
        .get("writers")
        .and_then(serde_json::Value::as_array)
        .map_or(&[], Vec::as_slice);
    writeln!(out, "  writers: {}", writers.len()).map_err(|e| format!("format: {e}"))?;
    if !writers.is_empty() {
        writeln!(out, "  per-writer frontiers:").map_err(|e| format!("format: {e}"))?;
        for w in writers {
            writeln!(
                out,
                "    target={} writer={} published_f={} confirmed={}/{}B pending={}/{}B",
                w.get("target_id").and_then(|v| v.as_str()).unwrap_or(""),
                w.get("writer_id").and_then(|v| v.as_str()).unwrap_or(""),
                field(w, "durable_published_f"),
                field(w, "confirmed_rows"),
                field(w, "confirmed_bytes"),
                field(w, "pending_rows"),
                field(w, "pending_bytes"),
            )
            .map_err(|e| format!("format: {e}"))?;
        }
    }
    if report
        .get("more_remaining")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
        if let Some(cursor) = report
            .get("next_after_key")
            .and_then(serde_json::Value::as_str)
        {
            writeln!(out, "  more remaining; next_after_key={cursor}")
                .map_err(|e| format!("format: {e}"))?;
        }
    }
    Ok(out.trim_end().to_string())
}

// lint:fn-size-ok moved verbatim from db_cmds.rs; splitting these functions is separate work.
pub(crate) fn db_order_log_bloat_audit(
    socket: &Path,
    max_keys: Option<usize>,
    json_only: bool,
) -> Result<(), String> {
    let max_keys = max_keys.unwrap_or(ORDER_LOG_AUDIT_KEYS_PER_CALL);
    let mut totals: std::collections::BTreeMap<String, u64> = std::collections::BTreeMap::new();
    let mut bloated: Vec<serde_json::Value> = Vec::new();
    let mut zero_live: Vec<serde_json::Value> = Vec::new();
    let mut per_schema = std::collections::BTreeMap::new();
    let mut after_key: Option<String> = None;
    let mut passes = 0u64;

    loop {
        let mut body = serde_json::json!({ "max_keys": max_keys });
        if let Some(after) = &after_key {
            body["after_key"] = serde_json::Value::String(after.clone());
        }
        let value = db_post_json(socket, "/api/db/order-log-bloat-audit", &body)?;
        let pass = value
            .get("order_log_bloat_audit")
            .cloned()
            .ok_or_else(|| "response missing order_log_bloat_audit".to_string())?;
        passes += 1;
        merge_order_log_bloat_pass(
            &mut totals,
            &mut bloated,
            &mut zero_live,
            &mut per_schema,
            &pass,
        );
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
                    passes,
                    totals.get("keys_scanned").copied().unwrap_or(0),
                    &[],
                )
            );
        }
    }

    bloated.sort_by_key(|row| std::cmp::Reverse(order_log_bloat_field(row, "stale_entries")));
    zero_live.sort_by_key(|row| std::cmp::Reverse(order_log_bloat_field(row, "order_log_bytes")));
    let mut schema_rows: Vec<serde_json::Value> = per_schema
        .into_values()
        .map(serde_json::Value::Object)
        .collect();
    schema_rows.sort_by_key(|row| std::cmp::Reverse(order_log_bloat_field(row, "order_log_bytes")));

    if json_only {
        let mut out = serde_json::Map::new();
        for (key, value) in &totals {
            out.insert(key.clone(), serde_json::json!(value));
        }
        out.insert("passes".into(), serde_json::json!(passes));
        out.insert("bloated_molecules".into(), serde_json::json!(bloated));
        out.insert("zero_live_molecules".into(), serde_json::json!(zero_live));
        out.insert("per_schema".into(), serde_json::json!(schema_rows));
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::Value::Object(out))
                .map_err(|e| format!("serialize: {e}"))?
        );
    } else {
        println!(
            "Order-log bloat audit — excess = order_count - live_unique_keys (append-only residue)"
        );
        for key in [
            "order_counts_read",
            "order_counts_unreadable",
            "keys_scanned",
            "molecules_decided",
            "molecules_ok",
            "molecules_without_order_count",
            "molecules_bloated",
            "molecules_zero_live",
            "stale_entries",
            "order_log_bytes",
            "order_count_bytes",
            "zero_live_bytes",
        ] {
            println!("  {key}: {}", totals.get(key).copied().unwrap_or(0));
        }
        if !schema_rows.is_empty() {
            println!();
            println!("Per schema (findings only, worst first):");
            for row in schema_rows.iter().take(50) {
                println!(
                    "  {}: molecules={} zero_live={} order_log_entries={} order_log_bytes={} stale_entries={} zero_live_bytes={}",
                    row.get("schema_name")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("?"),
                    order_log_bloat_field(row, "molecules"),
                    order_log_bloat_field(row, "zero_live_molecules"),
                    order_log_bloat_field(row, "order_log_entries"),
                    order_log_bloat_field(row, "order_log_bytes"),
                    order_log_bloat_field(row, "stale_entries"),
                    order_log_bloat_field(row, "zero_live_bytes"),
                );
            }
        }
        if !bloated.is_empty() {
            println!();
            println!("Bloated molecules (live keys > 0, worst stale margin first):");
            for row in bloated.iter().take(50) {
                let schema = row
                    .get("schema")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("<unattributed>");
                println!(
                    "  {} [{schema}]: moc={} live_unique={} stale={} order_log_entries={} order_log_bytes={}",
                    row.get("molecule")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("?"),
                    order_log_bloat_field(row, "order_count"),
                    order_log_bloat_field(row, "live_unique_keys"),
                    order_log_bloat_field(row, "stale_entries"),
                    order_log_bloat_field(row, "order_log_entries"),
                    order_log_bloat_field(row, "order_log_bytes"),
                );
            }
        }
        if !zero_live.is_empty() {
            println!();
            println!("Zero-live molecules (order log with no live mk: tips):");
            for row in zero_live.iter().take(50) {
                let schema = row
                    .get("schema")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("<unattributed>");
                println!(
                    "  {} [{schema}]: moc={} order_log_entries={} order_log_bytes={} order_count_bytes={}",
                    row.get("molecule")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("?"),
                    order_log_bloat_field(row, "order_count"),
                    order_log_bloat_field(row, "order_log_entries"),
                    order_log_bloat_field(row, "order_log_bytes"),
                    order_log_bloat_field(row, "order_count_bytes"),
                );
            }
        }
        println!();
        let bloated_n = totals.get("molecules_bloated").copied().unwrap_or(0);
        let zero_n = totals.get("molecules_zero_live").copied().unwrap_or(0);
        let stale = totals.get("stale_entries").copied().unwrap_or(0);
        if bloated_n == 0 && zero_n == 0 {
            println!("No order-log excess found on this store.");
        } else {
            println!(
                "{bloated_n} bloated + {zero_n} zero-live molecule(s); {stale} stale order-log entr(y/ies). \
                 Read-only measurement — no keys were changed."
            );
        }
    }
    Ok(())
}

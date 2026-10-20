use super::*;

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

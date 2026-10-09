use super::*;

pub(crate) struct TombstoneFlagAuditCliOpts {
    pub(crate) schema: Option<String>,
    pub(crate) execute: bool,
    pub(crate) max_keys: Option<usize>,
    pub(crate) after_key: Option<String>,
    pub(crate) once: bool,
    pub(crate) json_only: bool,
}

pub(crate) fn db_tombstone_flag_audit(
    socket: &Path,
    opts: &TombstoneFlagAuditCliOpts,
) -> Result<(), String> {
    db_tombstone_flag_audit_with(
        opts,
        |body| db_post_json(socket, "/api/db/tombstone-flag-audit", body),
        std::io::stdout(),
        std::io::stderr(),
    )
}

// lint:fn-size-ok moved verbatim from db_cmds.rs; splitting these functions is separate work.
pub(crate) fn db_tombstone_flag_audit_with<F, Out, Err>(
    opts: &TombstoneFlagAuditCliOpts,
    mut post: F,
    mut stdout: Out,
    mut stderr: Err,
) -> Result<(), String>
where
    F: FnMut(&serde_json::Value) -> Result<serde_json::Value, String>,
    Out: Write,
    Err: Write,
{
    let max_keys = opts.max_keys.unwrap_or(TOMBSTONE_AUDIT_KEYS_PER_CALL);
    let started = Instant::now();
    let mut report = serde_json::Value::Null;
    let mut after_key = opts.after_key.clone();
    let mut more: bool;
    let mut passes = 0u32;
    loop {
        let mut body = serde_json::json!({
            "dry_run": !opts.execute,
            "max_keys": max_keys,
        });
        if let Some(schema) = &opts.schema {
            body["schema"] = serde_json::Value::String(schema.clone());
        }
        if let Some(after) = &after_key {
            body["after_key"] = serde_json::Value::String(after.clone());
        }
        let value = post(&body)?;
        let pass = value
            .get("tombstone_flag_audit")
            .cloned()
            .ok_or_else(|| "response missing tombstone_flag_audit".to_string())?;
        passes += 1;
        more = pass
            .get("more_remaining")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        after_key = pass
            .get("next_after_key")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string);
        report = merge_tombstone_audit_passes(report, pass);
        let scanned = report
            .get("audit")
            .and_then(|a| a.get("keys_scanned"))
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        if opts.json_only && !opts.once && (more || passes > 1) {
            writeln!(
                stderr,
                "tombstone-flag-audit progress: pass={passes} cumulative_keys_decided={scanned} elapsed_ms={} more_remaining={more}",
                started.elapsed().as_millis()
            )
            .map_err(|e| format!("write stderr: {e}"))?;
        }
        if opts.once || !more || after_key.is_none() {
            break;
        }
        if !opts.json_only {
            writeln!(stdout, "  … pass {passes}: {scanned} keys decided so far")
                .map_err(|e| format!("write stdout: {e}"))?;
        }
    }
    if opts.json_only {
        writeln!(
            stdout,
            "{}",
            serde_json::to_string_pretty(&report).map_err(|e| format!("serialize: {e}"))?
        )
        .map_err(|e| format!("write stdout: {e}"))?;
        return Ok(());
    }
    let audit = report.get("audit").cloned().unwrap_or_default();
    let field = |v: &serde_json::Value, key: &str| {
        v.get(key).and_then(serde_json::Value::as_u64).unwrap_or(0)
    };
    let mode = if opts.execute { "EXECUTED" } else { "DRY RUN" };
    let scope = opts.schema.as_deref().unwrap_or("<whole store>");
    writeln!(stdout, "Tombstone flag audit — {mode} — scope: {scope}")
        .map_err(|e| format!("write stdout: {e}"))?;
    for key in [
        "keys_scanned",
        "meta_tombstoned",
        "live",
        "content_tombstoned_meta_false",
        "atoms_missing",
        "keys_unreadable",
        "atoms_fetched",
    ] {
        writeln!(stdout, "  {key}: {}", field(&audit, key))
            .map_err(|e| format!("write stdout: {e}"))?;
    }
    if opts.execute {
        writeln!(stdout, "  keys_stamped: {}", field(&report, "keys_stamped"))
            .map_err(|e| format!("write stdout: {e}"))?;
    }
    writeln!(stdout, "  more_remaining: {more}").map_err(|e| format!("write stdout: {e}"))?;
    if more {
        if let Some(cursor) = &after_key {
            writeln!(stdout, "  next_after_key: {cursor}")
                .map_err(|e| format!("write stdout: {e}"))?;
            writeln!(
                stdout,
                "  resume with: --after-key {} --once",
                cursor.escape_default()
            )
            .map_err(|e| format!("write stdout: {e}"))?;
        }
    }

    let legacy = field(&audit, "content_tombstoned_meta_false");
    let worst: Vec<&serde_json::Value> = audit
        .get("per_molecule")
        .and_then(serde_json::Value::as_array)
        .map(|rows| {
            rows.iter()
                .filter(|r| field(r, "content_tombstoned_meta_false") > 0)
                .take(20)
                .collect()
        })
        .unwrap_or_default();
    if !worst.is_empty() {
        writeln!(stdout).map_err(|e| format!("write stdout: {e}"))?;
        writeln!(
            stdout,
            "Molecules with unflagged content-tombstones (worst first):"
        )
        .map_err(|e| format!("write stdout: {e}"))?;
        for row in worst {
            let label = row
                .get("label")
                .and_then(serde_json::Value::as_str)
                .or_else(|| row.get("molecule").and_then(serde_json::Value::as_str))
                .unwrap_or("?");
            writeln!(
                stdout,
                "  {label}: {} of {} keys",
                field(row, "content_tombstoned_meta_false"),
                field(row, "keys")
            )
            .map_err(|e| format!("write stdout: {e}"))?;
        }
    }

    writeln!(stdout).map_err(|e| format!("write stdout: {e}"))?;
    if more {
        writeln!(
            stdout,
            "This is one page, not a store census. Omit --once to walk the rest."
        )
        .map_err(|e| format!("write stdout: {e}"))?;
    } else if legacy == 0 {
        writeln!(
            stdout,
            "No unflagged content-tombstones. The flag and the content agree; no backfill is owed."
        )
        .map_err(|e| format!("write stdout: {e}"))?;
    } else if opts.execute {
        writeln!(
            stdout,
            "Stamped {legacy} record(s). Re-run to confirm the count is now zero."
        )
        .map_err(|e| format!("write stdout: {e}"))?;
    } else {
        writeln!(
            stdout,
            "{legacy} record(s) can still shorten a page. Re-run with --execute to stamp the flag."
        )
        .map_err(|e| format!("write stdout: {e}"))?;
    }
    Ok(())
}

// lint:fn-size-ok moved verbatim from db_cmds.rs; splitting these functions is separate work.
pub(crate) fn db_drain_legacy_tombstones(
    socket: &Path,
    schema: Option<&str>,
    execute: bool,
    max_keys: Option<usize>,
    json_only: bool,
) -> Result<(), String> {
    let max_keys = max_keys.unwrap_or(TOMBSTONE_AUDIT_KEYS_PER_CALL);
    let numeric = [
        "keys_scanned",
        "keys_unreadable",
        "atoms_fetched",
        "atoms_missing",
        "tombstones_found",
        "tombstones_drained",
        "unowned_tombstones",
        "search_tombstones_queued",
    ];
    let mut totals = serde_json::json!({
        "dry_run": !execute,
        "per_schema_drained": {},
        "more_remaining": false,
    });
    for key in numeric {
        totals[key] = serde_json::json!(0u64);
    }
    let mut after_key: Option<String> = None;
    loop {
        let mut body = serde_json::json!({
            "dry_run": !execute,
            "max_keys": max_keys,
        });
        if let Some(schema) = schema {
            body["schema"] = serde_json::Value::String(schema.to_string());
        }
        if let Some(after) = &after_key {
            body["after_key"] = serde_json::Value::String(after.clone());
        }
        let value = db_post_json(socket, "/api/db/drain-legacy-tombstones", &body)?;
        let pass = value
            .get("legacy_tombstone_drain")
            .ok_or_else(|| "response missing legacy_tombstone_drain".to_string())?;
        for key in numeric {
            let total = totals[key].as_u64().unwrap_or(0);
            let add = pass
                .get(key)
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0);
            totals[key] = serde_json::json!(total.saturating_add(add));
        }
        if let Some(rows) = pass
            .get("per_schema_drained")
            .and_then(serde_json::Value::as_object)
        {
            for (name, count) in rows {
                let current = totals["per_schema_drained"]
                    .get(name)
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0);
                totals["per_schema_drained"][name] =
                    serde_json::json!(current.saturating_add(count.as_u64().unwrap_or(0)));
            }
        }
        let more = pass
            .get("more_remaining")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        after_key = pass
            .get("next_after_key")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string);
        totals["more_remaining"] = serde_json::Value::Bool(more);
        if !more || after_key.is_none() {
            break;
        }
    }
    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&totals).map_err(|e| format!("serialize: {e}"))?
        );
        return Ok(());
    }
    let mode = if execute { "EXECUTED" } else { "DRY RUN" };
    println!(
        "Legacy tombstone drain — {mode} — scope: {}",
        schema.unwrap_or("<whole store>")
    );
    for key in numeric {
        println!("  {key}: {}", totals[key].as_u64().unwrap_or(0));
    }
    if !execute {
        println!("\nNo rows were erased. Re-run with --execute on a CoW clone first.");
    }
    Ok(())
}

/// `mk:` records one `order-log-audit` daemon call walks. Higher than the
/// tombstone audit's cap because a row here costs a key decode, not an atom
/// body read.
pub(crate) const ORDER_LOG_AUDIT_KEYS_PER_CALL: usize = 250_000;

pub(crate) const LEGACY_KEY_FORK_KEYS_PER_CALL: usize = 50_000;

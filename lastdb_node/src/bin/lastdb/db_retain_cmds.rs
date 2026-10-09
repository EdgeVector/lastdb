use super::*;

// lint:fn-size-ok moved verbatim from db_cmds.rs; splitting these functions is separate work.
pub(crate) fn db_retain_superseded_versions(
    socket: &Path,
    opts: &RetainSupersededVersionsCliOpts,
) -> Result<(), String> {
    let execute = opts.execute;
    let max_keys = opts.max_keys;
    let max_prunes = opts.max_prunes;
    let after_key = opts.after_key.clone();
    let from_checkpoint = opts.from_checkpoint;
    let retention_seconds = opts.retention_seconds;
    let json_only = opts.json_only;
    let started = std::time::Instant::now();
    let mut totals = serde_json::json!({
        "dry_run": !execute,
        "passes": 0u64,
        "keys_scanned": 0u64,
        "tips_with_chain": 0u64,
        "tips_skipped_tombstoned": 0u64,
        "tips_skipped_changed": 0u64,
        "tips_skipped_unreadable": 0u64,
        "tips_truncated": 0u64,
        "tip_versions_pruned": 0u64,
        "tip_versions_kept": 0u64,
        "tip_version_bytes_approx": 0u64,
        "settled": false,
        "skipped_backup_cut": false,
        "retention_seconds": retention_seconds.unwrap_or(7 * 24 * 60 * 60),
        "from_checkpoint": from_checkpoint,
    });
    let mut cursor = after_key;
    loop {
        let mut body = serde_json::json!({
            "dry_run": !execute,
            "from_checkpoint": from_checkpoint,
        });
        if let Some(n) = max_keys {
            body["max_keys"] = serde_json::json!(n);
        }
        if let Some(n) = max_prunes {
            body["max_prunes"] = serde_json::json!(n);
        }
        if let Some(secs) = retention_seconds {
            body["retention_seconds"] = serde_json::json!(secs);
        }
        if let Some(k) = &cursor {
            body["after_key"] = serde_json::Value::String(k.clone());
        }
        let value = db_post_json(socket, "/api/db/retain-superseded-versions", &body)?;
        let pass = value
            .get("retain_superseded_versions")
            .ok_or_else(|| "response missing retain_superseded_versions".to_string())?;
        totals["passes"] =
            serde_json::json!(totals["passes"].as_u64().unwrap_or(0).saturating_add(1));
        for key in [
            "keys_scanned",
            "tips_with_chain",
            "tips_skipped_tombstoned",
            "tips_skipped_changed",
            "tips_skipped_unreadable",
            "tips_truncated",
            "tip_versions_pruned",
            "tip_versions_kept",
            "tip_version_bytes_approx",
        ] {
            totals[key] = serde_json::json!(totals[key].as_u64().unwrap_or(0).saturating_add(
                pass.get(key)
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0)
            ));
        }
        if pass
            .get("skipped_backup_cut")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
        {
            totals["skipped_backup_cut"] = serde_json::json!(true);
        }
        if pass
            .get("settled")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
        {
            totals["settled"] = serde_json::json!(true);
        }
        if let Some(secs) = pass
            .get("retention_seconds")
            .and_then(serde_json::Value::as_u64)
        {
            totals["retention_seconds"] = serde_json::json!(secs);
        }
        let more = pass
            .get("more_remaining")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        cursor = pass
            .get("next_after_key")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string);
        if !json_only {
            println!(
                "{}",
                pass_progress_line(
                    totals["passes"].as_u64().unwrap_or(0),
                    totals["keys_scanned"].as_u64().unwrap_or(0),
                    &[
                        (
                            "truncated",
                            pass.get("tips_truncated")
                                .and_then(serde_json::Value::as_u64)
                                .unwrap_or(0)
                        ),
                        (
                            "tv: pruned",
                            pass.get("tip_versions_pruned")
                                .and_then(serde_json::Value::as_u64)
                                .unwrap_or(0)
                        ),
                    ],
                )
            );
        }
        if totals["skipped_backup_cut"].as_bool().unwrap_or(false) || !more || cursor.is_none() {
            break;
        }
        // Checkpointed mode advances the durable cursor itself; looping with
        // from_checkpoint=true is how a full sweep settles.
        if from_checkpoint {
            cursor = None;
        }
    }
    totals["wall_ms"] = serde_json::json!(started.elapsed().as_millis() as u64);

    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&totals).map_err(|e| format!("serialize: {e}"))?
        );
        return Ok(());
    }
    if totals["skipped_backup_cut"].as_bool().unwrap_or(false) {
        println!("Superseded-version retention skipped: a backup cut is held.");
        return Ok(());
    }
    let mode = if execute { "EXECUTED" } else { "DRY RUN" };
    println!("Superseded-version retention (7d, live records) — {mode}");
    for key in [
        "passes",
        "keys_scanned",
        "tips_with_chain",
        "tips_skipped_tombstoned",
        "tips_truncated",
        "tip_versions_pruned",
        "tip_versions_kept",
        "tip_version_bytes_approx",
        "settled",
        "retention_seconds",
    ] {
        if let Some(v) = totals.get(key) {
            println!("  {key}: {v}");
        }
    }
    if !execute {
        println!();
        println!("Re-run with --execute to drop tv: nodes older than 7 days on live records.");
    }
    Ok(())
}

/// Per-key detail rows a `--schema` repair reports when `--audit-limit` is
/// not given.
pub(crate) const SCOPED_REPAIR_AUDIT_LIMIT: usize = 1000;

/// Bounds and key scope for `lastdb db repair-dangling-tips`.
pub(crate) struct RepairDanglingTipsArgs {
    pub(crate) max_ops: Option<usize>,
    pub(crate) tip_page: Option<usize>,
    pub(crate) audit_limit: Option<usize>,
    pub(crate) schema: Option<String>,
    pub(crate) hash_key: Option<String>,
}

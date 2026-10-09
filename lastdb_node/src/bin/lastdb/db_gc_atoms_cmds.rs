use super::*;

pub(crate) fn db_gc_atoms(
    socket: &Path,
    schema: Option<&str>,
    execute: bool,
    prune_live_history: bool,
    json_only: bool,
) -> Result<(), String> {
    if schema.is_some() && prune_live_history {
        return Err("--schema cannot be combined with --prune-live-history".to_string());
    }
    let value = db_post_json(
        socket,
        "/api/db/gc-atoms",
        &serde_json::json!({
            "dry_run": !execute,
            "prune_live_history": prune_live_history,
            "schema": schema,
        }),
    )?;
    let report = value
        .get("gc_atoms")
        .cloned()
        .ok_or_else(|| "response missing gc_atoms".to_string())?;
    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|e| format!("serialize: {e}"))?
        );
        return Ok(());
    }
    let mode = if execute { "EXECUTED" } else { "DRY RUN" };
    let live = if prune_live_history {
        " (+ prune-live-history)"
    } else {
        ""
    };
    let scope = schema.map_or(String::new(), |name| format!(" — schema {name}"));
    println!("Orphan atom GC — {mode}{live}{scope}");
    println!(
        "  tips_chain_cleared:     {}",
        report
            .get("tips_chain_cleared")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
    );
    println!(
        "  tip_versions_pruned:    {}",
        report
            .get("tip_versions_pruned")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
    );
    let tv_bytes = report
        .get("tip_version_bytes_approx")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    println!("  tip_version_bytes:     {}", format_bytes(tv_bytes));
    println!(
        "  atoms_scanned:          {}",
        report
            .get("atoms_scanned")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
    );
    println!(
        "  atoms_referenced:       {}",
        report
            .get("atoms_referenced")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
    );
    println!(
        "  atoms_deleted:          {}",
        report
            .get("atoms_deleted")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
    );
    let bytes = report
        .get("bytes_freed_approx")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    println!("  atom_bytes_approx:      {}", format_bytes(bytes));
    if !execute {
        println!();
        println!(
            "Re-run with --execute to prune tombstoned tip-version chains and delete orphan atoms."
        );
    }
    Ok(())
}

pub(crate) fn db_drain_tip_history(
    socket: &Path,
    execute: bool,
    max_keys: Option<usize>,
    max_prunes: Option<usize>,
    after_key: Option<String>,
    from_checkpoint: bool,
    json_only: bool,
) -> Result<(), String> {
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
    if let Some(k) = after_key {
        body["after_key"] = serde_json::json!(k);
    }
    let value = db_post_json(socket, "/api/db/drain-tip-history", &body)?;
    if json_only {
        // Preserve checkpoint when present so operators can resume from the
        // same envelope the background scheduler uses.
        println!(
            "{}",
            serde_json::to_string_pretty(&value).map_err(|e| format!("serialize: {e}"))?
        );
        return Ok(());
    }
    let report = value
        .get("drain_tip_history")
        .cloned()
        .ok_or_else(|| "response missing drain_tip_history".to_string())?;
    let mode = if execute { "EXECUTED" } else { "DRY RUN" };
    let via = if from_checkpoint {
        "from-checkpoint"
    } else {
        "one-shot"
    };
    println!("Tip-history drain — {mode} ({via})");
    for key in [
        "keys_scanned",
        "tips_with_chain",
        "tips_chain_cleared",
        "tip_versions_pruned",
        "tip_version_bytes_approx",
        "tips_skipped_changed",
        "tips_skipped_unreadable",
        "more_remaining",
    ] {
        if let Some(v) = report.get(key) {
            println!("  {key}: {v}");
        }
    }
    if let Some(k) = report.get("next_after_key") {
        println!("  next_after_key: {k}");
    }
    if let Some(cp) = value.get("checkpoint") {
        println!(
            "  checkpoint: {}",
            serde_json::to_string(cp).unwrap_or_else(|_| "{}".into())
        );
    }
    Ok(())
}

/// Format one per-pass progress line for a resumable, multi-pass admin verb.
///
/// `keys_walked` is cumulative on purpose: it is the resume position, and the
/// label says "so far". Every other counter is THIS pass's own delta.
///
/// The distinction is not cosmetic. These verbs run for hours and an operator
/// watches this line to decide whether to keep waiting or kill the run. A
/// running `saturating_add` total printed under a per-pass label produces a
/// monotonically rising number, which reads as a plan that keeps growing
/// instead of draining — the reading most likely to cause a premature kill.
/// It has already produced one wrong mechanism conclusion in a recorded
/// investigation: a 52 -> 88 -> 131 -> 169 -> 204 -> 238 -> 265 -> 302 sequence
/// was taken as evidence that pages were not committing their own plans, when
/// the counter accumulates whether or not they commit and so is evidence about
/// neither. Brain:
/// `papercut-lastdb-compact-order-log-per-pass-progress-line-prints-running-totals`.
///
/// The cumulative figures are not lost — every one of these verbs prints the
/// full `totals` block after the loop.
pub(crate) fn pass_progress_line(pass_no: u64, keys_walked: u64, deltas: &[(&str, u64)]) -> String {
    let mut line = format!("  … pass {pass_no}: {keys_walked} keys walked so far");
    if !deltas.is_empty() {
        let body = deltas
            .iter()
            .map(|(label, n)| format!("+{n} {label}"))
            .collect::<Vec<_>>()
            .join(" / ");
        line.push_str(&format!(", {body} this pass"));
    }
    line
}

pub(crate) struct RetainSupersededVersionsCliOpts {
    pub(crate) execute: bool,
    pub(crate) max_keys: Option<usize>,
    pub(crate) max_prunes: Option<usize>,
    pub(crate) after_key: Option<String>,
    pub(crate) from_checkpoint: bool,
    pub(crate) retention_seconds: Option<u64>,
    pub(crate) json_only: bool,
}

use super::*;

pub(crate) fn db_delete_ledger(socket: &Path, limit: u64, json_only: bool) -> Result<(), String> {
    let target = if limit == 0 {
        "/api/db/delete-ledger".to_string()
    } else {
        format!("/api/db/delete-ledger?limit={limit}")
    };
    // A prefix scan over an append-only class shares the admin-scan budget.
    let response = get_path(socket, &target, admin_scan_client_timeout())?;
    let value = parse_json_response(&response, "delete-ledger")?;
    let ledger = value
        .get("delete_ledger")
        .cloned()
        .ok_or_else(|| "response missing delete_ledger field".to_string())?;

    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&ledger).map_err(|e| format!("serialize: {e}"))?
        );
        return Ok(());
    }

    let entries = ledger
        .get("entries")
        .and_then(|e| e.as_array())
        .cloned()
        .unwrap_or_default();
    if entries.is_empty() {
        println!("Atom delete ledger: EMPTY — no delete, purge or gc batch has ever run here.");
        println!("  A missing atom body on this store was not removed by a delete path.");
        return Ok(());
    }
    println!(
        "Atom delete ledger — {} row(s), oldest first",
        entries.len()
    );
    for e in &entries {
        let s = |k: &str| e.get(k).and_then(|v| v.as_str()).unwrap_or("").to_string();
        let n = |k: &str| e.get(k).and_then(serde_json::Value::as_u64).unwrap_or(0);
        let committed = e
            .get("committed")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        let state = if committed { "ok" } else { "UNCONFIRMED" };
        let mut line = format!(
            "  {}  {:<15} {:<11} atoms={}",
            s("at"),
            s("verb"),
            state,
            n("atoms_deleted"),
        );
        if let Some(schema) = e.get("schema").and_then(|v| v.as_str()) {
            line.push_str(&format!(" schema={schema}"));
        }
        if let Some(fp) = e.get("key_fingerprint").and_then(|v| v.as_str()) {
            line.push_str(&format!(
                " key_fp={}",
                observability::truncate::truncate_on_char_boundary(fp, 12)
            ));
        }
        if n("history_rows_deleted") > 0 {
            line.push_str(&format!(" history={}", n("history_rows_deleted")));
        }
        // A `delete-converge` row always reads `atoms=0` by design, so without
        // these two the human view cannot tell it from a batch that did
        // nothing — and "did the delete path run in this window" is the
        // question the ledger exists to answer.
        if n("records_converged") > 0 {
            line.push_str(&format!(" records={}", n("records_converged")));
        }
        if n("tips_removed") > 0 {
            line.push_str(&format!(" tips={}", n("tips_removed")));
        }
        println!("{line}");
        if !committed {
            println!(
                "      ^ this batch started and never confirmed; its counts are what was ATTEMPTED"
            );
        }
    }
    Ok(())
}

pub(crate) fn db_clear_history(
    socket: &Path,
    schema: Option<&str>,
    keep_last: usize,
    execute: bool,
    json_only: bool,
) -> Result<(), String> {
    let dry_run = !execute;
    let body = serde_json::json!({
        "schema": schema,
        "keep_last": keep_last,
        "dry_run": dry_run,
    });
    let response = post_json_admin(socket, "/api/db/clear-history", &body)?;
    let value = parse_json_response(&response, "clear-history")?;
    let report = value
        .get("clear_history")
        .cloned()
        .ok_or_else(|| "response missing clear_history field".to_string())?;

    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|e| format!("serialize report: {e}"))?
        );
        return Ok(());
    }

    print_clear_history_human(&report, dry_run);
    Ok(())
}

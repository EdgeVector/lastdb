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
        println!("Record delete ledger: EMPTY — no delete or purge batch has run here.");
        println!("  A missing record on this store was not removed by a delete path.");
        return Ok(());
    }
    println!(
        "Record delete ledger — {} row(s), oldest first",
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
        // A record model erase row counts its records in `records_converged`.
        // `atoms_deleted` and `tips_removed` stay in the JSON for its readers;
        // the record model keeps both at zero, so the text leaves them out.
        let mut line = format!(
            "  {}  {:<15} {:<11} records={}",
            s("at"),
            s("verb"),
            state,
            n("records_converged"),
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
        println!("{line}");
        if !committed {
            println!(
                "      ^ this batch started and never confirmed; its counts are what was ATTEMPTED"
            );
        }
    }
    Ok(())
}

use super::*;

pub(crate) fn db_delete_ledger(socket: &Path, limit: u64, json_only: bool) -> Result<(), String> {
    let target = if limit == 0 {
        "/api/db/delete-ledger".to_string()
    } else {
        format!("/api/db/delete-ledger?limit={limit}")
    };
    let req = format!(
        "GET {target} HTTP/1.1\r\nHost: localhost\r\n{}Connection: close\r\n\r\n",
        client_headers()
    );
    // A prefix scan over an append-only class, so it shares the admin-scan
    // budget rather than the default read timeout.
    let response = request_with_timeout(socket, req.as_bytes(), admin_scan_client_timeout())?;
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
            line.push_str(&format!(" key_fp={}", &fp[..fp.len().min(12)]));
        }
        if n("history_rows_deleted") > 0 {
            line.push_str(&format!(" history={}", n("history_rows_deleted")));
        }
        if n("tip_versions_pruned") > 0 {
            line.push_str(&format!(" tv={}", n("tip_versions_pruned")));
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
    let body_bytes =
        serde_json::to_vec(&body).map_err(|e| format!("serialize clear-history body: {e}"))?;
    let header = format!(
        "POST /api/db/clear-history HTTP/1.1\r\n\
         Host: localhost\r\n\
         {}Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n",
        client_headers(),
        body_bytes.len()
    );
    let mut req = header.into_bytes();
    req.extend_from_slice(&body_bytes);
    let response = request_with_timeout(socket, &req, admin_scan_client_timeout())?;
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

pub(crate) fn print_one_compact_plane(
    plane: &serde_json::Value,
    collection: Option<&str>,
    dry_run: bool,
) {
    let coll = plane
        .get("collection")
        .and_then(|v| v.as_str())
        .unwrap_or(collection.unwrap_or("?"));
    let live = plane
        .get("live_keys")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let before = plane
        .get("bytes_before")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let after = plane.get("bytes_after").and_then(serde_json::Value::as_u64);
    let executed = plane
        .get("executed")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let skipped = plane
        .get("skipped_reason")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    println!("compact collection={coll} dry_run={dry_run} live_keys={live} bytes_before={before}");
    // Record-byte residue: the store's own count of dead bytes (superseded
    // and deleted records plus delete markers). This is the number that says
    // whether a rewrite is worth it; filesystem slack alone does not move on
    // a delete.
    if let Some(dead) = plane.get("dead_bytes").and_then(serde_json::Value::as_u64) {
        let live_bytes = plane
            .get("live_bytes")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        let unknown = plane
            .get("residue_unknown_bytes")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        let measured = live_bytes.saturating_add(dead);
        let dead_pct = if measured == 0 {
            0.0
        } else {
            dead as f64 * 100.0 / measured as f64
        };
        println!(
            "  residue: dead_bytes={dead} live_bytes={live_bytes} dead_pct={dead_pct:.1} unmeasured_bytes={unknown}"
        );
    }
    if let Some(a) = after {
        println!("  bytes_after={a} executed={executed}");
        if dry_run {
            println!("  projected_reclaim_bytes={}", before.saturating_sub(a));
        }
    } else if !skipped.is_empty() {
        println!("  skipped: {skipped}");
    } else if dry_run {
        println!("  (dry-run — pass --execute to rewrite live keys and drop dead segs)");
    }
}

pub(crate) fn db_stamp_purged_atom_retirements(
    socket: &Path,
    execute: bool,
    json_only: bool,
) -> Result<(), String> {
    let dry_run = !execute;
    let body = serde_json::json!({ "dry_run": dry_run });
    let body_bytes = serde_json::to_vec(&body)
        .map_err(|e| format!("serialize stamp-purged-atom-retirements body: {e}"))?;
    let header = format!(
        "POST /api/db/stamp-purged-atom-retirements HTTP/1.1\r\n\
         Host: localhost\r\n\
         {}Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n",
        client_headers(),
        body_bytes.len()
    );
    let mut req = header.into_bytes();
    req.extend_from_slice(&body_bytes);
    let response = request_with_timeout(socket, &req, admin_scan_client_timeout())?;
    let value = parse_json_response(&response, "stamp-purged-atom-retirements")?;
    let report = value
        .get("stamp_purged_atom_retirements")
        .cloned()
        .ok_or_else(|| "response missing stamp_purged_atom_retirements field".to_string())?;
    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|e| format!("serialize report: {e}"))?
        );
        return Ok(());
    }
    println!("stamp-purged-atom-retirements");
    println!(
        "  dry_run={}",
        report
            .get("dry_run")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(dry_run)
    );
    println!(
        "  scope_ok={}",
        report
            .get("scope_ok")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
    );
    println!(
        "  retired_sha_count={}",
        report
            .get("retired_sha_count")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
    );
    println!(
        "  retired_bytes={}",
        report
            .get("retired_bytes")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
    );
    println!(
        "  groups_without_local_file={}",
        report
            .get("groups_without_local_file")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
    );
    println!(
        "  unstamped_successor_history_refs={}",
        report
            .get("unstamped_successor_history_refs")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
    );
    println!(
        "  pending_after={}",
        report
            .get("pending_after")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
    );
    if dry_run {
        println!("  (dry-run — pass --execute to write pending_shas; no sidecar write)");
    }
    Ok(())
}

// lint:fn-size-ok moved verbatim from db_cmds.rs; splitting these functions is separate work.
pub(crate) fn db_compact(
    socket: &Path,
    collection: Option<&str>,
    all: bool,
    execute: bool,
    json_only: bool,
) -> Result<(), String> {
    if all && collection.is_some() {
        return Err("pass --collection or --all, not both".to_string());
    }
    let dry_run = !execute;
    let body = if let Some(collection) = collection {
        serde_json::json!({
            "collection": collection,
            "dry_run": dry_run,
        })
    } else {
        serde_json::json!({
            "all": true,
            "dry_run": dry_run,
        })
    };
    let body_bytes =
        serde_json::to_vec(&body).map_err(|e| format!("serialize compact body: {e}"))?;
    let header = format!(
        "POST /api/db/compact HTTP/1.1\r\n\
         Host: localhost\r\n\
         {}Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n",
        client_headers(),
        body_bytes.len()
    );
    let mut req = header.into_bytes();
    req.extend_from_slice(&body_bytes);
    let response = request_with_timeout(socket, &req, admin_scan_client_timeout())?;
    let value = parse_json_response(&response, "compact")?;
    let report = value
        .get("compact")
        .cloned()
        .ok_or_else(|| "response missing compact field".to_string())?;

    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|e| format!("serialize report: {e}"))?
        );
        return Ok(());
    }

    if let Some(reports) = report.get("reports").and_then(|v| v.as_array()) {
        for plane in reports {
            print_one_compact_plane(plane, collection, dry_run);
        }
    } else {
        print_one_compact_plane(&report, collection, dry_run);
    }
    print_named_only_note(&report);
    let skipped_cut = report
        .get("skipped_backup_cut")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
        || report
            .get("reports")
            .and_then(|v| v.as_array())
            .is_some_and(|reports| {
                reports.iter().any(|plane| {
                    plane
                        .get("skipped_backup_cut")
                        .and_then(serde_json::Value::as_bool)
                        .unwrap_or(false)
                })
            });
    if skipped_cut {
        println!("Compact skipped: a backup cut is held.");
    }
    if let Some(cloud) = report.get("cloud") {
        let paused = cloud
            .get("paused")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        let restored = cloud
            .get("restored")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        let was_on = cloud
            .get("was_enabled")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        let needed = cloud
            .get("isolation_needed")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        if let Some(err) = cloud
            .get("restore_error")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
        {
            println!("  cloud: RESTORE FAILED — {err}");
        } else if paused && restored {
            println!("  cloud: paused for rewrite, then restored (was on)");
        } else if needed && !was_on && !dry_run {
            println!("  cloud: already off/unset — left as-is");
        }
    }
    Ok(())
}

use super::*;

pub(crate) fn db_purge_ref_blobs(
    socket: &Path,
    execute: bool,
    json_only: bool,
) -> Result<(), String> {
    let value = db_post_json(
        socket,
        "/api/db/purge-ref-blobs",
        &serde_json::json!({ "dry_run": !execute }),
    )?;
    let report = value
        .get("purge_ref_blobs")
        .cloned()
        .ok_or_else(|| "response missing purge_ref_blobs".to_string())?;
    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|e| format!("serialize: {e}"))?
        );
        return Ok(());
    }
    let mode = if execute { "EXECUTED" } else { "DRY RUN" };
    println!("Legacy ref: blob purge (legacy_blob_refs) — {mode}");
    println!(
        "  collection:    {}",
        report
            .get("collection")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("legacy_blob_refs")
    );
    for key in ["keys_found", "keys_safe", "keys_blocked", "keys_deleted"] {
        println!(
            "  {key}: {}",
            report
                .get(key)
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0)
        );
    }
    let bytes = report
        .get("bytes_found_approx")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    println!("  bytes_approx:  {}", format_bytes(bytes));
    let safe_b = report
        .get("bytes_safe_approx")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let blocked_b = report
        .get("bytes_blocked_approx")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    println!("  bytes_safe:    {}", format_bytes(safe_b));
    println!("  bytes_blocked: {}", format_bytes(blocked_b));
    let complete = report
        .get("purge_complete")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    println!("  purge_complete: {complete}");
    if let Some(samples) = report.get("blocked_samples").and_then(|v| v.as_array()) {
        if !samples.is_empty() {
            println!("  blocked_samples:");
            for s in samples.iter().take(16) {
                if let Some(k) = s.as_str() {
                    println!("    - {k}");
                }
            }
        }
    }
    if !execute {
        println!();
        println!(
            "Re-run with --execute to delete only safe (per-key rehydrated) ref: blobs; blocked keys stay."
        );
    } else if !complete {
        println!();
        println!(
            "Blocked residue remains — do not force-delete; rehydrate from tips/atoms or file a follow-up."
        );
    }
    Ok(())
}

pub(crate) fn db_migrate_photo_blobs(
    socket: &Path,
    execute: bool,
    json_only: bool,
) -> Result<(), String> {
    let value = db_post_json(
        socket,
        "/api/db/migrate-photo-blobs",
        &serde_json::json!({ "dry_run": !execute }),
    )?;
    let report = value
        .get("migrate_photo_blobs")
        .cloned()
        .ok_or_else(|| "response missing migrate_photo_blobs".to_string())?;
    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|e| format!("serialize: {e}"))?
        );
        return Ok(());
    }
    let mode = if execute { "EXECUTED" } else { "DRY RUN" };
    println!("Photo file_bytes → CAS blobs — {mode}");
    for key in [
        "photos_seen",
        "photos_migrated",
        "photos_skipped",
        "photos_failed",
    ] {
        println!(
            "  {key}: {}",
            report
                .get(key)
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0)
        );
    }
    let bytes = report
        .get("bytes_to_cas")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    println!("  bytes_to_cas: {}", format_bytes(bytes));
    if let Some(errs) = report.get("errors").and_then(|v| v.as_array()) {
        for e in errs.iter().take(10) {
            if let Some(s) = e.as_str() {
                println!("  error: {s}");
            }
        }
    }
    if !execute {
        println!();
        println!("Re-run with --execute, then: lastdb db gc-atoms --execute");
    }
    Ok(())
}

pub(crate) struct ResealAtRestCliOpts<'a> {
    pub(crate) collection: &'a str,
    pub(crate) target: Option<ResealAtRestTargetArg>,
    pub(crate) execute: bool,
    pub(crate) max_rows: Option<usize>,
    pub(crate) max_secs: Option<u64>,
    pub(crate) progress_only: bool,
    pub(crate) restart: bool,
    pub(crate) json_only: bool,
    pub(crate) out: Option<&'a Path>,
    pub(crate) timeout_secs: Option<u64>,
}

pub(crate) fn db_reseal_at_rest(
    socket: &Path,
    opts: &ResealAtRestCliOpts<'_>,
) -> Result<(), String> {
    let collection = opts.collection;
    let target = opts.target;
    let execute = opts.execute;
    let max_rows = opts.max_rows;
    let max_secs = opts.max_secs;
    let progress_only = opts.progress_only;
    let restart = opts.restart;
    let json_only = opts.json_only;
    let out = opts.out;
    let timeout_secs = opts.timeout_secs;
    let deadline = inventory_client_timeout(timeout_secs);
    if !json_only {
        eprintln!(
            "lastdb db reseal-at-rest: heavy plane walk (client deadline {}s; \
             raise with --timeout or LASTDB_UDS_ADMIN_TIMEOUT_SECS). \
             This is not a cheap status gauge. Do not run as a routine \
             against the live primary.",
            deadline.as_secs().max(1)
        );
    }
    let mut body = serde_json::json!({
        "collection": collection,
        "dry_run": !execute,
        "progress_only": progress_only,
        "restart": restart,
    });
    if let Some(target) = target {
        body["target"] = serde_json::json!(target.as_str());
    }
    if let Some(max_rows) = max_rows {
        body["max_rows"] = serde_json::json!(max_rows);
    }
    if let Some(max_secs) = max_secs {
        body["max_secs"] = serde_json::json!(max_secs);
    }
    let body_bytes =
        serde_json::to_vec(&body).map_err(|e| format!("serialize reseal-at-rest body: {e}"))?;
    let header = format!(
        "POST /api/db/reseal-at-rest HTTP/1.1\r\n\
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
    let response = request_with_timeout(socket, &req, deadline)?;
    let value = parse_json_response(&response, "reseal-at-rest")?;
    let report = value
        .get("reseal_at_rest")
        .cloned()
        .ok_or_else(|| "response missing reseal_at_rest field".to_string())?;

    let pretty =
        serde_json::to_string_pretty(&report).map_err(|e| format!("serialize report: {e}"))?;
    if let Some(path) = out {
        write_product_file_atomic(path, pretty.as_bytes())?;
        if !json_only {
            eprintln!("Wrote {}", path.display());
        }
    }
    if json_only {
        println!("{pretty}");
        return Ok(());
    }
    print_reseal_at_rest_human(&report);
    Ok(())
}

pub(crate) fn print_reseal_at_rest_human(report: &serde_json::Value) {
    let collection = report
        .get("collection")
        .and_then(|v| v.as_str())
        .unwrap_or("?");
    let measured = report
        .get("measured_at")
        .and_then(|v| v.as_str())
        .unwrap_or("?");
    let dry_run = report
        .get("dry_run")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(true);
    let target = report.get("target").and_then(|v| v.as_str()).unwrap_or("?");
    let format_version = report
        .get("format_version")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    println!(
        "reseal-at-rest  collection={collection}  target={target}  \
         format_version={format_version}  measured_at={measured}  heavy=true"
    );
    if dry_run {
        println!("  mode: dry-run (pass --execute to rewrite)");
    } else {
        println!("  mode: execute");
    }
    if let Some(reason) = report
        .get("skipped_reason")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
    {
        println!("  skipped: {reason}");
        return;
    }
    let num = |key: &str| {
        report
            .get(key)
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
    };
    println!("  rows_scanned:         {}", num("rows_scanned"));
    println!("  rows_already_target:  {}", num("rows_already_target"));
    println!("  rows_to_convert:      {}", num("rows_to_convert"));
    println!("  rows_converted:       {}", num("rows_converted"));
    println!("  rows_cas_skipped:     {}", num("rows_cas_skipped"));
    println!("  rows_unreadable:      {}", num("rows_unreadable"));
    println!("  rows_plaintext:       {}", num("rows_plaintext"));
    println!(
        "  bytes_before -> after: {} -> {}",
        format_bytes(num("bytes_before")),
        format_bytes(num("bytes_after"))
    );
    let more = report
        .get("more_remaining")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    println!("  more_remaining: {more}");
    if let Some(cp) = report.get("checkpoint") {
        let completed = cp
            .get("completed")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        println!(
            "  checkpoint: completed={completed} scanned_total={} converted_total={}",
            cp.get("rows_scanned_total")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0),
            cp.get("rows_converted_total")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0)
        );
    }
}

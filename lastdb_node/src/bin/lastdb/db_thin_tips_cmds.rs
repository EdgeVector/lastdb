use super::*;

pub(crate) struct ReapUnsealedCliOpts<'a> {
    pub(crate) collection: &'a str,
    pub(crate) execute: bool,
    pub(crate) max_rows: Option<usize>,
    pub(crate) max_secs: Option<u64>,
    pub(crate) progress_only: bool,
    pub(crate) restart: bool,
    pub(crate) json_only: bool,
    pub(crate) out: Option<&'a Path>,
    pub(crate) timeout_secs: Option<u64>,
}

pub(crate) fn db_reap_unsealed(
    socket: &Path,
    opts: &ReapUnsealedCliOpts<'_>,
) -> Result<(), String> {
    let deadline = inventory_client_timeout(opts.timeout_secs);
    if !opts.json_only {
        eprintln!(
            "lastdb db reap-unsealed: heavy plane walk (client deadline {}s; \
             raise with --timeout or LASTDB_UDS_ADMIN_TIMEOUT_SECS). \
             Rows it removes already read as absent. On request only; \
             do not run as a routine.",
            deadline.as_secs().max(1)
        );
    }
    let mut body = serde_json::json!({
        "collection": opts.collection,
        "dry_run": !opts.execute,
        "progress_only": opts.progress_only,
        "restart": opts.restart,
    });
    if let Some(max_rows) = opts.max_rows {
        body["max_rows"] = serde_json::json!(max_rows);
    }
    if let Some(max_secs) = opts.max_secs {
        body["max_secs"] = serde_json::json!(max_secs);
    }
    let body_bytes =
        serde_json::to_vec(&body).map_err(|e| format!("serialize reap-unsealed body: {e}"))?;
    let header = format!(
        "POST /api/db/reap-unsealed HTTP/1.1\r\n\
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
    let value = parse_json_response(&response, "reap-unsealed")?;
    let report = value
        .get("reap_unsealed")
        .cloned()
        .ok_or_else(|| "response missing reap_unsealed field".to_string())?;

    let pretty =
        serde_json::to_string_pretty(&report).map_err(|e| format!("serialize report: {e}"))?;
    if let Some(path) = opts.out {
        write_product_file_atomic(path, pretty.as_bytes())?;
        if !opts.json_only {
            eprintln!("Wrote {}", path.display());
        }
    }
    if opts.json_only {
        println!("{pretty}");
        return Ok(());
    }
    print_reap_unsealed_human(&report);
    Ok(())
}

pub(crate) fn print_reap_unsealed_human(report: &serde_json::Value) {
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
    println!("reap-unsealed  collection={collection}  measured_at={measured}  heavy=true");
    if dry_run {
        println!("  mode: dry-run (pass --execute to delete)");
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
    println!("  rows_scanned:      {}", num("rows_scanned"));
    println!("  rows_sealed:       {}", num("rows_sealed"));
    println!("  rows_unsealed:     {}", num("rows_unsealed"));
    println!("  rows_removed:      {}", num("rows_removed"));
    println!("  rows_cas_skipped:  {}", num("rows_cas_skipped"));
    println!(
        "  bytes_reclaimed:   {}",
        format_bytes(num("bytes_reclaimed"))
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
            "  checkpoint: completed={completed} scanned_total={} removed_total={} reclaimed_total={}",
            cp.get("rows_scanned_total")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0),
            cp.get("rows_removed_total")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0),
            format_bytes(
                cp.get("bytes_reclaimed_total")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0)
            )
        );
    }
    if dry_run && num("rows_unsealed") > 0 {
        println!();
        println!(
            "A non-zero count on a healthy home is a signal, not a chore: check whether this \
             home was restored before its replay was sealed. Re-run with --execute to delete."
        );
    }
}

/// `mk:` records one `migrate-thin-tips` call decides.
///
/// The daemon yields on its own wall-clock budget too; this cap only keeps a
/// single call's response small and gives the operator a progress line at a
/// human interval on a multi-million-tip plane.
pub(crate) const THIN_TIP_KEYS_PER_CALL: u64 = 200_000;

// lint:fn-size-ok moved verbatim from db_cmds.rs; splitting these functions is separate work.
pub(crate) fn db_migrate_thin_tips(
    socket: &Path,
    execute: bool,
    json_only: bool,
) -> Result<(), String> {
    // Follow the daemon's cursor to the end. One call is bounded by design, so
    // the totals an operator can act on only exist if the client accumulates
    // them across passes.
    let mut totals = serde_json::Map::new();
    let mut after_key: Option<String> = None;
    let mut passes = 0_u64;
    loop {
        let mut body = serde_json::json!({
            "dry_run": !execute,
            "max_keys": THIN_TIP_KEYS_PER_CALL,
        });
        if let Some(cursor) = &after_key {
            body["after_key"] = serde_json::Value::String(cursor.clone());
        }
        let value = db_post_json(socket, "/api/db/migrate-thin-tips", &body)?;
        let report = value
            .get("migrate_thin_tips")
            .cloned()
            .ok_or_else(|| "response missing migrate_thin_tips".to_string())?;
        passes += 1;
        for key in [
            "tips_scanned",
            "tips_already_thin",
            "tips_rewritten",
            "tips_unreadable",
            "bytes_before_approx",
            "bytes_after_approx",
        ] {
            let add = report
                .get(key)
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0);
            let slot = totals.entry(key).or_insert_with(|| serde_json::json!(0));
            let sum = slot.as_u64().unwrap_or(0) + add;
            *slot = serde_json::json!(sum);
        }
        let more = report
            .get("more_remaining")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        let next = report
            .get("next_after_key")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned);
        if !json_only {
            let scanned = totals
                .get("tips_scanned")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0);
            let rewritten = totals
                .get("tips_rewritten")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0);
            eprintln!("  pass {passes}: {scanned} tips decided, {rewritten} fat so far");
        }
        // A cursor that did not advance would resume forever at the same key;
        // stop rather than loop, and let the counts say how far this got.
        match (more, next) {
            (true, Some(cursor)) if Some(&cursor) != after_key.as_ref() => {
                after_key = Some(cursor);
            }
            _ => break,
        }
    }
    totals.insert("dry_run".into(), serde_json::json!(!execute));
    totals.insert("passes".into(), serde_json::json!(passes));
    let report = serde_json::Value::Object(totals);
    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|e| format!("serialize: {e}"))?
        );
        return Ok(());
    }
    let mode = if execute { "EXECUTED" } else { "DRY RUN" };
    println!("Fat → thin tip migration — {mode} ({passes} pass(es))");
    for key in [
        "tips_scanned",
        "tips_already_thin",
        "tips_rewritten",
        "tips_unreadable",
    ] {
        println!(
            "  {key}: {}",
            report
                .get(key)
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0)
        );
    }
    let before = report
        .get("bytes_before_approx")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let after = report
        .get("bytes_after_approx")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    println!("  bytes_before: {}", format_bytes(before));
    println!("  bytes_after:  {}", format_bytes(after));
    println!(
        "  would return: {}",
        format_bytes(before.saturating_sub(after))
    );
    if !execute {
        println!();
        println!("Re-run with --execute to rewrite fat mk: tips in place.");
    }
    Ok(())
}

use super::*;

pub(crate) struct ReapUnsealedCliOpts<'a> {
    pub(crate) collection: &'a str,
    pub(crate) execute: bool,
    pub(crate) max_rows: Option<usize>,
    pub(crate) max_secs: Option<u64>,
    pub(crate) progress_only: bool,
    pub(crate) restart: bool,
    /// Repeat the call until the collection is done (needs `execute`).
    pub(crate) all: bool,
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
    let mut report = reap_unsealed_call(socket, opts, deadline, opts.restart)?;
    let mut calls = 1u32;
    while opts.all && reap_more_remaining(&report) {
        let scanned = reap_checkpoint_total(&report, "rows_scanned_total");
        eprintln!(
            "reap-unsealed call {calls}: scanned {scanned} rows, removed {} rows so far",
            reap_checkpoint_total(&report, "rows_removed_total")
        );
        let next = reap_unsealed_call(socket, opts, deadline, false)?;
        if reap_checkpoint_total(&next, "rows_scanned_total") <= scanned {
            return Err("reap-unsealed made no progress between two calls; stopped".to_string());
        }
        report = next;
        calls += 1;
    }
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

/// One `POST /api/db/reap-unsealed` call. Returns the report.
fn reap_unsealed_call(
    socket: &Path,
    opts: &ReapUnsealedCliOpts<'_>,
    deadline: Duration,
    restart: bool,
) -> Result<serde_json::Value, String> {
    let mut body = serde_json::json!({
        "collection": opts.collection,
        "dry_run": !opts.execute,
        "progress_only": opts.progress_only,
        "restart": restart,
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
    value
        .get("reap_unsealed")
        .cloned()
        .ok_or_else(|| "response missing reap_unsealed field".to_string())
}

fn reap_more_remaining(report: &serde_json::Value) -> bool {
    report
        .get("more_remaining")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
}

fn reap_checkpoint_total(report: &serde_json::Value, key: &str) -> u64 {
    report
        .get("checkpoint")
        .and_then(|checkpoint| checkpoint.get(key))
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0)
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

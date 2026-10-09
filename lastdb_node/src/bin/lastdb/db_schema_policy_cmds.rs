//! Log filter, schema name claim and retention commands.

use super::*;

/// `lastdb log-filter [directive]` — read or swap the daemon's tracing filter.
///
/// Read and write share one function because they share every failure mode
/// (home resolution, socket reachability, envelope shape) and differ only in
/// the HTTP method.
pub(crate) fn log_filter_command(
    data_dir: Option<PathBuf>,
    directive: Option<&str>,
    json: bool,
) -> Result<(), String> {
    let home = lastdb_node::host::resolve_home(data_dir)?;
    let socket = home.join("data").join("folddb.sock");
    let path = "/api/system/log-filter";

    let value = match directive.map(str::trim) {
        Some(d) if !d.is_empty() => {
            db_post_json(&socket, path, &serde_json::json!({ "directive": d }))?
        }
        Some(_) => return Err("log-filter: directive must be non-empty".to_string()),
        None => {
            let req = format!(
                "GET {path} HTTP/1.1\r\nHost: localhost\r\n{}Connection: close\r\n\r\n",
                client_headers()
            );
            let response =
                request_with_timeout(&socket, req.as_bytes(), admin_scan_client_timeout())?;
            parse_json_response(&response, path)?
        }
    };

    let filter = value
        .get("log_filter")
        .ok_or_else(|| "response missing log_filter field".to_string())?;
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(filter)
                .map_err(|e| format!("serialize log_filter: {e}"))?
        );
        return Ok(());
    }

    // `installed: false` is a real answer from a healthy node, not an error:
    // it says this daemon build has no runtime control. Say so plainly rather
    // than printing an empty directive.
    if filter.get("installed").and_then(serde_json::Value::as_bool) != Some(true) {
        println!("log-filter: not available on this daemon (no runtime control installed)");
        return Ok(());
    }
    let current = filter
        .get("directive")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("(unknown)");
    match filter.get("previous").and_then(serde_json::Value::as_str) {
        Some(previous) => println!("log-filter: {previous} -> {current}"),
        None => println!("log-filter: {current}"),
    }
    Ok(())
}

pub(crate) fn db_post_json(
    socket: &Path,
    path: &str,
    body: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    let body_bytes = serde_json::to_vec(body).map_err(|e| format!("serialize body: {e}"))?;
    let header = format!(
        "POST {path} HTTP/1.1\r\n\
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
    parse_json_response(&response, path)
}

pub(crate) fn schema_name_claim_command(
    data_dir: Option<PathBuf>,
    action: SchemaNameClaimCommand,
) -> Result<(), String> {
    let home = lastdb_node::host::resolve_home(data_dir)?;
    let socket = home.join("data").join("folddb.sock");
    let (schema, retired, json_only) = match action {
        SchemaNameClaimCommand::Retire { schema, json } => (schema, true, json),
        SchemaNameClaimCommand::Restore { schema, json } => (schema, false, json),
    };
    let response = db_post_json(
        &socket,
        "/api/schemas/retire-name-claim",
        &serde_json::json!({ "schema": schema, "retired": retired }),
    )?;
    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&response)
                .map_err(|error| format!("serialize name claim response: {error}"))?
        );
        return Ok(());
    }
    let changed = response
        .get("changed")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let verb = if retired { "retired" } else { "restored" };
    if changed {
        println!("Name claim {verb} for {schema}");
    } else {
        println!("Name claim for {schema} was already {verb}; nothing changed");
    }
    Ok(())
}

pub(crate) fn schema_retention_command(
    data_dir: Option<PathBuf>,
    action: SchemaRetentionCommand,
) -> Result<(), String> {
    let home = lastdb_node::host::resolve_home(data_dir)?;
    let socket = home.join("data").join("folddb.sock");
    let (action, schema, ttl_seconds, hash_partitions, json_only) = match action {
        SchemaRetentionCommand::Get { schema, json } => ("get", schema, None, Vec::new(), json),
        SchemaRetentionCommand::Set {
            schema,
            ttl,
            hash_partitions,
            json,
        } => (
            "set",
            schema,
            Some(parse_retention_duration(&ttl)?),
            hash_partitions,
            json,
        ),
        SchemaRetentionCommand::Clear { schema, json } => ("clear", schema, None, Vec::new(), json),
    };
    let response = db_post_json(
        &socket,
        "/api/db/schema-retention",
        &serde_json::json!({
            "action": action,
            "schema": schema,
            "ttl_seconds": ttl_seconds,
            "hash_partitions": hash_partitions,
        }),
    )?;
    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&response)
                .map_err(|error| format!("serialize schema retention response: {error}"))?
        );
        return Ok(());
    }
    let shown_schema = response
        .get("schema")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("<unknown>");
    match response.get("retention_policy") {
        Some(serde_json::Value::Object(policy)) => {
            let ttl_seconds = policy
                .get("ttl_seconds")
                .and_then(serde_json::Value::as_u64)
                .ok_or_else(|| "response retention_policy missing ttl_seconds".to_string())?;
            let hash_partitions = policy
                .get("hash_partitions")
                .and_then(serde_json::Value::as_array)
                .map_or(0, Vec::len);
            println!(
                "Schema retention for {shown_schema}: {ttl_seconds}s (hash partitions: {hash_partitions})"
            );
        }
        _ => println!("Schema retention for {shown_schema}: unset"),
    }
    Ok(())
}

pub(crate) fn parse_retention_duration(input: &str) -> Result<u64, String> {
    let split = input
        .find(|ch: char| !ch.is_ascii_digit())
        .unwrap_or(input.len());
    let (number, suffix) = input.split_at(split);
    if number.is_empty() || suffix.is_empty() || suffix.len() > 1 {
        return Err(format!(
            "invalid retention duration {input:?}; use a positive integer followed by s, m, h, or d"
        ));
    }
    let magnitude = number.parse::<u64>().map_err(|_| {
        format!("invalid retention duration {input:?}; duration must fit in u64 seconds")
    })?;
    let multiplier = match suffix {
        "s" => 1,
        "m" => 60,
        "h" => 60 * 60,
        "d" => 24 * 60 * 60,
        _ => {
            return Err(format!(
                "invalid retention duration {input:?}; use s, m, h, or d"
            ))
        }
    };
    magnitude
        .checked_mul(multiplier)
        .filter(|seconds| *seconds > 0)
        .ok_or_else(|| {
            format!("invalid retention duration {input:?}; duration must be greater than zero")
        })
}

pub(crate) fn print_clear_history_human(report: &serde_json::Value, dry_run: bool) {
    let mode = if dry_run {
        "DRY RUN (no deletes)"
    } else {
        "EXECUTED"
    };
    let keep = report
        .get("keep_last_per_key")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(1);
    let deleted = report
        .get("history_rows_deleted")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let freed = report
        .get("history_bytes_freed_approx")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let schemas = report
        .get("schemas_touched")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let kind = if keep == 0 {
        "full purge (latest tip only; no history kept)"
    } else {
        "trim older events"
    };
    println!("Clear mutation history — {mode} — {kind}");
    println!("  keep_last_per_key: {keep}");
    println!("  schemas_touched:   {schemas}");
    println!("  history_rows:      {deleted}");
    println!("  bytes_approx:      {}", format_bytes(freed));
    if let Some(rows) = report.get("per_schema").and_then(|v| v.as_array()) {
        if !rows.is_empty() {
            println!();
            println!("Per schema:");
            for (i, row) in rows.iter().enumerate() {
                if i >= 30 {
                    println!("  … {} more", rows.len().saturating_sub(30));
                    break;
                }
                let name = row
                    .get("schema_name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("?");
                let d = row
                    .get("history_rows_deleted")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0);
                let b = row
                    .get("history_bytes_freed_approx")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0);
                println!("  {:>10}  {:>8} rows  {}", format_bytes(b), d, name);
            }
        }
    }
    if dry_run {
        println!();
        if keep == 0 {
            println!("Re-run with --keep-last 0 --execute to purge ALL history rows.");
        } else {
            println!("Re-run with --execute to delete these history rows.");
        }
        println!("Atoms and current tips (mk:) are kept; freelist reclaim may need later compact.");
    } else {
        println!();
        println!(
            "History rows deleted. On-disk freelist may still hold space until compact/rebuild."
        );
    }
}

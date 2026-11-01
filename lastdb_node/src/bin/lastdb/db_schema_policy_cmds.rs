//! Log filter, schema name claim and retention commands.

use super::*;

#[path = "db_schema_policy_cmds/retention_backfill.rs"]
mod retention_backfill;

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
            let response = get_path(&socket, path, admin_scan_client_timeout())?;
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
    let response = post_json_admin(socket, path, body)?;
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
    let mut confirm = false;
    let (action, schema, ttl_seconds, hash_partitions, json_only) = match action {
        SchemaRetentionCommand::Backfill {
            schema,
            page_size,
            confirm_expire_existing,
            json,
        } => {
            return retention_backfill::run(
                &socket,
                &schema,
                page_size,
                confirm_expire_existing,
                json,
            );
        }
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
        SchemaRetentionCommand::Clear {
            schema,
            confirm_expire_existing,
            json,
        } => {
            confirm = confirm_expire_existing;
            ("clear", schema, None, Vec::new(), json)
        }
    };
    let response = db_post_json(
        &socket,
        "/api/db/schema-retention",
        &serde_json::json!({
            "action": action,
            "schema": schema,
            "ttl_seconds": ttl_seconds,
            "hash_partitions": hash_partitions,
            "confirm_expire_existing": confirm,
        }),
    )?;
    print_retention_policy(&response, json_only)
}

fn print_retention_policy(response: &serde_json::Value, json_only: bool) -> Result<(), String> {
    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(response)
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
            let source = policy
                .get("source")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("operator");
            println!(
                "Schema retention for {shown_schema}: {ttl_seconds}s source={source} (hash partitions: {hash_partitions})"
            );
            if let Some(manifest_ttl) = policy
                .get("overridden_manifest_ttl_seconds")
                .and_then(serde_json::Value::as_u64)
            {
                println!(
                    "  the operator policy overrides a manifest policy of {manifest_ttl}s; `clear` puts it back"
                );
            }
            if response
                .get("existing_records_may_expire")
                .and_then(serde_json::Value::as_bool)
                == Some(true)
            {
                println!(
                    "  WARNING: existing_records_may_expire: the shorter manifest TTL is in force; records older than {ttl_seconds}s are deleted for good"
                );
            }
            match response.get("anchor").and_then(serde_json::Value::as_str) {
                Some(anchor) if anchor != shown_schema => {
                    println!(
                        "  policy state is held under {anchor}, shared by both installed names"
                    );
                }
                _ => {}
            }
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

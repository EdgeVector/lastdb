//! Hash-range key field repair command.

use super::*;

pub(crate) fn db_repair_hashrange_key_fields(
    socket: &Path,
    schema: &str,
    hash: &str,
    execute: bool,
    json_only: bool,
) -> Result<(), String> {
    let response = db_post_json(
        socket,
        "/api/db/repair-hashrange-key-fields",
        &serde_json::json!({
            "schema": schema,
            "api_hash": hash,
            "execute": execute,
        }),
    )?;
    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&response)
                .map_err(|error| format!("serialize repair report: {error}"))?
        );
        return Ok(());
    }
    println!(
        "HashRange key-field repair: schema={} hash={} mode={}",
        response["schema"].as_str().unwrap_or(schema),
        response["api_hash"].as_str().unwrap_or(hash),
        if response["dry_run"].as_bool().unwrap_or(!execute) {
            "dry-run"
        } else {
            "execute"
        }
    );
    println!(
        "  before: members={} hash_field={} range_field={}",
        response["member_rows_before"].as_u64().unwrap_or(0),
        response["hash_rows_before"].as_u64().unwrap_or(0),
        response["range_rows_before"].as_u64().unwrap_or(0)
    );
    println!(
        "  planned={} written={} after: members={} hash_field={} range_field={}",
        response["rows_planned"].as_u64().unwrap_or(0),
        response["rows_written"].as_u64().unwrap_or(0),
        response["member_rows_after"].as_u64().unwrap_or(0),
        response["hash_rows_after"].as_u64().unwrap_or(0),
        response["range_rows_after"].as_u64().unwrap_or(0)
    );
    Ok(())
}

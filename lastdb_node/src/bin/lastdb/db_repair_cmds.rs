//! Hash-range key field and schema molecule map repair commands.

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

pub(crate) fn db_repair_schema_molecule_map(
    socket: &Path,
    schema: &str,
    map_file: Option<&Path>,
    write_map_file: Option<&Path>,
    execute: bool,
    expected_current_fingerprint: Option<&str>,
    json_only: bool,
) -> Result<(), String> {
    // lint:fn-size-ok moved verbatim from the original file; splitting is a separate change
    if map_file.is_none() && execute {
        return Err("--execute requires --map-file".to_string());
    }
    let mut field_molecule_uuids: Option<std::collections::HashMap<String, String>> = None;
    if let Some(map_file) = map_file {
        let raw = std::fs::read(map_file)
            .map_err(|error| format!("read molecule map {}: {error}", map_file.display()))?;
        let parsed: std::collections::HashMap<String, String> = serde_json::from_slice(&raw)
            .map_err(|error| format!("parse molecule map {}: {error}", map_file.display()))?;
        if parsed.is_empty() {
            return Err("molecule map must not be empty".to_string());
        }
        field_molecule_uuids = Some(parsed);
    }
    let inspect_only = field_molecule_uuids.is_none();
    let response = db_post_json(
        socket,
        "/api/db/repair-schema-molecule-map",
        &serde_json::json!({
            "schema": schema,
            "field_molecule_uuids": field_molecule_uuids,
            "execute": execute,
            "expected_current_fingerprint": expected_current_fingerprint,
        }),
    )?;
    if let Some(path) = write_map_file {
        let suggested = response
            .get("suggested_field_molecule_uuids")
            .filter(|value| !value.is_null())
            .ok_or_else(|| {
                "no candidate map has live rows, so there is nothing to write".to_string()
            })?;
        let body = serde_json::to_string_pretty(suggested)
            .map_err(|error| format!("serialize suggested molecule map: {error}"))?;
        std::fs::write(path, format!("{body}\n"))
            .map_err(|error| format!("write molecule map {}: {error}", path.display()))?;
        if !json_only {
            println!("Wrote suggested molecule map to {}", path.display());
        }
    }
    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&response)
                .map_err(|error| format!("serialize molecule-map repair response: {error}"))?
        );
        return Ok(());
    }

    let shown_schema = response
        .get("schema")
        .and_then(serde_json::Value::as_str)
        .unwrap_or(schema);
    let changed = response
        .get("changed")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let changed_count = response
        .get("changed_fields")
        .and_then(serde_json::Value::as_array)
        .map_or(0, Vec::len);
    let current = response
        .get("current_fingerprint")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("<missing>");
    let proposed = response
        .get("proposed_fingerprint")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("<missing>");
    let proposed_has_rows = response
        .get("proposed_key_has_live_rows")
        .and_then(serde_json::Value::as_bool);
    let mode = if execute {
        "repair"
    } else if inspect_only {
        "inspect"
    } else {
        "dry run"
    };
    println!(
        "Schema molecule-map {mode} for {shown_schema}: changed={changed} fields={changed_count}"
    );
    println!("  current fingerprint:  {current}");
    if !inspect_only {
        println!("  proposed fingerprint: {proposed}");
        match proposed_has_rows {
            Some(value) => println!("  proposed key has live rows: {value}"),
            None => println!("  proposed key has live rows: unknown"),
        }
    }
    if inspect_only {
        print_schema_molecule_map_inspection(&response);
    }
    if !execute && changed {
        println!("  next: rerun with --execute --expected-current-fingerprint {current}");
    }
    Ok(())
}

/// Print the inspect-mode report: the installed map and the candidate maps.
pub(crate) fn print_schema_molecule_map_inspection(response: &serde_json::Value) {
    if let Some(field) = response
        .get("key_field")
        .and_then(serde_json::Value::as_str)
    {
        let live = match response
            .get("current_key_has_live_rows")
            .and_then(serde_json::Value::as_bool)
        {
            Some(value) => value.to_string(),
            None => "unknown".to_string(),
        };
        println!("  key field: {field} (current key molecule has live rows: {live})");
    }
    if let Some(fields) = response.get("fields").and_then(serde_json::Value::as_array) {
        println!("  installed field map:");
        for entry in fields {
            let name = entry
                .get("field")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("<unnamed>");
            let molecule = entry
                .get("molecule")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("<unmapped>");
            let live = match entry
                .get("has_live_rows")
                .and_then(serde_json::Value::as_bool)
            {
                Some(value) => value.to_string(),
                None => "unknown".to_string(),
            };
            println!("    {name}: {molecule} (live rows: {live})");
        }
    }
    let candidates = response
        .get("candidates")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    if candidates.is_empty() {
        println!("  candidates: none — no other installed schema maps every field of this one");
    } else {
        println!("  candidates:");
        for candidate in &candidates {
            let name = candidate
                .get("schema")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("<unnamed>");
            let key_molecule = candidate
                .get("key_molecule")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("<unmapped>");
            let live = match candidate
                .get("key_has_live_rows")
                .and_then(serde_json::Value::as_bool)
            {
                Some(value) => value.to_string(),
                None => "unknown".to_string(),
            };
            println!("    {name}: key molecule {key_molecule} (live rows: {live})");
        }
    }
    if response
        .get("suggested_field_molecule_uuids")
        .is_some_and(|value| !value.is_null())
    {
        println!("  a candidate holds live rows: rerun with --write-map-file <path> to save it");
    }
}

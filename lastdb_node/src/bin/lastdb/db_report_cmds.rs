//! Schema storage reports.

use super::*;

pub(crate) fn db_schemas(
    socket: &Path,
    json_only: bool,
    out: Option<&Path>,
    timeout_secs: Option<u64>,
    limit: usize,
) -> Result<(), String> {
    let deadline = inventory_client_timeout(timeout_secs);
    if !json_only {
        eprintln!(
            "lastdb admin storage inventory --by-schema: heavy record walk (client deadline {}s; \
             raise with --timeout or LASTDB_UDS_ADMIN_TIMEOUT_SECS). \
             This is not a cheap status gauge.",
            deadline.as_secs().max(1)
        );
    }
    let response = get_path(socket, "/api/db/schemas", deadline)?;
    let value = parse_json_response(&response, "schema storage")?;
    let report = value
        .get("schema_storage")
        .cloned()
        .ok_or_else(|| "response missing schema_storage field".to_string())?;

    let pretty = serde_json::to_string_pretty(&report)
        .map_err(|e| format!("serialize schema storage: {e}"))?;

    if let Some(path) = out.as_ref() {
        write_product_file_atomic(path, pretty.as_bytes())?;
        if !json_only {
            eprintln!("Wrote {}", path.display());
        }
    }

    if json_only {
        println!("{pretty}");
        return Ok(());
    }

    print_schema_storage_human(&report, limit);
    Ok(())
}

pub(crate) fn print_schema_storage_human(report: &serde_json::Value, limit: usize) {
    let measured = report
        .get("measured_at")
        .and_then(|v| v.as_str())
        .unwrap_or("?");
    let total_bytes = report
        .get("total_logical_bytes")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let schema_count = report
        .get("schema_count")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let total_records = report
        .get("total_records")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    println!("Per-schema record storage  measured_at={measured}  heavy=true");
    println!(
        "  {:>10}  {:>8} records  {} schemas",
        format_bytes(total_bytes),
        total_records,
        schema_count
    );
    println!();
    println!("  {:>10}  {:>8}  {:<32}  SCHEMA", "SIZE", "RECORDS", "NAME");
    let rows = report
        .get("per_schema")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let shown = if limit == 0 { rows.len() } else { limit };
    for (i, row) in rows.iter().enumerate() {
        if i >= shown {
            println!(
                "  … {} more schemas (pass --limit 0 for all)",
                rows.len() - shown
            );
            break;
        }
        let identity = row
            .get("schema_name")
            .and_then(|v| v.as_str())
            .unwrap_or("?");
        let name = row
            .get("display_name")
            .and_then(|v| v.as_str())
            .unwrap_or("-");
        let bytes = row
            .get("bytes")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        let count = row
            .get("record_count")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        println!(
            "  {:>10}  {:>8}  {:<32}  {}",
            format_bytes(bytes),
            count,
            truncate_label(name, 32),
            identity
        );
    }
}

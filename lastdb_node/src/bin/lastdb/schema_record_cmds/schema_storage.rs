use super::*;

pub(crate) fn schema_storage_command(
    data_dir: Option<PathBuf>,
    schema: &str,
    json_only: bool,
) -> Result<(), String> {
    let (_, socket) = resolve_client_home_and_socket(data_dir)?;
    let response = post_json(
        &socket,
        "/api/storage/schema",
        &serde_json::json!({ "schema": schema }),
    )?;
    let value = parse_json_response(&response, "schema storage")?;
    let report = value
        .get("schema_storage")
        .cloned()
        .ok_or_else(|| "response missing schema_storage field".to_string())?;
    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|error| format!("serialize: {error}"))?
        );
        return Ok(());
    }
    let schema_binding = report
        .get("schema_binding")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("?");
    let current = report
        .get("schema_current_bytes")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let complete = report
        .get("complete")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    println!("Schema logical current storage: {schema_binding}");
    println!("  {}  complete={complete}", format_bytes(current));
    println!("  Metric: logical_current_schema_bytes (shared data can appear in more than one schema total)");
    if !complete {
        println!(
            "  Counter bootstrap or replay is incomplete; this value is a safe partial result."
        );
    }
    Ok(())
}

pub(crate) fn schema_storage_report_command(
    data_dir: Option<PathBuf>,
    json_only: bool,
) -> Result<(), String> {
    let (_, socket) = resolve_client_home_and_socket(data_dir)?;
    let response = get_path(
        &socket,
        "/api/storage/schemas",
        lastdb_node::exec::admin_handler_timeout(),
    )?;
    let value = parse_json_response(&response, "schema storage report")?;
    let report = value
        .get("schema_storage_report")
        .cloned()
        .ok_or_else(|| "response missing schema_storage_report field".to_string())?;
    let pretty = serde_json::to_string_pretty(&report)
        .map_err(|error| format!("serialize schema storage report: {error}"))?;
    if json_only {
        println!("{pretty}");
        return Ok(());
    }
    print_schema_storage_report_human(&report);
    Ok(())
}

pub(crate) fn print_schema_storage_report_human(report: &serde_json::Value) {
    let complete = report
        .get("complete")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let total = report
        .get("total_bytes")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let schema_count = report
        .get("schema_count")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    println!(
        "Labelled logical schema storage  {}  schemas  complete={complete}",
        format_bytes(total)
    );
    println!();
    println!("  {:>12}  {:<32}  LABEL", "BYTES", "SCHEMA");
    if let Some(rows) = report
        .get("per_schema")
        .and_then(serde_json::Value::as_array)
    {
        for row in rows {
            let bytes = row
                .get("bytes")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0);
            let binding = row
                .get("schema_binding")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("?");
            let label = row
                .get("label")
                .and_then(serde_json::Value::as_str)
                .unwrap_or(binding);
            println!("  {:>12}  {:<32}  {label}", format_bytes(bytes), binding);
        }
    }
    if !complete {
        let reason = report
            .get("incomplete_reason")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("stored counters are incomplete");
        println!("  CAUTION: {reason}");
    }
    if schema_count == 0 {
        println!("  (no installed schemas)");
    }
}

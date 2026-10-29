use super::*;

pub(crate) fn schema_show_command(
    data_dir: Option<PathBuf>,
    name: &str,
    json_only: bool,
) -> Result<(), String> {
    let schema = fetch_schema(data_dir, name)?;
    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&schema).map_err(|e| format!("serialize: {e}"))?
        );
        return Ok(());
    }
    print_schema_show_human(&schema);
    Ok(())
}

pub(crate) fn fetch_schema(
    data_dir: Option<PathBuf>,
    name: &str,
) -> Result<serde_json::Value, String> {
    let (_, socket) = resolve_client_home_and_socket(data_dir)?;
    if lastdb_node::health_alert::probe_health(&socket).is_err() {
        return Err(format!(
            "lastdbd not reachable at {} — start the daemon first",
            socket.display()
        ));
    }
    let path = format!("/api/schema/{}", percent_encode_query(name));
    let response = get_path(&socket, &path, Duration::from_secs(10))?;
    let value = parse_json_response(&response, "schema")?;
    value
        .get("schema")
        .cloned()
        .ok_or_else(|| "response missing schema field".to_string())
}

pub(crate) fn print_schema_show_human(schema: &serde_json::Value) {
    for line in schema_show_lines(schema) {
        println!("{line}");
    }
}

pub(crate) fn schema_show_lines(schema: &serde_json::Value) -> Vec<String> {
    let name = schema
        .get("name")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("?");
    let descriptive = schema
        .get("descriptive_name")
        .and_then(serde_json::Value::as_str);
    let identity = schema
        .get("identity_hash")
        .and_then(serde_json::Value::as_str);
    let owner = schema
        .get("owner_app_id")
        .and_then(serde_json::Value::as_str);
    let schema_type = schema
        .get("schema_type")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("?");
    let state = schema
        .get("state")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("?");
    let hash_field = schema
        .pointer("/key/hash_field")
        .and_then(serde_json::Value::as_str)
        .filter(|s| !s.is_empty());
    let range_field = schema
        .pointer("/key/range_field")
        .and_then(serde_json::Value::as_str)
        .filter(|s| !s.is_empty());
    let fields = schema
        .get("fields")
        .and_then(serde_json::Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(serde_json::Value::as_str)
                .collect::<Vec<_>>()
                .join(", ")
        })
        .unwrap_or_default();

    let mut lines = Vec::new();
    lines.push(format!("schema {name}"));
    if let Some(d) = descriptive {
        lines.push(format!("descriptive_name {d}"));
    }
    if let Some(id) = identity {
        lines.push(format!("identity_hash {id}"));
    }
    if let Some(app) = owner {
        lines.push(format!("owner_app {app}"));
    }
    lines.push(format!("type {schema_type}"));
    lines.push(format!("state {state}"));
    match (hash_field, range_field) {
        (Some(h), Some(r)) => {
            lines.push(format!("hash_field {h}"));
            lines.push(format!("range_field {r}"));
        }
        (Some(h), None) => lines.push(format!("hash_field {h}")),
        (None, Some(r)) => lines.push(format!("range_field {r}")),
        (None, None) => lines.push("key_fields (none)".to_string()),
    }
    if fields.is_empty() {
        lines.push("fields (none)".to_string());
    } else {
        lines.push(format!("fields {fields}"));
    }
    lines.push("queries:".to_string());
    for q in legal_query_shapes(schema_type, hash_field, range_field) {
        lines.push(format!("  {q}"));
    }
    lines
}

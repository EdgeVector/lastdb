use super::*;

pub(super) fn legal_query_shapes(
    schema_type: &str,
    hash_field: Option<&str>,
    range_field: Option<&str>,
) -> Vec<String> {
    let hash = hash_field.unwrap_or("HASH");
    let range = range_field.unwrap_or("RANGE");
    match schema_type {
        "HashRange" => vec![
            format!(
                "point get O(1): lastdb get <schema> --key-hash <{hash}> --key-range <{range}>"
            ),
            format!("range under one hash O(log M): lastdb get-keys <schema> --key-hash <{hash}>"),
            "scan does not exist".to_string(),
        ],
        "Hash" => vec![
            format!("point get O(1): lastdb get <schema> --key-hash <{hash}>"),
            "keys page (not a census): lastdb get-keys <schema>".to_string(),
            "scan does not exist".to_string(),
        ],
        "Range" => vec![
            format!("point get O(1): lastdb get <schema> --key-hash <{range}>"),
            "keys page (not a census): lastdb get-keys <schema>".to_string(),
            "scan does not exist".to_string(),
        ],
        _ => vec![
            "point get: lastdb get <schema> --key-hash <KEY>".to_string(),
            "keys page (not a census): lastdb get-keys <schema>".to_string(),
            "scan does not exist".to_string(),
        ],
    }
}

pub(super) fn schema_is_hash_range(schema: &serde_json::Value) -> bool {
    let typed = schema
        .get("schema_type")
        .and_then(serde_json::Value::as_str)
        == Some("HashRange");
    let both_key_halves = schema
        .pointer("/key/range_field")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|s| !s.is_empty())
        && schema
            .pointer("/key/hash_field")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|s| !s.is_empty());
    typed || both_key_halves
}

pub(super) fn schema_field_names(schema: &serde_json::Value) -> Vec<String> {
    schema
        .get("fields")
        .and_then(serde_json::Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(serde_json::Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

pub(super) fn get_query_body(
    schema: &str,
    fields: &[String],
    key_hash: &str,
    key_range: Option<&str>,
) -> serde_json::Value {
    let filter = match key_range.filter(|s| !s.is_empty()) {
        Some(range) => serde_json::json!({
            "HashRangeKey": { "hash": key_hash, "range": range }
        }),
        None => serde_json::json!({ "HashKey": key_hash }),
    };
    serde_json::json!({
        "schema_name": schema,
        "fields": fields,
        "filter": filter,
    })
}

pub(super) fn compact_record_command(
    data_dir: Option<PathBuf>,
    schema: &str,
    key_hash: &str,
    key_range: &str,
    json_only: bool,
) -> Result<(), String> {
    if schema.trim().is_empty() || key_hash.trim().is_empty() || key_range.trim().is_empty() {
        return Err("compact-record needs <schema> --key-hash --key-range".into());
    }
    let (_, socket) = resolve_client_home_and_socket(data_dir)?;
    let body = serde_json::json!({
        "schema": schema.trim(),
        "hash": key_hash.trim(),
        "range": key_range.trim(),
    });
    let response = post_json_admin(&socket, "/api/db/compact-record", &body)?;
    let value = parse_json_response(&response, "compact-record")?;
    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&value).map_err(|e| format!("serialize: {e}"))?
        );
        return Ok(());
    }
    let keys = value
        .get("keys_compacted")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let r = value
        .get("record_molecule_uuid")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("");
    let skipped = value
        .get("keys_skipped")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    println!("compact-record keys_compacted={keys} keys_skipped={skipped} r={r}");
    if skipped > 0 {
        eprintln!(
            "compact-record: {skipped} key(s) skipped: the field zip was empty or missed a live \
             field tip, so no envelope was stamped. Retry after pending writes persist."
        );
    }
    Ok(())
}

pub(super) fn get_record(
    data_dir: Option<PathBuf>,
    schema_name: &str,
    key_hash: Option<&str>,
    key_range: Option<&str>,
    json_only: bool,
) -> Result<(), String> {
    let Some(hash) = key_hash.map(str::trim).filter(|s| !s.is_empty()) else {
        return Err("get needs --key-hash (and --key-range when the schema is HashRange)".into());
    };
    let schema = fetch_schema(data_dir.clone(), schema_name)?;
    if schema_is_hash_range(&schema) && key_range.map(str::trim).filter(|s| !s.is_empty()).is_none()
    {
        return Err("this schema is HashRange; pass --key-range or use get-keys --key-hash".into());
    }
    let fields = schema_field_names(&schema);
    let body = get_query_body(schema_name, &fields, hash, key_range);
    let (_, socket) = resolve_client_home_and_socket(data_dir)?;
    let response = post_json_with_timeout(
        &socket,
        "/api/query",
        &body,
        Duration::from_secs(lastdb_node::exec::DEFAULT_CLI_UDS_TIMEOUT_SECS),
    )?;
    let value = parse_json_response(&response, "query")?;
    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&value).map_err(|e| format!("serialize: {e}"))?
        );
        return Ok(());
    }
    print_get_human(&value);
    Ok(())
}

pub(super) fn print_get_human(value: &serde_json::Value) {
    let results = value
        .get("results")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    if results.is_empty() {
        eprintln!("lastdb get: no row");
        return;
    }
    for (i, row) in results.iter().enumerate() {
        if results.len() > 1 {
            eprintln!("--- row {} ---", i + 1);
        }
        if let Some(key) = row.get("key") {
            match key {
                serde_json::Value::String(s) => println!("key {s}"),
                serde_json::Value::Object(map) => {
                    let hash = map
                        .get("hash")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("");
                    let range = map
                        .get("range")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("");
                    if range.is_empty() {
                        println!("key {hash}");
                    } else {
                        println!("key {hash}\t{range}");
                    }
                }
                _ => {}
            }
        }
        if let Some(fields) = row.get("fields").and_then(serde_json::Value::as_object) {
            let mut names: Vec<&String> = fields.keys().collect();
            names.sort();
            for name in names {
                let rendered = match fields.get(name) {
                    Some(serde_json::Value::String(s)) => s.clone(),
                    Some(other) => other.to_string(),
                    None => String::new(),
                };
                println!("{name} {rendered}");
            }
        }
    }
}

pub(super) fn get_path(socket: &Path, path: &str, timeout: Duration) -> Result<String, String> {
    let req = format!(
        "GET {path} HTTP/1.1\r\nHost: localhost\r\n{}Connection: close\r\n\r\n",
        client_headers()
    );
    request_with_timeout(socket, req.as_bytes(), timeout)
}

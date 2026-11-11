use super::*;

pub(crate) fn percent_encode_query(value: &str) -> String {
    let mut out = String::new();
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char);
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

pub(crate) fn list_record_keys(
    target: &DatabaseTarget,
    args: &crate::cli_defs::RecordListArgs,
) -> Result<(), String> {
    let socket = &target.socket;
    if probe_health(socket).is_err() {
        return Err(format!(
            "lastdbd not reachable at {} — start the daemon first",
            socket.display()
        ));
    }
    let mut path = format!(
        "/api/list?schema={}&limit={}",
        percent_encode_query(&args.schema),
        args.limit.max(1)
    );
    if let Some(name) = args.key_name.as_deref() {
        path.push_str("&key_name=");
        path.push_str(&percent_encode_query(name));
    }
    if let Some(hash) = args.key_hash.as_deref() {
        path.push_str("&hash=");
        path.push_str(&percent_encode_query(hash));
    }
    if let Some(cursor) = args.cursor.as_deref() {
        path.push_str("&cursor=");
        path.push_str(&percent_encode_query(cursor));
    }
    let response = get_path(
        socket,
        &target.path(&path),
        Duration::from_secs(lastdb_node::exec::DEFAULT_CLI_UDS_TIMEOUT_SECS),
    )?;
    let value = parse_json_response(&response, "list")?;
    let report = value
        .get("list")
        .cloned()
        .ok_or_else(|| "response missing list field".to_string())?;
    if let Some(name) = args.key_name.as_deref() {
        if report["key_name"].as_str() != Some(name) {
            return Err("node did not honor the requested alternate key; upgrade LastDB".into());
        }
        let keys = report["keys"].as_array();
        let primary = report["primary_keys"].as_array();
        if keys.is_none() || keys.map(Vec::len) != primary.map(Vec::len) {
            return Err("alternate list lost its primary mutation keys".into());
        }
    }
    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|e| format!("serialize: {e}"))?
        );
        return Ok(());
    }
    print_list_record_keys_human(&report);
    Ok(())
}

fn print_list_record_keys_human(report: &serde_json::Value) {
    let keys = report
        .get("keys")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    let has_more = report
        .get("has_more")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let schema = report
        .get("schema")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("?");
    let ranged = report
        .pointer("/key_definition/kind")
        .and_then(serde_json::Value::as_str)
        == Some("hash_range");
    let hash_filter = report
        .get("hash_filter")
        .and_then(serde_json::Value::as_str);
    let layout = if ranged { " hash+range" } else { " hash" };
    let partition = hash_filter
        .map(|h| format!(" --key-hash {h:?}"))
        .unwrap_or_default();
    let key_flag = report
        .get("key_name")
        .and_then(serde_json::Value::as_str)
        .map(|name| format!(" --key-name {name}"))
        .unwrap_or_default();
    eprintln!(
        "lastdb get-keys {schema}{key_flag}{partition} — {} key{}{layout}{}",
        keys.len(),
        if keys.len() == 1 { "" } else { "s" },
        if has_more { " (more)" } else { "" }
    );
    if !key_flag.is_empty() {
        eprintln!("For writes, use primary_keys from --json.");
    }
    eprintln!("{}", if ranged { "hash\trange" } else { "hash" });
    for key in &keys {
        let hash = key
            .get("hash")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("");
        let range = key
            .get("range")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("");
        if range.is_empty() {
            println!("{hash}");
        } else {
            println!("{hash}\t{range}");
        }
    }
    if has_more {
        if let Some(cursor) = report
            .get("next_cursor")
            .and_then(serde_json::Value::as_str)
        {
            let hash_flag = hash_filter
                .map(|h| format!(" --key-hash '{h}'"))
                .unwrap_or_default();
            eprintln!("next: lastdb get-keys {schema}{key_flag}{hash_flag} --cursor '{cursor}'");
        }
    }
}

pub(crate) fn schema_drop_command(
    data_dir: Option<PathBuf>,
    schema: Option<String>,
    owner_app: Option<String>,
    must_exist: bool,
    json_only: bool,
) -> Result<(), String> {
    let home = lastdb_node::host::resolve_home(data_dir)?;
    let socket = home.join("data").join("folddb.sock");
    let mut body = serde_json::json!({ "must_exist": must_exist });
    if let Some(schema) = schema {
        body["schema"] = serde_json::Value::String(schema);
    }
    if let Some(owner_app) = owner_app {
        body["owner_app"] = serde_json::Value::String(owner_app);
    }
    let response = db_post_json(&socket, "/api/schemas/drop", &body)?;
    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&response)
                .map_err(|error| format!("serialize schema drop response: {error}"))?
        );
        return Ok(());
    }
    let dropped = response
        .get("dropped")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    if dropped.is_empty() {
        println!("no installed schema dropped");
        return Ok(());
    }
    for name in dropped {
        if let Some(name) = name.as_str() {
            println!("dropped {name}");
        }
    }
    Ok(())
}

pub(crate) fn schema_show_command(
    target: &DatabaseTarget,
    name: &str,
    json_only: bool,
) -> Result<(), String> {
    let schema = fetch_schema(target, name)?;
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
    target: &DatabaseTarget,
    name: &str,
) -> Result<serde_json::Value, String> {
    let socket = &target.socket;
    if probe_health(socket).is_err() {
        return Err(format!(
            "lastdbd not reachable at {} — start the daemon first",
            socket.display()
        ));
    }
    let path = format!("/api/schema/{}", percent_encode_query(name));
    let response = get_path(socket, &target.path(&path), Duration::from_secs(10))?;
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
    let ranged = schema
        .pointer("/record_keys/primary/kind")
        .and_then(serde_json::Value::as_str)
        == Some("hash_range");
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
    if let Some(keys) = schema.get("record_keys") {
        lines.push(format!("record_keys {keys}"));
    }
    if fields.is_empty() {
        lines.push("fields (none)".to_string());
    } else {
        lines.push(format!("fields {fields}"));
    }
    lines.push("queries:".to_string());
    for q in legal_query_shapes(schema_type, Some("HASH"), ranged.then_some("RANGE")) {
        lines.push(format!("  {q}"));
    }
    lines
}

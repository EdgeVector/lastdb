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
    data_dir: Option<PathBuf>,
    schema: &str,
    key_hash: Option<&str>,
    limit: usize,
    cursor: Option<&str>,
    json_only: bool,
    verb: &str,
) -> Result<(), String> {
    let (_, socket) = resolve_client_home_and_socket(data_dir)?;
    if lastdb_node::health_alert::probe_health(&socket).is_err() {
        return Err(format!(
            "lastdbd not reachable at {} — start the daemon first",
            socket.display()
        ));
    }
    let mut path = format!(
        "/api/list?schema={}&limit={}",
        percent_encode_query(schema),
        limit.max(1)
    );
    if let Some(hash) = key_hash {
        path.push_str("&hash=");
        path.push_str(&percent_encode_query(hash));
    }
    if let Some(cursor) = cursor {
        path.push_str("&cursor=");
        path.push_str(&percent_encode_query(cursor));
    }
    let response = get_path(
        &socket,
        &path,
        Duration::from_secs(lastdb_node::exec::DEFAULT_CLI_UDS_TIMEOUT_SECS),
    )?;
    let value = parse_json_response(&response, "list")?;
    let report = value
        .get("list")
        .cloned()
        .ok_or_else(|| "response missing list field".to_string())?;
    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|e| format!("serialize: {e}"))?
        );
        return Ok(());
    }
    print_list_record_keys_human(&report, verb);
    Ok(())
}

pub(crate) fn print_list_record_keys_human(report: &serde_json::Value, verb: &str) {
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
    let hash_field = report
        .get("hash_field")
        .and_then(serde_json::Value::as_str)
        .filter(|s| !s.is_empty());
    let range_field = report
        .get("range_field")
        .and_then(serde_json::Value::as_str)
        .filter(|s| !s.is_empty());
    let key_field = report
        .get("key_field")
        .and_then(serde_json::Value::as_str)
        .filter(|s| !s.is_empty());
    let hash_filter = report
        .get("hash_filter")
        .and_then(serde_json::Value::as_str);
    let layout = match (hash_field, range_field, key_field) {
        (Some(h), Some(r), _) => format!(" hash={h} range={r}"),
        (Some(h), None, _) => format!(" hash={h}"),
        (None, Some(r), _) => format!(" range={r}"),
        (None, None, Some(k)) => format!(" key={k}"),
        _ => String::new(),
    };
    let partition = hash_filter
        .map(|h| format!(" --key-hash {h:?}"))
        .unwrap_or_default();
    eprintln!(
        "lastdb {verb} {schema}{partition} — {} key{}{layout}{}",
        keys.len(),
        if keys.len() == 1 { "" } else { "s" },
        if has_more { " (more)" } else { "" }
    );
    if hash_field.is_some() || range_field.is_some() {
        match (hash_field, range_field) {
            (Some(h), Some(r)) => eprintln!("{h}\t{r}"),
            (Some(h), None) => eprintln!("{h}"),
            (None, Some(r)) => eprintln!("{r}"),
            _ => {}
        }
    }
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
            eprintln!("next: lastdb {verb} {schema}{hash_flag} --cursor '{cursor}'");
        }
    }
}
